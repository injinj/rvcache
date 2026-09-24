#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rvcache/cache.h>
#include <raimd/rwf_msg.h>
#include <raimd/rwf_writer.h>
#include <raikv/key_hash.h>
#include <new>

using namespace rai;
using namespace kv;
using namespace md;
using namespace rvcache;

/* In-place RWF Map merge (SPEC Milestone 5 phase 2).
 *
 * The image stays a valid RWF Map plus tombstones: a dead entry keeps its
 * bytes with MAP_ENTRY_DEAD (0x80) in the header byte, the header's entry
 * count is the live count (raimd's iterator skips dead entries by parsing
 * them).  An UPDATE that still fits rewrites its entry in place, absorbing
 * a shorter result with the variable-width length prefixes or a dead
 * filler entry; one that grew tombstones the old entry and appends.  ADD
 * of a known key tombstones + appends, DELETE tombstones.  Dead bytes over
 * half the image trigger a compaction (strip pass).
 *
 * The key -> offset index (MapIndex) is the writer's, in process memory,
 * never in the image: shm mode validates it with KeyCtx::serial, which
 * value_update()/resize() bump on every write from any process, so a
 * foreign write is seen as a stale index and rebuilt by a prefix-only
 * scan.  Offsets are relative to the value start (resize moves it). */

namespace {

struct RawEntry {          /* one entry as laid out in the image */
  size_t   off,            /* header byte */
           key_off,
           key_len,
           data_off,       /* field list bytes (after the fe prefix) */
           data_len,
           end;            /* one past the entry */
  uint8_t  hdr;
  bool     dead;
  size_t size( void ) const { return this->end - this->off; }
};

/* RWF length prefixes */
static inline size_t u15_len( size_t x ) { return x < 0x80 ? 1 : 2; }
static inline size_t fe_len( size_t x )  { return x < 0xfe ? 1 : 3; }
static inline size_t put_u15( uint8_t *p,  size_t x,  size_t width ) {
  if ( width == 1 ) { p[ 0 ] = (uint8_t) x; return 1; }
  p[ 0 ] = (uint8_t) ( 0x80 | ( x >> 8 ) ); p[ 1 ] = (uint8_t) x; return 2;
}
static inline size_t put_fe( uint8_t *p,  size_t x,  size_t width ) {
  if ( width == 1 ) { p[ 0 ] = (uint8_t) x; return 1; }
  p[ 0 ] = 0xfe; p[ 1 ] = (uint8_t) ( x >> 8 ); p[ 2 ] = (uint8_t) x; return 3;
}
static inline size_t get_u15( const uint8_t *p,  const uint8_t *eob,
                              size_t &x ) {
  if ( p >= eob ) return 0;
  if ( p[ 0 ] < 0x80 ) { x = p[ 0 ]; return 1; }
  if ( p + 1 >= eob ) return 0;
  x = ( (size_t) ( p[ 0 ] & 0x7f ) << 8 ) | p[ 1 ]; return 2;
}
static inline size_t get_fe( const uint8_t *p,  const uint8_t *eob,
                             size_t &x ) {
  if ( p >= eob ) return 0;
  if ( p[ 0 ] < 0xfe ) { x = p[ 0 ]; return 1; }
  if ( p + 2 >= eob ) return 0;
  x = ( (size_t) p[ 1 ] << 8 ) | p[ 2 ]; return 3;
}

/* the map header, as far as in-place editing needs it */
struct MapShape {
  size_t  data_start,  /* first entry */
          cnt_off;     /* u16 live count */
  bool    per_entry_perm,
          no_data;
  MapShape() : data_start( 0 ), cnt_off( 0 ), per_entry_perm( false ),
               no_data( false ) {}
  bool parse( const uint8_t *img,  size_t len ) {
    RwfMapHdr h;
    if ( h.parse( img, 0, len ) != 0 )
      return false;
    this->data_start     = h.data_start;
    this->cnt_off        = h.data_start - 2;
    this->per_entry_perm = ( h.flags & RwfMapHdr::HAS_PERM_DATA ) != 0;
    this->no_data        = ( h.container_type == RWF_NO_DATA );
    return true;
  }
};

static bool
parse_entry( const uint8_t *img,  size_t len,  size_t off,
             const MapShape &sh,  RawEntry &r )
{
  const uint8_t * eob = &img[ len ];
  size_t i = off, sz;
  if ( i >= len )
    return false;
  r.off  = off;
  r.hdr  = img[ i++ ];
  r.dead = ( r.hdr & 0x80 ) != 0;
  if ( sh.per_entry_perm && ( r.hdr & 0x10 ) != 0 ) {
    size_t plen;
    if ( (sz = get_u15( &img[ i ], eob, plen )) == 0 ) return false;
    i += sz + plen;
  }
  if ( (sz = get_u15( &img[ i ], eob, r.key_len )) == 0 ) return false;
  r.key_off  = i + sz;
  i          = r.key_off + r.key_len;
  r.data_off = i;
  r.data_len = 0;
  if ( ( r.hdr & 0xf ) != MAP_DELETE_ENTRY && ! sh.no_data ) {
    if ( (sz = get_fe( &img[ i ], eob, r.data_len )) == 0 ) return false;
    r.data_off = i + sz;
    i = r.data_off + r.data_len;
  }
  r.end = i;
  return i <= len;
}

static inline uint32_t
key_hash( const uint8_t *k,  size_t l )
{
  return kv_crc_c( k, l, 0x9e37 );
}

/* a planned change; applied after the image is sized */
struct MapOp {
  enum Kind { OP_DEAD, OP_WRITE, OP_APPEND };
  Kind            kind;
  size_t          off,       /* DEAD/WRITE target; APPEND: assigned */
                  len,       /* WRITE/APPEND byte count */
                  old_size;  /* DEAD: bytes going dead */
  const uint8_t * bytes;
  const uint8_t * key;       /* APPEND: index upsert */
  size_t          key_len;
  uint32_t        hash;
};

struct MapPlan {
  MDMsgMem  & mem;
  MapOp     * ops;
  uint32_t    cnt, cap;
  size_t      append_total;
  int32_t     live_delta;
  MapPlan( MDMsgMem &m,  uint32_t n ) : mem( m ), cnt( 0 ), cap( n * 3 + 4 ),
                                        append_total( 0 ), live_delta( 0 ) {
    this->ops = (MapOp *) m.make( sizeof( MapOp ) * this->cap );
  }
  MapOp * add( MapOp::Kind k ) {
    if ( this->cnt >= this->cap ) return NULL;
    MapOp & op = this->ops[ this->cnt++ ];
    ::memset( &op, 0, sizeof( op ) );
    op.kind = k;
    return &op;
  }
};

/* a dead DELETE entry of exactly f >= 2 bytes: hdr 0x83 + a key of garbage
 * length (DELETE carries no data, so no data prefix).  The iterator skips
 * it, strip drops it, ETA never sees it.  Max 0x7fff + 3 bytes. */
static const size_t MAX_FILLER = 0x7fff + 3;
static size_t
put_filler( uint8_t *p,  size_t f )
{
  if ( f < 2 ) return 0;
  if ( f > MAX_FILLER ) f = MAX_FILLER;
  *p++ = 0x80 | MAP_DELETE_ENTRY;
  size_t klen = f - 2;
  if ( klen < 0x80 ) { p += put_u15( p, klen, 1 ); }
  else { klen = f - 3; p += put_u15( p, klen, 2 ); }
  ::memset( p, 0, klen );
  return f;
}

/* encode an image entry (always ADD): hdr, [perm], key, [fe data].  When
 * target != 0 the result must be exactly target bytes: 1 byte of slack is
 * taken by widening the key prefix, 2+ by a dead DELETE filler entry
 * (hdr 0x83 + key of garbage length; DELETE carries no data so the filler
 * needs no data prefix).  Returns 0 when it cannot fit. */
static size_t
encode_entry( uint8_t *out,  size_t cap,  const uint8_t *perm,  size_t perm_len,
              const uint8_t *key,  size_t key_len,
              const uint8_t *data,  size_t data_len,  bool no_data,
              size_t target )
{
  size_t kw = u15_len( key_len ), dw = no_data ? 0 : fe_len( data_len );
  size_t natural = 1 + ( perm_len ? u15_len( perm_len ) + perm_len : 0 ) +
                   kw + key_len + dw + ( no_data ? 0 : data_len );
  size_t filler = 0;
  if ( target != 0 ) {
    if ( target < natural )
      return 0;
    size_t slack = target - natural;
    if ( slack == 1 ) {
      if ( key_len >= 0x80 ) return 0;
      kw = 2;
    }
    else if ( slack >= 2 )
      filler = slack;
  }
  size_t need = natural + ( kw == 2 && key_len < 0x80 ? 1 : 0 ) + filler;
  if ( need > cap )
    return 0;
  uint8_t * p = out;
  *p++ = (uint8_t) ( ( perm_len ? 0x10 : 0 ) | MAP_ADD_ENTRY );
  if ( perm_len ) {
    p += put_u15( p, perm_len, u15_len( perm_len ) );
    ::memcpy( p, perm, perm_len ); p += perm_len;
  }
  p += put_u15( p, key_len, kw );
  ::memcpy( p, key, key_len ); p += key_len;
  if ( ! no_data ) {
    p += put_fe( p, data_len, dw );
    ::memcpy( p, data, data_len ); p += data_len;
  }
  if ( filler != 0 ) /* dead DELETE entry of exactly filler bytes */
    p += put_filler( p, filler );
  return p - out;
}

} /* anon */

/* ---- MapIndex ---- */

void
MapIndex::release( void ) noexcept
{
  if ( this->slots != NULL )
    ::free( this->slots );
  this->slots = NULL;
  this->mask  = 0;
  this->used  = 0;
  this->valid = false;
}

void
MapIndex::clear( uint32_t expect ) noexcept
{
  uint32_t n = 16;
  while ( n < expect * 2 )
    n <<= 1;
  if ( this->slots == NULL || this->mask + 1 != n ) {
    if ( this->slots != NULL )
      ::free( this->slots );
    this->slots = (Slot *) ::malloc( sizeof( Slot ) * n );
    this->mask  = n - 1;
  }
  ::memset( this->slots, 0, sizeof( Slot ) * n );
  this->used       = 0;
  this->live_cnt   = 0;
  this->dead_bytes = 0;
  this->valid      = false;
}

/* find a slot for hash whose entry key matches; the caller checks dead.
 * returns the slot or the first empty slot (off == 0) on the probe path */
MapIndex::Slot *
MapIndex::probe( uint32_t hash,  const uint8_t *img,  size_t len,
                 const uint8_t *key,  size_t key_len,  const void *shp,
                 void *out_p ) noexcept
{
  const MapShape & sh  = *(const MapShape *) shp;
  RawEntry       * out = (RawEntry *) out_p;
  for ( uint32_t i = hash & this->mask; ; i = ( i + 1 ) & this->mask ) {
    Slot & s = this->slots[ i ];
    if ( s.off == 0 )
      return &s;
    if ( s.hash == hash ) {
      RawEntry r;
      if ( parse_entry( img, len, s.off, sh, r ) && r.key_len == key_len &&
           ::memcmp( &img[ r.key_off ], key, key_len ) == 0 ) {
        if ( out != NULL ) *out = r;
        return &s;
      }
    }
  }
}

/* rebuild from the image: prefix-only scan, no dictionary */
bool
MapIndex::build( const uint8_t *img,  size_t len ) noexcept
{
  MapShape sh;
  if ( ! sh.parse( img, len ) )
    return false;
  /* count first for the table size */
  uint32_t n = 0;
  RawEntry r;
  for ( size_t off = sh.data_start; off < len; off = r.end ) {
    if ( ! parse_entry( img, len, off, sh, r ) )
      return false;
    n++;
  }
  this->clear( n );
  this->tail_slack = 0;
  for ( size_t off = sh.data_start; off < len; off = r.end ) {
    parse_entry( img, len, off, sh, r );
    if ( r.dead ) {
      if ( r.end == len && ( r.hdr & 0xf ) == MAP_DELETE_ENTRY )
        this->tail_slack = (uint32_t) r.size(); /* append room */
      else
        this->dead_bytes += (uint32_t) r.size();
      continue;
    }
    uint32_t h = key_hash( &img[ r.key_off ], r.key_len );
    Slot * s = this->probe( h, img, len, &img[ r.key_off ], r.key_len, &sh,
                            NULL );
    if ( s->off == 0 )
      this->used++;
    s->hash = h;   /* a duplicate live key: the later one wins */
    s->off  = (uint32_t) r.off;
    this->live_cnt++;
  }
  this->valid = true;
  return true;
}

/* ---- CacheTab ---- */

/* linear lookup for small books / no index */
static bool
scan_key( const uint8_t *img,  size_t len,  const MapShape &sh,
          const uint8_t *key,  size_t key_len,  RawEntry &found )
{
  RawEntry r;
  bool hit = false;
  for ( size_t off = sh.data_start; off < len; off = r.end ) {
    if ( ! parse_entry( img, len, off, sh, r ) )
      break;
    if ( ! r.dead && r.key_len == key_len &&
         ::memcmp( &img[ r.key_off ], key, key_len ) == 0 ) {
      found = r;    /* the last live one wins, as the iterator would */
      hit   = true;
    }
  }
  return hit;
}

/* phase A: plan the changes for one update against the image */
bool
CacheTab::plan_map_update( CacheEntry &e,  const uint8_t *img,  size_t len,
                           const void *upd,  size_t upd_len,
                           MDMsgMem &mem,  void *plan_p,
                           bool &use_index ) noexcept
{
  MapPlan & plan = *(MapPlan *) plan_p;
  MapShape  sh;
  if ( ! sh.parse( img, len ) )
    return false;
  RwfMsg * um = RwfMsg::unpack_map( (void *) upd, 0, upd_len,
                                    RWF_MAP_TYPE_ID, this->dict.dict, mem );
  if ( um == NULL )
    return false;
  RwfMapHdr & uh = um->map;
  if ( uh.summary_size != 0 )
    return false;  /* summary change: header rewrite, take the rebuild */
  MDFieldIter * uit = NULL;
  if ( um->get_field_iter( uit ) != 0 )
    return false;
  const uint8_t * ub = (const uint8_t *) upd;

  /* index: valid and current, else rebuild (or skip for small books) */
  MapIndex * ix = e.midx;
  use_index = false;
  if ( this->map_index_min != 0 ) {
    if ( ix == NULL ) {
      /* how many entries? the header count is the live count */
      uint32_t cnt = ( (uint32_t) img[ sh.cnt_off ] << 8 ) | img[ sh.cnt_off+1 ];
      if ( cnt >= this->map_index_min ) {
        ix = e.midx = new ( ::malloc( sizeof( MapIndex ) ) ) MapIndex();
      }
    }
    if ( ix != NULL ) {
      if ( ! ix->valid || ix->serial != e.image_serial ) {
        if ( ! ix->build( img, len ) )
          return false;
        ix->serial = e.image_serial;
        this->stats_index_rebuilds++;
      }
      use_index = true;
    }
  }

  /* pending appends in this update, for a chain on one new key */
  struct Pending { MapOp * op; };
  if ( uit->first() != 0 )
    return true; /* empty update */
  RwfFieldIter & it = *(RwfFieldIter *) uit;
  do {
    if ( it.u.map.action == MAP_SUMMARY )
      continue;
    const uint8_t * key     = (const uint8_t *) it.u.map.key;
    size_t          key_len = it.u.map.keylen;
    const uint8_t * udata   = &ub[ it.data_start ];
    size_t          udata_len = it.field_end - it.data_start;
    RwfMapAction    action  = it.u.map.action;
    uint32_t        h       = key_hash( key, key_len );
    /* set-encoded update data is re-encoded standard (its defs aren't ours) */
    const uint8_t * ndata = udata;
    size_t          ndata_len = udata_len;
    if ( action != MAP_DELETE_ENTRY && ! sh.no_data && udata_len > 0 &&
         ( udata[ 0 ] & RwfFieldListHdr::HAS_SET_DATA ) != 0 ) {
      RwfMsg * fm = um->unpack_sub_msg( RWF_FIELD_LIST, it.data_start,
                                        it.field_end );
      MDFieldIter * fi = NULL;
      if ( fm == NULL || fm->get_field_iter( fi ) != 0 )
        return false;
      size_t cap = udata_len * 2 + 256;
      void * nb  = mem.make( cap );
      RwfFieldListWriter w( mem, this->dict.dict, nb, cap );
      if ( fi->first() == 0 )
        do { w.append_iter( fi ); } while ( fi->next() == 0 );
      ndata_len = w.update_hdr();
      if ( w.err != 0 ) return false;
      ndata = (const uint8_t *) w.buf;
    }
    /* a pending append on the same key in this message? */
    MapOp * pend = NULL;
    for ( uint32_t i = 0; i < plan.cnt; i++ ) {
      MapOp & op = plan.ops[ i ];
      if ( op.kind == MapOp::OP_APPEND && op.key_len == key_len &&
           op.hash == h && ::memcmp( op.key, key, key_len ) == 0 )
        pend = &op;
    }
    /* find the live entry in the image */
    RawEntry cur;
    bool found = false;
    if ( pend == NULL ) {
      if ( use_index ) {
        MapIndex::Slot * s = ix->probe( h, img, len, key, key_len, &sh, &cur );
        found = ( s->off != 0 && ! cur.dead );
      }
      else
        found = scan_key( img, len, sh, key, key_len, cur );
    }
    const uint8_t * perm = NULL;
    size_t perm_len = 0;
    if ( found && sh.per_entry_perm && ( cur.hdr & 0x10 ) != 0 ) {
      size_t sz = get_u15( &img[ cur.off + 1 ], &img[ len ], perm_len );
      perm = &img[ cur.off + 1 + sz ];
    }

    switch ( action ) {
      case MAP_DELETE_ENTRY:
        if ( pend != NULL ) {          /* never landed: drop the append */
          plan.append_total -= pend->len;
          pend->kind = MapOp::OP_DEAD; pend->off = 0; pend->old_size = 0;
          plan.live_delta--;
        }
        else if ( found ) {
          MapOp * op = plan.add( MapOp::OP_DEAD );
          if ( op == NULL ) return false;
          op->off = cur.off; op->old_size = cur.size();
          op->key = key; op->key_len = key_len; op->hash = h;
          plan.live_delta--;
        }
        break;

      case MAP_UPDATE_ENTRY:
        if ( found && ! sh.no_data ) { /* fid-merge, refit or move */
          void * m = NULL;
          size_t ml = this->merge_field_lists( mem, &img[ cur.data_off ],
                                               cur.data_len, ndata, ndata_len,
                                               m );
          if ( ml == 0 ) return false;
          ndata = (const uint8_t *) m; ndata_len = ml;
          size_t cap = cur.size() + 8;
          uint8_t * ob = (uint8_t *) mem.make( cap );
          size_t n = encode_entry( ob, cap, perm, perm_len, key, key_len,
                                   ndata, ndata_len, sh.no_data, cur.size() );
          if ( n == cur.size() ) {     /* fits: rewrite in place */
            MapOp * op = plan.add( MapOp::OP_WRITE );
            if ( op == NULL ) return false;
            op->off = cur.off; op->len = n; op->bytes = ob;
            break;
          }
          /* grew: tombstone + append below */
        }
        else if ( pend != NULL && ! sh.no_data ) { /* merge into the append */
          RawEntry pr;
          MapShape psh = sh;
          if ( ! parse_entry( pend->bytes, pend->len, 0, psh, pr ) )
            return false;
          void * m = NULL;
          size_t ml = this->merge_field_lists( mem, &pend->bytes[ pr.data_off ],
                                               pr.data_len, ndata, ndata_len, m );
          if ( ml == 0 ) return false;
          size_t cap = ml + key_len + 64;
          uint8_t * ob = (uint8_t *) mem.make( cap );
          size_t n = encode_entry( ob, cap, NULL, 0, key, key_len,
                                   (const uint8_t *) m, ml, sh.no_data, 0 );
          if ( n == 0 ) return false;
          plan.append_total += n - pend->len;
          pend->bytes = ob; pend->len = n;
          break;
        }
        /* unknown key or grew -> (tombstone +) append */
        /* fall through */
      case MAP_ADD_ENTRY:
      default: {
        if ( pend != NULL ) {          /* replace the pending append */
          size_t cap = ndata_len + key_len + 64;
          uint8_t * ob = (uint8_t *) mem.make( cap );
          size_t n = encode_entry( ob, cap, NULL, 0, key, key_len,
                                   ndata, ndata_len, sh.no_data, 0 );
          if ( n == 0 ) return false;
          plan.append_total += n - pend->len;
          pend->bytes = ob; pend->len = n;
          break;
        }
        if ( found ) {
          MapOp * op = plan.add( MapOp::OP_DEAD );
          if ( op == NULL ) return false;
          op->off = cur.off; op->old_size = cur.size();
          plan.live_delta--;
        }
        else {                          /* new key: perm from the update */
          if ( uh.flags & RwfMapHdr::HAS_PERM_DATA ) {
            const uint8_t * eh = &ub[ it.field_start ];
            if ( ( eh[ 0 ] & 0x10 ) != 0 ) {
              size_t sz = get_u15( eh + 1, &ub[ upd_len ], perm_len );
              perm = eh + 1 + sz;
            }
          }
        }
        size_t cap = ndata_len + key_len + perm_len + 64;
        uint8_t * ob = (uint8_t *) mem.make( cap );
        size_t n = encode_entry( ob, cap, perm, perm_len, key, key_len,
                                 ndata, ndata_len, sh.no_data, 0 );
        if ( n == 0 ) return false;
        MapOp * op = plan.add( MapOp::OP_APPEND );
        if ( op == NULL ) return false;
        op->len = n; op->bytes = ob;
        op->key = key; op->key_len = key_len; op->hash = h;
        plan.append_total += n;
        plan.live_delta++;
        break;
      }
    }
  } while ( uit->next() == 0 );
  return true;
}

/* phase B: the image is sized img_len + append_total; apply */
/* the image length after this plan: appends go into the trailing slack
 * when they fit (leaving 0 or >= 2 bytes of it), else the image grows to
 * live + appends + fresh slack (1/8 of the image, 1 KB .. 32 KB) so the
 * next many appends do not copy it again */
size_t
CacheTab::map_grow_len( CacheEntry &e,  size_t img_len,  size_t append_total,
                        size_t &slack_new ) noexcept
{
  size_t avail = e.tail_slack;
  slack_new = 0;
  if ( append_total == 0 )
    return img_len;
  if ( avail >= append_total &&
       ( avail - append_total == 0 || avail - append_total >= 2 ) )
    return img_len;
  size_t live = img_len - avail + append_total;
  slack_new = live / 8;
  if ( slack_new < 1024 ) slack_new = 1024;
  if ( slack_new > MAX_FILLER ) slack_new = MAX_FILLER;
  return live + slack_new;
}

void
CacheTab::commit_map_update( CacheEntry &e,  uint8_t *img,  size_t img_len,
                             size_t new_len,  void *plan_p,
                             bool use_index ) noexcept
{
  MapPlan  & plan = *(MapPlan *) plan_p;
  MapIndex * ix   = ( use_index ? e.midx : NULL );
  MapShape   sh;
  if ( ! sh.parse( img, img_len ) ) /* planned on this image: cannot fail */
    return;
  size_t app = img_len - e.tail_slack; /* appends overwrite the filler */
  for ( uint32_t i = 0; i < plan.cnt; i++ ) {
    MapOp & op = plan.ops[ i ];
    switch ( op.kind ) {
      case MapOp::OP_DEAD:
        if ( op.old_size != 0 ) {
          img[ op.off ] |= 0x80;
          if ( ix != NULL ) ix->dead_bytes += (uint32_t) op.old_size;
          else e.dead_bytes_hint += (uint32_t) op.old_size;
        }
        break;
      case MapOp::OP_WRITE:
        ::memcpy( &img[ op.off ], op.bytes, op.len );
        break;
      case MapOp::OP_APPEND:
        ::memcpy( &img[ app ], op.bytes, op.len );
        if ( ix != NULL ) {
          MapIndex::Slot * s = ix->probe( op.hash, img, app + op.len, op.key,
                                          op.key_len, &sh, NULL );
          /* probe stops at a matching key (the now-dead one) or an empty
           * slot; either way this slot now names the appended entry */
          if ( s->off == 0 ) ix->used++;
          s->hash = op.hash;
          s->off  = (uint32_t) app;
        }
        app += op.len;
        break;
    }
  }
  /* the tail filler: whatever remains up to new_len (0 or >= 2 bytes) */
  if ( plan.append_total != 0 || new_len != img_len ) {
    size_t rem = new_len - app;
    e.tail_slack = (uint32_t) put_filler( &img[ app ], rem );
    if ( ix != NULL ) ix->tail_slack = e.tail_slack;
  }
  /* live count */
  int32_t live = (int32_t) ( ( (uint32_t) img[ sh.cnt_off ] << 8 ) |
                             img[ sh.cnt_off + 1 ] ) + plan.live_delta;
  if ( live < 0 ) live = 0;
  img[ sh.cnt_off ]     = (uint8_t) ( live >> 8 );
  img[ sh.cnt_off + 1 ] = (uint8_t) live;
  if ( ix != NULL ) {
    ix->live_cnt = (uint32_t) live;
    /* the table fills with dead-pointing slots over time; regrow */
    if ( ix->used * 4 > ( ix->mask + 1 ) * 3 )
      ix->valid = false;  /* rebuilt on the next merge */
  }
}

/* strip: header + live entries only, count patched.  out must hold len */
size_t
CacheTab::strip_map( const uint8_t *img,  size_t len,  uint8_t *out ) noexcept
{
  MapShape sh;
  if ( ! sh.parse( img, len ) )
    return 0;
  ::memcpy( out, img, sh.data_start );
  size_t   o    = sh.data_start;
  uint32_t live = 0;
  RawEntry r;
  for ( size_t off = sh.data_start; off < len; off = r.end ) {
    if ( ! parse_entry( img, len, off, sh, r ) )
      return 0;
    if ( r.dead )
      continue;
    ::memcpy( &out[ o ], &img[ r.off ], r.size() );
    o += r.size();
    live++;
  }
  out[ sh.cnt_off ]     = (uint8_t) ( live >> 8 );
  out[ sh.cnt_off + 1 ] = (uint8_t) live;
  return o;
}

/* an image for the wire: raw only when this process's index is current
 * and says there are no tombstones; otherwise stripped into scratch (a
 * dead entry must never reach a consumer).  A reader process (shm serve,
 * no index) always strips: it cannot know what the writer left behind. */
bool
CacheTab::live_image( CacheEntry &e,  void *&bytes,  size_t &len,
                      uint32_t enc ) noexcept
{
  if ( enc != RWF_MAP_TYPE_ID ) /* get_image's enc: valid for a reader too */
    return true;
  if ( e.midx != NULL && e.midx->valid &&
       ( ! this->shm_mode() || e.midx->serial == e.image_serial ) &&
       e.midx->dead_bytes == 0 && e.tail_slack == 0 )
    return true;
  this->ensure_scratch( len );
  size_t n = this->strip_map( (const uint8_t *) bytes, len,
                              (uint8_t *) this->scratch );
  if ( n == 0 )
    return false;
  bytes = this->scratch;
  len   = n;
  return true;
}

/* the in-place merge; 0 = not applicable / failed (caller rebuilds) */
size_t
CacheTab::merge_map_inplace( CacheEntry &e,  const void *upd,
                             size_t upd_len ) noexcept
{
  MDMsgMem mem;
  bool     use_index = false;
  MapPlan  plan( mem, 64 );

  if ( this->shm_mode() ) {
    KeyCtx & kc = *this->kctx;
    this->set_key( e, kc );
    this->wrk.reset();
    if ( kc.acquire( &this->wrk ) != KEY_OK ) /* must exist */
      { kc.release(); return 0; }
    /* acquire loads the entry's serial: the value as it was sealed by the
     * last writer (us, if the index is current); value_update() bumps it
     * for this write, so compare before */
    e.image_serial = kc.serial;
    void   * p  = NULL;
    uint64_t sz = 0;
    if ( kc.value_update( &p, sz ) != KEY_OK || sz == 0 ||
         kc.get_type() != (uint8_t) RWF_MAP_TYPE_ID ) {
      kc.release();
      return 0;
    }
    if ( ! this->plan_map_update( e, (const uint8_t *) p, sz, upd, upd_len,
                                  mem, &plan, use_index ) ) {
      kc.release();
      return 0;
    }
    if ( use_index && e.midx != NULL )
      e.tail_slack = e.midx->tail_slack; /* rebuilt from the image */
    size_t slack_new = 0;
    size_t new_len = this->map_grow_len( e, sz, plan.append_total, slack_new );
    if ( new_len != sz ) {
      if ( kc.resize( &p, new_len, true ) != KEY_OK ) {
        kc.release();
        return 0;
      }
    }
    this->commit_map_update( e, (uint8_t *) p, sz, new_len, &plan, use_index );
    /* value_update / resize bumped kc.serial; release seals it into the
     * entry, so the next acquire reads exactly this value */
    e.image_serial = kc.serial;
    if ( e.midx != NULL ) e.midx->serial = e.image_serial;
    this->image_bytes += new_len - sz;
    e.image_len = new_len;
    uint32_t dead = ( e.midx != NULL ? e.midx->dead_bytes : e.dead_bytes_hint );
    bool compact = ( dead * 2 > new_len && dead > 1024 );
    if ( compact ) {
      this->ensure_scratch( new_len );
      size_t n = this->strip_map( (uint8_t *) p, new_len,
                                  (uint8_t *) this->scratch );
      void * dst;
      if ( n != 0 && kc.resize( &dst, n ) == KEY_OK ) {
        ::memcpy( dst, this->scratch, n );
        this->image_bytes += n - new_len;
        e.image_len = new_len = n;
        e.dead_bytes_hint = 0;
        e.tail_slack = 0;
        if ( e.midx != NULL ) e.midx->valid = false;
        this->stats_compactions++;
      }
    }
    kc.release();
    this->stats_inplace++;
    return new_len;
  }

  /* heap */
  if ( e.image == NULL || e.image_enc != RWF_MAP_TYPE_ID )
    return 0;
  if ( ! this->plan_map_update( e, (const uint8_t *) e.image, e.image_len,
                                upd, upd_len, mem, &plan, use_index ) )
    return 0;
  if ( use_index && e.midx != NULL )
    e.tail_slack = e.midx->tail_slack;
  size_t slack_new = 0;
  size_t new_len = this->map_grow_len( e, e.image_len, plan.append_total,
                                       slack_new );
  if ( new_len != e.image_len ) {
    void * ni = ::realloc( e.image, new_len );
    if ( ni == NULL )
      return 0;
    e.image = ni;
  }
  this->commit_map_update( e, (uint8_t *) e.image, e.image_len, new_len,
                           &plan, use_index );
  this->image_bytes += new_len - e.image_len;
  e.image_len = new_len;
  e.image_serial++;
  if ( e.midx != NULL ) e.midx->serial = e.image_serial;
  uint32_t dead = ( e.midx != NULL ? e.midx->dead_bytes : e.dead_bytes_hint );
  if ( dead * 2 > new_len && dead > 1024 ) {
    this->ensure_scratch( new_len );
    size_t n = this->strip_map( (uint8_t *) e.image, new_len,
                                (uint8_t *) this->scratch );
    if ( n != 0 ) {
      ::memcpy( e.image, this->scratch, n );
      this->image_bytes += n - new_len;
      e.image_len = new_len = n;
      e.dead_bytes_hint = 0;
      e.tail_slack = 0;
      e.image_serial++;
      if ( e.midx != NULL ) e.midx->valid = false;
      this->stats_compactions++;
    }
  }
  this->stats_inplace++;
  return new_len;
}
