#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rvcache/cache.h>
#include <raimd/md_dict.h>
#include <raimd/rv_msg.h>
#include <raimd/sass.h>
#include <raikv/key_hash.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace rvcache;

void
CacheTab::ensure_scratch( size_t n ) noexcept
{
  if ( n > this->scratch_len ) {
    size_t sz = this->scratch_len == 0 ? 1024 : this->scratch_len;
    while ( sz < n )
      sz *= 2;
    this->scratch = (char *) ::realloc( this->scratch, sz );
    this->scratch_len = sz;
  }
}

void
CacheTab::ensure_scratch2( size_t n ) noexcept
{
  if ( n > this->scratch2_len ) {
    size_t sz = this->scratch2_len == 0 ? 1024 : this->scratch2_len;
    while ( sz < n )
      sz *= 2;
    this->scratch2 = (char *) ::realloc( this->scratch2, sz );
    this->scratch2_len = sz;
  }
}

CacheEntry *
CacheTab::find( const char *subj,  size_t len ) noexcept
{
  uint32_t h = kv_crc_c( subj, len, 0 );
  RouteLoc loc;
  return this->tab.find( h, subj, len, loc );
}

CacheEntry *
CacheTab::upsert( const char *subj,  size_t len,  bool &is_new ) noexcept
{
  uint32_t     h = kv_crc_c( subj, len, 0 );
  RouteLoc     loc;
  CacheEntry * e = this->tab.upsert( h, subj, len, loc );
  is_new = ( e != NULL && loc.is_new );
  if ( is_new )
    e->init( this->next_id++ );
  return e;
}

/* MSG_TYPE normalization: force MSG_TYPE to be the first field with a
 * fixed-width integer type, so that serving an initial or a snapshot is a
 * find-first-field + MDFieldIter::update() in-place stamp — never a message
 * rebuild.  Returns true when bytes/len/enc were repointed at scratch2. */
bool
CacheTab::normalize_msg_type( const void *&bytes,  size_t &len,
                              uint32_t &enc ) noexcept
{
  MDMsgMem mem;
  MDMsg  * m = MDMsg::unpack( (void *) bytes, 0, len, enc, NULL, mem );
  if ( m == NULL )
    return false;
  MDFieldIter * it = NULL;
  if ( m->get_field_iter( it ) != 0 )
    return false;
  MDReference mref;
  if ( it->find( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, mref ) != 0 )
    return false; /* typeless: store as-is, never inject the field */

  /* already normalized?  leading field, fixed-width int, stampable */
  bool ok_inplace = false;
  if ( mref.ftype == MD_UINT || mref.ftype == MD_INT ) {
    MDName      nm;
    MDReference fr;
    if ( it->first() == 0 && it->get_name( nm ) == 0 &&
         nm.equals( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN ) &&
         it->get_reference( fr ) == 0 &&
         ( fr.ftype == MD_UINT || fr.ftype == MD_INT ) &&
         fr.fsize >= 1 && fr.fsize <= 8 )
      ok_inplace = true;
  }
  if ( ok_inplace )
    return false;

  /* rebuild: MSG_TYPE first as uint16, then every other field in order */
  uint16_t t = get_uint<uint16_t>( mref );
  this->ensure_scratch2( len * 2 + 256 );
  RvMsgWriter w( mem, this->scratch2, this->scratch2_len );
  w.append_uint( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, t );
  if ( it->first() == 0 ) {
    do {
      MDName      nm;
      MDReference fr;
      if ( it->get_name( nm ) != 0 )
        continue;
      if ( nm.equals( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN ) )
        continue;
      if ( it->get_reference( fr ) == 0 )
        w.append_ref( nm.fname, nm.fnamelen, fr );
    } while ( it->next() == 0 );
  }
  size_t out = w.update_hdr();
  if ( w.err != 0 )
    return false;      /* overflow/unwritable field: keep original bytes */
  if ( (char *) w.buf != this->scratch2 ) {
    /* writer resized into MDMsgMem (dies with mem): copy out to scratch2 */
    this->ensure_scratch2( out );
    ::memcpy( this->scratch2, w.buf, out );
  }
  bytes = this->scratch2;
  len   = out;
  enc   = RVMSG_TYPE_ID;
  return true;
}

/* ------------------------------------------------------------------ */
/* shm image store: subject-keyed values in the raikv map (-m map_name).
 * EvKeyCtx computes the 128-bit key hash from the map's seed and primes
 * KeyCtx (the same operand struct raids queues for prefetch, so the
 * Milestone 3 batching pipeline slots in without another refactor).
 * The value is the bare image bytes; the encoding rides in the
 * HashEntry's type byte ((uint8_t) TYPE_ID, matcher ftype convention),
 * the same slot raids uses for redis value types.  MDMsg::unpack()
 * accepts the byte as a msg_enc hint. */

/* HashEntry type byte -> full raimd TYPE_ID (for EvPublish msg_enc /
 * make_rv_msg, which switch on the 32-bit ids).  Built once from the
 * registered matcher table; unknown bytes pass through unchanged. */
static uint32_t
enc_of_type_byte( uint8_t b ) noexcept
{
  static uint32_t tab[ 256 ];
  static bool     init;
  if ( ! init ) {
    uint32_t i = 0;
    for ( MDMatch *ma = MDMsg::first_match( i ); ma != NULL;
          ma = MDMsg::next_match( i ) ) {
      if ( ma->hint_size > 0 && tab[ ma->ftype ] == 0 )
        tab[ ma->ftype ] = ma->hint[ 0 ];
    }
    init = true;
  }
  return tab[ b ] != 0 ? tab[ b ] : b;
}

void
CacheTab::init_shm( EvShm &shm ) noexcept
{
  if ( shm.map == NULL )
    return;
  this->map  = shm.map;
  this->kctx = new ( ::malloc( sizeof( KeyCtx ) ) )
               KeyCtx( *shm.map, shm.dbx_id );
  this->map->hdr.get_hash_seed( this->kctx->db_num, this->hseed );
}

EvKeyCtx *
CacheTab::key_of( const CacheEntry &e ) noexcept
{
  size_t sz = EvKeyCtx::size( e.len );
  if ( sz > this->keybuf_len ) {
    size_t n = this->keybuf_len == 0 ? 256 : this->keybuf_len;
    while ( n < sz )
      n *= 2;
    this->keybuf = (char *) ::realloc( this->keybuf, n );
    this->keybuf_len = n;
  }
  return new ( this->keybuf )
         EvKeyCtx( *this->map, NULL, e.value, e.len, 0, 0, this->hseed );
}

bool
CacheTab::shm_set( CacheEntry &e,  const void *bytes,  size_t len,
                   uint32_t enc ) noexcept
{
  KeyCtx & kc = *this->kctx;
  this->key_of( e )->set( kc );
  this->wrk.reset();
  KeyStatus status = kc.acquire( &this->wrk );
  if ( status <= KEY_IS_NEW ) {
    void   * p;
    uint64_t oldsz;
    if ( status == KEY_OK && kc.value( &p, oldsz ) == KEY_OK )
      this->image_bytes -= oldsz;
    if ( kc.resize( &p, len ) == KEY_OK ) {
      ::memcpy( p, bytes, len );
      kc.set_type( (uint8_t) enc ); /* matcher ftype = (uint8_t) TYPE_ID */
      this->image_bytes += len;
      e.image_len = len;   /* local mirror: stats / last-known size only */
      e.image_enc = enc;
      kc.release();
      return true;
    }
    kc.release();
  }
  return false;
}

size_t
CacheTab::shm_merge( CacheEntry &e,  const void *upd,  size_t upd_len,
                     uint32_t upd_enc ) noexcept
{
  /* single-lock read-modify-write: acquire, unpack old value in place,
   * rebuild merged into scratch, normalize, resize + copy, release */
  KeyCtx & kc = *this->kctx;
  this->key_of( e )->set( kc );
  this->wrk.reset();
  KeyStatus status = kc.acquire( &this->wrk );
  if ( status > KEY_IS_NEW )
    return 0;
  void   * p       = NULL;
  uint64_t oldsz   = 0;
  size_t   out     = 0;
  uint32_t out_enc = 0;
  if ( status == KEY_OK && kc.value( &p, oldsz ) == KEY_OK && oldsz > 0 ) {
    /* the HashEntry type byte is the msg_enc hint: unpack always
     * produces a message of the codec the byte declares */
    out = this->build_merge( p, oldsz, kc.get_type(),
                             upd, upd_len, upd_enc, out_enc );
  }
  const void * res;
  size_t       res_len;
  uint32_t     res_enc;
  if ( out != 0 ) {   /* merged bytes in scratch, cached codec preserved */
    res     = this->scratch;
    res_len = out;
    res_enc = out_enc;
  }
  else {              /* no old image / parse fail: replace outright */
    res     = upd;
    res_len = upd_len;
    res_enc = upd_enc;
  }
  this->normalize_msg_type( res, res_len, res_enc );
  void * dst;
  this->image_bytes -= oldsz;
  if ( kc.resize( &dst, res_len ) == KEY_OK ) {
    ::memcpy( dst, res, res_len );
    kc.set_type( (uint8_t) res_enc );
    this->image_bytes += res_len;
    e.image_len = res_len;
    e.image_enc = res_enc;
  }
  else {
    res_len = 0;
  }
  kc.release();
  return res_len;
}

bool
CacheTab::shm_get( CacheEntry &e,  void *&bytes,  size_t &len,
                   uint32_t &enc ) noexcept
{
  KeyCtx & kc = *this->kctx;
  this->key_of( e )->set( kc );
  this->wrk.reset();
  if ( kc.find( &this->wrk ) != KEY_OK )
    return false;
  void   * p;
  uint64_t sz;
  if ( kc.value( &p, sz ) != KEY_OK || sz == 0 )
    return false;
  size_t n = sz;
  if ( n > this->imgbuf_len ) {
    size_t b = this->imgbuf_len == 0 ? 1024 : this->imgbuf_len;
    while ( b < n )
      b *= 2;
    this->imgbuf = (char *) ::realloc( this->imgbuf, b );
    this->imgbuf_len = b;
  }
  ::memcpy( this->imgbuf, p, n );
  bytes = this->imgbuf;
  len   = n;
  /* publish paths (make_rv_msg / EvPublish.msg_enc) switch on the full
   * 32-bit type id; expand the HashEntry byte back through the matcher
   * table */
  enc   = enc_of_type_byte( kc.get_type() );
  return true;
}

void
CacheTab::shm_evict( CacheEntry &e ) noexcept
{
  KeyCtx & kc = *this->kctx;
  this->key_of( e )->set( kc );
  this->wrk.reset();
  KeyStatus status = kc.acquire( &this->wrk );
  if ( status <= KEY_IS_NEW ) {
    void   * p;
    uint64_t sz;
    if ( status == KEY_OK && kc.value( &p, sz ) == KEY_OK )
      this->image_bytes -= sz;
    kc.tombstone();
    kc.release();
  }
  e.image_len = 0;
}

bool
CacheTab::get_image( CacheEntry &e,  void *&bytes,  size_t &len,
                     uint32_t &enc ) noexcept
{
  if ( this->shm_mode() )
    return this->shm_get( e, bytes, len, enc );
  if ( e.image == NULL )
    return false;
  bytes = e.image;
  len   = e.image_len;
  enc   = e.image_enc;
  return true;
}

/* ------------------------------------------------------------------ */

void
CacheTab::set_image( CacheEntry &e,  const void *bytes,  size_t len,
                     uint32_t enc ) noexcept
{
  this->normalize_msg_type( bytes, len, enc );
  if ( this->shm_mode() ) {
    this->shm_set( e, bytes, len, enc );
    return;
  }
  if ( e.image != NULL ) {
    this->image_bytes -= e.image_len;
    ::free( e.image );
  }
  e.image = ::malloc( len );
  ::memcpy( e.image, bytes, len );
  e.image_len = len;
  e.image_enc = enc;
  this->image_bytes += len;
}

/* two-pass field-merge of upd over oldb into scratch: existing field
 * order preserved, values overwritten when present in the update, fields
 * only in the update appended.  Field name is the merge key.  Returns the
 * merged length (bytes guaranteed in this->scratch) or 0 on parse or
 * writer failure (caller falls back to replace). */
size_t
CacheTab::build_merge( const void *oldb,  size_t old_len,  uint32_t old_enc,
                       const void *upd,  size_t upd_len,
                       uint32_t upd_enc,  uint32_t &out_enc ) noexcept
{
  MDMsgMem mem;
  MDMsg * oldm = MDMsg::unpack( (void *) oldb, 0, old_len, old_enc,
                                NULL, mem );
  MDMsg * newm = MDMsg::unpack( (void *) upd, 0, upd_len, upd_enc, NULL, mem );
  if ( oldm == NULL || newm == NULL )
    return 0;
  MDFieldIter * oit = NULL,
              * nit = NULL;
  if ( oldm->get_field_iter( oit ) != 0 || newm->get_field_iter( nit ) != 0 )
    return 0;
  this->ensure_scratch( old_len + upd_len + 256 );
  /* writer of the cached message's own codec (MDMsg::create_writer);
   * the merged image keeps its encoding instead of converting to RVMSG.
   * Codecs without a writer fall back to RvMsgWriter as before. */
  RvMsgWriter       rvw( mem, this->scratch, this->scratch_len );
  MDMsgWriterBase * w = NULL;
  if ( oldm->create_writer( w, mem, NULL, this->scratch,
                            this->scratch_len ) == 0 && w != NULL ) {
    out_enc = oldm->get_type_id();
  }
  else {
    w = &rvw;
    out_enc = RVMSG_TYPE_ID;
  }

  /* pass 1: existing field order preserved; overwrite value if in update */
  if ( oit->first() == 0 ) {
    do {
      MDName nm;
      MDReference mref;
      if ( oit->get_name( nm ) != 0 )
        continue;
      if ( nit->find( nm, mref ) == 0 )
        w->append_iter( nit );             /* updated value */
      else if ( oit->get_reference( mref ) == 0 )
        w->append_iter( oit );             /* retained value */
    } while ( oit->next() == 0 );
  }
  /* pass 2: append fields that appear only in the update */
  if ( nit->first() == 0 ) {
    do {
      MDName nm;
      MDReference tmp;
      if ( nit->get_name( nm ) != 0 )
        continue;
      if ( oit->find( nm, tmp ) != 0 )     /* not in old image -> append */
        w->append_iter( nit );
    } while ( nit->next() == 0 );
  }
  size_t out_len = w->update_hdr();
  if ( w->err != 0 )
    return 0;      /* writer overflow/error */
  if ( (char *) w->buf != this->scratch ) {
    /* writer resized into MDMsgMem (dies with mem): copy out to scratch */
    this->ensure_scratch( out_len );
    ::memcpy( this->scratch, w->buf, out_len );
  }
  return out_len;
}

/* field-merge: rebuild image from old image, overwriting fields present in
 * the update and appending fields only in the update.  shm mode does the
 * read-modify-write under a single entry lock (shm_merge); heap mode
 * rebuilds into scratch then swaps via set_image. */
size_t
CacheTab::merge( CacheEntry &e,  const void *upd,  size_t upd_len,
                 uint32_t upd_enc ) noexcept
{
  if ( this->shm_mode() )
    return this->shm_merge( e, upd, upd_len, upd_enc );
  /* no existing image -> update becomes the image outright */
  if ( e.image == NULL ) {
    this->set_image( e, upd, upd_len, upd_enc );
    return e.image_len;
  }
  uint32_t out_enc = 0;
  size_t out_len = this->build_merge( e.image, e.image_len, e.image_enc,
                                      upd, upd_len, upd_enc, out_enc );
  if ( out_len == 0 ) {
    /* cannot parse one side / writer overflow: fall back to replace */
    this->set_image( e, upd, upd_len, upd_enc );
    return e.image_len;
  }
  this->set_image( e, this->scratch, out_len, out_enc );
  return e.image_len;
}

void
CacheTab::evict( const char *subj,  size_t len ) noexcept
{
  uint32_t h = kv_crc_c( subj, len, 0 );
  RouteLoc loc;
  CacheEntry * e = this->tab.find( h, subj, len, loc );
  if ( e != NULL ) {
    if ( this->shm_mode() )
      this->shm_evict( *e );
    if ( e->image != NULL ) {
      this->image_bytes -= e->image_len;
      ::free( e->image );
      e->image = NULL;
      e->image_len = 0;
    }
    /* live forwarding interest survives image eviction (DROP): keep the
     * entry so fwd_mask persists; the image is gone, so a subsequent
     * _SNAP gets the NOT_FOUND reply as before */
    if ( e->fwd_mask == 0 )
      this->tab.remove( loc );
  }
}

CacheEntry *
CacheTab::interest_set( const char *subj,  size_t len,  uint32_t net ) noexcept
{
  bool is_new = false;
  CacheEntry * e = this->upsert( subj, len, is_new );
  if ( e != NULL )
    e->fwd_mask |= (uint64_t) 1 << net;
  return e;
}

void
CacheTab::interest_clear( const char *subj,  size_t len,
                          uint32_t net ) noexcept
{
  uint32_t h = kv_crc_c( subj, len, 0 );
  RouteLoc loc;
  CacheEntry * e = this->tab.find( h, subj, len, loc );
  if ( e != NULL ) {
    e->fwd_mask &= ~( (uint64_t) 1 << net );
    /* idle: no interest anywhere and nothing cached */
    if ( e->fwd_mask == 0 && e->image == NULL )
      this->tab.remove( loc );
  }
}
