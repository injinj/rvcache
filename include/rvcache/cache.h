#ifndef __rv_cache__cache_h__
#define __rv_cache__cache_h__

#include <stdint.h>
#include <stddef.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>
#include <raimd/md_msg.h>
#include <raimd/dict_load.h>
#include <raikv/route_ht.h>
#include <raikv/array_space.h>
#include <raikv/shm_ht.h>
#include <raikv/key_ctx.h>
#include <raikv/ev_key.h>

namespace rvcache {

enum SeqPolicy {
  SEQ_OBSERVE = 0, /* apply every update in arrival order (default) */
  SEQ_STRICT  = 1, /* drop non-increasing seqnos */
  SEQ_STAMP   = 2  /* ignore feed seqno, stamp own monotonic seqno */
};

/* one (d,n,s) attachment triple */
struct NetParm {
  const char * daemon,
             * network,
             * service;
  NetParm() : daemon( 0 ), network( 0 ), service( 0 ) {}
};

static const uint32_t MAX_NETS = 64; /* fwd_mask is 64 bits */

/* one network attachment: -N role proto [daemon [network [service
 * [wildcard]]]] (argv-separated -- network configs contain commas) or an
 * entry in the -c json/yaml nets array.  Numbered by position: CLI -N
 * flags in argv order, then the nets array in file order.
 * mask bit = idx - 1 */
struct NetDef {
  uint32_t idx;      /* position, 1 .. MAX_NETS */
  bool     is_feed,  /* feed | sub */
           s2,       /* sass2: feed = _TIC broadcast consumer;
                      *        sub  = _RV.INFO advisories + _SNAP */
           s3,       /* sass3: feed = _SASS.<feed>.PUB envelope consumer;
                      *        sub  = _SASS.<feed>.SUB wildcard interest */
           omm;      /* omm:   feed = EvOmmClient consumer (daemon =
                      *        provider host[:port], service = OMM
                      *        service name, interactive interest);
                      *        sub  = EvOmmListen provider (daemon =
                      *        listen [host:]port, service = announced
                      *        service name) */
  NetParm  parm;
  const char * wildcard; /* per-net subject filter:
                          * sub  = submgr filter, both sass2 and sass3
                          *        (start_subscriptions all=false);
                          * feed = subscribe _TIC.<wild>.> (sass2) or
                          *        _SASS.<wild>.PUB (sass3) */
  NetDef() : idx( 0 ), is_feed( false ), s2( false ), s3( false ),
             omm( false ), wildcard( 0 ) {}
};

struct Config {
  NetParm      base;             /* -d -n -s */
  rai::kv::ArrayCount< NetDef, 8 > nets; /* -<idx> tuples / -c file */
  const char * map_name,             /* -m  */
             * accounting_file,      /* -A  ('-' == stdout) */
             * dict_path;            /* -p  */
  uint32_t     message_eviction_secs,/* -x  (0 == never) */
               pending_initial_secs; /* -P  (default 10) */
  /* omm login attributes (config-file keys; apply to all omm nets) */
  const char * omm_user,
             * omm_app_id,
             * omm_app_name,
             * omm_instance_id,
             * omm_token;
  uint32_t     omm_service_id;       /* provider-side directory id (1) */
  bool         map_inplace;          /* map_merge: inplace | rebuild */
  uint32_t     map_index_min;        /* map_index_min: entries (1=always, 0=never) */
  SeqPolicy    sequence_policy;      /* -Q  */
  bool         replace_typeless_msgs, /* -r  replace (not merge) typeless */
               route_after_merge,    /* -M  */
               quiet,                /* -q  */
               verbose;              /* -v  */

  Config() : map_name( 0 ), accounting_file( 0 ), dict_path( 0 ),
             message_eviction_secs( 0 ), pending_initial_secs( 10 ),
             omm_user( 0 ), omm_app_id( 0 ), omm_app_name( 0 ),
             omm_instance_id( 0 ), omm_token( 0 ), omm_service_id( 1 ),
             map_inplace( true ), map_index_min( 1 ),
             sequence_policy( SEQ_OBSERVE ), replace_typeless_msgs( false ),
             route_after_merge( false ), quiet( false ), verbose( false ) {}

  /* resolve a net's (d,n,s) triple, base filling gaps */
  void resolve( const NetDef &nd,  const char *&d,  const char *&n,
                const char *&s ) const {
    d = nd.parm.daemon  ? nd.parm.daemon  : this->base.daemon;
    n = nd.parm.network ? nd.parm.network : this->base.network;
    s = nd.parm.service ? nd.parm.service : this->base.service;
  }
};

/* config parsing (config.cpp): -<idx> net tuples (argv slice), -Q seqno
 * policy names and the -c json/yaml config file (long-name keys; only
 * the important knobs are CLI flags) */
bool parse_net_tuple( uint32_t idx,  const char **f,  uint32_t cnt,
                      NetDef &nd ) noexcept;
SeqPolicy parse_seq( const char *s ) noexcept;
bool load_config( const char *path,  Config &cfg ) noexcept;

struct Stats {
  uint64_t log_ns,
           cache_msg_count,   /* count of message in cache */
           cache_msg_bytes,   /* count of bytes for message cache */
           msgs_recv,         /* _TIC.> messages consumed */
           msgs_sent,         /* msgs re-published to sub nets */
           bytes_recv,        /* _TIC.> bytes consumbed */
           bytes_sent,        /* bytes re-published to sub nets */
           msgs_forwarded,    /* recv msgs forward so a listner */
           msgs_transient_fwd,/* TRANSIENT ticks forwarded, not cached */
           msgs_no_listener,  /* msgs not fwd, no sub */
           initials_sent,     /* initial images served */
           initials_not_found,/* initial misses (TRANSIENT/NOT_FOUND) */
           snaps_sent,        /* snapshot images served */
           snaps_not_found,   /* snapshot misses (TRANSIENT/NOT_FOUND) */
           subscriptions_active,/* total subs */
           subscription_starts,/* sub added (0->1 on a subject) */
           subscription_stops, /* sub dropped to 0 holders */
           msgs_evicted,      /* cache entries evicted (DROP) */
           sequence_regress,  /* seqno went backwards */
           sequence_gap,      /* seqno gap detected */
           heap_mem_info,
           user_cpu_usecs,
           sys_cpu_usecs;
  Stats() { this->reset_totals(); }
  void reset_totals( void ) {
    ::memset( (void *) this, 0, sizeof( *this ) );
  }
};

/* subject cache entry (raikv RouteVec Data: trailing hash/len/value[]) */
/* writer-side key -> offset index over an RWF Map image (map_merge.cpp).
 * Process memory only; shm mode validates it against KeyCtx::serial. */
struct MapIndex {
  struct Slot { uint32_t hash, off; }; /* off 0 = empty (0 is the header) */
  uint64_t serial;        /* image serial the index was built/kept for */
  Slot   * slots;
  uint32_t mask, used,    /* slots in use, including dead-pointing ones */
           live_cnt, dead_bytes,
           tail_slack;     /* trailing filler seen by build() */
  bool     valid;
  MapIndex() : serial( 0 ), slots( 0 ), mask( 0 ), used( 0 ), live_cnt( 0 ),
               dead_bytes( 0 ), tail_slack( 0 ), valid( false ) {}
  void   release( void ) noexcept;
  void   clear( uint32_t expect ) noexcept;
  Slot * probe( uint32_t hash,  const uint8_t *img,  size_t len,
                const uint8_t *key,  size_t key_len,  const void *shape,
                void *raw_entry_out ) noexcept;
  bool   build( const uint8_t *img,  size_t len ) noexcept;
};

struct CacheEntry {
  uint64_t fwd_mask;       /* per-net forwarding bools: bit (idx-1) set by
                            * a subscribe with refcnt > 0 on that net,
                            * cleared by an unsubscribe with refcnt == 0 */
  uint64_t update_count,   /* ticks received for this subject */
           forward_count,  /* ticks re-published */
           snap_count;     /* snapshots served */
  uint64_t last_update_ns;
  void   * image;          /* latest image blob (RVMSG bytes), malloc'd */
  size_t   image_len;
  uint32_t image_enc;      /* md msg encoding of image */
  uint32_t subject_id;
  uint32_t last_seqno;     /* SASS seqno when present (16-bit wrap tracked) */
  uint32_t own_seqno;      /* -Q stamp: cache's own monotonic seqno */
  uint64_t khash1, khash2; /* shm: 128-bit key hash of the subject, computed
                            * once (map seed); 0,0 = not yet */
  MapIndex * midx;         /* RWF Map image: writer's key index, or NULL */
  uint64_t image_serial;   /* heap: bumps per image write; shm: KeyCtx serial */
  uint32_t dead_bytes_hint,/* Map tombstone bytes when no index tracks them */
           tail_slack;     /* Map: trailing dead filler = append room, so a
                            * grow does not copy the image every tick */
  uint16_t msg_type;       /* last MD_SASS msg type seen */
  bool     has_seqno,
           image_partial;  /* RWF multipart refresh in progress: the image
                            * has CLEAR_CACHE's part but not REFRESH_COMPLETE
                            * yet; serve waits (pending) until it lands */
  /* RouteSub trailing members */
  uint32_t hash;
  uint16_t len;
  char     value[ 2 ];

  void init( uint32_t sub_id ) {
    this->fwd_mask = 0;
    this->update_count = this->forward_count = this->snap_count = 0;
    this->last_update_ns = 0;
    this->image = NULL;
    this->image_len = 0;
    this->image_enc = 0;
    this->subject_id = sub_id;
    this->last_seqno = 0;
    this->own_seqno = 0;
    this->msg_type = 0;
    this->has_seqno = false;
    this->image_partial = false;
    this->midx = NULL;
    this->khash1 = this->khash2 = 0;
    this->image_serial = 0;
    this->dead_bytes_hint = 0;
    this->tail_slack = 0;
  }
  void release_index( void ) {
    if ( this->midx != NULL ) {
      this->midx->release();
      ::free( this->midx );
      this->midx = NULL;
    }
  }
};


/* subject cache table.  Merge policy lives in cache_tab.cpp. */
struct CacheTab {
  rai::kv::RouteVec< CacheEntry > tab;
  rai::md::MDMsgDict       & dict;
  uint32_t                   next_id;
  char                     * scratch,     /* merge/build scratch buffer */
                           * scratch2;    /* MSG_TYPE normalize output */
  size_t                     scratch_len,
                             scratch2_len;
  uint64_t                   image_bytes; /* sum of image_len across entries */
  /* shm image store (-m map_name): when map != NULL, image bytes live in
   * the raikv HashTab keyed by subject (value = bare msg bytes; the
   * encoding is the single type byte in the kv HashEntry, set_type() =
   * (uint8_t) of the raimd TYPE_ID -- the same slot raids uses for redis
   * value types, and MDMsg::unpack() accepts it as the msg_enc hint, so
   * unpacking always produces a message).  CacheEntry::image stays NULL.
   * CacheTab::set_key primes KeyCtx with the NUL-terminated subject key
   * and the entry's cached 128-bit hash (khash1/2). */
  rai::kv::HashTab         * map;         /* EvShm.map when -m given */
  rai::kv::KeyCtx          * kctx;        /* shm key op context */
  rai::kv::HashSeed          hseed;       /* map hash seed for db 0 */
  rai::kv::WorkAllocT< 1024 > wrk;        /* kv work mem, reset per op */
  char                     * imgbuf,      /* shm get_image copy-out */
                           * keybuf;      /* KeyFragment: subject + NUL */
  size_t                     imgbuf_len,
                             keybuf_len;
  /* RWF Map merge strategy (SPEC Milestone 5): in place with tombstones,
   * or the phase-1 whole rebuild; a book below map_index_min live entries
   * looks keys up by linear scan (0 = never index).  Measured 2026-09-23:
   * the index is flat ~4us/update from 4 to 8400 keys, scan and rebuild
   * grow with the book, so the default indexes everything. */
  bool                       map_inplace;
  uint32_t                   map_index_min;
  uint64_t                   stats_inplace, stats_rebuild,
                             stats_compactions, stats_index_rebuilds,
                             stats_map_ns; /* merge time, both paths */

  CacheTab( rai::md::MDMsgDict &d ) : dict( d ), next_id( 1 ),
    scratch( 0 ), scratch2( 0 ), scratch_len( 0 ), scratch2_len( 0 ),
    image_bytes( 0 ), map( 0 ), kctx( 0 ),
    imgbuf( 0 ), keybuf( 0 ), imgbuf_len( 0 ), keybuf_len( 0 ),
    map_inplace( true ), map_index_min( 1 ), stats_inplace( 0 ),
    stats_rebuild( 0 ), stats_compactions( 0 ), stats_index_rebuilds( 0 ),
    stats_map_ns( 0 ) {}

  /* attach the shm image store; no-op when shm.map == NULL (no -m) */
  void init_shm( rai::kv::EvShm &shm ) noexcept;
  bool shm_mode( void ) const { return this->map != NULL; }

  CacheEntry * find( const char *subj,  size_t len ) noexcept;
  CacheEntry * upsert( const char *subj,  size_t len,  bool &is_new ) noexcept;
  /* forwarding-interest bits (see CacheEntry::fwd_mask).  set creates the
   * entry if needed (imageless); clear removes an idle entry (mask 0, no
   * image).  net is the mask bit index (idx - 1). */
  CacheEntry * interest_set( const char *subj,  size_t len,
                             uint32_t net ) noexcept;
  void interest_clear( const char *subj,  size_t len,  uint32_t net ) noexcept;
  /* store image bytes; normalizes MSG_TYPE to leading fixed-width uint */
  void set_image( CacheEntry &e,  const void *bytes,  size_t len,
                  uint32_t enc ) noexcept;
  /* when MSG_TYPE exists but is not the leading fixed-width int field,
   * rebuild with MSG_TYPE first (uint16) so outgoing deliveries can stamp
   * the type in place (MDFieldIter::update).  repoints bytes/len/enc into
   * scratch2 and returns true when a rebuild happened; typeless images are
   * left untouched (the field is normalized when present, never injected) */
  bool normalize_msg_type( const void *&bytes,  size_t &len,
                           uint32_t &enc ) noexcept;
  /* field-merge update bytes into e's image; rebuild + swap.  returns len */
  size_t merge_heap( CacheEntry &e,  const void *upd,  size_t upd_len,
                     uint32_t upd_enc ) noexcept; /* the field-list path */
  size_t merge( CacheEntry &e,  const void *upd,  size_t upd_len,
                uint32_t upd_enc ) noexcept;
  /* fetch e's image for serving/forwarding: heap mode returns e.image
   * (stamping mutates the cached bytes, as before); shm mode copies the
   * kv value into imgbuf (stamping mutates the copy; every send
   * re-stamps, so the stored MSG_TYPE is dead weight either way).
   * bytes stays valid until the next CacheTab operation.  false = no
   * image (miss or tombstoned) */
  bool get_image( CacheEntry &e,  void *&bytes,  size_t &len,
                  uint32_t &enc ) noexcept;
  void evict( const char *subj,  size_t len ) noexcept;
  size_t count( void ) const { return this->tab.pop_count(); }
  void ensure_scratch( size_t n ) noexcept;
  void ensure_scratch2( size_t n ) noexcept;
  /* --- shm internals (cache_tab.cpp) --- */
  /* prime kctx with the entry's subject: the key is the NUL-terminated
   * subject (keylen = len + 1, the raikv / raids string-key convention,
   * copied into keybuf), the 128-bit hash is cached in the entry -- no
   * per-op EvKeyCtx, no rehash */
  void set_key( CacheEntry &e,  rai::kv::KeyCtx &kc ) noexcept;
  /* start the HashEntry line towards the core while the update is still
   * being parsed / planned; needs the cached hash (a prior op on the
   * entry), a no-op in heap mode or on a fresh entry */
  void prefetch( const CacheEntry &e,  bool for_write ) const {
    if ( this->map != NULL && ( e.khash1 | e.khash2 ) != 0 )
      this->map->prefetch( e.khash1, ! for_write );
  }
  /* two-pass field-merge of upd over old into scratch; 0 = parse/overflow
   * failure (caller falls back to replace).  shared by heap + shm merge.
   * The writer comes from MDMsg::create_writer() so the merged image
   * keeps the cached message's own codec; out_enc = its type id */
  /* RWF Map x Map (MARKET_BY_ORDER / MARKET_BY_PRICE / SYMBOL_LIST ...):
   * entries keyed by the encoded key; ADD replaces, UPDATE field-merges
   * the entry's field list, DELETE removes, summary field-merges.  The
   * image keeps the refresh's set definitions; entries copied from the
   * update that use set data are re-encoded as standard data.  Result
   * in scratch, 0 on failure. */
  size_t build_merge_map( const void *oldb,  size_t old_len,
                          const void *upd,  size_t upd_len ) noexcept;
  /* map_merge.cpp: in-place Map merge with tombstones + MapIndex */
  size_t merge_map_inplace( CacheEntry &e,  const void *upd,
                            size_t upd_len ) noexcept;
  bool   plan_map_update( CacheEntry &e,  const uint8_t *img,  size_t len,
                          const void *upd,  size_t upd_len,
                          rai::md::MDMsgMem &mem,  void *plan,
                          bool &use_index ) noexcept;
  size_t map_grow_len( CacheEntry &e,  size_t img_len,  size_t append_total,
                       size_t &slack_new ) noexcept;
  void   commit_map_update( CacheEntry &e,  uint8_t *img,  size_t img_len,
                            size_t new_len,  void *plan,
                            bool use_index ) noexcept;
  size_t strip_map( const uint8_t *img,  size_t len,  uint8_t *out ) noexcept;
  /* serve: a Map image without tombstones (raw, or stripped into scratch) */
  bool   live_image( CacheEntry &e,  void *&bytes,  size_t &len,
                     uint32_t enc ) noexcept;
  /* fid-keyed two-pass field list merge into mem; returns len, 0 fail */
  size_t merge_field_lists( rai::md::MDMsgMem &mem,  const void *oldb,  size_t old_len,
                            const void *upd,  size_t upd_len,
                            void *&out ) noexcept;
  size_t build_merge( const void *oldb,  size_t old_len,  uint32_t old_enc,
                      const void *upd,  size_t upd_len,
                      uint32_t upd_enc,  uint32_t &out_enc ) noexcept;
  bool   shm_set( CacheEntry &e,  const void *bytes,  size_t len,
                  uint32_t enc ) noexcept;
  size_t shm_merge( CacheEntry &e,  const void *upd,  size_t upd_len,
                    uint32_t upd_enc ) noexcept;
  bool   shm_get( CacheEntry &e,  void *&bytes,  size_t &len,
                  uint32_t &enc ) noexcept;
  void   shm_evict( CacheEntry &e ) noexcept;
};

} // namespace rvcache

#endif
