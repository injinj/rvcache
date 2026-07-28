#ifndef __rv_cache__cache_h__
#define __rv_cache__cache_h__

#include <stdint.h>
#include <stddef.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>
#include <raimd/md_msg.h>
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

/* one network attachment: -<idx> role proto [daemon [network [service
 * [wildcard]]]] (argv-separated -- network configs contain commas) or an
 * entry in the -c json/yaml nets array.  mask bit = idx - 1 */
struct NetDef {
  uint32_t idx;      /* CLI integer, 1 .. MAX_NETS */
  bool     is_feed,  /* feed | sub */
           s2,       /* sass2: feed = _TIC broadcast consumer;
                      *        sub  = _RV.INFO advisories + _SNAP */
           s3;       /* sass3: feed = _SASS.<feed>.PUB envelope consumer;
                      *        sub  = _SASS.<feed>.SUB wildcard interest */
  NetParm  parm;
  const char * wildcard; /* per-net subject filter:
                          * sub  = submgr filter, both sass2 and sass3
                          *        (start_subscriptions all=false);
                          * feed = subscribe _TIC.<wild>.> (sass2) or
                          *        _SASS.<wild>.PUB (sass3) */
  NetDef() : idx( 0 ), is_feed( false ), s2( false ), s3( false ),
             wildcard( 0 ) {}
};

struct Config {
  NetParm      base;             /* -d -n -s */
  rai::kv::ArrayCount< NetDef, 8 > nets; /* -<idx> tuples / -c file */
  const char * map_name,             /* -m  */
             * accounting_file,      /* -A  ('-' == stdout) */
             * dict_path;            /* -p  */
  uint32_t     message_eviction_secs,/* -x  (0 == never) */
               pending_initial_secs; /* -P  (default 10) */
  SeqPolicy    sequence_policy;      /* -Q  */
  bool         replace_typeless_msgs, /* -r  replace (not merge) typeless */
               route_after_merge,    /* -M  */
               quiet,                /* -q  */
               verbose;              /* -v  */

  Config() : map_name( 0 ), accounting_file( 0 ), dict_path( 0 ),
             message_eviction_secs( 0 ), pending_initial_secs( 10 ),
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
  uint16_t msg_type;       /* last MD_SASS msg type seen */
  bool     has_seqno;
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
  }
};

/* subject cache table.  Merge policy lives in cache_tab.cpp. */
struct CacheTab {
  rai::kv::RouteVec< CacheEntry > tab;
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
   * EvKeyCtx carries the subject key + 128-bit hash into KeyCtx ops (and
   * is the unit the raids-style prefetch pipeline queues, when that
   * lands - SPEC Milestone 3 notes) */
  rai::kv::HashTab         * map;         /* EvShm.map when -m given */
  rai::kv::KeyCtx          * kctx;        /* shm key op context */
  rai::kv::HashSeed          hseed;       /* map hash seed for db 0 */
  rai::kv::WorkAllocT< 1024 > wrk;        /* kv work mem, reset per op */
  char                     * keybuf,      /* EvKeyCtx placement buffer */
                           * imgbuf;      /* shm get_image copy-out */
  size_t                     keybuf_len,
                             imgbuf_len;

  CacheTab() : next_id( 1 ), scratch( 0 ), scratch2( 0 ), scratch_len( 0 ),
               scratch2_len( 0 ), image_bytes( 0 ), map( 0 ), kctx( 0 ),
               keybuf( 0 ), imgbuf( 0 ), keybuf_len( 0 ), imgbuf_len( 0 ) {}

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
  rai::kv::EvKeyCtx * key_of( const CacheEntry &e ) noexcept;
  /* two-pass field-merge of upd over old into scratch; 0 = parse/overflow
   * failure (caller falls back to replace).  shared by heap + shm merge.
   * The writer comes from MDMsg::create_writer() so the merged image
   * keeps the cached message's own codec; out_enc = its type id */
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
