#ifndef __rv_cache__cache_h__
#define __rv_cache__cache_h__

#include <stdint.h>
#include <stddef.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>
#include <raimd/md_msg.h>
#include <raikv/route_ht.h>
#include <raikv/array_space.h>

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

/* one network attachment: -<idx> role,proto[,daemon[,network[,service]]]
 * or an entry in the -c json/yaml nets array.  mask bit = idx - 1 */
struct NetDef {
  uint32_t idx;      /* CLI integer, 1 .. MAX_NETS */
  bool     is_feed,  /* feed | sub */
           s2,       /* sass2: feed = _TIC broadcast consumer;
                      *        sub  = _RV.INFO advisories + _SNAP */
           s3;       /* sass3: feed = _SASS PUB consumer (-S, milestone 2);
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
  const char * sass3_feed;       /* -S <feed>  (feed-side sass3) */
  const char * sass3_name;       /* -F <name>  (downstream service) */
  rai::kv::ArrayCount< const char *, 4 > wildcards; /* -w (repeatable) */
  uint32_t     hold_secs;        /* -D  SASS3 hold timer, sass3 nets only
                                  *     (internal to sass3_db / -S reassert;
                                  *     no effect on RV-side interest) */
  bool         merge_default;    /* -m  */
  SeqPolicy    seq;              /* -Q  */
  bool         route_after_merge;/* -M  */
  uint32_t     stale_secs;       /* -x  (0 == never) */
  uint32_t     pending_secs;     /* -P  (default 10) */
  const char * acct_file;        /* -A  ('-' == stdout) */
  bool         quiet;            /* -q  */
  bool         verbose;          /* -v  */

  Config() : sass3_feed( 0 ), sass3_name( 0 ), hold_secs( 480 ),
             merge_default( false ), seq( SEQ_OBSERVE ),
             route_after_merge( false ),
             stale_secs( 0 ), pending_secs( 10 ), acct_file( 0 ),
             quiet( false ), verbose( false ) {}
  /* resolve a net's (d,n,s) triple, base filling gaps */
  void resolve( const NetDef &nd,  const char *&d,  const char *&n,
                const char *&s ) const {
    d = nd.parm.daemon  ? nd.parm.daemon  : this->base.daemon;
    n = nd.parm.network ? nd.parm.network : this->base.network;
    s = nd.parm.service ? nd.parm.service : this->base.service;
  }
};

struct Stats {
  uint64_t ticks_in,          /* _TIC.> messages consumed on net 1 */
           ticks_forwarded,   /* re-published on net 2 (live holder) */
           dropped_no_listener,/* dropped: no live holder */
           snaps_served,      /* _SNAP images served */
           snaps_missed,      /* _SNAP misses (TRANSIENT/NOT_FOUND) */
           interest_opens,    /* holder added (0->1 on a subject) */
           interest_closes,   /* subject dropped to 0 holders */
           evicted,           /* cache entries evicted (DROP) */
           seq_regress,       /* seqno went backwards */
           seq_gap,           /* seqno gap detected */
           transient_pass,    /* TRANSIENT ticks forwarded, not cached */
           nosub_sent;        /* NOSUBSCRIBERS DROP emitted */
  Stats() { this->reset_totals(); }
  void reset_totals( void ) {
    this->ticks_in = this->ticks_forwarded = this->dropped_no_listener = 0;
    this->snaps_served = this->snaps_missed = 0;
    this->interest_opens = this->interest_closes = 0;
    this->evicted = this->seq_regress = this->seq_gap = 0;
    this->transient_pass = this->nosub_sent = 0;
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

  CacheTab() : next_id( 1 ), scratch( 0 ), scratch2( 0 ), scratch_len( 0 ),
               scratch2_len( 0 ), image_bytes( 0 ) {}

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
  void evict( const char *subj,  size_t len ) noexcept;
  size_t count( void ) const { return this->tab.pop_count(); }
  void ensure_scratch( size_t n ) noexcept;
  void ensure_scratch2( size_t n ) noexcept;
};

} // namespace rvcache

#endif
