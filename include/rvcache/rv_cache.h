#ifndef __rv_cache__rv_cache_h__
#define __rv_cache__rv_cache_h__

#include <stdio.h>
#include <rvcache/cache.h>
#include <raikv/ev_publish.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>

namespace rvcache {

/* foreign types used unqualified by RvCache (scoped to this namespace,
 * the .cpp files still pull in the full namespaces) */
using rai::kv::EvPoll;
using rai::kv::EvShm;
using rai::kv::EvPublish;
using rai::kv::NotifySub;
using rai::kv::EvSocket;
using rai::kv::EvConnectionNotify;
using rai::kv::RouteNotify;
using rai::kv::RoutePublish;
using rai::md::MDMsgDict;
using rai::md::MDDict;
using rai::md::RwfMsg;
using rai::sassrv::EvRvClient;
using rai::sassrv::RvSubscriptionDB;
using rai::sassrv::RvSubscriptionListener;
using rai::sassrv::RvSessionEntry;
using rai::sassrv::RvSass3Entry;
using rai::omm::EvOmmClient;
using rai::omm::EvOmmListen;
using rai::omm::OmmDict;
using rai::omm::OmmSourceDB;
using rai::omm::OmmClientCB;

/* the cache process: rv sub/feed nets (sass2 + sass3 interest channels),
 * omm feed/provider nets, one subject table.  Method bodies are split by
 * protocol:  rv_cache.cpp (core tick path, publish, stats, main),
 * sass2.cpp (LISTEN advisories + snapshot serving), sass3.cpp (sass3
 * interest channel), omm.cpp (OMM feed + provider sides). */
struct RvCache {
  EvPoll         & poll;
  EvShm          & shm;
  Config         & cfg;
  CacheTab         cache;
  Stats            stats,
                   old;
  /* network attachments by mask bit (idx - 1); feeds have conns only */
  EvRvClient       * sub_conns[ MAX_NETS ];
  RvSubscriptionDB * sub_dbs[ MAX_NETS ];
  uint64_t           sub_nets;  /* mask of configured sub nets */
  /* omm nets (SPEC Milestone 4): feed side = EvOmmClient consumers,
   * provider side = EvOmmListen serving OMM clients from the cache */
  EvOmmClient      * omm_conns[ MAX_NETS ]; /* omm feed clients */
  const char       * omm_wild[ MAX_NETS ];  /* omm feed wildcard filters */
  uint64_t           omm_feeds,             /* mask: omm feed nets READY */
                     omm_subs;              /* mask: omm provider nets */
  EvOmmListen      * omm_listener;          /* provider listener */
  /* omm streams opened on a subject with no image yet: the image arrives
   * from a feed later (interest edge) or the request times out with a
   * CLOSED / NOT_FOUND status after cfg.pending_initial_secs */
  struct OmmPending {      /* RouteVec Data: trailing hash/len/value[] */
    uint64_t open_ns;
    uint32_t hash;
    uint16_t len;
    char     value[ 2 ];
  };
  rai::kv::RouteVec<OmmPending> omm_pending;
  FILE             * acct;
  char               pubbuf[ 64 * 1024 ];

  RvCache( EvPoll &p,  EvShm &s,  MDMsgDict &d,  Config &c )
    : poll( p ), shm( s ), cfg( c ), cache( d ), sub_nets( 0 ),
      omm_feeds( 0 ), omm_subs( 0 ), omm_listener( 0 ),
      acct( 0 ) {
    for ( uint32_t i = 0; i < MAX_NETS; i++ ) {
      this->sub_conns[ i ] = NULL;
      this->sub_dbs[ i ]   = NULL;
      this->omm_conns[ i ] = NULL;
      this->omm_wild[ i ]  = NULL;
    }
    this->stats.log_ns = poll.now_ns;
    this->cache.init_shm( s ); /* -m map_name: images live in raikv shm */
  }

  uint32_t cur_mono( void ) const {
    return (uint32_t) ( this->poll.mono_ns / (uint64_t) 1000000000 );
  }
  uint64_t now_ns( void ) const { return rai::kv::current_realtime_ns(); }

  /* feed path (net 1) */
  void on_feed_msg( EvPublish &pub ) noexcept;
  void on_sass3_feed_msg( EvPublish &pub ) noexcept;
  void handle_tic( const char *subj,  size_t len,  const void *msg,
                   size_t msg_len,  uint32_t enc,
                   bool has_type_ovr = false,
                   uint16_t type_ovr = 0 ) noexcept;
  /* interest (submgr callbacks; net = mask bit of the sub network) */
  void on_listen_start( RvSubscriptionListener::Start &add,
                        uint32_t net ) noexcept;
  void on_listen_stop( RvSubscriptionListener::Stop &rem,
                       uint32_t net ) noexcept;
  void on_snapshot( RvSubscriptionListener::Snap &snp,
                    uint32_t net ) noexcept;
  void on_sass3( RvSubscriptionListener::Sass3 &sa3,  uint32_t net ) noexcept;
  void on_tic_reply( RvSubscriptionListener::Tic &tic,  uint32_t net ) noexcept;
  /* omm feed side: EvOmmClient consumer (RWF -> sass at ingest, then
   * the normal handle_tic path; SPEC Milestone 4 feed side) */
  bool on_omm_feed_msg( uint32_t net,  const char *subj,  size_t len,
                        RwfMsg &m ) noexcept;
  void omm_feed_ready( uint32_t net ) noexcept;
  bool omm_wild_match( uint32_t net,  const char *subj,
                       size_t len ) noexcept;
  void omm_feed_subscribe( const char *subj,  size_t len ) noexcept;
  void omm_feed_unsubscribe( const char *subj,  size_t len ) noexcept;
  /* interest edge: total fwd_mask 0 <-> nonzero drives the upstream
   * omm subscriptions (interactive-feed pattern) */
  void interest_edge_set( const char *subj,  size_t len,
                          uint32_t net ) noexcept;
  void interest_edge_clear( const char *subj,  size_t len,
                            uint32_t net ) noexcept;
  /* omm provider side: EvOmmListen clients fed from the sass2/sass3
   * tick flow (sass -> RWF once per tick; EvOmmConn stamps per-client
   * stream ids; SPEC Milestone 4 client side) */
  void on_omm_sub( NotifySub &sub,  uint32_t net ) noexcept;
  /* another stream / a reissue on a subject already open: solicited
   * image again, no interest change */
  void on_omm_resub( NotifySub &sub,  uint32_t net ) noexcept;
  void on_omm_unsub( NotifySub &sub,  uint32_t net ) noexcept;
  /* solicited initial from the cache, or STATUS suspect/open + pending */
  void omm_send_initial( const char *subj,  size_t len ) noexcept;
  void omm_pending_add( const char *subj,  size_t len ) noexcept;
  void omm_pending_check( void ) noexcept; /* on_timer: expire pendings */
  void omm_forward( const char *subj,  size_t len,  const void *msg,
                    size_t msg_len,  uint32_t enc,  uint16_t msg_type,
                    uint32_t seqno,  bool solicited ) noexcept;
  void omm_send_status( const char *subj,  size_t len,
                        bool closed ) noexcept;
  /* timers */
  void on_timer( void ) noexcept;
  void print_stats( bool final_totals ) noexcept;

  /* helpers */
  /* stamp MSG_TYPE (leading fixed-width int, normalized at store time)
   * directly into the cached image via MDFieldIter::update() */
  bool stamp_msg_type( void *bytes,  size_t len,  uint32_t enc,
                       uint16_t msg_type ) noexcept;
  /* serve-path lookup: in shm mode another rv_cache process may have
   * cached the subject's image, so a missing local entry doesn't mean a
   * miss -- mint the local metadata entry and let get_image consult the
   * map.  Tick/accounting paths keep the plain find(). */
  CacheEntry * find_for_image( const char *subj,  size_t len ) noexcept {
    CacheEntry * e = this->cache.find( subj, len );
    if ( e == NULL && this->cache.shm_mode() ) {
      bool is_new;
      e = this->cache.upsert( subj, len, is_new );
    }
    return e;
  }
  /* publish to one sub net (replies, per-net broadcasts) */
  void publish_msg( uint32_t net,  const char *subj,  size_t len,
                    const char *reply,  size_t reply_len,  const void *msg,
                    size_t msg_len,  uint32_t enc ) noexcept;
  /* publish to every sub net whose fwd_mask bit is set (tick forwards) */
  void publish_mask( uint64_t mask,  const char *subj,  size_t len,
                     const void *msg,  size_t msg_len,
                     uint32_t enc ) noexcept;
  size_t build_status( uint16_t msg_type,  uint16_t rec_status,
                       const char *subj,  size_t len,  char *buf,
                       size_t buflen ) noexcept;
  void emit_nosubscribers( uint32_t net,  const char *subj,
                           size_t len ) noexcept;
  void serve_snapshot( uint32_t net,  const char *subj,  size_t len,
                       const char *reply,  size_t reply_len,
                       const RvSessionEntry *sess,  uint16_t flags,
                       const RvSass3Entry *s3 = NULL ) noexcept;
  /* miss: TRANSIENT / NOT_FOUND to the reply inbox (bcast-nack); the one
   * code path shared by _SNAP, listen-start-inbox and sass3 requests */
  void serve_miss( uint32_t net,  const char *subj,  size_t len,
                   const char *reply,  size_t reply_len ) noexcept;
  /* asserted interest (sass2 query discovery / sass3 resubscribe of an
   * unknown holder): broadcast an initial on the subject -- listeners
   * that predate rv_cache converge on the image; no inbox involved */
  void broadcast_initial( uint32_t net,  const char *subj,  size_t len,
                          const RvSessionEntry *sess,
                          const RvSass3Entry *s3,
                          const char *proto ) noexcept;
  void acct_event( const char *event,  const char *subj,  size_t sublen,
                   const RvSessionEntry *sess,  const char *proto,
                   uint16_t query_flags,  const char *reason,
                   double open_secs,  uint64_t msgs,
                   uint64_t images,
                   const RvSass3Entry *s3 = NULL ) noexcept;
};

/* omm.cpp: omm feed net callback -- connection lifecycle + inbound RWF
 * messages.  ready fires after login+directory+dictionary resolve
 * (interest replays there, never at TCP connect); loss is fail-fast */
struct OmmFeedCB : public EvConnectionNotify, public OmmClientCB {
  EvPoll      & poll;
  EvOmmClient & client;
  RvCache     & cache;
  uint32_t      net;     /* 1-based net idx (omm convention) */

  OmmFeedCB( EvPoll &p,  EvOmmClient &c,  RvCache &rc,  uint32_t idx )
    : poll( p ), client( c ), cache( rc ), net( idx ) {}

  virtual void on_connect( EvSocket &conn ) noexcept;
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t errlen ) noexcept;
  virtual bool on_omm_msg( const char *sub,  size_t sub_len,  uint32_t,
                           RwfMsg &msg ) noexcept;
};

/* omm.cpp: omm provider net -- an OMM client's item stream open/close
 * surfaces as NotifySub (src_type 'O', EvOmmConn::add_subj_stream) on the
 * listener's sub_route; everything else on that route is ignored */
struct OmmSubNotify : public RouteNotify {
  RvCache & cache;
  uint32_t  net;         /* 1-based net idx (omm convention) */

  OmmSubNotify( RoutePublish &sr,  RvCache &rc,  uint32_t idx )
    : RouteNotify( sr ), cache( rc ), net( idx ) {}

  virtual void on_sub( NotifySub &sub ) noexcept;
  virtual void on_resub( NotifySub &sub ) noexcept;
  virtual void on_unsub( NotifySub &sub ) noexcept;
};

/* omm.cpp: provider-side source directory announcement for the cache's
 * service (main calls it once per omm provider net) */
bool announce_cache_service( OmmSourceDB &db,  MDDict *rdm_dict,
                             const char *svc,  uint32_t service_id ) noexcept;

} // namespace rvcache

#endif
