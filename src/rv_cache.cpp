#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <malloc.h>
#include <sys/resource.h>
#include <new>
#include <rvcache/cache.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/rv_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/sass.h>
#include <raimd/md_field_iter.h>
#include <raimd/dict_load.h>
#include <raikv/ev_publish.h>
#include <raikv/key_hash.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace sassrv;
using namespace omm;
using namespace rvcache;

/* ------------------------------------------------------------------ */
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
  OmmDict          * omm_dict;              /* rdm dict for omm nets */
  EvOmmListen      * omm_listener;          /* provider listener */
  FILE             * acct;
  char               pubbuf[ 64 * 1024 ];

  RvCache( EvPoll &p,  EvShm &s,  MDMsgDict &d,  Config &c )
    : poll( p ), shm( s ), cfg( c ), cache( d ), sub_nets( 0 ),
      omm_feeds( 0 ), omm_subs( 0 ), omm_dict( 0 ), omm_listener( 0 ),
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
  uint64_t now_ns( void ) const { return current_realtime_ns(); }

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
                        md::RwfMsg &m ) noexcept;
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
  void on_omm_sub( kv::NotifySub &sub,  uint32_t net ) noexcept;
  void on_omm_unsub( kv::NotifySub &sub,  uint32_t net ) noexcept;
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

/* parse SASS header fields out of a raimd message */
static bool
parse_sass( MDMsg *m,  uint16_t &msg_type,  uint32_t &seqno,  bool &has_type,
            bool &has_seqno,  uint16_t &rec_status,  bool &has_status ) noexcept
{
  has_type = has_seqno = has_status = false;
  msg_type = 0; seqno = 0; rec_status = 0;
  if ( m == NULL )
    return false;
  MDFieldIter * it = NULL;
  if ( m->get_field_iter( it ) != 0 )
    return false;
  MDReference mref;
  if ( it->find( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, mref ) == 0 &&
       ( mref.ftype == MD_UINT || mref.ftype == MD_INT ) ) {
    msg_type = get_uint<uint16_t>( mref );
    has_type = true;
  }
  if ( it->find( MD_SASS_SEQ_NO, MD_SASS_SEQ_NO_LEN, mref ) == 0 &&
       ( mref.ftype == MD_UINT || mref.ftype == MD_INT ) ) {
    seqno = get_uint<uint32_t>( mref );
    has_seqno = true;
  }
  if ( it->find( MD_SASS_REC_STATUS, MD_SASS_REC_STATUS_LEN, mref ) == 0 &&
       ( mref.ftype == MD_UINT || mref.ftype == MD_INT ) ) {
    rec_status = get_uint<uint16_t>( mref );
    has_status = true;
  }
  return true;
}

/* stamp the delivery MSG_TYPE into the image bytes in place.  set_image
 * normalized the field to be first + fixed-width int, so this is a
 * find-first-field + same-size/type MDFieldIter::update().  MDMsg::unpack
 * references the bytes (no copy), so update() mutates them directly --
 * heap mode stamps the cached image (every send re-stamps, the stored
 * value is dead weight), shm mode stamps get_image's copy-out buffer.
 * Typeless images return false and are served as-is. */
bool
RvCache::stamp_msg_type( void *bytes,  size_t len,  uint32_t enc,
                         uint16_t msg_type ) noexcept
{
  if ( bytes == NULL )
    return false;
  MDMsgMem mem;
  MDMsg  * m = MDMsg::unpack( bytes, 0, len, enc,
                              this->cache.dict.dict, mem );
  if ( m == NULL )
    return false;
  MDFieldIter * it = NULL;
  if ( m->get_field_iter( it ) != 0 || it->first() != 0 )
    return false;
  MDName      nm;
  MDReference mref;
  if ( it->get_name( nm ) != 0 ||
       ! nm.equals( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN ) )
    return false;
  if ( it->get_reference( mref ) != 0 )
    return false;
  if ( ( mref.ftype != MD_UINT && mref.ftype != MD_INT ) ||
       mref.fsize < 1 || mref.fsize > 8 )
    return false;
  /* encode msg_type in the width/endianness the field already uses */
  uint8_t  buf[ 8 ];
  uint64_t v = msg_type;
  for ( size_t i = 0; i < mref.fsize; i++ ) {
    size_t shift = ( mref.fendian == MD_BIG ) ? ( mref.fsize - 1 - i ) : i;
    buf[ i ] = (uint8_t) ( v >> ( shift * 8 ) );
  }
  MDReference nref = mref;
  nref.fptr = buf;
  return it->update( nref ) == 0;
}

/* ------------------------------------------------------------------ */
void
RvCache::publish_msg( uint32_t net,  const char *subj,  size_t len,
                      const char *reply,  size_t reply_len,  const void *msg,
                      size_t msg_len,  uint32_t enc ) noexcept
{
  EvRvClient * c = net < MAX_NETS ? this->sub_conns[ net ] : NULL;
  if ( c == NULL )
    return;
  uint32_t h = kv_crc_c( subj, len, 0 );
  EvPublish pub( subj, len, reply, reply_len, msg, msg_len,
                 c->sub_route, *c, h, enc );
  c->publish( pub );
  this->stats.msgs_sent++;
  this->stats.bytes_sent += len + reply_len + msg_len;
}

void
RvCache::publish_mask( uint64_t mask,  const char *subj,  size_t len,
                       const void *msg,  size_t msg_len,
                       uint32_t enc ) noexcept
{
  mask &= this->sub_nets;
  while ( mask != 0 ) {
    uint32_t net = (uint32_t) __builtin_ctzll( mask );
    mask &= mask - 1;
    this->publish_msg( net, subj, len, NULL, 0, msg, msg_len, enc );
  }
}

size_t
RvCache::build_status( uint16_t msg_type,  uint16_t rec_status,
                       const char *subj,  size_t len,  char *buf,
                       size_t buflen ) noexcept
{
  MDMsgMem mem;
  RvMsgWriter w( mem, buf, buflen );
  w.append_uint( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, msg_type );
  w.append_uint( MD_SASS_SEQ_NO, MD_SASS_SEQ_NO_LEN, (uint16_t) 0 );
  w.append_uint( MD_SASS_REC_STATUS, MD_SASS_REC_STATUS_LEN, rec_status );
  w.append_string( MD_SASS_SYMBOL, MD_SASS_SYMBOL_LEN, subj, len );
  return w.update_hdr();
}

void
RvCache::emit_nosubscribers( uint32_t net,  const char *subj,
                             size_t len ) noexcept
{
  char buf[ 1024 ];
  size_t n = this->build_status( (uint16_t) MD_DROP_TYPE,
                                 (uint16_t) MD_NOSUBSCRIBERS_STATUS,
                                 subj, len, buf, sizeof( buf ) );
  this->publish_msg( net, subj, len, NULL, 0, buf, n, RVMSG_TYPE_ID );
}

/* ------------------------------------------------------------------ */
void
RvCache::on_feed_msg( EvPublish &pub ) noexcept
{
  const char * subj = pub.subject;
  size_t       len  = pub.subject_len;
  /* only _TIC.<subject> ticks are cache input on net 1 */
  if ( len <= 5 || ::memcmp( subj, "_TIC.", 5 ) != 0 )
    return;
  this->handle_tic( subj + 5, len - 5, pub.msg, pub.msg_len, pub.msg_enc );
}

/* sass3 feed path: _SASS.<feed>.PUB broadcast envelope (Sass3Svc::doFeed
 * shape):  { M : 23177, T : MSG_TYPE, D : { <subject> : <opaque msg>
 * [, <subject> : <opaque msg> ] } }.  T overrides the payload's MSG_TYPE
 * when present; S, I, A, G, E are ignored for now. */
void
RvCache::on_sass3_feed_msg( EvPublish &pub ) noexcept
{
  const char * subj = pub.subject;
  size_t       len  = pub.subject_len;
  if ( len <= 10 || ::memcmp( subj, "_SASS.", 6 ) != 0 ||
       ::memcmp( &subj[ len - 4 ], ".PUB", 4 ) != 0 )
    return;
  MDMsgMem mem;
  MDMsg  * m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, pub.msg_enc,
                              this->cache.dict.dict, mem );
  if ( m == NULL )
    return;
  MDFieldReader rd( *m );
  uint16_t      magic = 0;
  if ( ! rd.find( "M", 2 ) || ! rd.get_uint( magic ) ||
       magic != SASS3_PUB_MAGIC )
    return;
  uint16_t type_ovr = 0;
  bool     has_ovr  = false;
  if ( rd.find( "T", 2 ) && rd.get_uint( type_ovr ) )
    has_ovr = true;
  MDMsg * d = NULL;
  if ( ! rd.find( "D", 2 ) || ! rd.get_sub_msg( d ) || d == NULL )
    return;
  /* each D field: name = data subject, value = opaque message bytes */
  MDFieldReader dr( *d );
  MDName        n;
  for ( bool b = dr.first( n ); b; b = dr.next( n ) ) {
    void * data;
    size_t dlen,
           slen = n.fnamelen;
    while ( slen > 0 && n.fname[ slen - 1 ] == '\0' )
      slen--;
    if ( slen > 0 && dr.get_opaque( data, dlen ) )
      this->handle_tic( n.fname, slen, data, dlen, 0, has_ovr, type_ovr );
  }
}

void
RvCache::handle_tic( const char *subj,  size_t len,  const void *msg,
                     size_t msg_len,  uint32_t enc,
                     bool has_type_ovr,  uint16_t type_ovr ) noexcept
{
  this->stats.msgs_recv++;
  this->stats.bytes_recv += len + msg_len;

  MDMsgMem mem;
  MDMsg *  m = MDMsg::unpack( (void *) msg, 0, msg_len, enc,
                              this->cache.dict.dict, mem );
  uint16_t msg_type = 0, rec_status = 0;
  uint32_t seqno = 0;
  bool     has_type = false, has_seqno = false, has_status = false;
  parse_sass( m, msg_type, seqno, has_type, has_seqno, rec_status, has_status );
  if ( has_type_ovr ) { /* sass3 envelope T overrides the payload MSG_TYPE */
    msg_type = type_ovr;
    has_type = true;
  }

  /* determine effective policy when no MSG_TYPE field present */
  bool is_initial   = has_type && msg_type == MD_INITIAL_TYPE;
  bool is_verify    = has_type && msg_type == MD_VERIFY_TYPE;
  bool is_update    = has_type && ( msg_type == MD_UPDATE_TYPE ||
                                    msg_type == MD_CORRECT_TYPE );
  bool is_closing   = has_type && msg_type == MD_CLOSING_TYPE;
  bool is_drop      = has_type && msg_type == MD_DROP_TYPE;
  bool is_transient = has_type && msg_type == MD_TRANSIENT_TYPE;

  CacheEntry * e = this->cache.find( subj, len );

  /* seqno policy (applies to cache-affecting ticks) */
  bool apply = true;
  if ( has_seqno && ! is_transient && ! is_drop ) {
    if ( e != NULL && e->has_seqno ) {
      uint16_t last16 = (uint16_t) e->last_seqno,
               new16  = (uint16_t) seqno;
      int16_t  d      = (int16_t) ( new16 - last16 );
      switch ( this->cfg.sequence_policy ) {
        case SEQ_OBSERVE:
          if ( d < 0 ) this->stats.sequence_regress++;
          else if ( d > 1 ) this->stats.sequence_gap++;
          break;
        case SEQ_STRICT:
          if ( d <= 0 ) { this->stats.sequence_regress++; apply = false; }
          break;
        case SEQ_STAMP:
          break; /* ignore feed seqno for ordering */
      }
    }
  }

  /* forwarding payload defaults to the raw tick */
  const void * fwd_msg = msg;
  size_t       fwd_len = msg_len;
  uint32_t     fwd_enc = enc;

  if ( is_transient ) {
    /* status notification: forward, never cache */
    this->stats.msgs_transient_fwd++;
  }
  else if ( is_drop ) {
    /* instrument death: forward to listeners, evict entry */
    if ( e != NULL ) {
      this->cache.evict( subj, len );
      this->stats.msgs_evicted++;
    }
  }
  else if ( apply ) {
    bool is_new = false;
    CacheEntry * ce = this->cache.upsert( subj, len, is_new );
    if ( ce != NULL ) {
      ce->update_count++;
      ce->last_update_ns = this->now_ns();
      if ( has_type ) ce->msg_type = msg_type;
      if ( has_seqno ) { ce->last_seqno = seqno; ce->has_seqno = true; }

      bool do_merge;
      if ( is_initial ) {
        do_merge = false;                 /* replace outright */
      }
      else if ( is_update || is_closing ) {
        do_merge = true;                  /* field-merge */
      }
      else if ( is_verify ) {
        do_merge = ! is_new;              /* merge if cached, else create */
      }
      else { /* no MSG_TYPE field: -m merge, else replace */
        do_merge = ! this->cfg.replace_typeless_msgs && ! is_new;
      }

      if ( do_merge )
        this->cache.merge( *ce, msg, msg_len, enc );
      else
        this->cache.set_image( *ce, msg, msg_len, enc );

      if ( this->cfg.sequence_policy == SEQ_STAMP ) {
        ce->own_seqno++;
        ce->last_seqno = ce->own_seqno;
      }
      if ( this->cfg.route_after_merge ) {
        void   * img;
        size_t   img_len;
        uint32_t img_enc;
        if ( this->cache.get_image( *ce, img, img_len, img_enc ) ) {
          fwd_msg = img;
          fwd_len = img_len;
          fwd_enc = img_enc;
        }
      }
    }
  }

  /* forwarding decision: per-net forwarding bools on the cache entry
   * (fwd_mask).  A subscribe with refcnt > 0 on a net set its bit, an
   * unsubscribe with refcnt == 0 cleared it; submgr still owns the
   * subscription's life -- the mask is just the materialized gate.
   * Forward goes ONLY to nets whose bit is set. */
  CacheEntry * fe = this->cache.find( subj, len );
  uint64_t     mask = ( fe != NULL ? fe->fwd_mask : 0 );
  if ( mask != 0 ) {
    this->publish_mask( mask, subj, len, fwd_msg, fwd_len, fwd_enc );
    /* omm provider nets: one canonical RWF conversion covers every
     * attached OMM client (per-conn stream ids stamped in EvOmmConn) */
    if ( ( mask & this->omm_subs ) != 0 )
      this->omm_forward( subj, len, fwd_msg, fwd_len, fwd_enc,
                         has_type ? msg_type : (uint16_t) MD_UPDATE_TYPE,
                         has_seqno ? seqno : 0, false );
    this->stats.msgs_forwarded++;
    fe->forward_count++;
  }
  else {
    this->stats.msgs_no_listener++;
  }
}

/* ------------------------------------------------------------------ */
void
RvCache::on_listen_start( RvSubscriptionListener::Start &add,
                          uint32_t net ) noexcept
{
  const char * subj = add.sub.value;
  size_t       len  = add.sub.len;
  /* default interest filter: ignore _-prefixed subjects (advisories, _SNAP,
   * _TIC, and rv_cache's own subscriptions) -- they are never downstream
   * consumer interest. */
  if ( len == 0 || subj[ 0 ] == '_' )
    return;
  const char * proto = add.session.has_daemon ? "rv7" : "rv5";

  /* subscribe with refcnt > 0: set this net's forwarding bit (and the
   * total-interest edge drives the upstream omm subscription) */
  if ( add.sub.refcnt > 0 )
    this->interest_edge_set( subj, len, net );
  /* submgr already ref'd the subscription; refcnt 1 == subject went live */
  if ( add.sub.refcnt == 1 ) {
    this->stats.subscription_starts++;
    this->stats.subscriptions_active++;
  }
  this->acct_event( "subscribe", subj, len, &add.session, proto, 0,
                    NULL, 0, 0, 0 );

  /* interest asserted from a session/subscription query reply rather
   * than a live advisory (no inbox on this path): rv_cache is
   * (re)discovering listeners that predate it -- e.g. at startup.  If
   * the subject just went live and an image exists, broadcast an
   * initial so those listeners converge. */
  if ( ! add.is_listen_start && add.sub.refcnt == 1 )
    this->broadcast_initial( net, subj, len, &add.session, NULL, proto );

  /* initial-on-listen (rv5 path): the CLIENT controls this -- attaching an
   * inbox to the listen-start is the request for an initial.  No option. */
  if ( add.reply_len > 0 ) {
    CacheEntry * e = this->find_for_image( subj, len );
    void   * img;
    size_t   img_len;
    uint32_t img_enc;
    if ( e != NULL && this->cache.get_image( *e, img, img_len, img_enc ) ) {
      this->stamp_msg_type( img, img_len, img_enc,
                            (uint16_t) MD_INITIAL_TYPE );
      this->publish_msg( net, add.reply, add.reply_len, NULL, 0, img,
                         img_len, img_enc );
      e->snap_count++;
      this->acct_event( "initial", subj, len, &add.session, proto, 0,
                        NULL, 0, 0, 0 );
      this->stats.initials_sent++;
    }
    else {
      /* miss -> status to the inbox, never silence (spec 2): the
       * requester attached an inbox precisely to learn the subject's
       * state.  Interest stays registered; a later INITIAL broadcasts. */
      this->serve_miss( net, subj, len, add.reply, add.reply_len );
      this->stats.initials_not_found++;
    }
  }
}

void
RvCache::on_listen_stop( RvSubscriptionListener::Stop &rem,
                         uint32_t net ) noexcept
{
  const char * subj = rem.sub.value;
  size_t       len  = rem.sub.len;
  if ( len == 0 || subj[ 0 ] == '_' )
    return;
  if ( rem.is_orphan ) /* stop without start: nothing was subscribed */
    return;
  const char * proto  = rem.session.has_daemon ? "rv7" : "rv5";
  /* advisory stop vs session/host sweep (submgr timeout machinery) */
  const char * reason = rem.is_listen_stop ? "listen_stop" : "host_stop";
  uint32_t     now    = this->cur_mono();
  double open_secs = ( now >= rem.sub.start_mono ) ?
                     (double) ( now - rem.sub.start_mono ) : 0.0;
  CacheEntry * e = this->cache.find( subj, len );
  this->acct_event( "unsubscribe", subj, len, &rem.session, proto, 0,
                    reason, open_secs,
                    e != NULL ? e->forward_count : 0,
                    e != NULL ? e->snap_count : 0 );

  /* submgr already deref'd; refcnt 0 == last holder gone on this net:
   * clear the forwarding bit */
  if ( rem.sub.refcnt == 0 ) {
    this->interest_edge_clear( subj, len, net );
    this->stats.subscription_stops++;
    this->stats.subscriptions_active--;
    this->emit_nosubscribers( net, subj, len );
  }
}

void
RvCache::on_snapshot( RvSubscriptionListener::Snap &snp,
                      uint32_t net ) noexcept
{
  /* submgr resolved the requester's session from the reply inbox; its
   * user_id attributes the request for accounting */
  this->serve_snapshot( net, snp.sub.value, snp.sub.len, snp.reply,
                        snp.reply_len, snp.session, snp.flags );
}

void
RvCache::serve_snapshot( uint32_t net,  const char *subj,  size_t len,
                         const char *reply,  size_t reply_len,
                         const RvSessionEntry *sess,  uint16_t flags,
                         const RvSass3Entry *s3 ) noexcept
{
  if ( reply == NULL || reply_len == 0 )
    return;
  CacheEntry * e = this->find_for_image( subj, len );
  void   * img;
  size_t   img_len;
  uint32_t img_enc;
  if ( e != NULL && this->cache.get_image( *e, img, img_len, img_enc ) ) {
    /* stamp for the delivery kind: INITIAL when the requester is
     * subscribing (INITIAL_VALUES), SNAPSHOT for a plain image poll */
    bool         is_initial = ( flags & QF_INITIAL_VALUES ) != 0;
    const char * event      = is_initial ? "initial" : "snapshot";
    this->stamp_msg_type( img, img_len, img_enc, (uint16_t)
                          ( is_initial ? MD_INITIAL_TYPE : MD_SNAPSHOT_TYPE ) );
    this->publish_msg( net, reply, reply_len, NULL, 0, img,
                       img_len, img_enc );
    e->snap_count++;
    this->stats.snaps_sent++;
    this->acct_event( event, subj, len, sess,
                      s3 != NULL ? "sass3" : "snap", flags,
                      NULL, 0, 0, 0, s3 );
  }
  else {
    /* broadcast-feed miss: TRANSIENT / NOT_FOUND immediately (bcast-nack) */
    this->serve_miss( net, subj, len, reply, reply_len );
    this->stats.snaps_not_found++;
  }
}

void
RvCache::serve_miss( uint32_t net,  const char *subj,  size_t len,
                     const char *reply,  size_t reply_len ) noexcept
{
  char buf[ 1024 ];
  size_t n = this->build_status( (uint16_t) MD_TRANSIENT_TYPE,
                                 (uint16_t) MD_NOT_FOUND_STATUS,
                                 subj, len, buf, sizeof( buf ) );
  this->publish_msg( net, reply, reply_len, NULL, 0, buf, n, RVMSG_TYPE_ID );
}

void
RvCache::broadcast_initial( uint32_t net,  const char *subj,  size_t len,
                            const RvSessionEntry *sess,
                            const RvSass3Entry *s3,
                            const char *proto ) noexcept
{
  CacheEntry * e = this->find_for_image( subj, len );
  void   * img;
  size_t   img_len;
  uint32_t img_enc;
  if ( e == NULL || ! this->cache.get_image( *e, img, img_len, img_enc ) )
    return; /* cold: the feed's next INITIAL broadcasts normally */
  this->stamp_msg_type( img, img_len, img_enc, (uint16_t) MD_INITIAL_TYPE );
  this->publish_msg( net, subj, len, NULL, 0, img, img_len, img_enc );
  e->snap_count++;
  this->acct_event( "initial", subj, len, sess, proto, 0,
                    NULL, 0, 0, 0, s3 );
}

/* sass3 interest on net 2 (submgr wildcard _SASS.<feed>.SUB channel).
 * submgr owns the holder's life: it refs the subscription on a new
 * holder, derefs on UNSUBSCRIBE and on lease expiry (480s), and fires
 * this callback for each subject in the S submessage. */
void
RvCache::on_sass3( RvSubscriptionListener::Sass3 &sa3,
                   uint32_t net ) noexcept
{
  const char * subj = sa3.sub.value;
  size_t       len  = sa3.sub.len;
  if ( len == 0 || subj[ 0 ] == '_' )
    return;

  if ( ( sa3.flags & QF_UNSUBSCRIBE ) != 0 ) {
    if ( sa3.is_orphan ) /* unsubscribe without subscribe */
      return;
    /* is_asserted on an UNSUBSCRIBE == submgr lease expiry sweep */
    const char * reason = sa3.is_asserted ? "hold_timer" : "unsubscribe";
    uint32_t     now    = this->cur_mono();
    double open_secs = ( sa3.sass3.start_mono != 0 &&
                         now >= sa3.sass3.start_mono ) ?
                       (double) ( now - sa3.sass3.start_mono ) : 0.0;
    CacheEntry * e = this->cache.find( subj, len );
    this->acct_event( "unsubscribe", subj, len, NULL, "sass3", sa3.flags,
                      reason, open_secs,
                      e != NULL ? e->forward_count : 0,
                      e != NULL ? e->snap_count : 0, &sa3.sass3 );
    /* submgr already deref'd; refcnt 0 == last holder gone on this net:
     * clear the forwarding bit */
    if ( sa3.sub.refcnt == 0 ) {
      this->interest_edge_clear( subj, len, net );
      this->stats.subscription_stops++;
      this->stats.subscriptions_active--;
      this->emit_nosubscribers( net, subj, len );
    }
    return;
  }

  /* SUBSCRIBE (or a RESUBSCRIBE asserting a holder submgr didn't know):
   * submgr already ref'd; refcnt 1 == subject went live */
  if ( ( sa3.flags & QF_SUBSCRIBE ) != 0 || sa3.is_asserted ) {
    if ( sa3.sub.refcnt > 0 )
      this->interest_edge_set( subj, len, net );
    if ( sa3.sub.refcnt == 1 ) {
      this->stats.subscription_starts++;
      this->stats.subscriptions_active++;
    }
    this->acct_event( "subscribe", subj, len, NULL, "sass3", sa3.flags,
                      NULL, 0, 0, 0, &sa3.sass3 );
  }

  if ( sa3.is_asserted ) {
    /* RESUBSCRIBE renewing a holder submgr didn't know: interest that
     * predates rv_cache (startup rediscovery).  The holder already
     * believes it is subscribed -- broadcast an initial on the subject
     * so it (and every other listener) converges on the image.  The
     * REFRESH bit here was OR'd in by submgr, not asked by the client,
     * so nothing goes to the inbox. */
    if ( sa3.sub.refcnt == 1 )
      this->broadcast_initial( net, subj, len, NULL, &sa3.sass3, "sass3" );
  }
  /* image request to the inbox: SNAPSHOT (poll), INITIAL_VALUES
   * (subscribe-image) or REFRESH (ask for another image); miss ->
   * TRANSIENT/NOT_FOUND, same one-code-path as _SNAP */
  else if ( sa3.reply_len > 0 &&
       ( sa3.flags & ( QF_SNAPSHOT | QF_INITIAL_VALUES | QF_REFRESH ) ) != 0 )
    this->serve_snapshot( net, subj, len, sa3.reply, sa3.reply_len, NULL,
                          sa3.flags, &sa3.sass3 );
}

void
RvCache::on_tic_reply( RvSubscriptionListener::Tic &tic,
                       uint32_t net ) noexcept
{
  static const char dd[] = "_TIC.REPLY.SASS.DATA.DICTIONARY";
  const char * subj = tic.sub.value;
  size_t       len  = tic.sub.len;

  if ( len == sizeof( dd ) - 1 && ::memcmp( subj, dd, len ) == 0 ) {
    if ( tic.reply_len > 0 && this->cache.dict.cfile_dict != NULL ) {
      MDMsgMem mem;
      size_t   sz  = 1024 * 1024;
      void   * bp  = mem.make( sz );
      TibMsgWriter w( mem, bp, sz );
      CFile::pack_sass( this->cache.dict.cfile_dict, w );
      this->publish_msg( net, tic.reply, tic.reply_len, NULL, 0, w.buf,
                         w.off + w.hdrlen, TIBMSG_TYPE_ID );
    }
  }
}

/* ------------------------------------------------------------------ */
/* ------------------------------------------------------------------ */
/* omm nets (SPEC Milestone 4).  Feed side: the RWF envelope class
 * replaces the MSG_TYPE sniff and the payload converts to sass RVMSG at
 * ingest, then runs the normal handle_tic path -- one conversion,
 * existing cache/merge/seqno/forward machinery unchanged (field-list-
 * native caching is the later optimization; SPEC "RWF / OMM nets").
 * Provider side: OMM client streams are a third interest source; ticks
 * convert sass -> RWF once and EvOmmConn stamps per-client stream ids. */

bool
RvCache::omm_wild_match( uint32_t net,  const char *subj,
                         size_t len ) noexcept
{
  const char * w = this->omm_wild[ net - 1 ];
  if ( w == NULL || w[ 0 ] == '\0' )
    return true;
  size_t n = 0;
  while ( w[ n ] != '\0' && w[ n ] != '>' )
    n++;
  if ( w[ n ] == '>' ) /* prefix match up to '>' (RSF.> style) */
    return len >= n && ::memcmp( subj, w, n ) == 0;
  return len == n && ::memcmp( subj, w, n ) == 0;
}

bool
RvCache::on_omm_feed_msg( uint32_t net,  const char *subj,  size_t len,
                          RwfMsg &m ) noexcept
{
  bool     has_seq    = m.msg.test( X_HAS_SEQ_NUM );
  uint32_t seq_num    = (uint32_t) ( has_seq ? m.msg.seq_num : 0 );
  RwfMsg * fields     = m.get_container_msg();
  uint16_t msg_type,
           rec_status = MD_OK_STATUS;

  if ( m.msg.msg_class == REFRESH_MSG_CLASS )
    msg_type = MD_INITIAL_TYPE;
  else if ( m.msg.msg_class == STATUS_MSG_CLASS ) {
    /* CLOSED / CLOSED_RECOVER -> drop (evict); stale/suspect -> forward
     * the status, keep the image (SPEC feed-side mapping) */
    if ( m.msg.state.stream_state != STREAM_STATE_OPEN )
      msg_type = MD_DROP_TYPE;
    else
      msg_type = MD_TRANSIENT_TYPE;
    rec_status = rwf_code_to_sass_rec_status( m );
  }
  else
    msg_type = rwf_to_sass_msg_type( m );

  /* build the sass-form RVMSG: header fields first (normalize keeps
   * MSG_TYPE leading), then the field list converted through the
   * generic writer.  The payload may itself carry an embedded sass
   * header (bridged feeds): convert_msg( ..., true ) skips it and the
   * envelope-derived header above stands. */
  MDMsgMem mem;
  size_t   sz  = ( fields != NULL ? ( m.msg_end - m.msg_off ) : 0 ) + 1024;
  void   * bp  = mem.make( sz );
  RvMsgWriter w( mem, bp, sz );
  w.append_uint( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, msg_type );
  if ( has_seq )
    w.append_uint( MD_SASS_SEQ_NO, MD_SASS_SEQ_NO_LEN, seq_num );
  w.append_uint( MD_SASS_REC_STATUS, MD_SASS_REC_STATUS_LEN, rec_status );
  int status = 0;
  if ( fields != NULL )
    status = w.convert_msg( *fields, true );
  size_t out = w.update_hdr();
  if ( status != 0 || w.err != 0 ) {
    fprintf( stderr, "omm feed(net%u): convert %.*s failed (%d)\n", net,
             (int) len, subj, status != 0 ? status : w.err );
    return true;
  }
  this->handle_tic( subj, len, w.buf, out, RVMSG_TYPE_ID, true, msg_type );
  return true;
}

void
RvCache::omm_feed_ready( uint32_t net ) noexcept
{
  EvOmmClient * c = this->omm_conns[ net - 1 ];
  if ( c == NULL )
    return;
  this->omm_feeds |= (uint64_t) 1 << ( net - 1 );
  /* replay every live-interest subject eligible under the wildcard
   * (ready fires after directory+dictionary; stream ids are fresh) */
  kv::RouteLoc loc;
  CacheEntry * e = this->cache.tab.first( loc );
  bool         any = false;
  for ( ; e != NULL; e = this->cache.tab.next( loc ) ) {
    if ( e->fwd_mask != 0 &&
         this->omm_wild_match( net, e->value, e->len ) ) {
      c->subscribe( e->value, e->len );
      any = true;
    }
  }
  /* cross-socket sends: send_msg() only buffers (append_iov); the
   * writer side must be scheduled explicitly when the call originates
   * in another socket's dispatch context */
  if ( any )
    c->idle_push_write();
}

void
RvCache::omm_feed_subscribe( const char *subj,  size_t len ) noexcept
{
  for ( uint64_t m = this->omm_feeds; m != 0; m &= m - 1 ) {
    uint32_t i = kv_ffsl( m ) - 1;
    if ( this->omm_conns[ i ] != NULL &&
         this->omm_wild_match( i + 1, subj, len ) ) {
      this->omm_conns[ i ]->subscribe( subj, len );
      this->omm_conns[ i ]->idle_push_write(); /* cross-socket flush */
    }
  }
}

void
RvCache::omm_feed_unsubscribe( const char *subj,  size_t len ) noexcept
{
  for ( uint64_t m = this->omm_feeds; m != 0; m &= m - 1 ) {
    uint32_t i = kv_ffsl( m ) - 1;
    if ( this->omm_conns[ i ] != NULL &&
         this->omm_wild_match( i + 1, subj, len ) ) {
      this->omm_conns[ i ]->unsubscribe( subj, len );
      this->omm_conns[ i ]->idle_push_write(); /* cross-socket flush */
    }
  }
}

/* interest edges: the upstream omm subscription follows the TOTAL
 * fwd_mask 0 <-> nonzero transition (any sub net, any protocol) */
void
RvCache::interest_edge_set( const char *subj,  size_t len,
                            uint32_t net ) noexcept
{
  CacheEntry * ce = this->cache.find( subj, len );
  uint64_t old_mask = ( ce != NULL ? ce->fwd_mask : 0 );
  ce = this->cache.interest_set( subj, len, net );
  if ( old_mask == 0 && ce != NULL && ce->fwd_mask != 0 )
    this->omm_feed_subscribe( subj, len );
}

void
RvCache::interest_edge_clear( const char *subj,  size_t len,
                              uint32_t net ) noexcept
{
  CacheEntry * ce = this->cache.find( subj, len );
  uint64_t old_mask = ( ce != NULL ? ce->fwd_mask : 0 );
  this->cache.interest_clear( subj, len, net );
  ce = this->cache.find( subj, len );
  if ( old_mask != 0 && ( ce == NULL || ce->fwd_mask == 0 ) )
    this->omm_feed_unsubscribe( subj, len );
}

/* provider side: an OMM client's item stream open/close (NotifySub with
 * src_type 'O' from EvOmmConn::add_subj_stream) is interest like any
 * other -- same lifecycle the RV LISTEN advisories drive */
void
RvCache::on_omm_sub( NotifySub &sub,  uint32_t net ) noexcept
{
  const char * subj = sub.subject;
  size_t       len  = sub.subject_len;
  if ( len == 0 || subj[ 0 ] == '_' )
    return;
  this->interest_edge_set( subj, len, net );
  this->stats.subscription_starts++;
  this->stats.subscriptions_active++;
  this->acct_event( "subscribe", subj, len, NULL, "omm", 0,
                    NULL, 0, 0, 0 );
  /* solicited initial from the cache when an image exists; else STATUS
   * suspect/open -- interest stays registered, a later INITIAL
   * refreshes (spec 2: miss -> status, never silence) */
  CacheEntry * e = this->find_for_image( subj, len );
  void   * img;
  size_t   img_len;
  uint32_t img_enc;
  if ( e != NULL && this->cache.get_image( *e, img, img_len, img_enc ) ) {
    this->omm_forward( subj, len, img, img_len, img_enc,
                       MD_INITIAL_TYPE, e->last_seqno, true );
    e->snap_count++;
    this->stats.initials_sent++;
    this->acct_event( "initial", subj, len, NULL, "omm", 0,
                      NULL, 0, 0, 0 );
  }
  else {
    this->omm_send_status( subj, len, false );
    this->stats.initials_not_found++;
  }
}

void
RvCache::on_omm_unsub( NotifySub &sub,  uint32_t net ) noexcept
{
  const char * subj = sub.subject;
  size_t       len  = sub.subject_len;
  if ( len == 0 || subj[ 0 ] == '_' )
    return;
  this->interest_edge_clear( subj, len, net );
  if ( this->stats.subscriptions_active > 0 )
    this->stats.subscriptions_active--;
  this->stats.subscription_stops++;
  this->acct_event( "unsubscribe", subj, len, NULL, "omm", 0,
                    "stream_close", 0, 0, 0 );
}

/* convert a sass-form tick/image to one canonical RWF envelope and
 * publish it; every subscribed EvOmmConn copies it per client and
 * stamps its own stream id (sub.cpp), so this runs ONCE per tick no
 * matter how many OMM clients are attached */
void
RvCache::omm_forward( const char *subj,  size_t len,  const void *msg,
                      size_t msg_len,  uint32_t enc,  uint16_t msg_type,
                      uint32_t seqno,  bool solicited ) noexcept
{
  if ( this->omm_dict == NULL || this->omm_dict->rdm_dict == NULL ||
       this->omm_listener == NULL )
    return;
  if ( msg_type == MD_TRANSIENT_TYPE || msg_type == MD_DROP_TYPE ) {
    this->omm_send_status( subj, len, msg_type == MD_DROP_TYPE );
    return;
  }
  MDMsgMem mem;
  MDMsg * m = MDMsg::unpack( (void *) msg, 0, msg_len, enc,
                             this->cache.dict.dict, mem );
  if ( m == NULL )
    return;
  bool is_refresh = ( msg_type == MD_INITIAL_TYPE ||
                      msg_type == MD_SNAPSHOT_TYPE ||
                      msg_type == MD_VERIFY_TYPE );
  uint32_t h  = kv_crc_c( subj, len, 0 );
  size_t   sz = msg_len + 1024;
  void   * bp = mem.make( sz );
  RwfMsgWriter em( mem, this->omm_dict->rdm_dict, bp, sz,
                   is_refresh ? REFRESH_MSG_CLASS : UPDATE_MSG_CLASS,
                   MARKET_PRICE_DOMAIN, h );
  RwfFieldListWriter * fl;
  if ( is_refresh ) {
    if ( solicited )
      em.set( X_CLEAR_CACHE, X_SOLICITED, X_REFRESH_COMPLETE );
    else
      em.set( X_CLEAR_CACHE, X_REFRESH_COMPLETE );
    em.add_seq_num( seqno )
      .add_state( DATA_STATE_OK, STREAM_STATE_OPEN )
      .add_msg_key()
        .service_id( this->cfg.omm_service_id )
        .name( subj, len )
        .name_type( NAME_TYPE_RIC )
      .end_msg_key();
    fl = &em.add_field_list();
  }
  else {
    em.add_seq_num( seqno )
      .add_msg_key()
        .service_id( this->cfg.omm_service_id )
        .name( subj, len )
        .name_type( NAME_TYPE_RIC )
      .end_msg_key();
    fl = &em.add_update( UPD_TYPE_QUOTE )
            .add_field_list();
  }
  /* payload: the sass fields minus the sass header (skip_hdr); the
   * convert belongs to the FIELD LIST writer (the envelope writer's
   * convert_msg is the unimplemented base = NO_MSG_IMPL) */
  int status = fl->convert_msg( *m, true );
  size_t off = em.end_msg();
  if ( status != 0 || em.err != 0 ) {
    fprintf( stderr, "omm fwd %.*s: convert failed (%d)\n",
             (int) len, subj, status != 0 ? status : em.err );
    return;
  }
  EvPublish pub( subj, len, NULL, 0, em.buf, off,
                 this->poll.sub_route, *this->omm_listener, h,
                 RWF_MSG_TYPE_ID );
  this->poll.sub_route.forward_msg( pub, NULL );
}

/* miss / drop status to OMM streams: suspect+open keeps the stream
 * (image may arrive later, mirroring rv5 interest-stays semantics);
 * closed tears it down (instrument death) */
void
RvCache::omm_send_status( const char *subj,  size_t len,
                          bool closed ) noexcept
{
  if ( this->omm_dict == NULL || this->omm_dict->rdm_dict == NULL ||
       this->omm_listener == NULL )
    return;
  MDMsgMem mem;
  size_t   sz = 1024;
  void   * bp = mem.make( sz );
  uint32_t h  = kv_crc_c( subj, len, 0 );
  RwfMsgWriter em( mem, this->omm_dict->rdm_dict, bp, sz,
                   STATUS_MSG_CLASS, MARKET_PRICE_DOMAIN, h );
  em.add_state( DATA_STATE_SUSPECT,
                closed ? STREAM_STATE_CLOSED : STREAM_STATE_OPEN )
    .add_msg_key()
      .service_id( this->cfg.omm_service_id )
      .name( subj, len )
      .name_type( NAME_TYPE_RIC )
    .end_msg_key();
  size_t off = em.end_msg();
  if ( em.err != 0 )
    return;
  EvPublish pub( subj, len, NULL, 0, em.buf, off,
                 this->poll.sub_route, *this->omm_listener, h,
                 RWF_MSG_TYPE_ID );
  this->poll.sub_route.forward_msg( pub, NULL );
}

/* ------------------------------------------------------------------ */
void
RvCache::on_timer( void ) noexcept
{
  /* eviction cache entry expiry */
  if ( this->cfg.message_eviction_secs > 0 ) {
    uint64_t cutoff = this->now_ns() -
                      (uint64_t) this->cfg.message_eviction_secs * 1000000000ULL;
    RouteLoc loc;
    for ( CacheEntry * e = this->cache.tab.first( loc ); e != NULL; ) {
      if ( e->last_update_ns != 0 && e->last_update_ns < cutoff ) {
        char tmp[ 512 ];
        size_t l = e->len < sizeof( tmp ) ? e->len : sizeof( tmp ) - 1;
        ::memcpy( tmp, e->value, l );
        CacheEntry * nxt = this->cache.tab.next( loc );
        this->cache.evict( tmp, l );
        this->stats.msgs_evicted++;
        e = nxt;
        continue;
      }
      e = this->cache.tab.next( loc );
    }
  }

  if ( ! this->cfg.quiet )
    this->print_stats( false );
}

void
RvCache::print_stats( bool final_totals ) noexcept
{
#if 0
  size_t subj = this->cache.count();
  printf( "%s cached=%zu img=%lluB held=%zu holders=%zu | "
          "in=%llu fwd=%llu drop=%llu | snap=%llu/%llu | "
          "open=%llu close=%llu evict=%llu nosub=%llu | seq[reg=%llu gap=%llu] "
          "tr=%llu",
#endif
  Stats & cur = this->stats, & old = this->old;
  uint64_t diff_ns = this->poll.now_ns - cur.log_ns;
  if ( ! final_totals ) {
    if ( this->poll.now_ns < cur.log_ns ) {
      cur.log_ns = this->poll.now_ns;
      return;
    }
    if ( diff_ns < 10e9 )
      return;
    old.log_ns = cur.log_ns;
    cur.log_ns = this->poll.now_ns;
  }
  uint64_t cache_cnt = 0, cache_byt = 0, mem_info = 0,
           msgs_recv = 0, msgs_sent = 0, bytes_recv = 0, bytes_sent = 0,
           msgs_fwd = 0, msgs_trans = 0, msgs_norte = 0, ini_sent = 0,
           ini_notfd = 0, snap_sent = 0, snap_notf = 0, sub_count = 0,
           sub_start = 0, sub_stop = 0, msgs_evict = 0, seq_regre = 0,
           seq_gap = 0, user_cpu_us = 0, sys_cpu_us = 0;
  bool b = false;
  size_t n = 0;
  char hdr[ 3 ][ 80 ], sta[ 3 ][ 80 ], mbuf[ 16 ];

  this->stats.cache_msg_count = this->cache.tab.pop_count();
  this->stats.cache_msg_bytes = this->cache.image_bytes;

#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
  struct mallinfo2 mi = mallinfo2();
#else
  struct mallinfo mi = mallinfo();
#endif
  this->stats.heap_mem_info = mi.arena;
  struct rusage ru;
  ::getrusage( RUSAGE_SELF, &ru );
  this->stats.user_cpu_usecs = ru.ru_utime.tv_sec * 1e6 + ru.ru_utime.tv_usec;
  this->stats.sys_cpu_usecs  = ru.ru_stime.tv_sec * 1e6 + ru.ru_stime.tv_usec;

#define P( S, X ) if ( final_totals ) { X = cur.S; b = true; } else if ( cur.S > old.S ) { X = cur.S - old.S; old.S = cur.S; b = true; }
#define T( S, X ) if ( (X = cur.S) != old.S ) { old.S = cur.S; b = true; }
#define S( F ) ::snprintf( &hdr[ n ][ o ], 80 - o, "%11s", #F )
#define X( F ) S( F ); ::snprintf( &sta[ n ][ o ], 80 - o, "%11lld", (long long) F ); o += 11
#define F( F ) S( F ); ::snprintf( &sta[ n ][ o ], 80 - o, "%11.3f", F ); o += 11
#define K( F ) S( F ); ::snprintf( &sta[ n ][ o ], 80 - o, "%11s", mem_to_string( F, mbuf, 1024 ) ); o += 11
  P( msgs_recv, msgs_recv );
  P( msgs_sent, msgs_sent );
  P( bytes_recv, bytes_recv );
  P( bytes_sent, bytes_sent );
  P( msgs_forwarded, msgs_fwd );
  P( msgs_transient_fwd, msgs_trans );
  P( msgs_no_listener, msgs_norte );
  if ( b || final_totals ) {
    size_t o = 0;
    X( msgs_recv ); X( msgs_sent ); K( bytes_recv ); K( bytes_sent ); X( msgs_fwd ); X( msgs_trans ); X( msgs_norte ); n++; b = false;
  }
  P( initials_sent, ini_sent );
  P( initials_not_found, ini_notfd );
  P( snaps_sent, snap_sent );
  P( snaps_not_found, snap_notf );
  P( subscription_starts, sub_start );
  P( subscription_stops, sub_stop );
  P( sequence_regress, seq_regre );
  P( sequence_gap, seq_gap );
  seq_gap += seq_regre;
  if ( b || final_totals ) {
    size_t o = 0;
    X( ini_sent ); X( ini_notfd ); X( snap_sent ); X( snap_notf ); X( sub_start ); X( sub_stop ); X( seq_gap ); n++; b = false;
  }

  T( cache_msg_count, cache_cnt );
  T( cache_msg_bytes, cache_byt );
  T( heap_mem_info, mem_info );
  P( user_cpu_usecs, user_cpu_us );
  P( sys_cpu_usecs, sys_cpu_us );
  T( subscriptions_active, sub_count );
  P( msgs_evicted, msgs_evict );
  double diff_us = diff_ns / 1000.0;
  if ( b || final_totals ) {
    size_t o = 0;
    double user_cpu = (double) user_cpu_us * 100.0 / diff_us,
           sys_cpu  = (double) sys_cpu_us * 100.0  / diff_us;
    X( cache_cnt ); K( cache_byt ); K( mem_info ); F( user_cpu ); F( sys_cpu ); X( sub_count ); X( msgs_evict ); n++; b = false;
  }
#undef P
#undef T
#undef S
#undef X
#undef F
#undef K
  if ( n != 0 ) {
    char sbuf[ 64 ];
    fprintf( stdout, "%s %s interval %.3fs:\n", timestamp( this->poll.now_ns, 3, sbuf, sizeof( sbuf ) ), final_totals ? "Final" : "Stats",
             diff_us / 1000000.0 );
    for ( size_t i = 0; i < n; i++ ) {
      fputs( hdr[ i ], stdout ); fputs( "\n", stdout ); fputs( sta[ i ], stdout ); fputs( "\n", stdout );
    }
    printf( "\n" );
    fflush( stdout );
  }
#if 0
  for ( uint32_t i = 0; i < MAX_NETS; i++ ) {
    RvSubscriptionDB * db = this->sub_dbs[ i ];
    if ( db != NULL )
      printf( " | net%u.sub[a=%u r=%u] hosts=%u sess=%u", i + 1,
        db->subscriptions.active, db->subscriptions.removed,
        db->hosts.active, db->sessions.active );
  }
#endif
}

/* ------------------------------------------------------------------ */
void
RvCache::acct_event( const char *event,  const char *subj,  size_t sublen,
                     const RvSessionEntry *sess,  const char *proto,
                     uint16_t query_flags,  const char *reason,
                     double open_secs,  uint64_t msgs,
                     uint64_t images,  const RvSass3Entry *s3 ) noexcept
{
  if ( this->acct == NULL )
    return;
  char ts[ 64 ];
  struct timespec tp;
  clock_gettime( CLOCK_REALTIME, &tp );
  struct tm tm;
  gmtime_r( &tp.tv_sec, &tm );
  ::snprintf( ts, sizeof( ts ), "%04d-%02d-%02dT%02d:%02d:%02d.%06uZ",
    tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
    tm.tm_sec, (unsigned) ( ( tp.tv_nsec / 1000 ) % 1000000 ) );

  fprintf( this->acct, "{\"ts\":\"%s\",\"event\":\"%s\"", ts, event );
  if ( subj != NULL )
    fprintf( this->acct, ",\"subject\":\"%.*s\"", (int) sublen, subj );
  if ( sess != NULL ) {
    /* user identity: submgr captures RvSessionEntry::user_id from
     * SESSION.START's userid field / subscription-query replies */
    if ( sess->user_id != NULL )
      fprintf( this->acct, ",\"user\":\"%s\"", sess->user_id );
    else
      fprintf( this->acct, ",\"user\":null" );
    char hostip[ 32 ];
    ::snprintf( hostip, sizeof( hostip ), "%u.%u.%u.%u",
      ( sess->host_id >> 24 ) & 0xff, ( sess->host_id >> 16 ) & 0xff,
      ( sess->host_id >> 8 ) & 0xff, sess->host_id & 0xff );
    fprintf( this->acct, ",\"host\":\"%s\",\"host_id\":\"%08x\"",
             hostip, sess->host_id );
    fprintf( this->acct, ",\"session\":\"%.*s\"",
             (int) sess->len, sess->value );
  }
  if ( s3 != NULL ) {
    /* sass3 holder identity from the SUB accounting submessage {U,H,A,P} */
    fprintf( this->acct,
             ",\"user\":\"%s\",\"host\":\"%s\",\"app\":\"%s\",\"pid\":%u",
             s3->user_id, s3->host, s3->app, s3->pid );
  }
  if ( proto != NULL )
    fprintf( this->acct, ",\"protocol\":\"%s\",\"query_flags\":%u",
             proto, query_flags );
  if ( reason != NULL )
    fprintf( this->acct, ",\"reason\":\"%s\",\"open_secs\":%.1f,"
             "\"msgs\":%llu,\"images\":%llu", reason, open_secs,
             (unsigned long long) msgs, (unsigned long long) images );
  fprintf( this->acct, "}\n" );
  fflush( this->acct );
}

/* ================================================================== */
/* feed net callback (any number of feed networks feed the one cache) */
struct FeedCB : public EvConnectionNotify, public RvClientCB {
  EvPoll     & poll;
  EvRvClient & client;
  RvCache    & cache;
  const char * wildcard; /* NULL = full firehose */
  char         label[ 24 ];
  bool         s2,       /* _TIC.> tick consumer */
               s3;       /* _SASS.<feed>.PUB envelope consumer */
  FeedCB( EvPoll &p,  EvRvClient &c,  RvCache &rc,  uint32_t idx,
          const char *wild,  bool sass2,  bool sass3 )
    : poll( p ), client( c ), cache( rc ), wildcard( wild ), s2( sass2 ),
      s3( sass3 ) {
    ::snprintf( this->label, sizeof( this->label ), "feed(net%u)", idx );
  }
  virtual void on_connect( EvSocket &conn ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "%s connected: %.*s\n", this->label, len,
            conn.peer_address.buf );
    char   sub[ 1024 ];
    int    slen;
    size_t wlen = ( this->wildcard == NULL ? 0 : ::strlen( this->wildcard ) );
    if ( this->s2 ) { /* _TIC.> firehose or _TIC.<wild>.> */
      if ( this->wildcard == NULL )
        slen = ::snprintf( sub, sizeof( sub ), "_TIC.>" );
      else {
        bool has_gt = ( wlen > 0 && this->wildcard[ wlen - 1 ] == '>' );
        slen = ::snprintf( sub, sizeof( sub ), "_TIC.%s%s", this->wildcard,
                           has_gt ? "" : ".>" );
      }
      printf( "%s subscribe: %s\n", this->label, sub );
      this->client.subscribe( sub, slen );
    }
    if ( this->s3 ) { /* _SASS.<wild>.PUB, or _SASS.> filtered on .PUB */
      if ( this->wildcard == NULL )
        slen = ::snprintf( sub, sizeof( sub ), "_SASS.>" );
      else {
        /* strip a trailing ".>" / ">" from the wildcard: PUB wants the
         * bare feed name */
        while ( wlen > 0 && ( this->wildcard[ wlen - 1 ] == '>' ||
                              this->wildcard[ wlen - 1 ] == '.' ) )
          wlen--;
        slen = ::snprintf( sub, sizeof( sub ), "_SASS.%.*s.PUB",
                           (int) wlen, this->wildcard );
      }
      printf( "%s subscribe: %s\n", this->label, sub );
      this->client.subscribe( sub, slen );
    }
    fflush( stdout );
  }
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t errlen ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "%s shutdown: %.*s %.*s\n", this->label, len,
            conn.peer_address.buf, (int) errlen, err );
    if ( this->poll.quit == 0 )
      this->poll.quit = 1;
  }
  virtual bool on_rv_msg( EvPublish &pub ) noexcept {
    if ( this->s3 && pub.subject_len > 6 &&
         ::memcmp( pub.subject, "_SASS.", 6 ) == 0 )
      this->cache.on_sass3_feed_msg( pub );
    else
      this->cache.on_feed_msg( pub );
    return true;
  }
};

/* sub net callback (any number).  The sass2 and sass3 interest channels
 * differ ONLY in the start_subscriptions() enables; one submgr instance
 * per sub network, each with its own sub_tab/refcnts.  net = mask bit. */
struct SubCB : public EvConnectionNotify, public RvClientCB,
               public EvTimerCallback, public RvSubscriptionListener {
  EvPoll         & poll;
  EvRvClient     & client;
  RvCache        & cache;
  RvSubscriptionDB sub_db;
  uint32_t         net;     /* fwd_mask bit == idx - 1 */
  const char     * wildcard;/* per-net submgr filter (NULL = none) */
  char             label[ 24 ];
  bool             s2, s3,  /* interest channels to enable */
                   primary; /* runs the cache timer (once per process) */
  SubCB( EvPoll &p,  EvRvClient &c,  RvCache &rc,  uint32_t idx,
         bool sass2,  bool sass3,  bool prim,  const char *wild )
    : poll( p ), client( c ), cache( rc ), sub_db( c, this ),
      net( idx - 1 ), wildcard( wild ), s2( sass2 ), s3( sass3 ),
      primary( prim ) {
    ::snprintf( this->label, sizeof( this->label ), "sub(net%u%s%s)", idx,
                sass2 ? ",s2" : "", sass3 ? ",s3" : "" );
  }

  virtual void on_connect( EvSocket &conn ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "%s connected: %.*s\n", this->label, len,
            conn.peer_address.buf );
    fflush( stdout );
    bool all = ( this->wildcard == NULL );
    if ( this->wildcard != NULL ) /* per-net filter, sass2 and sass3 */
      this->sub_db.add_wildcard( this->wildcard );
    this->sub_db.start_subscriptions( all, this->s2, this->s3, true );
    this->poll.timer.add_timer_millis( *this, 1000, 1, 0 );
  }
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t errlen ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "%s shutdown: %.*s %.*s\n", this->label, len,
            conn.peer_address.buf, (int) errlen, err );
    if ( this->poll.quit == 0 )
      this->poll.quit = 1;
  }
  virtual bool on_rv_msg( EvPublish &pub ) noexcept {
    this->sub_db.process_pub( pub );
    return true;
  }
  virtual bool timer_cb( uint64_t,  uint64_t ) noexcept {
    this->sub_db.process_events();
    if ( this->primary )
      this->cache.on_timer();
    return true;
  }
  virtual void on_listen_start( Start &add ) noexcept {
    this->cache.on_listen_start( add, this->net );
  }
  virtual void on_listen_stop( Stop &rem ) noexcept {
    this->cache.on_listen_stop( rem, this->net );
  }
  virtual void on_snapshot( Snap &snp ) noexcept {
    this->cache.on_snapshot( snp, this->net );
  }
  virtual void on_sass3( Sass3 &sa3 ) noexcept {
    this->cache.on_sass3( sa3, this->net );
  }
  virtual void on_tic_reply( Tic &tic ) noexcept {
    this->cache.on_tic_reply( tic, this->net );
  }
};

/* omm feed net: connection lifecycle + inbound RWF messages.  ready
 * fires after login+directory+dictionary resolve (interest replays
 * there, never at TCP connect); loss is fail-fast like the rv nets */
struct OmmFeedCB : public EvConnectionNotify, public OmmClientCB {
  EvPoll      & poll;
  EvOmmClient & client;
  RvCache     & cache;
  uint32_t      net;

  OmmFeedCB( EvPoll &p,  EvOmmClient &c,  RvCache &rc,  uint32_t idx )
    : poll( p ), client( c ), cache( rc ), net( idx ) {}

  virtual void on_connect( EvSocket &conn ) noexcept {
    printf( "omm feed(net%u) ready: %.*s\n", this->net,
            (int) conn.get_peer_address_strlen(), conn.peer_address.buf );
    this->cache.omm_feed_ready( this->net );
  }
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t errlen ) noexcept {
    fprintf( stderr, "omm feed(net%u) shutdown: %.*s %.*s\n", this->net,
             (int) conn.get_peer_address_strlen(), conn.peer_address.buf,
             (int) errlen, err != NULL ? err : "" );
    if ( this->poll.quit == 0 )
      this->poll.quit = 1; /* fail-fast, like the rv nets */
  }
  virtual bool on_omm_msg( const char *sub,  size_t sub_len,  uint32_t,
                           RwfMsg &msg ) noexcept {
    return this->cache.on_omm_feed_msg( this->net, sub, sub_len, msg );
  }
};

/* omm provider net: an OMM client's item stream open/close surfaces as
 * NotifySub (src_type 'O', EvOmmConn::add_subj_stream) on the
 * listener's sub_route; everything else on that route is ignored */
struct OmmSubNotify : public RouteNotify {
  RvCache & cache;
  uint32_t  net;

  OmmSubNotify( RoutePublish &sr,  RvCache &rc,  uint32_t idx )
    : RouteNotify( sr ), cache( rc ), net( idx ) {}

  virtual void on_sub( NotifySub &sub ) noexcept {
    if ( sub.src_type == 'O' )
      this->cache.on_omm_sub( sub, this->net );
  }
  virtual void on_unsub( NotifySub &sub ) noexcept {
    if ( sub.src_type == 'O' )
      this->cache.on_omm_unsub( sub, this->net );
  }
};

/* announce the cache's service in a provider-side source directory:
 * build the RWF directory map exactly the way a wire directory response
 * looks and run it through update_source_map(), which constructs the
 * OmmSource AND the sector routes subject matching resolves against
 * (TestPublish::add_test_source is the model; add_source() alone does
 * not build sector routes) */
static bool
announce_cache_service( OmmSourceDB &db,  MDDict *rdm_dict,
                        const char *svc,  uint32_t service_id ) noexcept
{
  static const char * dict_nm[ 2 ] = { "RWFFld", "RWFEnum" };
  static uint8_t cap[ 2 ] = { SOURCE_DOMAIN, MARKET_PRICE_DOMAIN };
  static RwfQos  qos      = { QOS_TIME_REALTIME, QOS_RATE_TICK_BY_TICK,
                              0, 0, 0 };
  char         buf[ 1024 ];
  MDMsgMem     mem;
  RwfMapWriter map( mem, rdm_dict, buf, sizeof( buf ) );
  RwfState     state = { STREAM_STATE_OPEN, DATA_STATE_OK, 0, { "OK", 2 } };

  RwfFilterListWriter
    & fil = map.add_filter_list( MAP_ADD_ENTRY, service_id, MD_UINT );
  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_INFO_ID )
     .append_string( NAME        , svc )
     .append_string( VEND        , "rvcache" )
     .append_uint  ( IS_SRC      , 1 )
     .append_array ( CAPAB       , cap , 2, MD_UINT )
     .append_array ( DICT_PROV   , dict_nm, 2 )
     .append_array ( DICT_USED   , dict_nm, 2 )
     .append_array ( QOS         , &qos, 1 )
     .append_uint  ( SUP_QOS_RNG , 0 )
     .append_string( ITEM_LST    , "_ITEM_LIST" )
     .append_uint  ( SUP_OOB_SNAP, 1 )
     .append_uint  ( ACC_CONS_STA, 0 )
   .end_element_list();
  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_STATE_ID )
     .append_uint  ( SVC_STATE, 1 )
     .append_uint  ( ACC_REQ  , 1 )
     .append_state ( STAT     , state )
   .end_element_list();
  map.end_map();
  if ( map.err != 0 )
    return false;
  RwfMsg * m = RwfMsg::unpack_map( map.buf, 0, map.off, RWF_MAP_TYPE_ID,
                                   NULL, mem );
  if ( m == NULL )
    return false;
  db.update_source_map( kv::current_realtime_ns(), *m );
  return true;
}

/* ------------------------------------------------------------------ */
static const char *
get_arg( int &x,  int argc,  const char *argv[],  int b,  const char *f,
         const char *def ) noexcept
{
  for ( int i = 1; i < argc - b; i++ ) {
    if ( ::strcmp( f, argv[ i ] ) == 0 ) {
      if ( x < i + b + 1 )
        x = i + b + 1;
      return argv[ i + b ];
    }
  }
  return def;
}

int
main( int argc,  const char *argv[] )
{
  SignalHandler sighndl;
  Config cfg;
  int x = 1;

  const char * map    = get_arg( x, argc, argv, 1, "-m", NULL ),
             * path   = get_arg( x, argc, argv, 1, "-p", ::getenv( "cfile_path" ) ),
             * daemon = get_arg( x, argc, argv, 1, "-d", NULL ),
             * network= get_arg( x, argc, argv, 1, "-n", NULL ),
             * service= get_arg( x, argc, argv, 1, "-s", NULL ),
             * cfile  = get_arg( x, argc, argv, 1, "-c", NULL ),
             * seqm   = get_arg( x, argc, argv, 1, "-Q", NULL ),
             * evict  = get_arg( x, argc, argv, 1, "-x", NULL ),
             * pend   = get_arg( x, argc, argv, 1, "-P", NULL ),
             * acctf  = get_arg( x, argc, argv, 1, "-A", NULL ),
             * replace= get_arg( x, argc, argv, 0, "-r", NULL ),
             * routem = get_arg( x, argc, argv, 0, "-M", NULL ),
             * quiet  = get_arg( x, argc, argv, 0, "-q", NULL ),
             * verb   = get_arg( x, argc, argv, 0, "-v", NULL ),
             * help   = get_arg( x, argc, argv, 0, "-h", NULL );

  if ( help != NULL ) {
    fprintf( stderr,
      "rv_cache [-d daemon] [-n network] [-s service] (defaults)\n"
      "  [-<idx> role proto[ daemon[ network[ service[ wildcard]]]]] net\n"
      "  [-p path]             = dictionary search path\n"
      "  [-c file]             = json/yaml config (.yaml/.yml = yaml)\n"
      "  [-m map_name]         = shm name to cache msgs\n"
      "  [-r]                  = replace typeless msgs\n"
      "  [-Q obs|strict|stamp] = message sequence policy\n"
      "  [-M]                  = route-after-merge\n"
      "  [-x secs]             = eviction expiry\n"
      "  [-P secs]             = pending timeout (10)\n"
      "  [-A file]             = accounting jsonl (- stdout)\n"
      "  [-q]                  = quiet stats\n"
      "  [-v]                  = verbose submgr log\n"
      "\n"
      "example:\n"
      "rv_cache -1 sub sass2 tcp:7500 'eth0;227.5.0.0' 7500 'RSF.>' \\\n"
      "         -2 feed sass2 tcp:7600 'eth1;227.6.0.0' 7600 'RSF.>' \\\n"
      "         -c cache.yaml \\\n"
      "         -m sysv:raikv.shm \\\n"
      "         -A subscript.log\n" );
    return 1;
  }

  /* -<idx> role proto [d [n [s [wild]]]] net attachments, argv-separated
   * (network configs contain commas); fields run to the next -flag */
  for ( int i = 1; i < argc - 1; i++ ) {
    const char * a = argv[ i ];
    int j;
    if ( a[ 0 ] != '-' || a[ 1 ] < '0' || a[ 1 ] > '9' )
      continue;
    char * end = NULL;
    long   idx = ::strtol( &a[ 1 ], &end, 10 );
    if ( *end != '\0' || idx < 1 || idx > (long) MAX_NETS ) {
      fprintf( stderr, "bad net index: %s (1..%u)\n", a, MAX_NETS );
      return 1;
    }
    for ( j = 0; j + i + 1 < argc; j++ ) {
      if ( argv[ j + i + 1 ][ 0 ] == '-' )
        break;
    }
    if ( j == 0 ) {
      fprintf( stderr, "zero net config: %s\n", a );
      return 1;
    }
    NetDef nd;
    if ( ! parse_net_tuple( (uint32_t) idx, &argv[ i + 1 ], (uint32_t) j, nd ) ) {
      fprintf( stderr,
        "bad net tuple: %s %s (want role proto[ daemon[ network[ service [wild]]]],"
        " role feed|sub, proto sass2|sass3|both)\n", a, argv[ i + 1 ] );
      return 1;
    }
    cfg.nets.push( nd );
    if ( x < i + 2 )
      x = i + 2;
  }
  if ( cfile != NULL && ! load_config( cfile, cfg ) )
    return 1;
  /* explicit CLI values override config-file values */
  if ( map     != NULL ) cfg.map_name     = map;
  if ( path    != NULL ) cfg.dict_path    = path;
  if ( daemon  != NULL ) cfg.base.daemon  = daemon;
  if ( network != NULL ) cfg.base.network = network;
  if ( service != NULL ) cfg.base.service = service;
  if ( replace != NULL ) cfg.replace_typeless_msgs = true;
  if ( routem  != NULL ) cfg.route_after_merge = true;
  if ( quiet   != NULL ) cfg.quiet   = true;
  if ( verb    != NULL ) cfg.verbose = true;
  if ( evict   != NULL ) cfg.message_eviction_secs = (uint32_t) atoi( evict );
  if ( pend    != NULL ) cfg.pending_initial_secs  = (uint32_t) atoi( pend );
  if ( acctf   != NULL ) cfg.accounting_file = acctf;
  if ( seqm    != NULL ) cfg.sequence_policy = parse_seq( seqm );
  /* default topology when no nets are declared */
  if ( cfg.nets.count == 0 ) {
    NetDef f, s;
    f.idx = 1; f.is_feed = true;  f.s2 = true; f.s3 = false;
    s.idx = 2; s.is_feed = false; s.s2 = true; s.s3 = true;
    cfg.nets.push( f );
    cfg.nets.push( s );
  }
  /* validate: unique indexes, at least one sub */
  {
    uint64_t seen = 0;
    bool     have_sub = false;
    for ( size_t i = 0; i < cfg.nets.count; i++ ) {
      NetDef &nd = cfg.nets.ptr[ i ];
      uint64_t bit = (uint64_t) 1 << ( nd.idx - 1 );
      if ( ( seen & bit ) != 0 ) {
        fprintf( stderr, "duplicate net index %u\n", nd.idx );
        return 1;
      }
      seen |= bit;
      if ( ! nd.is_feed )
        have_sub = true;
    }
    if ( ! have_sub ) {
      fprintf( stderr, "no sub network declared\n" );
      return 1;
    }
  }
  MDMsgDict dict;
  dict.load( cfg.dict_path );

  /* omm nets need the RDM dictionary (fname <-> fid is not optional for
   * the sass <-> RWF conversion); refuse a fid-blind omm net (SPEC M4) */
  OmmDict omm_dict;
  bool    have_omm = false;
  for ( size_t i = 0; i < cfg.nets.count; i++ )
    if ( cfg.nets.ptr[ i ].omm )
      have_omm = true;
  if ( have_omm ) {
    if ( cfg.dict_path != NULL )
      omm_dict.load_cfiles( cfg.dict_path );
    if ( omm_dict.rdm_dict == NULL ) {
      fprintf( stderr, "omm nets require an RDM dictionary (-p path)\n" );
      return 1;
    }
  }

  EvShm  shm( "rv_cache" );
  if ( shm.open( cfg.map_name, 0 ) != 0 )
    return 1;

  EvPoll poll;
  poll.init( 5, false );

  RvCache cache( poll, shm, dict, cfg );
  if ( have_omm ) {
    cache.omm_dict = &omm_dict;
    if ( cfg.verbose )
      omm_debug = 1; /* omm lib dispatch traces with -v */
  }
  if ( cfg.accounting_file != NULL ) {
    if ( ::strcmp( cfg.accounting_file, "-" ) == 0 )
      cache.acct = stdout;
    else {
      cache.acct = ::fopen( cfg.accounting_file, "a" );
      if ( cache.acct == NULL )
        perror( cfg.accounting_file );
    }
  }

  /* build every declared network attachment; heap-allocate since the
   * count is config-driven.  process-lifetime objects, freed on exit. */
  MDOutput mout;
  bool     first_sub = true;
  for ( size_t i = 0; i < cfg.nets.count; i++ ) {
    NetDef     & nd = cfg.nets.ptr[ i ];
    const char * d, * n, * s;
    cfg.resolve( nd, d, n, s );
    if ( nd.omm ) {
      if ( nd.is_feed ) {
        /* daemon = provider host[:port] (default 14002) */
        char host[ 256 ];
        int  port = 14002;
        const char * hp  = ( d != NULL ? d : "127.0.0.1" );
        const char * col = ::strrchr( hp, ':' );
        size_t       hl  = ( col != NULL ? (size_t) ( col - hp )
                                         : ::strlen( hp ) );
        if ( hl == 0 ) { hp = "127.0.0.1"; hl = 9; }
        if ( hl >= sizeof( host ) )
          hl = sizeof( host ) - 1;
        ::memcpy( host, hp, hl );
        host[ hl ] = '\0';
        if ( col != NULL )
          port = atoi( col + 1 );
        OmmSourceDB * db = new ( ::malloc( sizeof( OmmSourceDB ) ) )
                           OmmSourceDB();
        EvOmmClient * oc = new ( aligned_malloc( sizeof( EvOmmClient ) ) )
                           EvOmmClient( poll, omm_dict, *db );
        OmmFeedCB   * ocb = new ( aligned_malloc( sizeof( OmmFeedCB ) ) )
                            OmmFeedCB( poll, *oc, cache, nd.idx );
        /* login attrs need non-NULL defaults: the login request's msg
         * key name IS the user name (NULL = strlen crash in the key
         * writer) */
        const char * ouser = cfg.omm_user != NULL ? cfg.omm_user
                                                  : "rv_cache";
        const char * oapp  = cfg.omm_app_name != NULL ? cfg.omm_app_name
                                                      : "rv_cache";
        const char * oaid  = cfg.omm_app_id != NULL ? cfg.omm_app_id
                                                    : "256";
        EvOmmClientParameters oparm( host, oapp, oaid, ouser, NULL,
                                     cfg.omm_instance_id, cfg.omm_token,
                                     port );
        /* local RDM dictionary is mandatory for omm nets (guard above),
         * so never download one from the provider (SPEC M4) */
        oc->no_dictionary = true;
        if ( ! oc->omm_connect( oparm, ocb, ocb ) ) {
          fprintf( stderr, "Failed to connect net %u (omm feed)\n",
                   nd.idx );
          return 1;
        }
        cache.omm_conns[ nd.idx - 1 ] = oc;
        cache.omm_wild[ nd.idx - 1 ]  = nd.wildcard;
      }
      else {
        /* daemon = listen [host:]port; service = announced name */
        int port = 14002;
        if ( d != NULL ) {
          const char * lp = ::strrchr( d, ':' );
          port = atoi( lp != NULL ? lp + 1 : d );
        }
        OmmSourceDB * db = new ( ::malloc( sizeof( OmmSourceDB ) ) )
                           OmmSourceDB();
        EvOmmListen * ol = new ( aligned_malloc( sizeof( EvOmmListen ) ) )
                           EvOmmListen( poll, omm_dict, *db );
        if ( ol->listen( NULL, port, DEFAULT_TCP_LISTEN_OPTS ) != 0 ) {
          fprintf( stderr, "Failed to listen net %u (omm, port %d)\n",
                   nd.idx, port );
          return 1;
        }
        /* announce the cache's one service in the source directory
         * (directory-map path: builds the sector routes matching uses) */
        const char * svc = ( s != NULL ? s : "RVCACHE" );
        if ( ! announce_cache_service( *db, omm_dict.rdm_dict, svc,
                                       cfg.omm_service_id ) ) {
          fprintf( stderr, "net %u: omm service announce failed\n",
                   nd.idx );
          return 1;
        }
        OmmSubNotify * osn = new ( ::malloc( sizeof( OmmSubNotify ) ) )
                             OmmSubNotify( poll.sub_route, cache, nd.idx );
        poll.sub_route.add_route_notify( *osn );
        cache.omm_subs |= (uint64_t) 1 << ( nd.idx - 1 );
        cache.omm_listener = ol;
      }
      continue;
    }
    char user[ 32 ];
    ::snprintf( user, sizeof( user ), "rv_cache_net%u", nd.idx );
    EvRvClientParameters parm( d, n, s, ::strdup( user ), 0 );
    /* EvConnection carries 64-byte-aligned buffers; plain malloc's 16
     * is not enough */
    EvRvClient * conn = new ( aligned_malloc( sizeof( EvRvClient ) ) )
                        EvRvClient( poll );
    if ( nd.is_feed ) {
      FeedCB * fcb = new ( aligned_malloc( sizeof( FeedCB ) ) )
                     FeedCB( poll, *conn, cache, nd.idx, nd.wildcard,
                             nd.s2, nd.s3 );
      if ( ! conn->rv_connect( parm, fcb, fcb ) ) {
        fprintf( stderr, "Failed to connect net %u (feed)\n", nd.idx );
        return 1;
      }
    }
    else {
      SubCB * scb = new ( aligned_malloc( sizeof( SubCB ) ) )
                    SubCB( poll, *conn, cache, nd.idx, nd.s2, nd.s3,
                           first_sub, nd.wildcard );
      first_sub = false;
      cache.sub_conns[ nd.idx - 1 ] = conn;
      cache.sub_dbs[ nd.idx - 1 ]   = &scb->sub_db;
      cache.sub_nets |= (uint64_t) 1 << ( nd.idx - 1 );
      if ( cfg.verbose )
        scb->sub_db.mout = &mout;
      if ( ! conn->rv_connect( parm, scb, scb ) ) {
        fprintf( stderr, "Failed to connect net %u (sub)\n", nd.idx );
        return 1;
      }
    }
  }

  sighndl.install();
  int idle_count = 0;
  for (;;) {
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    poll.wait( idle_count > 255 ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  cache.print_stats( true );
  if ( cache.acct != NULL && cache.acct != stdout )
    ::fclose( cache.acct );
  return 0;
}
