#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <new>
#include <rvcache/cache.h>
#include <raimd/json.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/rv_msg.h>
#include <raimd/sass.h>
#include <raimd/md_field_iter.h>
#include <raikv/ev_publish.h>
#include <raikv/key_hash.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace sassrv;
using namespace rvcache;

/* SASS3 feed broadcast envelope magic (sass_const.h SASS3_PUB_MAGIC) */
static const uint16_t SASS3_PUB_MAGIC = 23177;

/* SASS3 QueryFlags (cache_if.h), parsed from _SNAP flags field */
enum QueryFlags {
  QF_SNAPSHOT       = 0x01,
  QF_SUBSCRIBE      = 0x02,
  QF_INITIAL_VALUES = 0x04,
  QF_UNSUBSCRIBE    = 0x08,
  QF_REFRESH        = 0x10,
  QF_RESUBSCRIBE    = 0x80
};

/* ------------------------------------------------------------------ */
struct RvCache {
  EvPoll         & poll;
  Config         & cfg;
  CacheTab         cache;
  Stats            stats;
  /* network attachments by mask bit (idx - 1); feeds have conns only */
  EvRvClient       * sub_conns[ MAX_NETS ];
  RvSubscriptionDB * sub_dbs[ MAX_NETS ];
  uint64_t           sub_nets;  /* mask of configured sub nets */
  FILE             * acct;
  char               pubbuf[ 64 * 1024 ];

  RvCache( EvPoll &p,  Config &c )
    : poll( p ), cfg( c ), sub_nets( 0 ), acct( 0 ) {
    for ( uint32_t i = 0; i < MAX_NETS; i++ ) {
      this->sub_conns[ i ] = NULL;
      this->sub_dbs[ i ]   = NULL;
    }
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
  /* timers */
  void on_timer( void ) noexcept;
  void print_stats( bool final_totals ) noexcept;

  /* helpers */
  /* stamp MSG_TYPE (leading fixed-width int, normalized at store time)
   * directly into the cached image via MDFieldIter::update() */
  bool stamp_msg_type( CacheEntry &e,  uint16_t msg_type ) noexcept;
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

/* stamp the delivery MSG_TYPE into the cached image in place.  set_image
 * normalized the field to be first + fixed-width int, so this is a
 * find-first-field + same-size/type MDFieldIter::update().  MDMsg::unpack
 * references e.image's bytes (no copy), so update() mutates the cached
 * image directly -- every send re-stamps, the stored value is dead weight.
 * Typeless images return false and are served as-is. */
bool
RvCache::stamp_msg_type( CacheEntry &e,  uint16_t msg_type ) noexcept
{
  if ( e.image == NULL )
    return false;
  MDMsgMem mem;
  MDMsg  * m = MDMsg::unpack( e.image, 0, e.image_len, e.image_enc,
                              NULL, mem );
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
  this->stats.nosub_sent++;
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
                              NULL, mem );
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
  this->stats.ticks_in++;

  MDMsgMem mem;
  MDMsg *  m = MDMsg::unpack( (void *) msg, 0, msg_len, enc, NULL, mem );
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
      switch ( this->cfg.seq ) {
        case SEQ_OBSERVE:
          if ( d < 0 ) this->stats.seq_regress++;
          else if ( d > 1 ) this->stats.seq_gap++;
          break;
        case SEQ_STRICT:
          if ( d <= 0 ) { this->stats.seq_regress++; apply = false; }
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
    this->stats.transient_pass++;
  }
  else if ( is_drop ) {
    /* instrument death: forward to listeners, evict entry */
    if ( e != NULL ) {
      this->cache.evict( subj, len );
      this->stats.evicted++;
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
        do_merge = this->cfg.merge_default && ! is_new;
      }

      if ( do_merge )
        this->cache.merge( *ce, msg, msg_len, enc );
      else
        this->cache.set_image( *ce, msg, msg_len, enc );

      if ( this->cfg.seq == SEQ_STAMP ) {
        ce->own_seqno++;
        ce->last_seqno = ce->own_seqno;
      }
      if ( this->cfg.route_after_merge && ce->image != NULL ) {
        fwd_msg = ce->image;
        fwd_len = ce->image_len;
        fwd_enc = ce->image_enc;
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
    this->stats.ticks_forwarded++;
    fe->forward_count++;
  }
  else {
    this->stats.dropped_no_listener++;
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

  /* subscribe with refcnt > 0: set this net's forwarding bit */
  if ( add.sub.refcnt > 0 )
    this->cache.interest_set( subj, len, net );
  /* submgr already ref'd the subscription; refcnt 1 == subject went live */
  if ( add.sub.refcnt == 1 )
    this->stats.interest_opens++;
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
    CacheEntry * e = this->cache.find( subj, len );
    if ( e != NULL && e->image != NULL ) {
      this->stamp_msg_type( *e, (uint16_t) MD_INITIAL_TYPE );
      this->publish_msg( net, add.reply, add.reply_len, NULL, 0, e->image,
                         e->image_len, e->image_enc );
      e->snap_count++;
      this->acct_event( "initial", subj, len, &add.session, proto, 0,
                        NULL, 0, 0, 0 );
    }
    else {
      /* miss -> status to the inbox, never silence (spec 2): the
       * requester attached an inbox precisely to learn the subject's
       * state.  Interest stays registered; a later INITIAL broadcasts. */
      this->serve_miss( net, subj, len, add.reply, add.reply_len );
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
    this->cache.interest_clear( subj, len, net );
    this->stats.interest_closes++;
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
                        snp.reply_len, &snp.session, snp.flags );
}

void
RvCache::serve_snapshot( uint32_t net,  const char *subj,  size_t len,
                         const char *reply,  size_t reply_len,
                         const RvSessionEntry *sess,  uint16_t flags,
                         const RvSass3Entry *s3 ) noexcept
{
  if ( reply == NULL || reply_len == 0 )
    return;
  CacheEntry * e = this->cache.find( subj, len );
  if ( e != NULL && e->image != NULL ) {
    /* stamp for the delivery kind: INITIAL when the requester is
     * subscribing (INITIAL_VALUES), SNAPSHOT for a plain image poll */
    bool         is_initial = ( flags & QF_INITIAL_VALUES ) != 0;
    const char * event      = is_initial ? "initial" : "snapshot";
    this->stamp_msg_type( *e, (uint16_t)
                          ( is_initial ? MD_INITIAL_TYPE : MD_SNAPSHOT_TYPE ) );
    this->publish_msg( net, reply, reply_len, NULL, 0, e->image,
                       e->image_len, e->image_enc );
    e->snap_count++;
    this->stats.snaps_served++;
    this->acct_event( event, subj, len, sess,
                      s3 != NULL ? "sass3" : "snap", flags,
                      NULL, 0, 0, 0, s3 );
  }
  else {
    /* broadcast-feed miss: TRANSIENT / NOT_FOUND immediately (bcast-nack) */
    this->serve_miss( net, subj, len, reply, reply_len );
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
  this->stats.snaps_missed++;
}

void
RvCache::broadcast_initial( uint32_t net,  const char *subj,  size_t len,
                            const RvSessionEntry *sess,
                            const RvSass3Entry *s3,
                            const char *proto ) noexcept
{
  CacheEntry * e = this->cache.find( subj, len );
  if ( e == NULL || e->image == NULL )
    return; /* cold: the feed's next INITIAL broadcasts normally */
  this->stamp_msg_type( *e, (uint16_t) MD_INITIAL_TYPE );
  this->publish_msg( net, subj, len, NULL, 0, e->image, e->image_len,
                     e->image_enc );
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
      this->cache.interest_clear( subj, len, net );
      this->stats.interest_closes++;
      this->emit_nosubscribers( net, subj, len );
    }
    return;
  }

  /* SUBSCRIBE (or a RESUBSCRIBE asserting a holder submgr didn't know):
   * submgr already ref'd; refcnt 1 == subject went live */
  if ( ( sa3.flags & QF_SUBSCRIBE ) != 0 || sa3.is_asserted ) {
    if ( sa3.sub.refcnt > 0 )
      this->cache.interest_set( subj, len, net );
    if ( sa3.sub.refcnt == 1 )
      this->stats.interest_opens++;
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

/* ------------------------------------------------------------------ */
void
RvCache::on_timer( void ) noexcept
{
  /* stale cache entry expiry */
  if ( this->cfg.stale_secs > 0 ) {
    uint64_t cutoff = this->now_ns() -
                      (uint64_t) this->cfg.stale_secs * 1000000000ULL;
    RouteLoc loc;
    for ( CacheEntry * e = this->cache.tab.first( loc ); e != NULL; ) {
      if ( e->last_update_ns != 0 && e->last_update_ns < cutoff ) {
        char tmp[ 512 ];
        size_t l = e->len < sizeof( tmp ) ? e->len : sizeof( tmp ) - 1;
        ::memcpy( tmp, e->value, l );
        CacheEntry * nxt = this->cache.tab.next( loc );
        this->cache.evict( tmp, l );
        this->stats.evicted++;
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
  size_t subj = this->cache.count();
  size_t holders = 0, subjects_held = 0;
  for ( uint32_t i = 0; i < MAX_NETS; i++ ) {
    RvSubscriptionDB * db = this->sub_dbs[ i ];
    if ( db == NULL )
      continue;
    RouteLoc loc;
    for ( RvSubscription * s = db->sub_tab.first( loc ); s != NULL;
          s = db->sub_tab.next( loc ) ) {
      if ( s->refcnt != 0 && s->len > 0 && s->value[ 0 ] != '_' ) {
        subjects_held++;
        holders += s->refcnt;
      }
    }
  }
  printf( "%s cached=%zu img=%lluB held=%zu holders=%zu | "
          "in=%llu fwd=%llu drop=%llu | snap=%llu/%llu | "
          "open=%llu close=%llu evict=%llu nosub=%llu | seq[reg=%llu gap=%llu] "
          "tr=%llu",
    final_totals ? "TOTALS" : "stats",
    subj, (unsigned long long) this->cache.image_bytes, subjects_held, holders,
    (unsigned long long) this->stats.ticks_in,
    (unsigned long long) this->stats.ticks_forwarded,
    (unsigned long long) this->stats.dropped_no_listener,
    (unsigned long long) this->stats.snaps_served,
    (unsigned long long) this->stats.snaps_missed,
    (unsigned long long) this->stats.interest_opens,
    (unsigned long long) this->stats.interest_closes,
    (unsigned long long) this->stats.evicted,
    (unsigned long long) this->stats.nosub_sent,
    (unsigned long long) this->stats.seq_regress,
    (unsigned long long) this->stats.seq_gap,
    (unsigned long long) this->stats.transient_pass );
  for ( uint32_t i = 0; i < MAX_NETS; i++ ) {
    RvSubscriptionDB * db = this->sub_dbs[ i ];
    if ( db != NULL )
      printf( " | net%u.sub[a=%u r=%u] hosts=%u sess=%u", i + 1,
        db->subscriptions.active, db->subscriptions.removed,
        db->hosts.active, db->sessions.active );
  }
  printf( "\n" );
  fflush( stdout );
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
    bool all = ( this->cache.cfg.wildcards.count == 0 &&
                 this->wildcard == NULL );
    for ( size_t i = 0; i < this->cache.cfg.wildcards.count; i++ )
      this->sub_db.add_wildcard( this->cache.cfg.wildcards.ptr[ i ] );
    if ( this->wildcard != NULL ) /* per-net filter, sass2 and sass3 */
      this->sub_db.add_wildcard( this->wildcard );
    this->sub_db.start_subscriptions( all, this->s2, this->s3 );
    this->poll.timer.add_timer_seconds( *this, 1, 1, 0 );
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
};

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

/* split s on ',' into up to n fields (empty fields -> NULL); modifies s */
static int
split_fields( char *s,  const char *f[],  int n ) noexcept
{
  int cnt = 0;
  while ( cnt < n ) {
    f[ cnt++ ] = ( *s != '\0' && *s != ',' ) ? s : NULL;
    char * c = ::strchr( s, ',' );
    if ( c == NULL )
      break;
    *c = '\0';
    s = c + 1;
  }
  return cnt;
}

static bool
parse_role_proto( NetDef &nd,  const char *role,  const char *proto ) noexcept
{
  if ( role == NULL )
    return false;
  if ( ::strcmp( role, "feed" ) == 0 )
    nd.is_feed = true;
  else if ( ::strcmp( role, "sub" ) == 0 )
    nd.is_feed = false;
  else
    return false;
  if ( proto == NULL || ::strcmp( proto, "both" ) == 0 ||
       ::strcmp( proto, "sass2+sass3" ) == 0 ) {
    nd.s2 = true; nd.s3 = true;
  }
  else if ( ::strcmp( proto, "sass2" ) == 0 ) {
    nd.s2 = true; nd.s3 = false;
  }
  else if ( ::strcmp( proto, "sass3" ) == 0 ) {
    nd.s2 = false; nd.s3 = true;
  }
  else
    return false;
  return true;
}

/* parse "role,proto[,daemon[,network[,service[,wildcard]]]]" into a NetDef */
static bool
parse_net_tuple( uint32_t idx,  const char *s,  NetDef &nd ) noexcept
{
  char       * dup = ::strdup( s );
  const char * f[ 6 ] = { NULL, NULL, NULL, NULL, NULL, NULL };
  split_fields( dup, f, 6 );
  nd.idx = idx;
  if ( ! parse_role_proto( nd, f[ 0 ], f[ 1 ] ) ) {
    ::free( dup );
    return false;
  }
  if ( f[ 2 ] != NULL ) nd.parm.daemon  = ::strdup( f[ 2 ] );
  if ( f[ 3 ] != NULL ) nd.parm.network = ::strdup( f[ 3 ] );
  if ( f[ 4 ] != NULL ) nd.parm.service = ::strdup( f[ 4 ] );
  if ( f[ 5 ] != NULL ) nd.wildcard     = ::strdup( f[ 5 ] );
  ::free( dup );
  return true;
}

/* json/yaml nets config: an array (or { "nets": [...] }) of objects:
 * { "index": 1, "role": "feed", "proto": "sass2",
 *   "daemon": "tcp:7500", "network": "", "service": "7500",
 *   "wildcard": "WILD" }
 * wildcard: sub nets filter the submgr (sass2 and sass3 interest channels,
 * start_subscriptions all=false); feed nets subscribe _TIC.<wild>.> (sass2)
 * or _SASS.<wild>.PUB (sass3) instead of the full firehose */
static const char *
json_str( JsonObject *o,  const char *name ) noexcept
{
  JsonValue * v = o->find( name );
  if ( v == NULL || v->type != JSON_STRING )
    return NULL;
  JsonString * s = v->to_str();
  if ( s->length == 0 )
    return NULL;
  char * cp = (char *) ::malloc( s->length + 1 );
  ::memcpy( cp, s->val, s->length );
  cp[ s->length ] = '\0';
  return cp;
}

static bool
load_nets_config( const char *path,  Config &cfg ) noexcept
{
  int fd = ::open( path, O_RDONLY );
  if ( fd < 0 ) {
    perror( path );
    return false;
  }
  size_t plen = ::strlen( path );
  bool is_yaml = ( plen > 5 && ::strcmp( &path[ plen - 5 ], ".yaml" ) == 0 ) ||
                 ( plen > 4 && ::strcmp( &path[ plen - 4 ], ".yml" ) == 0 );
  MDMsgMem        mem;
  JsonParser      parser( mem );
  JsonStreamInput input( fd );
  int status = is_yaml ? parser.parse_yaml( input ) : parser.parse( input );
  ::close( fd );
  if ( status != 0 || parser.value == NULL ) {
    fprintf( stderr, "%s: parse error %d line %u\n", path, status,
             (unsigned) input.line_count + 1 );
    return false;
  }
  JsonValue * root = parser.value;
  if ( root->type == JSON_OBJECT ) {
    JsonValue * n = root->to_obj()->find( "nets" );
    if ( n == NULL ) {
      fprintf( stderr, "%s: no \"nets\" array\n", path );
      return false;
    }
    root = n;
  }
  if ( root->type != JSON_ARRAY ) {
    fprintf( stderr, "%s: nets config is not an array\n", path );
    return false;
  }
  JsonArray * arr = root->to_arr();
  for ( size_t i = 0; i < arr->length; i++ ) {
    if ( arr->val[ i ]->type != JSON_OBJECT ) {
      fprintf( stderr, "%s: nets[%zu] is not an object\n", path, i );
      return false;
    }
    JsonObject * o = arr->val[ i ]->to_obj();
    NetDef       nd;
    int64_t      idx = 0;
    JsonValue  * iv = o->find( "index" );
    if ( iv == NULL || iv->to_int( idx ) != 0 || idx < 1 ||
         idx > (int64_t) MAX_NETS ) {
      fprintf( stderr, "%s: nets[%zu] needs \"index\" 1..%u\n", path, i,
               MAX_NETS );
      return false;
    }
    const char * role  = json_str( o, "role" ),
               * proto = json_str( o, "proto" );
    nd.idx = (uint32_t) idx;
    if ( ! parse_role_proto( nd, role, proto ) ) {
      fprintf( stderr, "%s: nets[%zu] bad role/proto\n", path, i );
      return false;
    }
    nd.parm.daemon  = json_str( o, "daemon" );
    nd.parm.network = json_str( o, "network" );
    nd.parm.service = json_str( o, "service" );
    nd.wildcard     = json_str( o, "wildcard" );
    cfg.nets.push( nd );
  }
  return true;
}

int
main( int argc,  const char *argv[] )
{
  SignalHandler sighndl;
  Config cfg;
  int x = 1;

  const char * daemon = get_arg( x, argc, argv, 1, "-d", "tcp:7500" ),
             * network= get_arg( x, argc, argv, 1, "-n", "" ),
             * service= get_arg( x, argc, argv, 1, "-s", "7500" ),
             * cfile  = get_arg( x, argc, argv, 1, "-c", NULL ),
             * sfeed  = get_arg( x, argc, argv, 1, "-S", NULL ),
             * fname  = get_arg( x, argc, argv, 1, "-F", NULL ),
             * hold   = get_arg( x, argc, argv, 1, "-D", NULL ),
             * seqm   = get_arg( x, argc, argv, 1, "-Q", NULL ),
             * stale  = get_arg( x, argc, argv, 1, "-x", NULL ),
             * pend   = get_arg( x, argc, argv, 1, "-P", NULL ),
             * acctf  = get_arg( x, argc, argv, 1, "-A", NULL ),
             * mergem = get_arg( x, argc, argv, 0, "-m", NULL ),
             * routem = get_arg( x, argc, argv, 0, "-M", NULL ),
             * quiet  = get_arg( x, argc, argv, 0, "-q", NULL ),
             * verb   = get_arg( x, argc, argv, 0, "-v", NULL ),
             * help   = get_arg( x, argc, argv, 0, "-h", NULL );

  if ( help != NULL ) {
    fprintf( stderr,
      "rv_cache [-d daemon] [-n network] [-s service] (defaults for nets)\n"
      "  [-<idx> role,proto[,daemon[,network[,service[,wildcard]]]]] net\n"
      "    attachment, repeatable: idx 1..%u, role feed|sub, proto\n"
      "    sass2|sass3|both; empty d/n/s fields fall back to -d/-n/-s\n"
      "    e.g. -1 feed,sass2 -2 sub,sass2 -4 sub,sass3,tcp:7501,,7501\n"
      "  [-c file] nets from json/yaml: [{index,role,proto,daemon,network,\n"
      "    service,wildcard},...] or { \"nets\": [...] } (.yaml/.yml = yaml)\n"
      "    wildcard: sub = submgr filter (sass2+sass3, subscriptions\n"
      "    start all=false); feed = subscribe _TIC.<wild>.> (sass2) or\n"
      "    _SASS.<wild>.PUB (sass3) instead of the firehose\n"
      "  (no nets given: -1 feed,sass2 -2 sub,both)\n"
      "  [-w wild] interest filter, all sub nets (repeatable)\n"
      "  [-S feed] SASS3 upstream feed (milestone 2)\n"
      "  [-F name] SASS3 downstream service (milestone 2)\n"
      "  [-D secs] sass3 hold timer (480; no effect without -S/-F)\n"
      "  [-m] merge typeless   [-Q obs|strict|stamp]\n"
      "  [-M] route-after-merge   [-x secs] stale expiry\n"
      "  [-P secs] pending timeout (10)   [-A file] accounting jsonl (- stdout)\n"
      "  [-q] quiet stats   [-v] verbose submgr log\n", MAX_NETS );
    return 1;
  }

  cfg.base.daemon = daemon;
  cfg.base.network = network;
  cfg.base.service = service;

  /* -<idx> role,proto,d,n,s net attachments (any integer 1..MAX_NETS) */
  for ( int i = 1; i < argc - 1; i++ ) {
    const char * a = argv[ i ];
    if ( a[ 0 ] != '-' || a[ 1 ] < '0' || a[ 1 ] > '9' )
      continue;
    char * end = NULL;
    long   idx = ::strtol( &a[ 1 ], &end, 10 );
    if ( *end != '\0' || idx < 1 || idx > (long) MAX_NETS ) {
      fprintf( stderr, "bad net index: %s (1..%u)\n", a, MAX_NETS );
      return 1;
    }
    NetDef nd;
    if ( ! parse_net_tuple( (uint32_t) idx, argv[ i + 1 ], nd ) ) {
      fprintf( stderr,
        "bad net tuple: %s %s (want role,proto[,daemon[,network[,service]]],"
        " role feed|sub, proto sass2|sass3|both)\n", a, argv[ i + 1 ] );
      return 1;
    }
    cfg.nets.push( nd );
    if ( x < i + 2 )
      x = i + 2;
  }
  if ( cfile != NULL && ! load_nets_config( cfile, cfg ) )
    return 1;
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
  cfg.sass3_feed = sfeed;
  cfg.sass3_name = fname;
  cfg.merge_default = ( mergem != NULL );
  cfg.route_after_merge = ( routem != NULL );
  cfg.quiet = ( quiet != NULL );
  cfg.verbose = ( verb != NULL );
  if ( hold )  cfg.hold_secs = (uint32_t) atoi( hold );
  if ( stale ) cfg.stale_secs = (uint32_t) atoi( stale );
  if ( pend )  cfg.pending_secs = (uint32_t) atoi( pend );
  cfg.acct_file = acctf;
  if ( seqm != NULL ) {
    if ( ::strcmp( seqm, "strict" ) == 0 )      cfg.seq = SEQ_STRICT;
    else if ( ::strcmp( seqm, "stamp" ) == 0 )  cfg.seq = SEQ_STAMP;
    else                                        cfg.seq = SEQ_OBSERVE;
  }
  /* collect repeatable -w */
  for ( int i = 1; i < argc - 1; i++ ) {
    if ( ::strcmp( argv[ i ], "-w" ) == 0 )
      cfg.wildcards.push( (const char *) argv[ i + 1 ] );
  }

  /* milestone 1: SASS3 feed-side attachments are not yet implemented */
  if ( cfg.sass3_feed != NULL ) {
    fprintf( stderr, "-S (SASS3 upstream feed) not yet implemented "
                     "(milestone 2)\n" );
    return 1;
  }
  if ( cfg.sass3_name != NULL ) {
    fprintf( stderr, "-F (SASS3 downstream service) not yet implemented "
                     "(milestone 2)\n" );
    return 1;
  }

  EvPoll poll;
  poll.init( 5, false );

  RvCache cache( poll, cfg );
  if ( cfg.acct_file != NULL ) {
    if ( ::strcmp( cfg.acct_file, "-" ) == 0 )
      cache.acct = stdout;
    else {
      cache.acct = ::fopen( cfg.acct_file, "a" );
      if ( cache.acct == NULL )
        perror( cfg.acct_file );
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
