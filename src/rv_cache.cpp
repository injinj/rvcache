#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <rvcache/cache.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/rv_msg.h>
#include <raimd/sass.h>
#include <raikv/ev_publish.h>
#include <raikv/key_hash.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace sassrv;
using namespace rvcache;

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
  EvRvClient     * feed_conn,   /* net 1 (upstream _TIC.>) */
                 * sub_conn;    /* net 2 (downstream) */
  RvSubscriptionDB * sub_db;    /* submgr on net 2 */
  FILE           * acct;
  char             pubbuf[ 64 * 1024 ];

  RvCache( EvPoll &p,  Config &c )
    : poll( p ), cfg( c ), feed_conn( 0 ), sub_conn( 0 ), sub_db( 0 ),
      acct( 0 ) {}

  uint32_t cur_mono( void ) const {
    return (uint32_t) ( this->poll.mono_ns / (uint64_t) 1000000000 );
  }
  uint64_t now_ns( void ) const { return current_realtime_ns(); }

  /* feed path (net 1) */
  void on_feed_msg( EvPublish &pub ) noexcept;
  void handle_tic( const char *subj,  size_t len,  const void *msg,
                   size_t msg_len,  uint32_t enc ) noexcept;
  /* interest (submgr callbacks on net 2) */
  void on_listen_start( RvSubscriptionListener::Start &add ) noexcept;
  void on_listen_stop( RvSubscriptionListener::Stop &rem ) noexcept;
  void on_snapshot( RvSubscriptionListener::Snap &snp ) noexcept;
  void on_sass3( RvSubscriptionListener::Sass3 &sa3 ) noexcept;
  /* timers */
  void on_timer( void ) noexcept;
  void print_stats( bool final_totals ) noexcept;

  /* helpers */
  /* forwarding gate: read-only find in submgr's sub_tab; submgr owns the
   * subscription's life (advisory START/STOP, session/host sweeps, GC) */
  uint32_t sub_refcnt( const char *subj,  size_t len ) noexcept;
  /* stamp MSG_TYPE (leading fixed-width int, normalized at store time)
   * directly into the cached image via MDFieldIter::update() */
  bool stamp_msg_type( CacheEntry &e,  uint16_t msg_type ) noexcept;
  void publish_msg( const char *subj,  size_t len,  const char *reply,
                    size_t reply_len,  const void *msg,  size_t msg_len,
                    uint32_t enc ) noexcept;
  size_t build_status( uint16_t msg_type,  uint16_t rec_status,
                       const char *subj,  size_t len,  char *buf,
                       size_t buflen ) noexcept;
  void emit_nosubscribers( const char *subj,  size_t len ) noexcept;
  void serve_snapshot( const char *subj,  size_t len,  const char *reply,
                       size_t reply_len,  const RvSessionEntry *sess,
                       uint16_t flags,
                       const RvSass3Entry *s3 = NULL ) noexcept;
  /* miss: TRANSIENT / NOT_FOUND to the reply inbox (bcast-nack); the one
   * code path shared by _SNAP, listen-start-inbox and sass3 requests */
  void serve_miss( const char *subj,  size_t len,  const char *reply,
                   size_t reply_len ) noexcept;
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

/* ------------------------------------------------------------------ */
uint32_t
RvCache::sub_refcnt( const char *subj,  size_t len ) noexcept
{
  if ( this->sub_db == NULL )
    return 0;
  uint32_t h = kv_crc_c( subj, len, 0 );
  RouteLoc loc;
  RvSubscription * s = this->sub_db->sub_tab.find( h, subj, len, loc );
  /* _SNAP-created entries exist with refcnt 0 -- not interest.
   * TODO(can-wildcard): a client wildcard listen (TEST.>) makes an entry
   * under the wildcard subject; matching concrete ticks against wildcard
   * subscriptions is not implemented yet. */
  return s == NULL ? 0 : s->refcnt;
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
RvCache::publish_msg( const char *subj,  size_t len,  const char *reply,
                      size_t reply_len,  const void *msg,  size_t msg_len,
                      uint32_t enc ) noexcept
{
  if ( this->sub_conn == NULL )
    return;
  uint32_t h = kv_crc_c( subj, len, 0 );
  EvPublish pub( subj, len, reply, reply_len, msg, msg_len,
                 this->sub_conn->sub_route, *this->sub_conn, h, enc );
  this->sub_conn->publish( pub );
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
RvCache::emit_nosubscribers( const char *subj,  size_t len ) noexcept
{
  char buf[ 1024 ];
  size_t n = this->build_status( (uint16_t) MD_DROP_TYPE,
                                 (uint16_t) MD_NOSUBSCRIBERS_STATUS,
                                 subj, len, buf, sizeof( buf ) );
  this->publish_msg( subj, len, NULL, 0, buf, n, RVMSG_TYPE_ID );
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

void
RvCache::handle_tic( const char *subj,  size_t len,  const void *msg,
                     size_t msg_len,  uint32_t enc ) noexcept
{
  this->stats.ticks_in++;

  MDMsgMem mem;
  MDMsg *  m = MDMsg::unpack( (void *) msg, 0, msg_len, enc, NULL, mem );
  uint16_t msg_type = 0, rec_status = 0;
  uint32_t seqno = 0;
  bool     has_type = false, has_seqno = false, has_status = false;
  parse_sass( m, msg_type, seqno, has_type, has_seqno, rec_status, has_status );

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

  /* forwarding decision: read-only find in submgr's sub_tab, gate on
   * refcnt != 0.  submgr controls the subscription's life; there is no
   * parallel interest table and no RV-side decay timer. */
  if ( this->sub_refcnt( subj, len ) != 0 ) {
    this->publish_msg( subj, len, NULL, 0, fwd_msg, fwd_len, fwd_enc );
    this->stats.ticks_forwarded++;
    CacheEntry * fe = this->cache.find( subj, len );
    if ( fe != NULL )
      fe->forward_count++;
  }
  else {
    this->stats.dropped_no_listener++;
  }
}

/* ------------------------------------------------------------------ */
void
RvCache::on_listen_start( RvSubscriptionListener::Start &add ) noexcept
{
  const char * subj = add.sub.value;
  size_t       len  = add.sub.len;
  /* default interest filter: ignore _-prefixed subjects (advisories, _SNAP,
   * _TIC, and rv_cache's own subscriptions) -- they are never downstream
   * consumer interest. */
  if ( len == 0 || subj[ 0 ] == '_' )
    return;
  const char * proto = add.session.has_daemon ? "rv7" : "rv5";

  /* submgr already ref'd the subscription; refcnt 1 == subject went live */
  if ( add.sub.refcnt == 1 )
    this->stats.interest_opens++;
  this->acct_event( "subscribe", subj, len, &add.session, proto, 0,
                    NULL, 0, 0, 0 );

  /* initial-on-listen (rv5 path): the CLIENT controls this -- attaching an
   * inbox to the listen-start is the request for an initial.  No option. */
  if ( add.reply_len > 0 ) {
    CacheEntry * e = this->cache.find( subj, len );
    if ( e != NULL && e->image != NULL ) {
      this->stamp_msg_type( *e, (uint16_t) MD_INITIAL_TYPE );
      this->publish_msg( add.reply, add.reply_len, NULL, 0, e->image,
                         e->image_len, e->image_enc );
      e->snap_count++;
      this->acct_event( "initial", subj, len, &add.session, proto, 0,
                        NULL, 0, 0, 0 );
    }
    else {
      /* miss -> status to the inbox, never silence (spec 2): the
       * requester attached an inbox precisely to learn the subject's
       * state.  Interest stays registered; a later INITIAL broadcasts. */
      this->serve_miss( subj, len, add.reply, add.reply_len );
    }
  }
}

void
RvCache::on_listen_stop( RvSubscriptionListener::Stop &rem ) noexcept
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

  /* submgr already deref'd; refcnt 0 == last holder gone */
  if ( rem.sub.refcnt == 0 ) {
    this->stats.interest_closes++;
    this->emit_nosubscribers( subj, len );
  }
}

void
RvCache::on_snapshot( RvSubscriptionListener::Snap &snp ) noexcept
{
  /* submgr resolved the requester's session from the reply inbox; its
   * user_id attributes the request for accounting */
  this->serve_snapshot( snp.sub.value, snp.sub.len, snp.reply, snp.reply_len,
                        &snp.session, snp.flags );
}

void
RvCache::serve_snapshot( const char *subj,  size_t len,  const char *reply,
                         size_t reply_len,  const RvSessionEntry *sess,
                         uint16_t flags,  const RvSass3Entry *s3 ) noexcept
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
    this->publish_msg( reply, reply_len, NULL, 0, e->image, e->image_len,
                       e->image_enc );
    e->snap_count++;
    this->stats.snaps_served++;
    this->acct_event( event, subj, len, sess,
                      s3 != NULL ? "sass3" : "snap", flags,
                      NULL, 0, 0, 0, s3 );
  }
  else {
    /* broadcast-feed miss: TRANSIENT / NOT_FOUND immediately (bcast-nack) */
    this->serve_miss( subj, len, reply, reply_len );
  }
}

void
RvCache::serve_miss( const char *subj,  size_t len,  const char *reply,
                     size_t reply_len ) noexcept
{
  char buf[ 1024 ];
  size_t n = this->build_status( (uint16_t) MD_TRANSIENT_TYPE,
                                 (uint16_t) MD_NOT_FOUND_STATUS,
                                 subj, len, buf, sizeof( buf ) );
  this->publish_msg( reply, reply_len, NULL, 0, buf, n, RVMSG_TYPE_ID );
  this->stats.snaps_missed++;
}

/* sass3 interest on net 2 (submgr wildcard _SASS.<feed>.SUB channel).
 * submgr owns the holder's life: it refs the subscription on a new
 * holder, derefs on UNSUBSCRIBE and on lease expiry (480s), and fires
 * this callback for each subject in the S submessage. */
void
RvCache::on_sass3( RvSubscriptionListener::Sass3 &sa3 ) noexcept
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
    /* submgr already deref'd; refcnt 0 == last holder gone */
    if ( sa3.sub.refcnt == 0 ) {
      this->stats.interest_closes++;
      this->emit_nosubscribers( subj, len );
    }
    return;
  }

  /* SUBSCRIBE (or a RESUBSCRIBE asserting a holder submgr didn't know):
   * submgr already ref'd; refcnt 1 == subject went live */
  if ( ( sa3.flags & QF_SUBSCRIBE ) != 0 || sa3.is_asserted ) {
    if ( sa3.sub.refcnt == 1 )
      this->stats.interest_opens++;
    this->acct_event( "subscribe", subj, len, NULL, "sass3", sa3.flags,
                      NULL, 0, 0, 0, &sa3.sass3 );
  }

  /* image request: SNAPSHOT / INITIAL_VALUES with a reply inbox;
   * miss -> TRANSIENT/NOT_FOUND, same one-code-path as _SNAP */
  if ( sa3.reply_len > 0 &&
       ( sa3.flags & ( QF_SNAPSHOT | QF_INITIAL_VALUES ) ) != 0 )
    this->serve_snapshot( subj, len, sa3.reply, sa3.reply_len, NULL,
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
  if ( this->sub_db != NULL ) {
    RouteLoc loc;
    for ( RvSubscription * s = this->sub_db->sub_tab.first( loc ); s != NULL;
          s = this->sub_db->sub_tab.next( loc ) ) {
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
  if ( this->sub_db != NULL )
    printf( " | db.sub[a=%u r=%u] hosts=%u sess=%u",
      this->sub_db->subscriptions.active, this->sub_db->subscriptions.removed,
      this->sub_db->hosts.active, this->sub_db->sessions.active );
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
/* net 1 (feed) callback */
struct FeedCB : public EvConnectionNotify, public RvClientCB {
  EvPoll     & poll;
  EvRvClient & client;
  RvCache    & cache;
  FeedCB( EvPoll &p,  EvRvClient &c,  RvCache &rc )
    : poll( p ), client( c ), cache( rc ) {}
  virtual void on_connect( EvSocket &conn ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "feed(net1) connected: %.*s\n", len, conn.peer_address.buf );
    fflush( stdout );
    this->client.subscribe( "_TIC.>", 6 );
  }
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t errlen ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "feed(net1) shutdown: %.*s %.*s\n", len, conn.peer_address.buf,
            (int) errlen, err );
    if ( this->poll.quit == 0 )
      this->poll.quit = 1;
  }
  virtual bool on_rv_msg( EvPublish &pub ) noexcept {
    this->cache.on_feed_msg( pub );
    return true;
  }
};

/* net 2 (sub / submgr) callback */
struct SubCB : public EvConnectionNotify, public RvClientCB,
               public EvTimerCallback, public RvSubscriptionListener {
  EvPoll         & poll;
  EvRvClient     & client;
  RvCache        & cache;
  RvSubscriptionDB sub_db;
  SubCB( EvPoll &p,  EvRvClient &c,  RvCache &rc )
    : poll( p ), client( c ), cache( rc ), sub_db( c, this ) {}

  virtual void on_connect( EvSocket &conn ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "sub(net2) connected: %.*s\n", len, conn.peer_address.buf );
    fflush( stdout );
    bool all = ( this->cache.cfg.wildcards.count == 0 );
    for ( size_t i = 0; i < this->cache.cfg.wildcards.count; i++ )
      this->sub_db.add_wildcard( this->cache.cfg.wildcards.ptr[ i ] );
    /* both interest channels: sass2 (_RV.INFO advisories + _SNAP) and
     * sass3 (_SASS.<feed>.SUB wildcard).  sass3-aware clients use sass3
     * even on sass2-only networks, so a cache must listen to both. */
    this->sub_db.start_subscriptions( all, true, true );
    this->poll.timer.add_timer_seconds( *this, 1, 1, 0 );
  }
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t errlen ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    printf( "sub(net2) shutdown: %.*s %.*s\n", len, conn.peer_address.buf,
            (int) errlen, err );
    if ( this->poll.quit == 0 )
      this->poll.quit = 1;
  }
  virtual bool on_rv_msg( EvPublish &pub ) noexcept {
    this->sub_db.process_pub( pub );
    return true;
  }
  virtual bool timer_cb( uint64_t,  uint64_t ) noexcept {
    this->sub_db.process_events();
    this->cache.on_timer();
    return true;
  }
  virtual void on_listen_start( Start &add ) noexcept {
    this->cache.on_listen_start( add );
  }
  virtual void on_listen_stop( Stop &rem ) noexcept {
    this->cache.on_listen_stop( rem );
  }
  virtual void on_snapshot( Snap &snp ) noexcept {
    this->cache.on_snapshot( snp );
  }
  virtual void on_sass3( Sass3 &sa3 ) noexcept {
    this->cache.on_sass3( sa3 );
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

/* parse "d,n,s" (any field may be empty) into a NetParm; returns true */
static void
parse_triple( const char *s,  NetParm &np ) noexcept
{
  if ( s == NULL )
    return;
  char * dup = ::strdup( s );
  char * d = dup;
  char * c1 = ::strchr( d, ',' );
  if ( c1 != NULL ) {
    *c1 = '\0';
    char * n = c1 + 1;
    char * c2 = ::strchr( n, ',' );
    if ( c2 != NULL ) {
      *c2 = '\0';
      char * sv = c2 + 1;
      if ( *sv ) np.service = ::strdup( sv );
    }
    if ( *n ) np.network = ::strdup( n );
  }
  if ( *d ) np.daemon = ::strdup( d );
  ::free( dup );
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
             * o1     = get_arg( x, argc, argv, 1, "-1", NULL ),
             * o2     = get_arg( x, argc, argv, 1, "-2", NULL ),
             * o3     = get_arg( x, argc, argv, 1, "-3", NULL ),
             * o4     = get_arg( x, argc, argv, 1, "-4", NULL ),
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
      "rv_cache [-d daemon] [-n network] [-s service] (defaults for 4 nets)\n"
      "  [-1 d,n,s] rv feed   [-2 d,n,s] rv sub\n"
      "  [-3 d,n,s] sass3 feed [-4 d,n,s] sass3 sub\n"
      "  [-w wild] interest filter (repeatable)\n"
      "  [-S feed] SASS3 upstream (net 3)   [-F name] SASS3 downstream (net 4)\n"
      "  [-D secs] sass3 hold timer (480; no effect without -S/-F)\n"
      "  [-m] merge typeless   [-Q obs|strict|stamp]\n"
      "  [-M] route-after-merge   [-x secs] stale expiry\n"
      "  [-P secs] pending timeout (10)   [-A file] accounting jsonl (- stdout)\n"
      "  [-q] quiet stats   [-v] verbose submgr log\n" );
    return 1;
  }

  cfg.base.daemon = daemon;
  cfg.base.network = network;
  cfg.base.service = service;
  if ( o1 ) { cfg.net_override[0] = true; parse_triple( o1, cfg.net[0] ); }
  if ( o2 ) { cfg.net_override[1] = true; parse_triple( o2, cfg.net[1] ); }
  if ( o3 ) { cfg.net_override[2] = true; parse_triple( o3, cfg.net[2] ); }
  if ( o4 ) { cfg.net_override[3] = true; parse_triple( o4, cfg.net[3] ); }
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

  /* milestone 1: SASS3 attachments (nets 3/4) are not yet implemented */
  if ( cfg.sass3_feed != NULL ) {
    fprintf( stderr, "-S (SASS3 upstream, net 3) not yet implemented "
                     "(milestone 2)\n" );
    return 1;
  }
  if ( cfg.sass3_name != NULL ) {
    fprintf( stderr, "-F (SASS3 downstream, net 4) not yet implemented "
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

  /* net 1: rv feed (data consumer) */
  const char * d1, * n1, * s1;
  cfg.resolve( 0, d1, n1, s1 );
  EvRvClientParameters p1( d1, n1, s1, "rv_cache_feed", 0 );
  EvRvClient conn1( poll );
  FeedCB feed( poll, conn1, cache );

  /* net 2: rv sub (data publisher + submgr) */
  const char * d2, * n2, * s2;
  cfg.resolve( 1, d2, n2, s2 );
  EvRvClientParameters p2( d2, n2, s2, "rv_cache_sub", 0 );
  EvRvClient conn2( poll );
  SubCB sub( poll, conn2, cache );

  cache.feed_conn = &conn1;
  cache.sub_conn  = &conn2;
  cache.sub_db    = &sub.sub_db;

  MDOutput mout;
  if ( cfg.verbose )
    sub.sub_db.mout = &mout;

  if ( ! conn1.rv_connect( p1, &feed, &feed ) ) {
    fprintf( stderr, "Failed to connect net 1 (feed)\n" );
    return 1;
  }
  if ( ! conn2.rv_connect( p2, &sub, &sub ) ) {
    fprintf( stderr, "Failed to connect net 2 (sub)\n" );
    return 1;
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
