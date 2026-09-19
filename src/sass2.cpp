#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <malloc.h>       /* mallinfo(): heap arena size for the stats line */
#include <sys/resource.h> /* getrusage(): user/sys cpu for the stats line */
#else
#include <windows.h>
#include <psapi.h>        /* GetProcessMemoryInfo() / GetProcessTimes() */
#endif
#include <new>
#include <rvcache/cache.h>
#include <rvcache/rv_cache.h>
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

/* sass2 interest channel: LISTEN.START/STOP advisories and _SNAP
 * requests served from the cache (RvSubscriptionListener via SubCB) */

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
