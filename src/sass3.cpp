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

/* sass3 interest channel: _SASS.<feed>.PUB feed envelopes, sass3
 * subscribe/resubscribe requests and tic replies */

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
