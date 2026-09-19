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
  /* omm nets carry the 1-based idx; the interest mask wants the bit */
  this->interest_edge_set( subj, len, net - 1 );
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
  this->interest_edge_clear( subj, len, net - 1 );
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
  if ( this->cache.dict.rdm_dict == NULL ||
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
  RwfMsgWriter em( mem, this->cache.dict.rdm_dict, bp, sz,
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
  if ( this->cache.dict.rdm_dict == NULL ||
       this->omm_listener == NULL )
    return;
  MDMsgMem mem;
  size_t   sz = 1024;
  void   * bp = mem.make( sz );
  uint32_t h  = kv_crc_c( subj, len, 0 );
  RwfMsgWriter em( mem, this->cache.dict.rdm_dict, bp, sz,
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

/* OmmFeedCB / OmmSubNotify (declared in rv_cache.h) */
void
OmmFeedCB::on_connect( EvSocket &conn ) noexcept
{
  printf( "omm feed(net%u) ready: %.*s\n", this->net,
          (int) conn.get_peer_address_strlen(), conn.peer_address.buf );
  this->cache.omm_feed_ready( this->net );
}

void
OmmFeedCB::on_shutdown( EvSocket &conn,  const char *err,
                        size_t errlen ) noexcept
{
  fprintf( stderr, "omm feed(net%u) shutdown: %.*s %.*s\n", this->net,
           (int) conn.get_peer_address_strlen(), conn.peer_address.buf,
           (int) errlen, err != NULL ? err : "" );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* fail-fast, like the rv nets */
}

bool
OmmFeedCB::on_omm_msg( const char *sub,  size_t sub_len,  uint32_t,
                       RwfMsg &msg ) noexcept
{
  return this->cache.on_omm_feed_msg( this->net, sub, sub_len, msg );
}

void
OmmSubNotify::on_sub( NotifySub &sub ) noexcept
{
  if ( sub.src_type == 'O' )
    this->cache.on_omm_sub( sub, this->net );
}

void
OmmSubNotify::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.src_type == 'O' )
    this->cache.on_omm_unsub( sub, this->net );
}

/* announce the cache's service in a provider-side source directory:
 * build the RWF directory map exactly the way a wire directory response
 * looks and run it through update_source_map(), which constructs the
 * OmmSource AND the sector routes subject matching resolves against
 * (TestPublish::add_test_source is the model; add_source() alone does
 * not build sector routes) */
bool
rvcache::announce_cache_service( OmmSourceDB &db,  MDDict *rdm_dict,
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
