/* map_bench: CacheTab::merge() on an RWF Map image, no network, no poll
 * loop -- the BookRoute model from omm/book_pub generates a refresh and a
 * deterministic stream of updates, and hardware counters (perf_event_open)
 * are read around the merge loop so the numbers are per merge, not per
 * process.  Heap or shm (-m), in-place or rebuild (-M), index or scan (-i).
 *
 *   map_bench -L 25 -O 8 -n 5000                    # heap, in-place, index
 *   map_bench -L 25 -O 8 -n 5000 -m sysv:rvbook.shm # shm
 *   map_bench -L 25 -O 8 -n 5000 -M rebuild
 *   map_bench -L 25 -O 8 -n 5000 -i 0               # in-place, linear scan
 *   map_bench -L 25 -O 8 -n 5000 -k 200             # 200 books, round robin
 *                                                    (cold entries / images)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <pthread.h>
#include <linux/perf_event.h>
#include <rvcache/cache.h>
#include <omm/book_pub.h>
#include <raimd/rwf_msg.h>
#include <raimd/rwf_writer.h>
#include <raikv/util.h>
#include <raikv/mainloop.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;
using namespace rvcache;

static const MDFid ORDER_ID_FID = 3426;
static const char * mkt_mkr[ 4 ] = { "MM01", "MM02", "MM03", "MM04" };

/* the same entry shape BookPublish::add_mbo_order writes */
static void
add_order( RwfMapWriter &map,  RwfMapAction action,  BookOrder &o )
{
  char   id[ 16 ];
  size_t id_len = ::snprintf( id, sizeof( id ), "%u", o.id );
  MDReference key( id, id_len, MD_OPAQUE, md_endian );
  if ( action == MAP_DELETE_ENTRY ) {
    map.add_delete_entry( key );
    return;
  }
  MDDecimal prc ( o.prc, MD_DEC_LOGn10_2 ),
            size( o.size, MD_DEC_INTEGER );
  RwfFieldListWriter & fl = map.add_field_list( action, key );
  fl.append_decimal( "ORDER_PRC" , prc )
    .append_decimal( "ORDER_SIZE", size )
    .append_uint   ( "QUOTIM_MS" , o.time_ms );
  if ( action == MAP_ADD_ENTRY )
    fl.append_uint  ( "ORDER_SIDE", o.side )
      .append_string( "MKT_MKR_ID", mkt_mkr[ o.mmid & 3 ] );
  fl.end_entry();
}

static size_t
build_refresh( MDMsgMem &mem,  MDDict *dict,  BookRoute &rt,  void *buf,
               size_t len )
{
  RwfMapWriter map( mem, dict, buf, len, MD_OPAQUE, RWF_FIELD_LIST,
                    ORDER_ID_FID, rt.order_cnt );
  map.add_summary_field_list()
     .append_uint  ( "PROD_PERM" , 1 )
     .append_string( "DSPLY_NAME", 10, "BENCH", 5 )
     .append_uint  ( "CURRENCY"  , 840 )
     .end_summary();
  for ( uint32_t i = 0; i < rt.order_cnt; i++ )
    add_order( map, MAP_ADD_ENTRY, rt.orders[ i ] );
  return map.end_map();
}

static size_t
build_update( MDMsgMem &mem,  MDDict *dict,  BookRoute &rt,  void *buf,
              size_t len )
{
  RwfMapWriter map( mem, dict, buf, len, MD_OPAQUE, RWF_FIELD_LIST,
                    ORDER_ID_FID, 0 );
  for ( uint32_t i = 0; i < rt.event_cnt; i++ )
    add_order( map, (RwfMapAction) rt.events[ i ].action, rt.events[ i ].ord );
  return map.end_map();
}

/* hardware counters around the loop */
struct Counter {
  int      fd;
  uint64_t start, stop;
  Counter() : fd( -1 ), start( 0 ), stop( 0 ) {}
  bool open( uint32_t type,  uint64_t config ) {
    struct perf_event_attr a;
    ::memset( &a, 0, sizeof( a ) );
    a.type = type; a.size = sizeof( a ); a.config = config;
    a.disabled = 1; a.exclude_kernel = 1; a.exclude_hv = 1;
    this->fd = (int) ::syscall( __NR_perf_event_open, &a, 0, -1, -1, 0 );
    return this->fd >= 0;
  }
  void read_to( uint64_t &v ) {
    if ( this->fd >= 0 && ::read( this->fd, &v, sizeof( v ) ) != sizeof( v ) )
      v = 0;
  }
  void begin( void ) { if ( fd >= 0 ) { ::ioctl( fd, PERF_EVENT_IOC_RESET, 0 );
                                        ::ioctl( fd, PERF_EVENT_IOC_ENABLE, 0 ); } }
  void end( void )   { if ( fd >= 0 ) { ::ioctl( fd, PERF_EVENT_IOC_DISABLE, 0 );
                                        this->read_to( this->stop ); } }
  uint64_t val( void ) const { return this->stop; }
};

int
main( int argc,  const char *argv[] )
{
  const char * dict_path = MainLoopVars::get_arg( argc, argv, 1, "-p",
                              "/home/chris/rai/RaiCore/rmds-config" ),
             * map_name  = MainLoopVars::get_arg( argc, argv, 1, "-m", NULL ),
             * mode      = MainLoopVars::get_arg( argc, argv, 1, "-M", "inplace" );
  int levels   = MainLoopVars::int_arg( argc, argv, 1, "-L", "25", NULL ),
      per_lvl  = MainLoopVars::int_arg( argc, argv, 1, "-O", "8", NULL ),
      ticks    = MainLoopVars::int_arg( argc, argv, 1, "-n", "5000", NULL ),
      idx_min  = MainLoopVars::int_arg( argc, argv, 1, "-i", "1", NULL ),
      seed     = MainLoopVars::int_arg( argc, argv, 1, "-s", "1", NULL ),
      nbooks   = MainLoopVars::int_arg( argc, argv, 1, "-k", "1", NULL );
  bool quiet   = MainLoopVars::get_arg( argc, argv, 0, "-q", NULL ) != NULL;
  if ( MainLoopVars::get_arg( argc, argv, 0, "-h", NULL ) != NULL ) {
    fprintf( stderr, "%s [-p dict] [-m map] [-M inplace|rebuild] [-i index_min]"
             " [-L levels] [-O orders/level] [-n ticks] [-k books] [-s seed]"
             " [-q]\n", argv[ 0 ] );
    return 1;
  }

  MDMsgDict dict;
  dict.load( dict_path, false );
  if ( dict.rdm_dict == NULL ) {
    fprintf( stderr, "no RDM dictionary at %s\n", dict_path );
    return 1;
  }
  CacheTab cache( dict );
  EvShm    shm( "map_bench" );
  if ( map_name != NULL ) {
    if ( shm.open( map_name, 0 ) != 0 ) {
      fprintf( stderr, "shm open %s failed\n", map_name );
      return 1;
    }
    cache.init_shm( shm );
  }
  cache.map_inplace   = ( ::strcmp( mode, "rebuild" ) != 0 );
  cache.map_index_min = (uint32_t) idx_min;

  /* the books: deterministic (hash from the seed, fixed tick count);
   * -k > 1 interleaves them round robin so every merge lands on an
   * entry / image the previous merges pushed out of L1 */
  if ( nbooks < 1 ) nbooks = 1;
  BookShape    shape( (uint32_t) levels, (uint32_t) per_lvl );
  BookRoute ** rt = (BookRoute **) ::malloc( sizeof( void * ) * nbooks );
  CacheEntry ** ce = (CacheEntry **) ::malloc( sizeof( void * ) * nbooks );
  char       ** sub = (char **) ::malloc( sizeof( void * ) * nbooks );
  MDMsgMem mem;
  size_t   cap = (size_t) shape.max_orders * 64 + 64 * 1024;
  void   * buf = mem.make( cap );
  uint32_t total_orders = 0;
  for ( int b = 0; b < nbooks; b++ ) {
    sub[ b ] = (char *) ::malloc( 32 );
    size_t sl = ::snprintf( sub[ b ], 32, "RSF.MBO.BENCH%d.X", b );
    rt[ b ] = (BookRoute *) ::calloc( 1, sizeof( BookRoute ) + 32 );
    rt[ b ]->hash = (uint32_t) ( seed + b ) * 0x9e3779b1U;
    rt[ b ]->len  = (uint16_t) sl;
    rt[ b ]->alloc( shape );
    rt[ b ]->init( 1000000000ULL, MARKET_BY_ORDER_DOMAIN );
    size_t ref_len = build_refresh( mem, dict.rdm_dict, *rt[ b ], buf, cap );
    bool is_new;
    ce[ b ] = cache.upsert( sub[ b ], sl, is_new );
    cache.set_image( *ce[ b ], buf, ref_len, RWF_MAP_TYPE_ID );
  }

  /* pre-generate the updates so the loop is merge only; tick i is book
   * i % nbooks */
  uint8_t ** upd     = (uint8_t **) ::malloc( sizeof( void * ) * ticks );
  size_t   * upd_len = (size_t *) ::malloc( sizeof( size_t ) * ticks );
  size_t     upd_bytes = 0;
  uint32_t   adds = 0, upds = 0, dels = 0;
  for ( int i = 0; i < ticks; i++ ) {
    BookRoute & r = *rt[ i % nbooks ];
    r.tick( 1000000000ULL + (uint64_t) i * 20000000ULL );
    for ( uint32_t k = 0; k < r.event_cnt; k++ ) {
      switch ( r.events[ k ].action ) {
        case MAP_ADD_ENTRY:    adds++; break;
        case MAP_UPDATE_ENTRY: upds++; break;
        default:               dels++; break;
      }
    }
    MDMsgMem tmp;
    size_t n = build_update( tmp, dict.rdm_dict, r, buf, cap );
    upd[ i ] = (uint8_t *) ::malloc( n );
    ::memcpy( upd[ i ], buf, n );
    upd_len[ i ] = n;
    upd_bytes += n;
  }
  for ( int b = 0; b < nbooks; b++ ) /* the books as they end up */
    total_orders += rt[ b ]->order_cnt;

  Counter ins, cyc, l1m, brm;
  bool have_hw = ins.open( PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS );
  cyc.open( PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES );
  l1m.open( PERF_TYPE_HW_CACHE, PERF_COUNT_HW_CACHE_L1D |
            ( PERF_COUNT_HW_CACHE_OP_READ << 8 ) |
            ( PERF_COUNT_HW_CACHE_RESULT_MISS << 16 ) );
  brm.open( PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES );

  /* warm: one merge per book so first-touch costs are not measured */
  int first = nbooks < ticks ? nbooks : ticks;
  for ( int i = 0; i < first; i++ )
    cache.merge( *ce[ i % nbooks ], upd[ i ], upd_len[ i ], RWF_MAP_TYPE_ID );

  ins.begin(); cyc.begin(); l1m.begin(); brm.begin();
  uint64_t t0 = current_monotonic_time_ns();
  for ( int i = first; i < ticks; i++ ) {
    if ( cache.merge( *ce[ i % nbooks ], upd[ i ], upd_len[ i ],
                      RWF_MAP_TYPE_ID ) == 0 ) {
      fprintf( stderr, "merge %d failed\n", i );
      return 2;
    }
  }
  uint64_t t1 = current_monotonic_time_ns();
  ins.end(); cyc.end(); l1m.end(); brm.end();
  double n = (double) ( ticks - first );

  /* every image must be a valid map with its book's live count */
  size_t   raw_total = 0, live_total = 0;
  uint32_t live_entries = 0, bad = 0;
  for ( int b = 0; b < nbooks; b++ ) {
    void   * img = NULL; size_t img_len = 0; uint32_t img_enc = 0;
    if ( ! cache.get_image( *ce[ b ], img, img_len, img_enc ) ) {
      bad++;
      continue;
    }
    raw_total += img_len;
    cache.live_image( *ce[ b ], img, img_len, img_enc );
    live_total += img_len;
    MDMsgMem   vm;
    RwfMsg   * m = RwfMsg::unpack_map( img, 0, img_len, RWF_MAP_TYPE_ID,
                                       dict.dict, vm );
    uint32_t live = ( m != NULL ? m->map.entry_cnt : 0 );
    live_entries += live;
    if ( live != rt[ b ]->order_cnt )
      bad++;
  }

  printf( "%s %s%s: %d book%s, %u orders (MBO), %d ticks (%u add %u upd"
          " %u del), images %zu B raw / %zu B live, %u entries %s\n",
          cache.shm_mode() ? "shm " : "heap",
          cache.map_inplace ? "inplace" : "rebuild",
          cache.map_inplace ? ( idx_min == 0 ? "+scan" : "+index" ) : "",
          nbooks, nbooks == 1 ? "" : "s", total_orders, ticks, adds, upds,
          dels, raw_total, live_total, live_entries,
          bad == 0 ? "OK" : "MISMATCH" );
  printf( "  %.2f us/merge wall", (double) ( t1 - t0 ) / 1000.0 / n );
  if ( have_hw )
    printf( "  %.0f instructions  %.0f cycles  %.1f L1d-miss  %.1f br-miss"
            "  (per merge, user)",
            (double) ins.val() / n, (double) cyc.val() / n,
            (double) l1m.val() / n, (double) brm.val() / n );
  printf( "\n  merges: inplace=%llu rebuild=%llu compactions=%llu"
          " index_rebuilds=%llu avg=%.2fus\n",
          (long long) cache.stats_inplace, (long long) cache.stats_rebuild,
          (long long) cache.stats_compactions,
          (long long) cache.stats_index_rebuilds,
          (double) cache.stats_map_ns / 1000.0 /
          (double) ( cache.stats_inplace + cache.stats_rebuild ) );
  if ( cache.shm_mode() )
    for ( int b = 0; b < nbooks; b++ )
      cache.evict( sub[ b ], ::strlen( sub[ b ] ) );
  if ( ! quiet && bad != 0 )
    return 3;
  return 0;
}
