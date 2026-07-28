#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <rvcache/cache.h>
#include <raimd/json.h>
#include <raimd/md_msg.h>

using namespace rai;
using namespace md;

namespace rvcache {

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

/* parse the argv-separated net tuple "role proto [daemon [network
 * [service [wildcard]]]]" (f = argv slice, cnt = fields until the next
 * -flag; empty string fields fall back to the -d/-n/-s base) */
bool
parse_net_tuple( uint32_t idx,  const char **f,  uint32_t cnt,  NetDef &nd ) noexcept
{
#define F(N) (N<cnt?f[N]:"")
  nd.idx = idx;
  if ( ! parse_role_proto( nd, F(0), F(1) ) ) {
    return false;
  }
  const char *d = F(2),
             *n = F(3),
             *s = F(4),
             *w = F(5);
  if ( *d != '\0' ) nd.parm.daemon  = ::strdup( d );
  if ( *n != '\0' ) nd.parm.network = ::strdup( n );
  if ( *s != '\0' ) nd.parm.service = ::strdup( s );
  if ( *w != '\0' ) nd.wildcard     = ::strdup( w );
#undef F
  return true;
}

SeqPolicy
parse_seq( const char *s ) noexcept
{
  if ( ::strcmp( s, "strict" ) == 0 ) return SEQ_STRICT;
  if ( ::strcmp( s, "stamp" ) == 0 )  return SEQ_STAMP;
  return SEQ_OBSERVE;
}

/* json/yaml config file (-c).  Only the important knobs are CLI flags;
 * everything else is config-file only.  Top-level keys:
 *   daemon, network, service        base (d,n,s) triple        (-d -n -s)
 *   nets: [ {index, role, proto, daemon, network, service, wildcard} ]
 *                                   net attachments            (-<idx>)
 *   map_name: str                   shm msg cache              (-m)
 *   dict_path: str                  dictionary search path     (-p)
 *   replace_typeless_msgs: bool     replace typeless ticks     (-r)
 *   sequence_policy: observe|strict|stamp  seqno policy        (-Q)
 *   route_after_merge: bool         forward merged image       (-M)
 *   message_eviction_secs: int      idle entry eviction        (-x)
 *   pending_initial_secs: int       pending-initial timeout    (-P)
 *   accounting_file: str            accounting jsonl           (-A)
 *   quiet: bool                     quiet stats                (-q)
 *   verbose: bool                   submgr debug log           (-v)
 * a bare top-level array is accepted as the nets list.  Explicit CLI
 * flags override file values.  Net wildcard: sub nets filter the submgr
 * (sass2 and sass3 interest channels, start_subscriptions all=false);
 * feed nets subscribe _TIC.<wild>.> (sass2) or _SASS.<wild>.PUB (sass3)
 * instead of the full firehose */
static const char *
json_val_str( JsonValue *v ) noexcept
{
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

static const char *
json_str( JsonObject *o,  const char *name ) noexcept
{
  return json_val_str( o->find( name ) );
}

/* bool: json true/false, yaml/string true|yes|on|1 (anything else false),
 * or a number (nonzero = true); absent key leaves b untouched */
static void
json_bool( JsonObject *o,  const char *name,  bool &b ) noexcept
{
  JsonValue * v = o->find( name );
  if ( v == NULL )
    return;
  if ( v->type == JSON_BOOLEAN )
    b = v->to_bool()->val;
  else if ( v->type == JSON_STRING ) {
    JsonString * s = v->to_str();
    b = ( ( s->length == 4 && ::strncasecmp( s->val, "true", 4 ) == 0 ) ||
          ( s->length == 3 && ::strncasecmp( s->val, "yes", 3 ) == 0 ) ||
          ( s->length == 2 && ::strncasecmp( s->val, "on", 2 ) == 0 ) ||
          ( s->length == 1 && s->val[ 0 ] == '1' ) );
  }
  else {
    int64_t i;
    if ( v->to_int( i ) == 0 )
      b = ( i != 0 );
  }
}

static void
json_uint( JsonObject *o,  const char *name,  uint32_t &u ) noexcept
{
  JsonValue * v = o->find( name );
  int64_t     i;
  if ( v != NULL && v->to_int( i ) == 0 && i >= 0 )
    u = (uint32_t) i;
}

static bool
load_nets_array( const char *path,  JsonValue *root,  Config &cfg ) noexcept
{
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

bool
load_config( const char *path,  Config &cfg ) noexcept
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
  if ( root->type == JSON_ARRAY ) /* bare nets list */
    return load_nets_array( path, root, cfg );
  if ( root->type != JSON_OBJECT ) {
    fprintf( stderr, "%s: config is not an object or a nets array\n", path );
    return false;
  }
  JsonObject * o = root->to_obj();
  JsonValue  * n = o->find( "nets" );
  if ( n != NULL && ! load_nets_array( path, n, cfg ) )
    return false;
  const char * s;
  if ( (s = json_str( o, "daemon" ))  != NULL )
    cfg.base.daemon = s;
  if ( (s = json_str( o, "network" )) != NULL )
    cfg.base.network = s;
  if ( (s = json_str( o, "service" )) != NULL )
    cfg.base.service = s;
  if ( (s = json_str( o, "sequence_policy" )) != NULL )
    cfg.sequence_policy = parse_seq( s );
  if ( (s = json_str( o, "accounting_file" )) != NULL )
    cfg.accounting_file = s;
  if ( (s = json_str( o, "map_name" ))  != NULL )
    cfg.map_name = s;
  if ( (s = json_str( o, "dict_path" )) != NULL )
    cfg.dict_path = s;
  json_bool( o, "replace_typeless_msgs", cfg.replace_typeless_msgs );
  json_bool( o, "route_after_merge",     cfg.route_after_merge );
  json_bool( o, "quiet",                 cfg.quiet );
  json_bool( o, "verbose",               cfg.verbose );
  json_uint( o, "message_eviction_secs", cfg.message_eviction_secs );
  json_uint( o, "pending_initial_secs",  cfg.pending_initial_secs );
  return true;
}

} // namespace rvcache
