#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rvcache/cache.h>
#include <raimd/md_dict.h>
#include <raimd/rv_msg.h>
#include <raimd/sass.h>
#include <raikv/key_hash.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace rvcache;

void
CacheTab::ensure_scratch( size_t n ) noexcept
{
  if ( n > this->scratch_len ) {
    size_t sz = this->scratch_len == 0 ? 1024 : this->scratch_len;
    while ( sz < n )
      sz *= 2;
    this->scratch = (char *) ::realloc( this->scratch, sz );
    this->scratch_len = sz;
  }
}

void
CacheTab::ensure_scratch2( size_t n ) noexcept
{
  if ( n > this->scratch2_len ) {
    size_t sz = this->scratch2_len == 0 ? 1024 : this->scratch2_len;
    while ( sz < n )
      sz *= 2;
    this->scratch2 = (char *) ::realloc( this->scratch2, sz );
    this->scratch2_len = sz;
  }
}

CacheEntry *
CacheTab::find( const char *subj,  size_t len ) noexcept
{
  uint32_t h = kv_crc_c( subj, len, 0 );
  RouteLoc loc;
  return this->tab.find( h, subj, len, loc );
}

CacheEntry *
CacheTab::upsert( const char *subj,  size_t len,  bool &is_new ) noexcept
{
  uint32_t     h = kv_crc_c( subj, len, 0 );
  RouteLoc     loc;
  CacheEntry * e = this->tab.upsert( h, subj, len, loc );
  is_new = ( e != NULL && loc.is_new );
  if ( is_new )
    e->init( this->next_id++ );
  return e;
}

/* MSG_TYPE normalization: force MSG_TYPE to be the first field with a
 * fixed-width integer type, so that serving an initial or a snapshot is a
 * find-first-field + MDFieldIter::update() in-place stamp — never a message
 * rebuild.  Returns true when bytes/len/enc were repointed at scratch2. */
bool
CacheTab::normalize_msg_type( const void *&bytes,  size_t &len,
                              uint32_t &enc ) noexcept
{
  MDMsgMem mem;
  MDMsg  * m = MDMsg::unpack( (void *) bytes, 0, len, enc, NULL, mem );
  if ( m == NULL )
    return false;
  MDFieldIter * it = NULL;
  if ( m->get_field_iter( it ) != 0 )
    return false;
  MDReference mref;
  if ( it->find( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, mref ) != 0 )
    return false; /* typeless: store as-is, never inject the field */

  /* already normalized?  leading field, fixed-width int, stampable */
  bool ok_inplace = false;
  if ( mref.ftype == MD_UINT || mref.ftype == MD_INT ) {
    MDName      nm;
    MDReference fr;
    if ( it->first() == 0 && it->get_name( nm ) == 0 &&
         nm.equals( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN ) &&
         it->get_reference( fr ) == 0 &&
         ( fr.ftype == MD_UINT || fr.ftype == MD_INT ) &&
         fr.fsize >= 1 && fr.fsize <= 8 )
      ok_inplace = true;
  }
  if ( ok_inplace )
    return false;

  /* rebuild: MSG_TYPE first as uint16, then every other field in order */
  uint16_t t = get_uint<uint16_t>( mref );
  this->ensure_scratch2( len * 2 + 256 );
  RvMsgWriter w( mem, this->scratch2, this->scratch2_len );
  w.append_uint( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, t );
  if ( it->first() == 0 ) {
    do {
      MDName      nm;
      MDReference fr;
      if ( it->get_name( nm ) != 0 )
        continue;
      if ( nm.equals( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN ) )
        continue;
      if ( it->get_reference( fr ) == 0 )
        w.append_ref( nm.fname, nm.fnamelen, fr );
    } while ( it->next() == 0 );
  }
  size_t out = w.update_hdr();
  if ( w.err != 0 )
    return false;      /* overflow/unwritable field: keep original bytes */
  if ( (char *) w.buf != this->scratch2 ) {
    /* writer resized into MDMsgMem (dies with mem): copy out to scratch2 */
    this->ensure_scratch2( out );
    ::memcpy( this->scratch2, w.buf, out );
  }
  bytes = this->scratch2;
  len   = out;
  enc   = RVMSG_TYPE_ID;
  return true;
}

void
CacheTab::set_image( CacheEntry &e,  const void *bytes,  size_t len,
                     uint32_t enc ) noexcept
{
  this->normalize_msg_type( bytes, len, enc );
  if ( e.image != NULL ) {
    this->image_bytes -= e.image_len;
    ::free( e.image );
  }
  e.image = ::malloc( len );
  ::memcpy( e.image, bytes, len );
  e.image_len = len;
  e.image_enc = enc;
  this->image_bytes += len;
}

/* field-merge: rebuild image from old image, overwriting fields present in
 * the update and appending fields only in the update.  Field name is the
 * merge key (RvMsgWriter into fresh scratch, then swap via set_image). */
size_t
CacheTab::merge( CacheEntry &e,  const void *upd,  size_t upd_len,
                 uint32_t upd_enc ) noexcept
{
  /* no existing image -> update becomes the image outright */
  if ( e.image == NULL ) {
    this->set_image( e, upd, upd_len, upd_enc );
    return e.image_len;
  }
  MDMsgMem mem;
  MDMsg * oldm = MDMsg::unpack( e.image, 0, e.image_len, e.image_enc,
                               NULL, mem );
  MDMsg * newm = MDMsg::unpack( (void *) upd, 0, upd_len, upd_enc, NULL, mem );
  if ( oldm == NULL || newm == NULL ) {
    /* cannot parse one side: fall back to replace */
    this->set_image( e, upd, upd_len, upd_enc );
    return e.image_len;
  }
  MDFieldIter * oit = NULL,
              * nit = NULL;
  if ( oldm->get_field_iter( oit ) != 0 || newm->get_field_iter( nit ) != 0 ) {
    this->set_image( e, upd, upd_len, upd_enc );
    return e.image_len;
  }
  this->ensure_scratch( e.image_len + upd_len + 256 );
  RvMsgWriter w( mem, this->scratch, this->scratch_len );

  /* pass 1: existing field order preserved; overwrite value if in update */
  if ( oit->first() == 0 ) {
    do {
      MDName nm;
      MDReference mref;
      if ( oit->get_name( nm ) != 0 )
        continue;
      if ( nit->find( nm, mref ) == 0 )
        w.append_ref( nm.fname, nm.fnamelen, mref );   /* updated value */
      else if ( oit->get_reference( mref ) == 0 )
        w.append_ref( nm.fname, nm.fnamelen, mref );   /* retained value */
    } while ( oit->next() == 0 );
  }
  /* pass 2: append fields that appear only in the update */
  if ( nit->first() == 0 ) {
    do {
      MDName nm;
      MDReference mref, tmp;
      if ( nit->get_name( nm ) != 0 )
        continue;
      if ( oit->find( nm, tmp ) != 0 ) { /* not in old image -> append */
        if ( nit->get_reference( mref ) == 0 )
          w.append_ref( nm.fname, nm.fnamelen, mref );
      }
    } while ( nit->next() == 0 );
  }
  size_t out_len = w.update_hdr();
  if ( w.err != 0 ) {
    /* writer overflow/error: fall back to replace so cache stays coherent */
    this->set_image( e, upd, upd_len, upd_enc );
    return e.image_len;
  }
  this->set_image( e, w.buf, out_len, RVMSG_TYPE_ID );
  return e.image_len;
}

void
CacheTab::evict( const char *subj,  size_t len ) noexcept
{
  uint32_t h = kv_crc_c( subj, len, 0 );
  RouteLoc loc;
  CacheEntry * e = this->tab.find( h, subj, len, loc );
  if ( e != NULL ) {
    if ( e->image != NULL ) {
      this->image_bytes -= e->image_len;
      ::free( e->image );
      e->image = NULL;
      e->image_len = 0;
    }
    this->tab.remove( loc );
  }
}
