package storage

import (
	"crypto/rand"
	"time"
)

// crockford is the Crockford base32 alphabet used by ULIDs (no I, L, O, U).
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newULID returns a 26-character ULID: a 48-bit millisecond timestamp followed by 80 bits
// of randomness, Crockford base32. It is lexicographically sortable by creation time and
// globally unique without coordination, so a segment ID never collides across shards — the
// property §10 needs for a segment to be relocatable with no global ID space.
func newULID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2] = byte(ms>>40), byte(ms>>32), byte(ms>>24)
	b[3], b[4], b[5] = byte(ms>>16), byte(ms>>8), byte(ms)
	rand.Read(b[6:]) // 80 bits of randomness
	return encodeULID(b)
}

// encodeULID renders the 128-bit value as 26 Crockford base32 characters using the ULID
// bit layout (MSB first; the leading character carries the top 2 bits).
func encodeULID(b [16]byte) string {
	return string([]byte{
		crockford[(b[0]&224)>>5],
		crockford[b[0]&31],
		crockford[(b[1]&248)>>3],
		crockford[((b[1]&7)<<2)|((b[2]&192)>>6)],
		crockford[(b[2]&62)>>1],
		crockford[((b[2]&1)<<4)|((b[3]&240)>>4)],
		crockford[((b[3]&15)<<1)|((b[4]&128)>>7)],
		crockford[(b[4]&124)>>2],
		crockford[((b[4]&3)<<3)|((b[5]&224)>>5)],
		crockford[b[5]&31],
		crockford[(b[6]&248)>>3],
		crockford[((b[6]&7)<<2)|((b[7]&192)>>6)],
		crockford[(b[7]&62)>>1],
		crockford[((b[7]&1)<<4)|((b[8]&240)>>4)],
		crockford[((b[8]&15)<<1)|((b[9]&128)>>7)],
		crockford[(b[9]&124)>>2],
		crockford[((b[9]&3)<<3)|((b[10]&224)>>5)],
		crockford[b[10]&31],
		crockford[(b[11]&248)>>3],
		crockford[((b[11]&7)<<2)|((b[12]&192)>>6)],
		crockford[(b[12]&62)>>1],
		crockford[((b[12]&1)<<4)|((b[13]&240)>>4)],
		crockford[((b[13]&15)<<1)|((b[14]&128)>>7)],
		crockford[(b[14]&124)>>2],
		crockford[((b[14]&3)<<3)|((b[15]&224)>>5)],
		crockford[b[15]&31],
	})
}
