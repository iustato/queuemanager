package storage

import (
	"encoding/binary"
)

// =======================
// Expiration index
// =======================

// expKey: 8 bytes big-endian expiresAtMs + guid
func makeExpKey(expiresAtMs int64, guid string) []byte {
	b := make([]byte, 8+len(guid))
	binary.BigEndian.PutUint64(b[:8], uint64(expiresAtMs))
	copy(b[8:], guid)
	return b
}

func parseExpKey(k []byte) (expiresAtMs int64, guid string, ok bool) {
	if len(k) < 8 {
		return 0, "", false
	}
	exp := int64(binary.BigEndian.Uint64(k[:8]))
	return exp, string(k[8:]), true
}

// =======================
// Processing lease index
// =======================

func makeProcKey(leaseUntilMs int64, guid string) []byte {
	b := make([]byte, 8+len(guid))
	binary.BigEndian.PutUint64(b[:8], uint64(leaseUntilMs))
	copy(b[8:], guid)
	return b
}

func parseProcKey(k []byte) (leaseUntilMs int64, guid string, ok bool) {
	if len(k) < 8 {
		return 0, "", false
	}
	lease := int64(binary.BigEndian.Uint64(k[:8]))
	return lease, string(k[8:]), true
}

// =======================
// Enqueue failed index
// =======================

// key: 8 bytes big-endian failedAtMs + guid
// enqFailKey: 8 bytes big-endian enqueueFailedAtMs + guid bytes
func makeEnqueueFailedKey(enqueueFailedAtMs int64, guid string) []byte {
	b := make([]byte, 8+len(guid))
	binary.BigEndian.PutUint64(b[:8], uint64(enqueueFailedAtMs))
	copy(b[8:], guid)
	return b
}

func parseEnqueueFailedKey(k []byte) (enqueueFailedAtMs int64, guid string, ok bool) {
	if len(k) < 8 {
		return 0, "", false
	}
	ts := int64(binary.BigEndian.Uint64(k[:8]))
	return ts, string(k[8:]), true
}