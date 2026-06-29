// MurmurHash3.swift
// DatabaseEngine - MurmurHash3 x64 128-bit implementation
//
// Reference: Austin Appleby, MurmurHash3
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp

import Foundation

/// MurmurHash3 x64 128-bit implementation
///
/// Returns the lower 64 bits of the 128-bit hash.
/// This is a deterministic, non-cryptographic hash function suitable for:
/// - Hash tables
/// - Bloom filters
/// - Cardinality estimation (HyperLogLog)
/// - Data fingerprinting
///
/// **Properties**:
/// - Deterministic: same input → same output (always)
/// - Fast: optimized for 64-bit platforms
/// - Good distribution: passes SMHasher test suite
///
/// **Not for**: cryptographic purposes, password hashing
internal enum MurmurHash3 {

    /// Hash byte array to 64-bit value
    ///
    /// - Parameters:
    ///   - data: Bytes to hash
    ///   - seed: Optional seed value (default: 0)
    /// - Returns: 64-bit hash value
    @usableFromInline
    static func hash(_ data: [UInt8], seed: UInt64 = 0) -> UInt64 {
        let c1: UInt64 = 0x87c37b91114253d5
        let c2: UInt64 = 0x4cf5ad432745937f

        var h1 = seed
        var h2 = seed

        let nblocks = data.count / 16

        // Body: process 16-byte blocks
        for i in 0..<nblocks {
            let offset = i * 16

            var k1 = getBlock64(data, offset: offset)
            var k2 = getBlock64(data, offset: offset + 8)

            k1 = k1 &* c1
            k1 = rotl64(k1, 31)
            k1 = k1 &* c2
            h1 ^= k1

            h1 = rotl64(h1, 27)
            h1 = h1 &+ h2
            h1 = h1 &* 5 &+ 0x52dce729

            k2 = k2 &* c2
            k2 = rotl64(k2, 33)
            k2 = k2 &* c1
            h2 ^= k2

            h2 = rotl64(h2, 31)
            h2 = h2 &+ h1
            h2 = h2 &* 5 &+ 0x38495ab5
        }

        // Tail: handle remaining bytes
        let tail = nblocks * 16
        var k1: UInt64 = 0
        var k2: UInt64 = 0

        switch data.count & 15 {
        case 15: k2 ^= UInt64(data[tail + 14]) << 48; fallthrough
        case 14: k2 ^= UInt64(data[tail + 13]) << 40; fallthrough
        case 13: k2 ^= UInt64(data[tail + 12]) << 32; fallthrough
        case 12: k2 ^= UInt64(data[tail + 11]) << 24; fallthrough
        case 11: k2 ^= UInt64(data[tail + 10]) << 16; fallthrough
        case 10: k2 ^= UInt64(data[tail + 9]) << 8; fallthrough
        case 9:
            k2 ^= UInt64(data[tail + 8])
            k2 = k2 &* c2
            k2 = rotl64(k2, 33)
            k2 = k2 &* c1
            h2 ^= k2
            fallthrough
        case 8: k1 ^= UInt64(data[tail + 7]) << 56; fallthrough
        case 7: k1 ^= UInt64(data[tail + 6]) << 48; fallthrough
        case 6: k1 ^= UInt64(data[tail + 5]) << 40; fallthrough
        case 5: k1 ^= UInt64(data[tail + 4]) << 32; fallthrough
        case 4: k1 ^= UInt64(data[tail + 3]) << 24; fallthrough
        case 3: k1 ^= UInt64(data[tail + 2]) << 16; fallthrough
        case 2: k1 ^= UInt64(data[tail + 1]) << 8; fallthrough
        case 1:
            k1 ^= UInt64(data[tail])
            k1 = k1 &* c1
            k1 = rotl64(k1, 31)
            k1 = k1 &* c2
            h1 ^= k1
        default: break
        }

        // Finalization
        h1 ^= UInt64(data.count)
        h2 ^= UInt64(data.count)

        h1 = h1 &+ h2
        h2 = h2 &+ h1

        h1 = fmix64(h1)
        h2 = fmix64(h2)

        h1 = h1 &+ h2
        // h2 = h2 &+ h1  // Not needed, we only return h1

        return h1
    }

    /// Read 64-bit little-endian block from data
    @inline(__always)
    private static func getBlock64(_ data: [UInt8], offset: Int) -> UInt64 {
        var result: UInt64 = 0
        for i in 0..<8 {
            result |= UInt64(data[offset + i]) << (i * 8)
        }
        return result
    }

    /// 64-bit left rotation
    @inline(__always)
    private static func rotl64(_ x: UInt64, _ r: Int) -> UInt64 {
        (x << r) | (x >> (64 - r))
    }

    /// MurmurHash3 64-bit finalizer (avalanche)
    @inline(__always)
    private static func fmix64(_ k: UInt64) -> UInt64 {
        var h = k
        h ^= h >> 33
        h = h &* 0xff51afd7ed558ccd
        h ^= h >> 33
        h = h &* 0xc4ceb9fe1a85ec53
        h ^= h >> 33
        return h
    }
}
