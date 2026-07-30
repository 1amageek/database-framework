@usableFromInline
internal enum MurmurHash3 {
    @usableFromInline
    struct Stream: Sendable {
        @usableFromInline
        var byteCount: Int = 0

        @usableFromInline
        var h1: UInt64

        @usableFromInline
        var h2: UInt64

        @usableFromInline
        var tailLow: UInt64 = 0

        @usableFromInline
        var tailHigh: UInt64 = 0

        @usableFromInline
        var tailCount: Int = 0

        @usableFromInline
        static let c1: UInt64 = 0x87c3_7b91_1142_53d5

        @usableFromInline
        static let c2: UInt64 = 0x4cf5_ad43_2745_937f

        @inlinable
        init(seed: UInt64 = 0) {
            self.h1 = seed
            self.h2 = seed
        }

        @usableFromInline
        mutating func update<S: Sequence>(_ bytes: S) where S.Element == UInt8 {
            for byte in bytes {
                update(byte: byte)
            }
        }

        @usableFromInline
        mutating func update(byte: UInt8) {
            if tailCount < 8 {
                tailLow |= UInt64(byte) << UInt64(tailCount * 8)
            } else {
                tailHigh |= UInt64(byte) << UInt64((tailCount - 8) * 8)
            }
            tailCount += 1
            byteCount += 1
            if tailCount == 16 {
                mixBlock(low: tailLow, high: tailHigh)
                tailLow = 0
                tailHigh = 0
                tailCount = 0
            }
        }

        @usableFromInline
        mutating func updateLittleEndian<T: FixedWidthInteger>(_ value: T) {
            var remaining = T.Magnitude(truncatingIfNeeded: value)
            for _ in 0..<MemoryLayout<T>.size {
                update(byte: UInt8(truncatingIfNeeded: remaining))
                remaining >>= 8
            }
        }

        @usableFromInline
        func finalize() -> UInt64 {
            var finalizedH1 = h1
            var finalizedH2 = h2

            if tailCount > 8 {
                var k2 = tailHigh
                k2 = k2 &* Self.c2
                k2 = Self.rotateLeft(k2, by: 33)
                k2 = k2 &* Self.c1
                finalizedH2 ^= k2
            }
            if tailCount > 0 {
                var k1 = tailLow
                k1 = k1 &* Self.c1
                k1 = Self.rotateLeft(k1, by: 31)
                k1 = k1 &* Self.c2
                finalizedH1 ^= k1
            }

            finalizedH1 ^= UInt64(byteCount)
            finalizedH2 ^= UInt64(byteCount)
            finalizedH1 = finalizedH1 &+ finalizedH2
            finalizedH2 = finalizedH2 &+ finalizedH1
            finalizedH1 = Self.finalMix(finalizedH1)
            finalizedH2 = Self.finalMix(finalizedH2)
            return finalizedH1 &+ finalizedH2
        }

        @usableFromInline
        mutating func mixBlock(low: UInt64, high: UInt64) {
            var k1 = low
            var k2 = high

            k1 = k1 &* Self.c1
            k1 = Self.rotateLeft(k1, by: 31)
            k1 = k1 &* Self.c2
            h1 ^= k1
            h1 = Self.rotateLeft(h1, by: 27)
            h1 = h1 &+ h2
            h1 = h1 &* 5 &+ 0x52dc_e729

            k2 = k2 &* Self.c2
            k2 = Self.rotateLeft(k2, by: 33)
            k2 = k2 &* Self.c1
            h2 ^= k2
            h2 = Self.rotateLeft(h2, by: 31)
            h2 = h2 &+ h1
            h2 = h2 &* 5 &+ 0x3849_5ab5
        }

        @usableFromInline
        static func rotateLeft(_ value: UInt64, by count: Int) -> UInt64 {
            (value << count) | (value >> (64 - count))
        }

        @usableFromInline
        static func finalMix(_ value: UInt64) -> UInt64 {
            var result = value
            result ^= result >> 33
            result = result &* 0xff51_afd7_ed55_8ccd
            result ^= result >> 33
            result = result &* 0xc4ce_b9fe_1a85_ec53
            result ^= result >> 33
            return result
        }
    }

    @usableFromInline
    static func hash(_ data: [UInt8], seed: UInt64 = 0) -> UInt64 {
        var stream = Stream(seed: seed)
        stream.update(data)
        return stream.finalize()
    }
}
