import DatabaseTypes

/// Deterministic cache discriminator for canonical RDF bytes.
///
/// A fingerprint only selects a cache bucket. Callers must compare canonical
/// bytes before treating two values as equal because hash collisions remain
/// possible by definition.
package struct RDFTermStorageFingerprint: Sendable, Hashable, Equatable {
    package let high: UInt64
    package let low: UInt64
    package let byteCount: Int

    init(_ bytes: UnsafeRawBufferPointer) {
        var high: UInt64 = 0xCBF2_9CE4_8422_2325
        var low: UInt64 = 0x9E37_79B1_85EB_CA87
        for byte in bytes {
            high ^= UInt64(byte)
            high &*= 0x0000_0100_0000_01B3
            low ^= UInt64(byte) &+ 0x9E37_79B9
            low = (low << 13) | (low >> 51)
            low &*= 0xC2B2_AE3D_27D4_EB4F
        }
        self.high = high
        self.low = low
        self.byteCount = bytes.count
    }
}
