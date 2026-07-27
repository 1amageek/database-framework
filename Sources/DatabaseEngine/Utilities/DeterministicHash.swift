import DatabaseTypes
import StorageKit

internal enum DeterministicHash {
    @usableFromInline
    static func hash<T: HashBytesConvertible>(_ value: T) -> UInt64 {
        var stream = MurmurHash3.Stream()
        value.appendHashBytes(to: &stream)
        return stream.finalize()
    }

    @usableFromInline
    static func hash(bytes: [UInt8]) -> UInt64 {
        var stream = MurmurHash3.Stream()
        stream.update(bytes)
        return stream.finalize()
    }
}

internal struct DeterministicHasher: Sendable {
    @usableFromInline
    var stream = MurmurHash3.Stream()

    @inlinable
    init() {}

    @usableFromInline
    mutating func combine<T: HashBytesConvertible>(_ value: T) {
        value.appendHashBytes(to: &stream)
    }

    @usableFromInline
    mutating func combine(bytes: [UInt8]) {
        stream.update(bytes)
    }

    @usableFromInline
    func finalize() -> UInt64 {
        stream.finalize()
    }

    @usableFromInline
    func finalizeToBytes() -> ByteString {
        let hash = finalize()
        return ByteString.copying(count: MemoryLayout<UInt64>.size) { output in
            for offset in 0..<MemoryLayout<UInt64>.size {
                output[offset] = UInt8(
                    truncatingIfNeeded: hash >> UInt64(offset * 8)
                )
            }
        }
    }
}
