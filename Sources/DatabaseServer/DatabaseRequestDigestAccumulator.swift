import DatabaseDigest
import DatabaseValue
import DatabaseWire

struct DatabaseRequestDigestAccumulator {
    private static let jobIdentifierDomain: DatabaseBytes = [
        0x4a, 0x4f, 0x50, 0x49,
    ]

    private var hasher: SHA256Accumulator

    init(operation: DatabaseOperationIdentifier) {
        var hasher = SHA256Accumulator()
        var operationValue = operation.rawValue.bigEndian
        withUnsafeBytes(of: &operationValue) {
            hasher.update($0)
        }
        self.hasher = hasher
    }

    init(jobOperation: DatabaseJobOperationIdentifier) {
        var hasher = SHA256Accumulator()
        Self.jobIdentifierDomain.withUnsafeBytes {
            hasher.update($0)
        }
        var family = jobOperation.family.rawValue.bigEndian
        withUnsafeBytes(of: &family) {
            hasher.update($0)
        }
        guard let kindByteCount = UInt32(
            exactly: jobOperation.kind.utf8.count
        ) else {
            preconditionFailure("Job operation kind exceeds UInt32")
        }
        var encodedKindByteCount = kindByteCount.bigEndian
        withUnsafeBytes(of: &encodedKindByteCount) {
            hasher.update($0)
        }
        let usedContiguousStorage = jobOperation.kind.utf8
            .withContiguousStorageIfAvailable { bytes -> Bool in
                hasher.update(UnsafeRawBufferPointer(bytes))
                return true
            } ?? false
        if !usedContiguousStorage {
            for byte in jobOperation.kind.utf8 {
                withUnsafeBytes(of: byte) {
                    hasher.update($0)
                }
            }
        }
        self.hasher = hasher
    }

    mutating func update(_ bytes: DatabaseBytes) {
        bytes.withUnsafeBytes {
            hasher.update($0)
        }
    }

    mutating func update(bigEndian value: UInt64) {
        var encoded = value.bigEndian
        withUnsafeBytes(of: &encoded) {
            hasher.update($0)
        }
    }

    mutating func update(utf8 value: String) {
        let usedContiguousStorage = value.utf8.withContiguousStorageIfAvailable {
            bytes -> Bool in
            hasher.update(UnsafeRawBufferPointer(bytes))
            return true
        } ?? false
        guard !usedContiguousStorage else {
            return
        }
        for byte in value.utf8 {
            withUnsafeBytes(of: byte) {
                hasher.update($0)
            }
        }
    }

    consuming func finalize() -> DatabaseBytes {
        hasher.finalize()
    }
}
