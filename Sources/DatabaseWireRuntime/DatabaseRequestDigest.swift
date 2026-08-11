import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

public enum DatabaseRequestDigest {
    public static let byteCount = 32

    public static func computeRequest(
        operation: DatabaseOperationIdentifier,
        target: DatabaseOperationTarget,
        prefix: ByteString = [],
        payload: ByteString
    ) -> ByteString {
        var accumulator = DatabaseRequestDigestAccumulator(
            operation: operation
        )
        switch target {
        case .database:
            accumulator.update(bigEndian: 0)
        case .base(let baseID):
            accumulator.update(bigEndian: 1)
            accumulator.update(bigEndian: UInt64(baseID.value.utf8.count))
            accumulator.update(utf8: baseID.value)
        case .composition(let compositionID):
            accumulator.update(bigEndian: 2)
            accumulator.update(
                bigEndian: UInt64(compositionID.value.utf8.count)
            )
            accumulator.update(utf8: compositionID.value)
        }
        accumulator.update(prefix)
        accumulator.update(payload)
        return accumulator.finalize()
    }

    static func compute(
        operation: DatabaseOperationIdentifier,
        prefix: ByteString = [],
        payload: ByteString
    ) -> ByteString {
        var accumulator = DatabaseRequestDigestAccumulator(
            operation: operation
        )
        accumulator.update(prefix)
        accumulator.update(payload)
        return accumulator.finalize()
    }

    static func compute(
        jobOperation: JobOperationIdentifier,
        prefix: ByteString = [],
        payload: ByteString
    ) -> ByteString {
        var accumulator = DatabaseRequestDigestAccumulator(
            jobOperation: jobOperation
        )
        accumulator.update(prefix)
        accumulator.update(payload)
        return accumulator.finalize()
    }
}
