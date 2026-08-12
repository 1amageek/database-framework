import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package enum DatabasePersistentJobDigest {
    private static let planDomain: ByteString = [0x4a, 0x50, 0x4c, 0x4e]
    private static let specificationDomain: ByteString = [0x4a, 0x53, 0x50, 0x43]
    private static let chunkDomain: ByteString = [0x4a, 0x43, 0x48, 0x4b]

    package static func plan(
        operation: JobOperationIdentifier,
        payload: ByteString
    ) -> ByteString {
        DatabaseRequestDigest.compute(
            jobOperation: operation,
            prefix: planDomain,
            payload: payload
        )
    }

    package static func specification(
        operation: JobOperationIdentifier,
        payload: ByteString
    ) -> ByteString {
        DatabaseRequestDigest.compute(
            jobOperation: operation,
            prefix: specificationDomain,
            payload: payload
        )
    }

    package static func result(
        operation: JobOperationIdentifier,
        target: DatabaseOperationTarget,
        payload: ByteString
    ) -> JobResultDigest {
        var accumulator = JobResultDigestAccumulator(
            operation: operation,
            target: target
        )
        accumulator.update(payload)
        return accumulator.finalize()
    }

    package static func chunk(
        operation: JobOperationIdentifier,
        index: UInt32,
        payload: ByteString
    ) -> ByteString {
        var accumulator = DatabaseRequestDigestAccumulator(
            jobOperation: operation
        )
        accumulator.update(chunkDomain)
        accumulator.update(bigEndian: UInt64(index))
        accumulator.update(payload)
        return accumulator.finalize()
    }
}
