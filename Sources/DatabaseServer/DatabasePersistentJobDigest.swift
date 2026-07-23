import DatabaseValue
import DatabaseWire

enum DatabasePersistentJobDigest {
    private static let planDomain: DatabaseBytes = [0x4a, 0x50, 0x4c, 0x4e]
    private static let specificationDomain: DatabaseBytes = [0x4a, 0x53, 0x50, 0x43]
    private static let chunkDomain: DatabaseBytes = [0x4a, 0x43, 0x48, 0x4b]

    static func plan(
        operation: DatabaseJobOperationIdentifier,
        payload: DatabaseBytes
    ) -> DatabaseBytes {
        DatabaseRequestDigest.compute(
            jobOperation: operation,
            prefix: planDomain,
            payload: payload
        )
    }

    static func specification(
        operation: DatabaseJobOperationIdentifier,
        payload: DatabaseBytes
    ) -> DatabaseBytes {
        DatabaseRequestDigest.compute(
            jobOperation: operation,
            prefix: specificationDomain,
            payload: payload
        )
    }

    static func result(
        operation: DatabaseJobOperationIdentifier,
        payload: DatabaseBytes
    ) -> DatabaseJobResultDigest {
        var accumulator = DatabaseJobResultDigestAccumulator(
            operation: operation
        )
        accumulator.update(payload)
        return accumulator.finalize()
    }

    static func chunk(
        operation: DatabaseJobOperationIdentifier,
        index: UInt32,
        payload: DatabaseBytes
    ) -> DatabaseBytes {
        var accumulator = DatabaseRequestDigestAccumulator(
            jobOperation: operation
        )
        accumulator.update(chunkDomain)
        accumulator.update(bigEndian: UInt64(index))
        accumulator.update(payload)
        return accumulator.finalize()
    }
}
