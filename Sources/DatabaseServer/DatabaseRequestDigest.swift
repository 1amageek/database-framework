import DatabaseValue
import DatabaseWire

public enum DatabaseRequestDigest {
    public static let byteCount = 32

    public static func compute(
        operation: DatabaseOperationIdentifier,
        prefix: DatabaseBytes = [],
        payload: DatabaseBytes
    ) -> DatabaseBytes {
        var accumulator = DatabaseRequestDigestAccumulator(
            operation: operation
        )
        accumulator.update(prefix)
        accumulator.update(payload)
        return accumulator.finalize()
    }

    static func compute(
        jobOperation: DatabaseJobOperationIdentifier,
        prefix: DatabaseBytes = [],
        payload: DatabaseBytes
    ) -> DatabaseBytes {
        var accumulator = DatabaseRequestDigestAccumulator(
            jobOperation: jobOperation
        )
        accumulator.update(prefix)
        accumulator.update(payload)
        return accumulator.finalize()
    }
}
