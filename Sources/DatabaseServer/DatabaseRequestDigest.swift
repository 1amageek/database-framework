import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public enum DatabaseRequestDigest {
    public static let byteCount = 32

    public static func compute(
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
