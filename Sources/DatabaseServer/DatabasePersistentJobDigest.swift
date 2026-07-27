import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

enum DatabasePersistentJobDigest {
    private static let planDomain: ByteString = [0x4a, 0x50, 0x4c, 0x4e]
    private static let specificationDomain: ByteString = [0x4a, 0x53, 0x50, 0x43]
    private static let chunkDomain: ByteString = [0x4a, 0x43, 0x48, 0x4b]

    static func plan(
        operation: JobOperationIdentifier,
        payload: ByteString
    ) -> ByteString {
        DatabaseRequestDigest.compute(
            jobOperation: operation,
            prefix: planDomain,
            payload: payload
        )
    }

    static func specification(
        operation: JobOperationIdentifier,
        payload: ByteString
    ) -> ByteString {
        DatabaseRequestDigest.compute(
            jobOperation: operation,
            prefix: specificationDomain,
            payload: payload
        )
    }

    static func result(
        operation: JobOperationIdentifier,
        payload: ByteString
    ) -> JobResultDigest {
        var accumulator = JobResultDigestAccumulator(
            operation: operation
        )
        accumulator.update(payload)
        return accumulator.finalize()
    }

    static func chunk(
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
