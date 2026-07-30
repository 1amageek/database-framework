import DatabaseKit
import DatabaseTypes
import DatabaseWire

/// Canonical state required to resume one typed query.
///
/// `nextOffset` is the absolute logical offset in the original query result,
/// including the query's initial offset. The query fingerprint binds that
/// position to every result-affecting query input.
internal struct ContinuationState: Sendable {
    let version: UInt8
    let nextOffset: UInt64
    let remainingLimit: UInt64?
    let queryFingerprint: ByteString

    init(
        version: UInt8 = ContinuationToken.currentVersion,
        nextOffset: UInt64,
        remainingLimit: UInt64?,
        queryFingerprint: ByteString
    ) {
        self.version = version
        self.nextOffset = nextOffset
        self.remainingLimit = remainingLimit
        self.queryFingerprint = queryFingerprint
    }

    func token() throws -> ContinuationToken {
        try ContinuationToken(
            data: ContinuationStateFormat.encode(self)
        )
    }

    static func decode(
        _ token: ContinuationToken
    ) throws -> ContinuationState {
        return try ContinuationStateFormat.decode(token.data)
    }
}

internal enum QueryFingerprint {
    private static let domain: UInt32 = 0x4355_5251

    static func compute<T: Persistable>(
        for query: Query<T>
    ) throws -> ByteString {
        let selectQuery = try query.toSelectQuery()
        var digest = SHA256Accumulator()
        append(domain, to: &digest)
        do {
            try QueryIRWireFormat.emitCanonicalEncoding(
                .select(selectQuery),
                prepare: { byteCount in
                    append(UInt64(byteCount), to: &digest)
                },
                consume: { bytes in
                    digest.update(bytes)
                }
            )
        } catch let error {
            switch error {
            case .encoding(let wireError):
                throw wireError
            case .destination:
                preconditionFailure(
                    "The query fingerprint destination cannot fail"
                )
            }
        }
        append(query.cachePolicy, to: &digest)
        return digest.finalize()
    }

    private static func append(
        _ policy: CachePolicy,
        to digest: inout SHA256Accumulator
    ) {
        switch policy {
        case .server:
            digest.update(0)
        case .cached:
            digest.update(1)
        case .stale(let duration):
            digest.update(2)
            let components = duration.components
            append(UInt64(bitPattern: components.seconds), to: &digest)
            append(UInt64(bitPattern: components.attoseconds), to: &digest)
        }
    }

    private static func append(
        _ value: UInt32,
        to digest: inout SHA256Accumulator
    ) {
        var value = value.littleEndian
        withUnsafeBytes(of: &value) { digest.update($0) }
    }

    private static func append(
        _ value: UInt64,
        to digest: inout SHA256Accumulator
    ) {
        var value = value.littleEndian
        withUnsafeBytes(of: &value) { digest.update($0) }
    }
}
