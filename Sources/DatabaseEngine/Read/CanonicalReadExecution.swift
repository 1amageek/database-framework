import DatabaseClientProtocol

/// Resolved execution policy for canonical reads.
///
/// `ReadConsistency` is a wire-level hint. In the current engine, it maps to the
/// transaction/cache policy that the framework can actually enforce:
/// - `.serializable` -> fresh read version (`.default`, `.server`)
/// - `.snapshot` -> cached/read-only transaction policy (`.readOnly`, `.cached`)
public struct CanonicalReadExecution: Sendable {
    public let consistency: ReadConsistency
    public let transactionConfiguration: TransactionConfiguration
    public let cachePolicy: CachePolicy

    public static func resolve(
        requested: ReadConsistency?,
        default defaultConsistency: ReadConsistency
    ) -> CanonicalReadExecution {
        let consistency = requested ?? defaultConsistency
        switch consistency {
        case .serializable:
            return CanonicalReadExecution(
                consistency: .serializable,
                transactionConfiguration: .default,
                cachePolicy: .server
            )
        case .snapshot:
            return CanonicalReadExecution(
                consistency: .snapshot,
                transactionConfiguration: .readOnly,
                cachePolicy: .cached
            )
        }
    }
}
