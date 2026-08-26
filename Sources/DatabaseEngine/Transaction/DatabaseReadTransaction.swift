import DatabaseTypes
import StorageKit

/// Container-owned read capability for one admitted database transaction.
///
/// The public surface exposes only `TransactionReadAccess`. DatabaseEngine
/// retains the admitted storage access internally so model materialization can
/// reuse the same snapshot without exposing mutation or lifecycle authority to
/// feature executors.
public struct DatabaseReadTransaction: TransactionReadAccess, Sendable {
    let storageAccess: ReadAuthorizedTransactionAccess
    let authorization: DatabaseReadAuthorization?

    init(
        storageAccess: ReadAuthorizedTransactionAccess,
        authorization: DatabaseReadAuthorization? = nil
    ) {
        self.storageAccess = storageAccess
        self.authorization = authorization
    }

    public var transactionDomain: StorageTransactionDomain {
        storageAccess.transactionDomain
    }

    /// Exposes the session-owned storage capability to package feature
    /// readers without exposing the database transaction or its lifecycle
    /// authority. The returned value remains read-only at the API boundary;
    /// mutation methods are rejected by `ReadAuthorizedTransactionAccess`.
    package var storageTransaction: any TransactionAccess {
        storageAccess
    }

    /// Returns true only when both capabilities attenuate the same admitted
    /// storage snapshot. Authorization wrappers preserve this identity.
    package func sharesSnapshot(
        with other: DatabaseReadTransaction
    ) -> Bool {
        storageAccess.matches(other.storageAccess)
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await storageAccess.getValue(
            for: key,
            snapshot: snapshot
        )
    }

    public func getValue(for key: ByteString) async throws -> ByteString? {
        try await storageAccess.getValue(for: key)
    }

    public func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await storageAccess.getKey(
            selector: selector,
            snapshot: snapshot
        )
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        storageAccess.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}
