import DatabaseTypes
import StorageKit

/// Persists validated index lifecycle transitions.
///
/// IndexLifecycleStore enforces the following transition rules:
/// - DISABLED → WRITE_ONLY: enable(_:)
/// - WRITE_ONLY → READABLE: makeReadable(_:)
/// - Any recognized or missing state → DISABLED: disable(_:)
///
/// **Thread-safety**: Uses database transactions for consistency
///
/// **State Persistence**: Index states are stored under:
/// `[subspace]["state"][indexName] = IndexState.rawValue`
package final class IndexLifecycleStore: Sendable {
    // MARK: - Properties

    /// Container used for transaction execution.
    let container: DBContainer
    private let subspace: Subspace
    private let logger: DatabaseLogger

    // MARK: - Initialization

    /// Initialize IndexLifecycleStore
    ///
    /// - Parameters:
    ///   - container: DBContainer for transaction execution
    ///   - subspace: Subspace for storing index states
    public init(
        container: DBContainer,
        subspace: Subspace
    ) {
        self.container = container
        self.subspace = subspace
        self.logger = container.configuration.logging.logger(
            label: "com.database.framework.index-lifecycle"
        )
    }

    // MARK: - State Queries

    /// Get the current state of an index
    ///
    /// - Parameter indexName: Name of the index
    /// - Returns: Current IndexState (defaults to .disabled if not found)
    /// - Throws: Error if state value is invalid
    public func state(of indexName: String) async throws -> IndexState {
        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try await self.state(
                of: indexName,
                transaction: transaction
            )
        }
    }

    /// Get the current state of an index within a transaction
    ///
    /// Use this when you need to read index state within an existing transaction
    /// to ensure consistency with other operations.
    ///
    /// - Parameters:
    ///   - indexName: Name of the index
    ///   - transaction: The transaction to use
    /// - Returns: Current IndexState (defaults to .disabled if not found)
    /// - Throws: Error if state value is invalid
    public func state(of indexName: String, transaction: any TransactionAccess) async throws -> IndexState {
        try await storedState(
            of: indexName,
            transaction: transaction,
            snapshot: false
        ) ?? .disabled
    }

    /// Reads the persisted lifecycle state without replacing absence with the
    /// externally visible disabled state.
    package func persistedState(
        of indexName: String,
        transaction: any TransactionAccess
    ) async throws -> IndexState? {
        try await storedState(
            of: indexName,
            transaction: transaction,
            snapshot: false
        )
    }

    // MARK: - State Transitions

    /// Enable an index (transition to WRITE_ONLY state)
    ///
    /// This sets the index to WRITE_ONLY state, meaning:
    /// - New writes will maintain the index
    /// - Queries will not use the index yet
    /// - Background index building can proceed
    ///
    /// - Parameter indexName: Name of the index
    /// - Throws: IndexStateError.invalidTransition if not in DISABLED state
    public func enable(_ indexName: String) async throws {
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try await self.enable(
                indexName,
                transaction: transaction
            )
        }
    }

    /// Make an index readable (transition to READABLE state)
    ///
    /// This should only be called after index building is complete.
    ///
    /// - Parameter indexName: Name of the index
    /// - Throws: IndexStateError.invalidTransition if not in WRITE_ONLY state
    public func makeReadable(_ indexName: String) async throws {
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try await self.makeReadable(indexName, transaction: transaction)
        }
    }

    /// Make an index readable within an existing transaction.
    public func makeReadable(
        _ indexName: String,
        transaction: any TransactionAccess
    ) async throws {
        let stateKey = makeStateKey(for: indexName)
        let currentState = try await storedState(
            of: indexName,
            transaction: transaction,
            snapshot: false
        ) ?? .disabled
        guard currentState == .writeOnly else {
            throw IndexStateError.invalidTransition(
                from: currentState,
                to: .readable,
                index: indexName,
                reason: "Index must be in WRITE_ONLY state before marking readable"
            )
        }
        try transaction.setValue([IndexState.readable.rawValue], for: stateKey)
        logger.info(
            "Marked index '\(indexName)' as readable: \(currentState) → readable"
        )
    }

    // MARK: - Initialization (Declarative Convergence)

    /// Validates existing index state and initializes only an empty store.
    ///
    /// Declarative operation for container initialization, distinct from the
    /// imperative `enable()` → `makeReadable()` lifecycle used by OnlineIndexer.
    ///
    /// A missing state may become `readable` only when the associated entity
    /// range is empty in the same transaction. Existing `disabled` and
    /// `writeOnly` states fail fast because their derived data is not proven
    /// complete.
    ///
    /// Safe for concurrent execution from multiple DBContainer instances.
    ///
    /// - Parameters:
    ///   - indexNames: Names of the indexes to validate.
    ///   - entityRange: Complete source entity range covered by the indexes.
    public func ensureReadable(
        _ indexNames: [String],
        entityRange: (begin: ByteString, end: ByteString)
    ) async throws {
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try await self.ensureReadable(
                indexNames,
                entityRange: entityRange,
                transaction: transaction
            )
        }
    }

    /// Transaction-scoped variant used by atomic schema bootstrap.
    public func ensureReadable(
        _ indexNames: [String],
        entityRange: (begin: ByteString, end: ByteString),
        transaction: any TransactionAccess
    ) async throws {
        try await ensureReadable(
            indexNames,
            entityRange: entityRange,
            pendingBuildIndexes: [],
            transaction: transaction
        )
    }

    /// Validates declared index state while admitting persisted schema builds.
    ///
    /// Only indexes named in `pendingBuildIndexes` may remain `writeOnly` or
    /// initialize as `writeOnly` for an existing source range. The durable
    /// pending marker and lifecycle state are read in the same transaction.
    package func ensureReadable(
        _ indexNames: [String],
        entityRange: (begin: ByteString, end: ByteString),
        pendingBuildIndexes: Set<String>,
        transaction: any TransactionAccess
    ) async throws {
        var sourceIsEmpty: Bool?
        for indexName in indexNames {
            guard let currentState = try await storedState(
                of: indexName,
                transaction: transaction,
                snapshot: false
            ) else {
                if sourceIsEmpty == nil {
                    let sourceRows = try await TransactionRangeCollection.collect(using: transaction,
                        from: .firstGreaterOrEqual(entityRange.begin),
                        to: .firstGreaterOrEqual(entityRange.end),
                        limit: 1,
                        reverse: false,
                        snapshot: false,
                        streamingMode: .small
                    )
                    sourceIsEmpty = sourceRows.isEmpty
                }
                guard sourceIsEmpty == true else {
                    if pendingBuildIndexes.contains(indexName) {
                        try transaction.setValue(
                            [IndexState.writeOnly.rawValue],
                            for: makeStateKey(for: indexName)
                        )
                        continue
                    }
                    throw IndexStateError.missingStateForNonEmptyStore(
                        index: indexName
                    )
                }
                try transaction.setValue(
                    [IndexState.readable.rawValue],
                    for: makeStateKey(for: indexName)
                )
                logger.info("Initialized index '\(indexName)' for an empty store")
                continue
            }
            switch currentState {
            case .readable:
                break
            case .writeOnly where pendingBuildIndexes.contains(indexName):
                break
            case .disabled, .writeOnly:
                throw IndexStateError.indexNotReady(
                    index: indexName,
                    state: currentState
                )
            }
        }
    }

    /// Initializes missing index states for an empty source store while
    /// preserving every explicit lifecycle state.
    ///
    /// Store resolution uses this operation because `disabled` and
    /// `writeOnly` are valid mutation states. Read admission remains the
    /// responsibility of `validateReadableForRead`.
    func initializeMissingStates(
        _ indexNames: [String],
        entityRange: (begin: ByteString, end: ByteString)
    ) async throws {
        try await container.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: container.monotonicClock
        ) { transaction in
            try await self.initializeMissingStates(
                indexNames,
                entityRange: entityRange,
                transaction: transaction
            )
        }
    }

    /// Transaction-scoped state initialization used while resolving a store
    /// for a database mutation.
    func initializeMissingStates(
        _ indexNames: [String],
        entityRange: (begin: ByteString, end: ByteString),
        transaction: any TransactionAccess
    ) async throws {
        try await initializeMissingStates(
            indexNames,
            entityRange: entityRange,
            pendingBuildIndexes: [],
            transaction: transaction
        )
    }

    /// Initializes states during mutation admission for a published schema.
    /// Missing indexes with durable build markers become `writeOnly` when the
    /// source is non-empty, so new mutations maintain them before backfill.
    package func initializeMissingStates(
        _ indexNames: [String],
        entityRange: (begin: ByteString, end: ByteString),
        pendingBuildIndexes: Set<String>,
        transaction: any TransactionAccess
    ) async throws {
        var sourceIsEmpty: Bool?
        for indexName in indexNames {
            if try await storedState(
                of: indexName,
                transaction: transaction,
                snapshot: false
            ) != nil {
                continue
            }

            if sourceIsEmpty == nil {
                let sourceRows = try await TransactionRangeCollection.collect(using: transaction,
                    from: .firstGreaterOrEqual(entityRange.begin),
                    to: .firstGreaterOrEqual(entityRange.end),
                    limit: 1,
                    reverse: false,
                    snapshot: false,
                    streamingMode: .small
                )
                sourceIsEmpty = sourceRows.isEmpty
            }
            guard sourceIsEmpty == true else {
                if pendingBuildIndexes.contains(indexName) {
                    try transaction.setValue(
                        [IndexState.writeOnly.rawValue],
                        for: makeStateKey(for: indexName)
                    )
                    logger.info(
                        "Initialized pending index '\(indexName)' as write-only"
                    )
                    continue
                }
                throw IndexStateError.missingStateForNonEmptyStore(
                    index: indexName
                )
            }
            try transaction.setValue(
                [IndexState.readable.rawValue],
                for: makeStateKey(for: indexName)
            )
            logger.info(
                "Initialized index '\(indexName)' for an empty store"
            )
        }
    }

    /// Creates the lifecycle state for an index introduced by schema apply.
    /// Returns `true` when existing source rows require a persistent backfill.
    package func prepareSchemaBuild(
        _ indexName: String,
        entityRange: (begin: ByteString, end: ByteString),
        transaction: any TransactionAccess
    ) async throws -> Bool {
        if let state = try await storedState(
            of: indexName,
            transaction: transaction,
            snapshot: false
        ) {
            guard state == .readable else {
                throw IndexStateError.indexNotReady(
                    index: indexName,
                    state: state
                )
            }
            return false
        }
        let sourceRows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(entityRange.begin),
            to: .firstGreaterOrEqual(entityRange.end),
            limit: 1,
            reverse: false,
            snapshot: false,
            streamingMode: .small
        )
        let requiresBuild = !sourceRows.isEmpty
        try transaction.setValue(
            [
                requiresBuild
                    ? IndexState.writeOnly.rawValue
                    : IndexState.readable.rawValue,
            ],
            for: makeStateKey(for: indexName)
        )
        return requiresBuild
    }

    /// Validates index readability without creating or changing index state.
    ///
    /// A missing state is an invariant violation for every existing namespace.
    /// Dynamic partitions that have never existed are rejected before this
    /// method by the partition catalog and therefore never reach admission.
    func validateReadableForRead(
        _ indexNames: [String],
        transaction: any TransactionAccess
    ) async throws {
        for indexName in indexNames {
            guard let currentState = try await storedState(
                of: indexName,
                transaction: transaction,
                snapshot: true
            ) else {
                throw IndexStateError.missingPersistedState(
                    index: indexName
                )
            }
            guard currentState.isReadable else {
                throw IndexStateError.indexNotReady(
                    index: indexName,
                    state: currentState
                )
            }
        }
    }

    // MARK: - Admin Operations

    /// Disable an index (transition to DISABLED state)
    ///
    /// This can be called from any recognized state or when state is absent.
    /// A malformed persisted state fails without being overwritten.
    ///
    /// - Parameter indexName: Name of the index
    public func disable(_ indexName: String) async throws {
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try await self.disable(indexName, transaction: transaction)
        }
    }

    /// Disable an index within an existing transaction
    ///
    /// Use this when you need to disable an index as part of a larger atomic operation.
    ///
    /// - Parameters:
    ///   - indexName: Name of the index
    ///   - transaction: The transaction to use
    public func disable(_ indexName: String, transaction: any TransactionAccess) async throws {
        let stateKey = makeStateKey(for: indexName)
        let currentState = try await storedState(
            of: indexName,
            transaction: transaction,
            snapshot: false
        ) ?? .disabled

        // Every persisted state was validated before this transition.
        try transaction.setValue([IndexState.disabled.rawValue], for: stateKey)

        logger.info("Disabled index '\(indexName)': \(currentState) → disabled")
    }

    /// Enable an index within an existing transaction
    ///
    /// Use this when you need to enable an index as part of a larger atomic operation.
    ///
    /// - Parameters:
    ///   - indexName: Name of the index
    ///   - transaction: The transaction to use
    /// - Throws: IndexStateError.invalidTransition if not in DISABLED state
    public func enable(_ indexName: String, transaction: any TransactionAccess) async throws {
        let stateKey = makeStateKey(for: indexName)
        let currentState = try await storedState(
            of: indexName,
            transaction: transaction,
            snapshot: false
        ) ?? .disabled

        // Validate transition: only from DISABLED
        guard currentState == .disabled else {
            throw IndexStateError.invalidTransition(
                from: currentState,
                to: .writeOnly,
                index: indexName,
                reason: "Index must be DISABLED before enabling"
            )
        }

        // Write new state
        try transaction.setValue([IndexState.writeOnly.rawValue], for: stateKey)

        logger.info("Enabled index '\(indexName)': \(currentState) → writeOnly")
    }

    // MARK: - Batch Operations

    /// Get states for multiple indexes efficiently
    ///
    /// - Parameter indexNames: List of index names
    /// - Returns: Dictionary mapping index names to states
    public func states(of indexNames: [String]) async throws -> [String: IndexState] {
        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try await self.states(
                of: indexNames,
                transaction: transaction
            )
        }
    }

    /// Get states for multiple indexes within a transaction
    ///
    /// Use this when you need to read multiple index states within an existing
    /// transaction to ensure consistency with other operations.
    ///
    /// - Parameters:
    ///   - indexNames: List of index names
    ///   - transaction: The transaction to use
    /// - Returns: Dictionary mapping index names to states
    public func states(of indexNames: [String], transaction: any TransactionAccess) async throws -> [String: IndexState] {
        var states: [String: IndexState] = [:]
        states.reserveCapacity(indexNames.count)

        for indexName in indexNames {
            states[indexName] = try await storedState(
                of: indexName,
                transaction: transaction,
                snapshot: false
            ) ?? .disabled
        }

        return states
    }

    // MARK: - Helper Methods

    /// Make state key for an index
    ///
    /// Key structure: `[subspace]["state"][indexName]`
    ///
    /// - Parameter indexName: Index name
    /// - Returns: Storage key for the persisted index state
    private func makeStateKey(for indexName: String) -> ByteString {
        return subspace.subspace("state").pack(Tuple(indexName))
    }

    /// Reads the complete persisted representation without materializing it.
    ///
    /// A missing key means the index has not entered its lifecycle yet. Every
    /// present value must contain exactly one known `IndexState` byte.
    private func storedState(
        of indexName: String,
        transaction: any TransactionAccess,
        snapshot: Bool
    ) async throws -> IndexState? {
        let stateKey = makeStateKey(for: indexName)
        guard let bytes = try await transaction.getValue(
            for: stateKey,
            snapshot: snapshot
        ) else {
            return nil
        }
        guard bytes.count == 1, let value = bytes.first else {
            throw IndexStateError.invalidPersistedStateSize(
                index: indexName,
                byteCount: bytes.count
            )
        }
        guard let state = IndexState(rawValue: value) else {
            throw IndexStateError.unknownPersistedStateValue(
                index: indexName,
                value: value
            )
        }
        return state
    }
}

// MARK: - Errors

/// Errors that can occur during index state management
public enum IndexStateError: Error, Sendable, Equatable, CustomStringConvertible {
    /// Persisted state does not contain exactly one byte.
    case invalidPersistedStateSize(index: String, byteCount: Int)

    /// Persisted state contains a one-byte value unknown to IndexState.
    case unknownPersistedStateValue(index: String, value: UInt8)

    /// A total state lookup did not resolve one of its requested indexes.
    case missingRequestedState(index: String)

    /// Invalid state transition attempted
    case invalidTransition(from: IndexState, to: IndexState, index: String, reason: String)

    /// Index metadata is absent even though source entities already exist.
    case missingStateForNonEmptyStore(index: String)

    /// An existing namespace has no persisted lifecycle state for an index.
    case missingPersistedState(index: String)

    /// An index is explicitly incomplete or disabled.
    case indexNotReady(index: String, state: IndexState)

    public var description: String {
        switch self {
        case .invalidPersistedStateSize(let index, let byteCount):
            return "Invalid persisted state size for index '\(index)': expected 1 byte, found \(byteCount)"
        case .unknownPersistedStateValue(let index, let value):
            return "Unknown persisted state value for index '\(index)': \(value)"
        case .missingRequestedState(let index):
            return "State lookup omitted requested index '\(index)'"
        case .invalidTransition(let from, let to, let index, let reason):
            return "Invalid state transition for index '\(index)': \(from) → \(to). Reason: \(reason)"
        case .missingStateForNonEmptyStore(let index):
            return "Index '\(index)' has no state but its source store is not empty"
        case .missingPersistedState(let index):
            return "Index '\(index)' has no persisted lifecycle state"
        case .indexNotReady(let index, let state):
            return "Index '\(index)' is not readable: \(state)"
        }
    }
}
