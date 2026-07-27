import DatabaseTypes
import StorageKit

/// Persists validated index lifecycle transitions.
///
/// IndexLifecycleStore enforces the following transition rules:
/// - DISABLED → WRITE_ONLY: enable(_:)
/// - WRITE_ONLY → READABLE: makeReadable(_:)
/// - Any state → DISABLED: disable(_:)
///
/// **Thread-safety**: Uses database transactions for consistency
///
/// **State Persistence**: Index states are stored under:
/// `[subspace]["state"][indexName] = IndexState.rawValue`
public final class IndexLifecycleStore: Sendable {
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
        return try await container.engine.withTransaction(configuration: .batch) { transaction in
            let stateKey = self.makeStateKey(for: indexName)

            guard let bytes = try await transaction.getValue(for: stateKey, snapshot: false),
                  let stateValue = bytes.first else {
                // Default: new indexes start as DISABLED
                return IndexState.disabled
            }

            guard let state = IndexState(rawValue: stateValue) else {
                throw IndexStateError.invalidStateValue(stateValue)
            }

            return state
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
        let stateKey = makeStateKey(for: indexName)

        guard let bytes = try await transaction.getValue(for: stateKey, snapshot: false),
              let stateValue = bytes.first else {
            // Default: new indexes start as DISABLED
            return IndexState.disabled
        }

        guard let state = IndexState(rawValue: stateValue) else {
            throw IndexStateError.invalidStateValue(stateValue)
        }

        return state
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
        try await container.engine.withTransaction(configuration: .batch) { transaction in
            let stateKey = self.makeStateKey(for: indexName)

            // Read current state within transaction
            let currentState: IndexState
            if let bytes = try await transaction.getValue(for: stateKey, snapshot: false),
               let stateValue = bytes.first,
               let state = IndexState(rawValue: stateValue) {
                currentState = state
            } else {
                currentState = .disabled
            }

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

            self.logger.info("Enabled index '\(indexName)': \(currentState) → writeOnly")
        }
    }

    /// Make an index readable (transition to READABLE state)
    ///
    /// This should only be called after index building is complete.
    ///
    /// - Parameter indexName: Name of the index
    /// - Throws: IndexStateError.invalidTransition if not in WRITE_ONLY state
    public func makeReadable(_ indexName: String) async throws {
        try await container.engine.withTransaction(configuration: .batch) { transaction in
            try await self.makeReadable(indexName, transaction: transaction)
        }
    }

    /// Make an index readable within an existing transaction.
    public func makeReadable(
        _ indexName: String,
        transaction: any TransactionAccess
    ) async throws {
        let stateKey = makeStateKey(for: indexName)
        let currentState: IndexState
        if let bytes = try await transaction.getValue(
            for: stateKey,
            snapshot: false
        ), let stateValue = bytes.first,
           let state = IndexState(rawValue: stateValue) {
            currentState = state
        } else {
            currentState = .disabled
        }
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
        try await container.engine.withTransaction(configuration: .batch) { transaction in
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
        let sourceRows = try await transaction.collectRange(
            from: .firstGreaterOrEqual(entityRange.begin),
            to: .firstGreaterOrEqual(entityRange.end),
            limit: 1,
            snapshot: false
        )
        let sourceIsEmpty = sourceRows.isEmpty
        for indexName in indexNames {
            let stateKey = makeStateKey(for: indexName)
            let storedBytes = try await transaction.getValue(
                for: stateKey,
                snapshot: false
            )
            guard let storedBytes else {
                guard sourceIsEmpty else {
                    throw IndexStateError.missingStateForNonEmptyStore(
                        index: indexName
                    )
                }
                try transaction.setValue([IndexState.readable.rawValue], for: stateKey)
                logger.info("Initialized index '\(indexName)' for an empty store")
                continue
            }
            guard let stateValue = storedBytes.first,
                  let currentState = IndexState(rawValue: stateValue) else {
                throw IndexStateError.invalidStateValue(storedBytes.first ?? 0)
            }
            switch currentState {
            case .readable:
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
        try await container.engine.withTransaction(
            configuration: .batch
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
        var sourceIsEmpty: Bool?
        for indexName in indexNames {
            let stateKey = makeStateKey(for: indexName)
            if let storedBytes = try await transaction.getValue(
                for: stateKey,
                snapshot: false
            ) {
                guard let stateValue = storedBytes.first,
                      IndexState(rawValue: stateValue) != nil else {
                    throw IndexStateError.invalidStateValue(
                        storedBytes.first ?? 0
                    )
                }
                continue
            }

            if sourceIsEmpty == nil {
                let sourceRows = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(entityRange.begin),
                    to: .firstGreaterOrEqual(entityRange.end),
                    limit: 1,
                    snapshot: false
                )
                sourceIsEmpty = sourceRows.isEmpty
            }
            guard sourceIsEmpty == true else {
                throw IndexStateError.missingStateForNonEmptyStore(
                    index: indexName
                )
            }
            try transaction.setValue(
                [IndexState.readable.rawValue],
                for: stateKey
            )
            logger.info(
                "Initialized index '\(indexName)' for an empty store"
            )
        }
    }

    /// Validates index readability without creating or changing index state.
    ///
    /// A missing state is safe only while the covered entity range is empty.
    /// This keeps query execution read-only while preserving the same
    /// fail-fast guarantee used during declarative initialization.
    func validateReadableForRead(
        _ indexNames: [String],
        entityRange: (begin: ByteString, end: ByteString),
        transaction: any TransactionAccess
    ) async throws {
        var sourceIsEmpty: Bool?
        for indexName in indexNames {
            let stateKey = makeStateKey(for: indexName)
            let storedBytes = try await transaction.getValue(
                for: stateKey,
                snapshot: true
            )
            guard let storedBytes else {
                if sourceIsEmpty == nil {
                    let sourceRows = try await transaction.collectRange(
                        from: .firstGreaterOrEqual(entityRange.begin),
                        to: .firstGreaterOrEqual(entityRange.end),
                        limit: 1,
                        snapshot: true
                    )
                    sourceIsEmpty = sourceRows.isEmpty
                }
                guard sourceIsEmpty == true else {
                    throw IndexStateError.missingStateForNonEmptyStore(
                        index: indexName
                    )
                }
                continue
            }
            guard let stateValue = storedBytes.first,
                  let currentState = IndexState(rawValue: stateValue) else {
                throw IndexStateError.invalidStateValue(storedBytes.first ?? 0)
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
    /// This can be called from any state.
    ///
    /// - Parameter indexName: Name of the index
    public func disable(_ indexName: String) async throws {
        try await container.engine.withTransaction(configuration: .batch) { transaction in
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

        // Read current state within transaction (for logging)
        let currentState: IndexState
        if let bytes = try await transaction.getValue(for: stateKey, snapshot: false),
           let stateValue = bytes.first,
           let state = IndexState(rawValue: stateValue) {
            currentState = state
        } else {
            currentState = .disabled
        }

        // Write new state (no validation - can disable from any state)
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

        // Read current state within transaction
        let currentState: IndexState
        if let bytes = try await transaction.getValue(for: stateKey, snapshot: false),
           let stateValue = bytes.first,
           let state = IndexState(rawValue: stateValue) {
            currentState = state
        } else {
            currentState = .disabled
        }

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
        return try await container.engine.withTransaction(configuration: .batch) { transaction in
            var states: [String: IndexState] = [:]

            for indexName in indexNames {
                let stateKey = self.makeStateKey(for: indexName)

                guard let bytes = try await transaction.getValue(for: stateKey, snapshot: false),
                      let stateValue = bytes.first,
                      let state = IndexState(rawValue: stateValue) else {
                    states[indexName] = .disabled
                    continue
                }

                states[indexName] = state
            }

            return states
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

        for indexName in indexNames {
            let stateKey = makeStateKey(for: indexName)

            guard let bytes = try await transaction.getValue(for: stateKey, snapshot: false),
                  let stateValue = bytes.first,
                  let state = IndexState(rawValue: stateValue) else {
                states[indexName] = .disabled
                continue
            }
            states[indexName] = state
        }

        return states
    }

    // MARK: - Helper Methods

    /// Make state key for an index
    ///
    /// Key structure: `[subspace]["state"][indexName]`
    ///
    /// - Parameter indexName: Index name
    /// - Returns: FDB key for storing index state
    private func makeStateKey(for indexName: String) -> ByteString {
        return subspace.subspace("state").pack(Tuple(indexName))
    }
}

// MARK: - Errors

/// Errors that can occur during index state management
public enum IndexStateError: Error, CustomStringConvertible {
    /// Invalid state value found in database
    case invalidStateValue(UInt8)

    /// Invalid state transition attempted
    case invalidTransition(from: IndexState, to: IndexState, index: String, reason: String)

    /// Index metadata is absent even though source entities already exist.
    case missingStateForNonEmptyStore(index: String)

    /// An index is explicitly incomplete or disabled.
    case indexNotReady(index: String, state: IndexState)

    public var description: String {
        switch self {
        case .invalidStateValue(let value):
            return "Invalid index state value: \(value)"
        case .invalidTransition(let from, let to, let index, let reason):
            return "Invalid state transition for index '\(index)': \(from) → \(to). Reason: \(reason)"
        case .missingStateForNonEmptyStore(let index):
            return "Index '\(index)' has no state but its source store is not empty"
        case .indexNotReady(let index, let state):
            return "Index '\(index)' is not readable: \(state)"
        }
    }
}
