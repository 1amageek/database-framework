import FoundationDB
import Logging

/// Manages index state transitions with validation
///
/// IndexStateManager enforces the following transition rules:
/// - DISABLED → WRITE_ONLY: enable(_:)
/// - WRITE_ONLY → READABLE: makeReadable(_:)
/// - Any state → DISABLED: disable(_:)
///
/// **Thread-safety**: Uses database transactions for consistency
///
/// **State Persistence**: Index states are stored in FoundationDB at:
/// `[subspace]["state"][indexName] = IndexState.rawValue`
public final class IndexStateManager: Sendable {
    // MARK: - Properties

    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let subspace: Subspace
    private let logger: Logger

    // MARK: - Initialization

    /// Initialize IndexStateManager
    ///
    /// - Parameters:
    ///   - database: FoundationDB database
    ///   - subspace: Subspace for storing index states
    ///   - logger: Optional logger
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        logger: Logger? = nil
    ) {
        self.database = database
        self.subspace = subspace
        self.logger = logger ?? Logger(label: "com.fdb.runtime.indexstate")
    }

    // MARK: - State Queries

    /// Get the current state of an index
    ///
    /// - Parameter indexName: Name of the index
    /// - Returns: Current IndexState (defaults to .disabled if not found)
    /// - Throws: Error if state value is invalid
    public func state(of indexName: String) async throws -> IndexState {
        return try await database.withTransaction { transaction in
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
    public func state(of indexName: String, transaction: any TransactionProtocol) async throws -> IndexState {
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
        try await database.withTransaction { transaction in
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
            transaction.setValue([IndexState.writeOnly.rawValue], for: stateKey)

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
        try await database.withTransaction { transaction in
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

            // Validate transition: only from WRITE_ONLY
            guard currentState == .writeOnly else {
                throw IndexStateError.invalidTransition(
                    from: currentState,
                    to: .readable,
                    index: indexName,
                    reason: "Index must be in WRITE_ONLY state before marking readable"
                )
            }

            // Write new state
            transaction.setValue([IndexState.readable.rawValue], for: stateKey)

            self.logger.info("Marked index '\(indexName)' as readable: \(currentState) → readable")
        }
    }

    /// Disable an index (transition to DISABLED state)
    ///
    /// This can be called from any state.
    ///
    /// - Parameter indexName: Name of the index
    public func disable(_ indexName: String) async throws {
        try await database.withTransaction { transaction in
            let stateKey = self.makeStateKey(for: indexName)

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
            transaction.setValue([IndexState.disabled.rawValue], for: stateKey)

            self.logger.info("Disabled index '\(indexName)': \(currentState) → disabled")
        }
    }

    // MARK: - Batch Operations

    /// Get states for multiple indexes efficiently
    ///
    /// - Parameter indexNames: List of index names
    /// - Returns: Dictionary mapping index names to states
    public func states(of indexNames: [String]) async throws -> [String: IndexState] {
        return try await database.withTransaction { transaction in
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
    public func states(of indexNames: [String], transaction: any TransactionProtocol) async throws -> [String: IndexState] {
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
    private func makeStateKey(for indexName: String) -> FDB.Bytes {
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

    public var description: String {
        switch self {
        case .invalidStateValue(let value):
            return "Invalid index state value: \(value)"
        case .invalidTransition(let from, let to, let index, let reason):
            return "Invalid state transition for index '\(index)': \(from) → \(to). Reason: \(reason)"
        }
    }
}
