// TestInfrastructure.swift
// Shared test infrastructure for indexer testing
//
// Provides:
// - CountingIndexMaintainer: Tracks processing counts per item
// - LargeTestDataGenerator: Generates large datasets for transaction limit testing
// - MockIndexStateManager: In-memory index state management for testing

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Synchronization
import ScalarIndex

// MARK: - CountingIndexMaintainer

/// Index maintainer that tracks processing counts per item
///
/// Used for testing:
/// - Duplicate processing detection (atomicity tests)
/// - Progress consistency verification
/// - Resume behavior validation
public final class CountingIndexMaintainer<Item: Persistable>: IndexMaintainer, Sendable {
    /// Tracks how many times each ID has been processed
    private let processCount: Mutex<[String: Int]>

    /// Set of all processed IDs
    private let processedIds: Mutex<Set<String>>

    /// Index subspace for writing entries
    private let indexSubspace: Subspace

    /// Index name
    private let indexName: String

    public init(indexSubspace: Subspace, indexName: String) {
        self.processCount = Mutex([:])
        self.processedIds = Mutex(Set())
        self.indexSubspace = indexSubspace
        self.indexName = indexName
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Not used in online indexer tests
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Use stable binary encoding for ID string
        let idString = Data(id.pack()).base64EncodedString()

        // Record processing
        processCount.withLock { counts in
            counts[idString, default: 0] += 1
        }
        processedIds.withLock { ids in
            _ = ids.insert(idString)
        }

        // Write to index to simulate actual work
        let indexKey = indexSubspace.subspace(indexName).pack(id)
        transaction.setValue([0x01], for: indexKey)
    }

    /// Get the number of times an ID was processed
    public func getProcessCount(for id: String) -> Int {
        processCount.withLock { $0[id] ?? 0 }
    }

    /// Get all IDs that were processed more than once (duplicates)
    public func getDuplicateProcessedIds() -> [String: Int] {
        processCount.withLock { counts in
            counts.filter { $0.value > 1 }
        }
    }

    /// Get total number of unique IDs processed
    public func getUniqueProcessedCount() -> Int {
        processedIds.withLock { $0.count }
    }

    /// Get total number of processing calls (including duplicates)
    public func getTotalProcessCount() -> Int {
        processCount.withLock { counts in
            counts.values.reduce(0, +)
        }
    }

    /// Reset all counters
    public func reset() {
        processCount.withLock { $0.removeAll() }
        processedIds.withLock { $0.removeAll() }
    }

    /// Get all processed IDs
    public func getAllProcessedIds() -> Set<String> {
        processedIds.withLock { $0 }
    }
}

// MARK: - LargeTestDataGenerator

/// Generates large datasets for testing transaction limits
///
/// FDB has a 10MB transaction size limit (5MB recommended).
/// This generator creates datasets that exceed these limits
/// to verify batch processing works correctly.
public struct LargeTestDataGenerator {

    /// Generate players with specified data size
    ///
    /// - Parameters:
    ///   - count: Number of players to generate
    ///   - nameLength: Length of the name field (affects serialized size)
    /// - Returns: Array of Player models
    public static func generatePlayers(count: Int, nameLength: Int = 100) -> [Player] {
        (0..<count).map { i in
            let name = String(repeating: "x", count: nameLength) + String(i)
            return Player(
                id: String(format: "player_%06d", i),
                name: name,
                score: Int64(i * 100),
                level: (i % 100) + 1
            )
        }
    }

    /// Generate a large dataset that exceeds 5MB when serialized
    ///
    /// Creates ~500 players with ~10KB names each = ~5MB total
    public static func generateLargeDataset() -> [Player] {
        generatePlayers(count: 500, nameLength: 10_000)
    }

    /// Generate a dataset that fits within a single transaction
    ///
    /// Creates ~50 players with small names = ~5KB total
    public static func generateSmallDataset() -> [Player] {
        generatePlayers(count: 50, nameLength: 50)
    }

    /// Generate dataset with exact count for batch boundary testing
    ///
    /// - Parameters:
    ///   - batchSize: The batch size being used
    ///   - batches: Number of complete batches
    ///   - remainder: Additional items beyond complete batches
    public static func generateForBatchTesting(
        batchSize: Int,
        batches: Int,
        remainder: Int = 0
    ) -> [Player] {
        let count = batchSize * batches + remainder
        return generatePlayers(count: count, nameLength: 50)
    }

    /// Estimated serialized size per player in bytes
    public static func estimatedSizePerPlayer(nameLength: Int) -> Int {
        // Rough estimate: id (~20 bytes) + name + score (8) + level (4) + protobuf overhead (~50)
        return 20 + nameLength + 8 + 4 + 50
    }
}

// MARK: - MockIndexStateManager

/// In-memory index state manager for testing
///
/// Provides a simple implementation that doesn't require FDB.
public actor MockIndexStateManager {
    private var states: [String: IndexState] = [:]
    private var transitionHistory: [(name: String, from: IndexState?, to: IndexState)] = []

    public init() {}

    /// Get current state for an index
    public func getState(_ indexName: String) -> IndexState {
        states[indexName] ?? .disabled
    }

    /// Set state for an index
    public func setState(_ indexName: String, to state: IndexState) {
        let oldState = states[indexName]
        states[indexName] = state
        transitionHistory.append((indexName, oldState, state))
    }

    /// Enable an index (set to writeOnly)
    public func enable(_ indexName: String) {
        setState(indexName, to: .writeOnly)
    }

    /// Make an index readable
    public func makeReadable(_ indexName: String) {
        setState(indexName, to: .readable)
    }

    /// Disable an index
    public func disable(_ indexName: String) {
        setState(indexName, to: .disabled)
    }

    /// Get transition history for verification
    public func getTransitionHistory() -> [(name: String, from: IndexState?, to: IndexState)] {
        transitionHistory
    }

    /// Reset all state
    public func reset() {
        states.removeAll()
        transitionHistory.removeAll()
    }
}

// MARK: - FailingIndexMaintainer

/// Index maintainer that fails after a specified number of items
///
/// Used for testing error handling and recovery scenarios.
public final class FailingIndexMaintainer<Item: Persistable>: IndexMaintainer, Sendable {
    /// Error to throw
    public struct SimulatedError: Error, Equatable {
        public let message: String
        public init(_ message: String = "Simulated failure") {
            self.message = message
        }
    }

    private let failAfterCount: Int
    private let processedCount: Mutex<Int>
    private let indexSubspace: Subspace
    private let indexName: String

    /// Create a maintainer that fails after processing `failAfterCount` items
    public init(
        failAfterCount: Int,
        indexSubspace: Subspace,
        indexName: String
    ) {
        self.failAfterCount = failAfterCount
        self.processedCount = Mutex(0)
        self.indexSubspace = indexSubspace
        self.indexName = indexName
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Not used
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let count = processedCount.withLock { count in
            count += 1
            return count
        }

        if count > failAfterCount {
            throw SimulatedError("Failed after \(failAfterCount) items")
        }

        // Write to index
        let indexKey = indexSubspace.subspace(indexName).pack(id)
        transaction.setValue([0x01], for: indexKey)
    }

    /// Get current processed count
    public func getProcessedCount() -> Int {
        processedCount.withLock { $0 }
    }

    /// Reset the counter
    public func reset() {
        processedCount.withLock { $0 = 0 }
    }
}

// MARK: - TestIndex

/// Simple index definition for testing
public struct TestIndex {
    public static func create(name: String) -> Index {
        Index(
            name: name,
            kind: ScalarIndexKind<Player>(fields: [\Player.id]),
            rootExpression: FieldKeyExpression(fieldName: "id")
        )
    }
}
