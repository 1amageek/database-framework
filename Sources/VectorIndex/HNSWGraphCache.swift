// HNSWGraphCache.swift
// VectorIndex - In-memory cache for loaded HNSW graph snapshots

import DatabaseEngine
import DatabaseTypes
import StorageKit
import Synchronization
import SwiftHNSW

/// Process-local cache for immutable HNSW graph snapshots loaded from storage.
///
/// The cache is keyed by index identity plus persisted graph metadata bytes. Writers do
/// not update this cache directly; a committed metadata revision makes the next reader
/// resolve a different key and load the new graph from storage.
internal final class HNSWGraphCache: Sendable {
    internal struct MetadataIdentity: Hashable, Sendable {
        let version: Int64
        let byteCount: Int
        let chunkSize: Int
        let chunkCount: Int
        let revision: Int64
    }

    internal struct Key: Hashable, Sendable {
        // Retaining the domain makes its reference identity stable for the
        // entire lifetime of a cached snapshot. An ObjectIdentifier alone can
        // be reused after its object is deallocated.
        let transactionDomain: StorageTransactionDomain
        let subspacePrefix: ByteString
        let dimensions: Int
        let metric: String
        // Cache identity retains validated scalar metadata instead of the
        // backend-owned result buffer that encoded it.
        let metadata: MetadataIdentity

        static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.transactionDomain === rhs.transactionDomain
                && lhs.subspacePrefix == rhs.subspacePrefix
                && lhs.dimensions == rhs.dimensions
                && lhs.metric == rhs.metric
                && lhs.metadata == rhs.metadata
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(ObjectIdentifier(transactionDomain))
            hasher.combine(subspacePrefix)
            hasher.combine(dimensions)
            hasher.combine(metric)
            hasher.combine(metadata)
        }
    }

    internal final class Snapshot: Sendable {
        private let searchLock = Mutex<Void>(())

        let index: HNSWIndexF32
        let primaryKeysByLabel: [UInt64: ByteString]

        init(index: HNSWIndexF32, primaryKeysByLabel: [UInt64: ByteString]) {
            self.index = index
            self.primaryKeysByLabel = primaryKeysByLabel
        }

        func search(
            queryVector: Vector,
            k: Int,
            efSearch: Int,
            workMeter: DatabaseWorkMeter? = nil
        ) throws -> [SearchResult] {
            try searchLock.withLock { _ in
                try index.setEfSearch(efSearch)
                guard let results = try queryVector.withFloat32Elements({ buffer in
                    guard let workMeter else {
                        return try index.search(buffer, k: k)
                    }
                    let observer = HNSWDatabaseWorkObserver(
                        workMeter: workMeter,
                        dimensions: queryVector.count
                    )
                    switch try index.search(
                        buffer,
                        k: k,
                        observing: observer
                    ) {
                    case .completed(let results):
                        return results
                    case .stopped:
                        try observer.rethrowFailure()
                        throw VectorIndexError.invalidStructure(
                            "HNSW search stopped without an observer failure"
                        )
                    }
                }) else {
                    throw VectorIndexError.invalidStructure(
                        "HNSW query does not contain Float32 elements"
                    )
                }
                return results
            }
        }

    }

    private struct Entry: Sendable {
        let snapshot: Snapshot
        let cost: Int
    }

    private struct State: Sendable {
        var entries: [Key: Entry] = [:]
        var order: [Key] = []
        var totalCost: Int = 0
    }

    private let state = Mutex<State>(State())
    private let maximumCost: Int

    init(maximumCost: Int = 24 * 1_024 * 1_024) {
        self.maximumCost = maximumCost
    }

    func get(_ key: Key) -> Snapshot? {
        state.withLock { state in
            guard let entry = state.entries[key] else {
                return nil
            }
            state.order.removeAll { $0 == key }
            state.order.append(key)
            return entry.snapshot
        }
    }

    @discardableResult
    func set(_ snapshot: Snapshot, for key: Key, cost: Int) -> Bool {
        state.withLock { state in
            guard cost >= 0, cost <= maximumCost else {
                return false
            }
            if let existing = state.entries.removeValue(forKey: key) {
                state.totalCost -= existing.cost
                state.order.removeAll { $0 == key }
            }

            state.entries[key] = Entry(snapshot: snapshot, cost: cost)
            state.order.append(key)
            state.totalCost += cost

            while state.totalCost > maximumCost, let evictedKey = state.order.first {
                state.order.removeFirst()
                if let evicted = state.entries.removeValue(forKey: evictedKey) {
                    state.totalCost -= evicted.cost
                }
            }
            return state.entries[key] != nil
        }
    }

}
