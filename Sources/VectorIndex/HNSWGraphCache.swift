// HNSWGraphCache.swift
// VectorIndex - In-memory cache for loaded HNSW graph snapshots

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import Synchronization
import SwiftHNSW

/// Process-local cache for immutable HNSW graph snapshots loaded from storage.
///
/// The cache is keyed by index identity plus persisted graph metadata bytes. Writers do
/// not update this cache directly; a committed metadata revision makes the next reader
/// resolve a different key and load the new graph from storage.
internal final class HNSWGraphCache: Sendable {
    internal struct Key: Hashable, Sendable {
        let subspacePrefix: Bytes
        let dimensions: Int
        let metric: String
        let metadata: Bytes
    }

    internal final class Snapshot: Sendable {
        private let searchLock = Mutex<Void>(())

        let index: HNSWIndexF32
        let primaryKeysByLabel: [UInt64: Tuple]

        init(index: HNSWIndexF32, primaryKeysByLabel: [UInt64: Tuple]) {
            self.index = index
            self.primaryKeysByLabel = primaryKeysByLabel
        }

        func search(
            queryVector: [Float],
            k: Int,
            efSearch: Int
        ) throws -> [SearchResult] {
            try searchLock.withLock { _ in
                index.setEfSearch(efSearch)
                return try queryVector.withUnsafeBufferPointer { buffer in
                    try index.search(buffer, k: k)
                }
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

    init(maximumCost: Int = 64 * 1024 * 1024) {
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

    func set(_ snapshot: Snapshot, for key: Key, cost: Int) {
        state.withLock { state in
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
        }
    }

}
