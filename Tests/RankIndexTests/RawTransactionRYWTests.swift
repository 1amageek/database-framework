#if FOUNDATION_DB
// RawTransactionRYWTests.swift
// Phase 0.1: Minimal reproduction tests for storage-kit RYW (Read-Your-Writes)
// and cross-commit visibility of clear + setValue operations on the same key.
//
// These tests bypass index maintenance entirely to isolate whether the
// "clear old, set new" pattern survives a commit boundary. A failure here
// would indicate a storage-kit / FDB bug; success points the investigation
// back into the framework layers.

import Testing
import Foundation
import StorageKit
import FDBStorage
import TestSupport

@Suite("Raw Transaction RYW Tests", .serialized, .heartbeat)
struct RawTransactionRYWTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    /// Within a single transaction, clear followed by setValue on the same key
    /// must satisfy Read-Your-Writes: a subsequent get in the same tx returns
    /// the new value. After commit, a fresh transaction also sees the new value.
    @Test("Clear then setValue on same key — RYW + cross-commit visibility")
    func clearThenSetSameKey() async throws {
        let engine = try await FDBTestSetup.shared.makeEngine()

        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "ryw", String(testId)).pack())
        let key = subspace.pack(Tuple("k"))
        let v1: [UInt8] = [0x01, 0x02, 0x03]
        let v2: [UInt8] = [0xAA, 0xBB, 0xCC]

        // Seed: write V1 and commit
        try await engine.withTransaction { transaction in
            transaction.setValue(v1, for: key)
        }

        // In a single transaction: clear, setValue, read. Then commit.
        let readWithinTx = try await engine.withTransaction { transaction -> [UInt8]? in
            transaction.clear(key: key)
            transaction.setValue(v2, for: key)
            return try await transaction.getValue(for: key, snapshot: false)
        }

        #expect(readWithinTx == v2, "RYW must return new value inside same tx (got \(String(describing: readWithinTx)))")

        // Fresh transaction: read after commit
        let readAfterCommit = try await engine.withTransaction { transaction -> [UInt8]? in
            try await transaction.getValue(for: key, snapshot: false)
        }

        #expect(readAfterCommit == v2, "Post-commit read must see new value (got \(String(describing: readAfterCommit)))")

        // Cleanup
        try await engine.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Clear key A and set key B in the same transaction. A range scan after
    /// commit must show only B, not A. This mirrors the RankIndex update path
    /// where the old score key is cleared and a new (different) score key is set.
    @Test("Clear key A + setValue key B in same tx — scan shows only B")
    func clearOneSetAnother() async throws {
        let engine = try await FDBTestSetup.shared.makeEngine()

        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "ryw", String(testId)).pack())
        let keyA = subspace.pack(Tuple("A"))
        let keyB = subspace.pack(Tuple("B"))
        let v1: [UInt8] = [0x11]
        let v2: [UInt8] = [0x22]

        // Seed: write A=V1
        try await engine.withTransaction { transaction in
            transaction.setValue(v1, for: keyA)
        }

        // In single tx: clear A, set B=V2
        try await engine.withTransaction { transaction in
            transaction.clear(key: keyA)
            transaction.setValue(v2, for: keyB)
        }

        // Fresh tx: range scan entire subspace
        let collected: [([UInt8], [UInt8])] = try await engine.withTransaction { transaction -> [([UInt8], [UInt8])] in
            let (begin, end) = subspace.range()
            let seq = try await transaction.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: false
            )
            var out: [([UInt8], [UInt8])] = []
            for (k, v) in seq {
                out.append((k, v))
            }
            return out
        }

        #expect(collected.count == 1, "Scan must see exactly one key (got \(collected.count))")
        if let first = collected.first {
            #expect(first.0 == keyB, "Remaining key must be B, not A")
            #expect(first.1 == v2, "Value must be V2")
        }

        // Cleanup
        try await engine.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}

#endif
