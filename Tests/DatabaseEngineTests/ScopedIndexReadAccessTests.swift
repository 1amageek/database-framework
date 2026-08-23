import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Scoped index read access")
struct ScopedIndexReadAccessTests {
    @Test("Point and range reads cannot escape the admitted index")
    func rejectsKeysAndRangesOutsideIndex() async throws {
        let engine = InMemoryEngine()
        let admitted = Subspace(prefix: Tuple("index", "admitted").pack())
        let neighboring = Subspace(prefix: Tuple("index", "neighbor").pack())
        let admittedKey = admitted.pack(Tuple("first"))
        let neighboringKey = neighboring.pack(Tuple("first"))

        try await engine.withTransaction { transaction in
            try transaction.setValue([0x01], for: admittedKey)
            try transaction.setValue([0x02], for: neighboringKey)
        }

        try await engine.withTransaction { transaction in
            let access = ScopedIndexReadAccess(
                transaction: transaction,
                subspace: admitted
            )
            #expect(
                try await access.getValue(
                    for: admittedKey,
                    snapshot: true
                ) == [0x01]
            )
            await #expect(
                throws: IndexReadAccessError.keyOutsideReadableIndex
            ) {
                try await access.getValue(
                    for: neighboringKey,
                    snapshot: true
                )
            }

            let (_, neighboringEnd) = neighboring.range()
            var rejected = access.rangeCursor(
                from: .firstGreaterOrEqual(admittedKey),
                to: .firstGreaterOrEqual(neighboringEnd),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            await #expect(
                throws: IndexReadAccessError.rangeOutsideReadableIndex
            ) {
                try await rejected.next()
            }
            try await rejected.finish()
        }
    }

    @Test("Boundary selectors return only admitted keys")
    func boundsKeySelectorsToIndex() async throws {
        let engine = InMemoryEngine()
        let admitted = Subspace(prefix: Tuple("selector", "admitted").pack())
        let neighboring = Subspace(prefix: Tuple("selector", "neighbor").pack())
        let first = admitted.pack(Tuple(Int64(1)))
        let last = admitted.pack(Tuple(Int64(2)))
        let neighboringKey = neighboring.pack(Tuple(Int64(1)))
        let (_, admittedUpperBound) = admitted.range()

        try await engine.withTransaction { transaction in
            try transaction.setValue([0x01], for: first)
            try transaction.setValue([0x02], for: last)
            try transaction.setValue([0x04], for: admittedUpperBound)
            try transaction.setValue([0x03], for: neighboringKey)
        }

        try await engine.withTransaction { transaction in
            let access = ScopedIndexReadAccess(
                transaction: transaction,
                subspace: admitted
            )
            let (_, end) = admitted.range()
            let selectedLast = try await access.getKey(
                selector: .lastLessThan(end),
                snapshot: true
            )
            #expect(selectedLast == last)
            let selectedAfterLast = try await access.getKey(
                selector: .firstGreaterThan(last),
                snapshot: true
            )
            #expect(selectedAfterLast == nil)
            #expect(try await access.getKey(
                selector: .firstGreaterOrEqual(end),
                snapshot: true
            ) == nil)
            #expect(try await access.getKey(
                selector: .firstGreaterThan(end),
                snapshot: true
            ) == nil)
            await #expect(
                throws: IndexReadAccessError.keyOutsideReadableIndex
            ) {
                try await access.getKey(
                    selector: .firstGreaterOrEqual(neighboringKey),
                    snapshot: true
                )
            }
        }
    }

    @Test("Range continuation starts strictly after the previous key")
    func rangeContinuationRemainsInsideIndex() async throws {
        let engine = InMemoryEngine()
        let admitted = Subspace(prefix: Tuple("range", "admitted").pack())
        let first = admitted.pack(Tuple(Int64(1)))
        let second = admitted.pack(Tuple(Int64(2)))
        let (_, end) = admitted.range()

        try await engine.withTransaction { transaction in
            try transaction.setValue([0x01], for: first)
            try transaction.setValue([0x02], for: second)
        }

        try await engine.withTransaction { transaction in
            let access = ScopedIndexReadAccess(
                transaction: transaction,
                subspace: admitted
            )
            var cursor = access.rangeCursor(
                from: .firstGreaterThan(first),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            #expect(try await cursor.next()?.0 == second)
            #expect(try await cursor.next() == nil)
            try await cursor.finish()
        }
    }

    @Test("Range continuation cannot start beyond the index boundary")
    func rangeContinuationRejectsUpperBound() async throws {
        let engine = InMemoryEngine()
        let admitted = Subspace(prefix: Tuple("range", "bounded").pack())
        let (_, end) = admitted.range()

        try await engine.withTransaction { transaction in
            let access = ScopedIndexReadAccess(
                transaction: transaction,
                subspace: admitted
            )
            var cursor = access.rangeCursor(
                from: .firstGreaterThan(end),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            await #expect(
                throws: IndexReadAccessError.rangeOutsideReadableIndex
            ) {
                _ = try await cursor.next()
            }
            try await cursor.finish()
        }
    }

    @Test("Unavailable and root subspaces fail closed")
    func invalidScopesFailClosed() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            let unavailable = ScopedIndexReadAccess(
                transaction: transaction,
                subspace: nil
            )
            await #expect(
                throws: IndexReadAccessError.indexPartitionAbsent
            ) {
                try await unavailable.getValue(for: [0x01], snapshot: true)
            }

            let root = ScopedIndexReadAccess(
                transaction: transaction,
                subspace: Subspace(prefix: [])
            )
            await #expect(
                throws: IndexReadAccessError.invalidReadableIndexSubspace
            ) {
                try await root.getValue(for: [0x01], snapshot: true)
            }
            await #expect(
                throws: IndexReadAccessError.invalidReadableIndexSubspace
            ) {
                try await root.getKey(
                    selector: .firstGreaterOrEqual([]),
                    snapshot: true
                )
            }
        }
    }

    @Test("Range metrics validate the same physical boundary")
    func rangeMetricsAreScoped() async throws {
        let engine = InMemoryEngine()
        let admitted = Subspace(prefix: Tuple("metrics", "admitted").pack())
        let neighboring = Subspace(prefix: Tuple("metrics", "neighbor").pack())
        let (begin, end) = admitted.range()
        let (_, neighboringEnd) = neighboring.range()

        try await engine.withTransaction { transaction in
            try transaction.setValue(
                [0x01],
                for: admitted.pack(Tuple("value"))
            )
        }

        try await engine.withTransaction { transaction in
            let access = ScopedIndexReadAccess(
                transaction: transaction,
                subspace: admitted
            )
            let estimatedSize = try await access.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )
            #expect(estimatedSize > 0)
            await #expect(
                throws: IndexReadAccessError.rangeOutsideReadableIndex
            ) {
                try await access.getRangeSplitPoints(
                    beginKey: begin,
                    endKey: neighboringEnd,
                    chunkSize: 1
                )
            }
        }
    }

    @Test("Multi-index ranges select the matching admitted subspace")
    func multiIndexRangesSelectTheirSubspace() async throws {
        let engine = InMemoryEngine()
        let first = Subspace(prefix: Tuple("multi", "first").pack())
        let second = Subspace(prefix: Tuple("multi", "second").pack())
        let firstKey = first.pack(Tuple("value"))
        let secondKey = second.pack(Tuple("value"))

        try await engine.withTransaction { transaction in
            try transaction.setValue([0x01], for: firstKey)
            try transaction.setValue([0x02], for: secondKey)
        }

        try await engine.withTransaction { transaction in
            let access = ScopedIndexReadAccess(
                transaction: transaction,
                subspaces: [first, second]
            )
            for (subspace, expectedKey) in [
                (first, firstKey),
                (second, secondKey),
            ] {
                let (begin, end) = subspace.range()
                var cursor = access.rangeCursor(
                    from: .firstGreaterOrEqual(begin),
                    to: .firstGreaterOrEqual(end),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                #expect(try await cursor.next()?.0 == expectedKey)
                #expect(try await cursor.next() == nil)
                try await cursor.finish()
            }

            let (firstBegin, _) = first.range()
            let (_, secondEnd) = second.range()
            var crossing = access.rangeCursor(
                from: .firstGreaterOrEqual(firstBegin),
                to: .firstGreaterOrEqual(secondEnd),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            await #expect(
                throws: IndexReadAccessError.rangeOutsideReadableIndex
            ) {
                try await crossing.next()
            }
            try await crossing.finish()
        }
    }
}
