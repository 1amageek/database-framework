import DatabaseKit

/// Resumable JOIN output shares right rows and optionally narrows physical equijoin candidates.
actor SQLJoinRowProducer: SQLRelationProducer {
    struct HashKey: Sendable {
        let values: [CanonicalSQLValueIdentity]
        let lease: DatabaseQueryPayloadLease
    }

    private struct HashEntry: Sendable {
        let key: HashKey
        let hash: Int
        let ordinal: Int
        let relation: CanonicalRelation
    }

    nonisolated let schema: CanonicalRelationSchema
    private let gate = SQLRelationSerialGate()
    private let meter: DatabaseWorkMeter
    private let reservation: DatabaseIntermediateReservation
    private var left: (any SQLRelationProducer)?
    private var right: (any SQLRelationProducer)?
    private var rightCache: SQLCTECache?
    private var factory: (@Sendable (CanonicalSourceRow?) async throws -> any SQLRelationProducer)?
    private var matches: (@Sendable (CanonicalSourceRow, CanonicalSourceRow) async throws -> Bool)?
    private var merge: (@Sendable (CanonicalSourceRow, CanonicalSourceRow) throws -> CanonicalSourceRow)?
    private let emptyLeft: CanonicalSourceRow
    private let emptyRight: CanonicalSourceRow
    private let type: JoinType
    private let lateral: Bool
    private var hashKey: (@Sendable (CanonicalSourceRow, Bool) throws -> HashKey?)?
    private var hashEntries: DatabaseSharedRetainedArray<HashEntry>?
    private var hashHeads: [Int] = []
    private var hashLinks: [Int] = []
    private var probeKey: HashKey?
    private var probeHash = 0
    private var probeIndex = -1
    private var currentLeft: CanonicalSourceRow?
    private var matchedLeft = false
    private var rightIndex = 0
    private var matchedRight: Set<Int> = []
    private var unmatchedRight = false
    private var closed = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, left: any SQLRelationProducer,
         rightSchema: CanonicalRelationSchema, type: JoinType, lateral: Bool,
         meter: DatabaseWorkMeter,
         hashKey: (@Sendable (CanonicalSourceRow, Bool) throws -> HashKey?)? = nil,
         rightFactory: @escaping @Sendable (CanonicalSourceRow?) async throws -> any SQLRelationProducer,
         matches: @escaping @Sendable (CanonicalSourceRow, CanonicalSourceRow) async throws -> Bool,
         merge: @escaping @Sendable (CanonicalSourceRow, CanonicalSourceRow) throws -> CanonicalSourceRow) throws {
        self.schema = schema; self.left = left; self.type = type; self.lateral = lateral
        self.meter = meter; self.factory = rightFactory; self.matches = matches; self.merge = merge
        self.hashKey = hashKey
        self.emptyLeft = left.schema.nullRow(); self.emptyRight = rightSchema.nullRow()
        self.reservation = try meter.reserveIntermediate(bytes: 512, at: .joinCandidate)
        if !lateral {
            self.rightCache = try SQLCTECache(schema: rightSchema, meter: meter) { try await rightFactory(nil) }
        }
    }

    func next() async throws -> CanonicalRelation? {
        try await gate.acquire(meter: meter)
        do {
            await Task.yield()
            try meter.checkpoint(at: .joinCandidate)
            if let failure { throw failure }
            let result = try await advance()
            await Task.yield()
            try meter.checkpoint(at: .joinCandidate)
            await gate.release()
            return result
        } catch {
            let operationError = error
            failure = operationError
            do { try await finish() }
            catch { await gate.release(); throw SQLRelationCleanupError(operationError: operationError, cleanupError: error) }
            await gate.release(); throw operationError
        }
    }

    private func advance() async throws -> CanonicalRelation? {
        guard !closed, let left, let factory, let matches, let merge else { return nil }
        while true {
            try meter.checkpoint(at: .joinCandidate)
            if unmatchedRight {
                guard let next = try await right?.next() else { try await finish(); return nil }
                let index = rightIndex; rightIndex += 1
                if matchedRight.contains(index) { continue }
                let row = try merge(emptyLeft, next.rows[0].retainingPayload(next.rows.payloadLease))
                let result = try sqlSingleRow(row, schema: schema, meter: meter)
                return result
            }
            if currentLeft == nil {
                guard let next = try await left.next() else {
                    if type == .right || type == .full {
                        right = rightCache?.consumer(); rightIndex = 0; unmatchedRight = true
                        continue
                    }
                    try await finish(); return nil
                }
                currentLeft = next.rows[0].retainingPayload(next.rows.payloadLease)
                matchedLeft = false; rightIndex = 0
                if let hashKey, let currentLeft {
                    try await prepareHashCandidates()
                    probeKey = try hashKey(currentLeft, true)
                    if let probeKey {
                        probeHash = try CanonicalSQLValueIdentity.hash(probeKey.values, workMeter: meter, stage: .joinCandidate)
                        probeIndex = hashHeads[canonicalHashLookupSlot(hashValue: probeHash, mask: hashHeads.count - 1)]
                    } else { probeIndex = -1 }
                } else {
                    right = lateral ? try await factory(currentLeft) : rightCache?.consumer()
                }
            }
            guard let lhs = currentLeft else { throw CanonicalReadError.unsupportedSelectQuery("Missing JOIN left frame") }
            if let candidate = try await nextRightCandidate() {
                let index = candidate.index
                let rhs = candidate.row
                try meter.consume(at: .joinCandidate)
                guard try await matches(lhs, rhs) else { continue }
                matchedLeft = true
                if (type == .right || type == .full), !matchedRight.contains(index) {
                    try reservation.reserveAdditional(bytes: 64, at: .joinCandidate)
                    matchedRight.insert(index)
                }
                let result = try sqlSingleRow(merge(lhs, rhs), schema: schema, meter: meter)
                return result
            }
            try await right?.close(); right = nil
            currentLeft = nil; probeKey = nil; probeIndex = -1
            if !matchedLeft, type == .left || type == .full {
                let result = try sqlSingleRow(merge(lhs, emptyRight), schema: schema, meter: meter)
                return result
            }
        }
    }

    private func nextRightCandidate() async throws -> (index: Int, row: CanonicalSourceRow)? {
        guard hashKey != nil else {
            guard let next = try await right?.next() else { return nil }
            let index = rightIndex; rightIndex += 1
            return (index, next.rows[0].retainingPayload(next.rows.payloadLease))
        }
        guard let entries = hashEntries, let probeKey else { return nil }
        while probeIndex >= 0 {
            try meter.consume(at: .joinCandidate)
            let entry = entries[probeIndex]
            probeIndex = hashLinks[probeIndex]
            guard entry.hash == probeHash,
                  try CanonicalSQLValueIdentity.equal(entry.key.values, probeKey.values,
                    workMeter: meter, stage: .joinCandidate) else { continue }
            return (entry.ordinal, entry.relation.rows[0].retainingPayload(entry.relation.rows.payloadLease))
        }
        return nil
    }

    private func prepareHashCandidates() async throws {
        guard hashEntries == nil, let hashKey, let rightCache else { return }
        let source = rightCache.consumer()
        var entries = try DatabaseRetainedArrayBuilder<HashEntry>(workMeter: meter,
            stage: .joinCandidate, layout: DatabaseRetainedArrayLayout.forElement(HashEntry.self))
        do {
            var ordinal = 0
            while let relation = try await source.next() {
                try meter.consume(at: .joinCandidate)
                if let key = try hashKey(relation.rows[0], false) {
                    let hash = try CanonicalSQLValueIdentity.hash(key.values, workMeter: meter, stage: .joinCandidate)
                    try entries.append(footprint: DatabaseIntermediateFootprint(rows: 1)) {
                        HashEntry(key: key, hash: hash, ordinal: ordinal, relation: relation)
                    }
                }
                let next = ordinal.addingReportingOverflow(1)
                guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                ordinal = next.partialValue
            }
            try await source.close()
        } catch {
            let primary = error
            do { try await source.close() }
            catch { throw SQLRelationCleanupError(operationError: primary, cleanupError: error) }
            throw primary
        }
        let owned = try entries.finish().moveToSharedOwnership(at: .joinCandidate)
        let layout = try DatabaseRetainedArrayLayout.forElement(Int.self)
        let capacity = try layout.growth(from: 0, toFit: max(1, owned.count)).capacity
        let bytes = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: capacity, element: Int.self)
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: owned.count, element: Int.self))
        try reservation.reserveAdditional(bytes: bytes.bytes, at: .joinCandidate)
        hashHeads = Array(repeating: -1, count: capacity)
        hashLinks = Array(repeating: -1, count: owned.count)
        // Prepending in reverse preserves the original right-input order within each bucket.
        for index in owned.indices.reversed() {
            try meter.consume(at: .joinCandidate)
            let bucket = canonicalHashLookupSlot(hashValue: owned[index].hash, mask: capacity - 1)
            hashLinks[index] = hashHeads[bucket]
            hashHeads[bucket] = index
        }
        hashEntries = owned
    }

    func close() async throws {
        await gate.acquire()
        do { try await finish(); await gate.release() }
        catch { await gate.release(); throw error }
    }

    private func finish() async throws {
        guard !closed else { return }
        closed = true
        let left = left; let right = right; let cache = rightCache
        self.left = nil; self.right = nil; rightCache = nil
        factory = nil; matches = nil; merge = nil; currentLeft = nil
        hashKey = nil; probeKey = nil; hashEntries = nil
        hashHeads.removeAll(keepingCapacity: false); hashLinks.removeAll(keepingCapacity: false)
        matchedRight.removeAll(keepingCapacity: false); reservation.release()
        // Child cleanup can recursively close a left-deep JOIN chain, even after cancellation.
        await Task.yield()
        var failure: (any Error)?
        do { try await right?.close() } catch { failure = error }
        do { try await left?.close() } catch { if failure == nil { failure = error } }
        do { try await cache?.close() } catch { if failure == nil { failure = error } }
        await Task.yield()
        if let failure { throw failure }
    }
}
