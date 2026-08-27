import DatabaseKit
import DatabaseTypes
import DatabaseRuntime
import ScalarIndex
import StorageKit
import Synchronization
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct FusionExecutionContractItem {
    #Index(
        .ordered(
            name: "fusion_contract_value",
            keys: [.ascending(\FusionExecutionContractItem.value)],
            unique: false
        )
    )

    var id: String
    var value: Int64
}

@Persistable
private struct FusionAuthorizationCountingItem: SecurityPolicy {
    #Index(
        .ordered(
            name: "fusion_authorization_counting_value",
            keys: [.ascending(\FusionAuthorizationCountingItem.value)],
            unique: false
        )
    )

    static let queryDecisionCount = Mutex(0)
    static let queryLimits = Mutex<[UInt64?]>([])
    static let readDecisionCount = Mutex(0)

    var id: String
    var value: Int64

    static func permitsRead(
        of resource: borrowing FusionAuthorizationCountingItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = resource
        _ = context
        readDecisionCount.withLock { $0 += 1 }
        return true
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = query
        _ = context
        queryDecisionCount.withLock { $0 += 1 }
        queryLimits.withLock { $0.append(query.limit) }
        return true
    }

    static func permitsCreate(
        _ newResource: borrowing FusionAuthorizationCountingItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = newResource
        _ = context
        return true
    }

    static func permitsUpdate(
        from resource: borrowing FusionAuthorizationCountingItem,
        to newResource: borrowing FusionAuthorizationCountingItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = resource
        _ = newResource
        _ = context
        return true
    }

    static func permitsDelete(
        _ resource: borrowing FusionAuthorizationCountingItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = resource
        _ = context
        return true
    }
}

@Persistable
private struct FusionProjectedAuthorizationItem: SecurityPolicy {
    #Index(
        .ordered(
            name: "fusion_projected_authorization_value",
            keys: [.ascending(\FusionProjectedAuthorizationItem.value)],
            unique: false
        )
    )

    @Restricted(read: .roles(["fusion-secret-reader"]))
    var id: String = ""
    var value: Int64 = 0

    static func permitsRead(
        of resource: borrowing FusionProjectedAuthorizationItem,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsCreate(
        _ newResource: borrowing FusionProjectedAuthorizationItem,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing FusionProjectedAuthorizationItem,
        to newResource: borrowing FusionProjectedAuthorizationItem,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing FusionProjectedAuthorizationItem,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Suite("Fusion execution contracts", .serialized)
struct FusionExecutionContractTests {
    @Test("Candidate domain and match sink enforce identity score and lifetime")
    func matchSinkEnforcesContract() throws {
        let meter = makeMeter()
        let entity = try FusionExecutionContractItem.schemaEntity
        let domain = try FusionCandidateDomain.make(
            rows: [
                QueryRow(fields: ["id": .string("b"), "value": .int64(2)]),
                QueryRow(fields: ["id": .string("a"), "value": .int64(1)]),
            ],
            entity: entity,
            workMeter: meter
        )
        let keyA = try primaryKey("a")
        let keyB = try primaryKey("b")
        let keyC = try primaryKey("c")
        #expect(domain.primaryKey(at: 0) == keyA)
        #expect(domain.primaryKey(at: 1) == keyB)
        let baselineRows = meter.retainedIntermediateRows
        let baselineBytes = meter.retainedIntermediateBytes

        do {
            let sink = try FusionMatchSink(
                candidates: domain,
                scoring: .annotation(
                    name: "score",
                    order: .higherIsBetter
                ),
                limit: 2,
                workMeter: meter
            )
            #expect {
                try sink.submit(primaryKey: keyC, numericSignal: 1)
            } throws: { error in
                error as? FusionExecutionContractError
                    == .candidateDomainViolation(keyC)
            }
            #expect {
                try sink.submit(primaryKey: keyA, numericSignal: nil)
            } throws: { error in
                error as? FusionExecutionContractError == .invalidScoreSignal
            }
            try sink.submit(primaryKey: keyA, numericSignal: 2)
            #expect {
                try sink.submit(primaryKey: keyA, numericSignal: 2)
            } throws: { error in
                error as? FusionExecutionContractError == .duplicateMatch(keyA)
            }
            try sink.submit(primaryKey: keyB, numericSignal: 1)
            #expect {
                try sink.submit(primaryKey: keyC, numericSignal: 0)
            } throws: { error in
                error as? FusionExecutionContractError
                    == .matchLimitExceeded(maximum: 2)
            }
            let result = try sink.freeze(coverage: .exhausted)
            #expect(result.matches.map(\.primaryKey) == [keyA, keyB])
            #expect {
                try sink.submit(primaryKey: keyB, numericSignal: 1)
            } throws: { error in
                error as? FusionExecutionContractError == .matchSinkInvalidated
            }
        }

        #expect(meter.retainedIntermediateRows == baselineRows)
        #expect(meter.retainedIntermediateBytes == baselineBytes)
    }

    @Test("Raw and scoped Fusion candidates share validation failures and order")
    func rawFixtureAndScopedPageShareCandidateValidation() throws {
        let entity = try FusionExecutionContractItem.schemaEntity
        let orderedRows = [
            QueryRow(fields: ["id": .string("b"), "value": .int64(2)]),
            QueryRow(fields: ["id": .string("a"), "value": .int64(1)]),
        ]
        let keyA = try primaryKey("a")
        let keyB = try primaryKey("b")

        do {
            let meter = makeMeter()
            let domain = try FusionCandidateDomain.make(
                rows: orderedRows,
                entity: entity,
                workMeter: meter
            )
            #expect(domain.primaryKey(at: 0) == keyA)
            #expect(domain.primaryKey(at: 1) == keyB)
        }

        do {
            let meter = makeMeter()
            let page = try makeScopedRowPage(
                rows: orderedRows,
                workMeter: meter
            )
            let domain = try FusionCandidateDomain.make(
                rows: page,
                entity: entity,
                workMeter: meter
            )
            #expect(domain.primaryKey(at: 0) == keyA)
            #expect(domain.primaryKey(at: 1) == keyB)
        }

        let missingIdentityRows = [
            QueryRow(fields: ["value": .int64(1)]),
        ]
        do {
            let meter = makeMeter()
            #expect {
                try FusionCandidateDomain.make(
                    rows: missingIdentityRows,
                    entity: entity,
                    workMeter: meter
                )
            } throws: { error in
                error as? FusionExecutionContractError
                    == .missingIdentity(field: "id")
            }
        }

        do {
            let meter = makeMeter()
            let page = try makeScopedRowPage(
                rows: missingIdentityRows,
                workMeter: meter
            )
            #expect {
                try FusionCandidateDomain.make(
                    rows: page,
                    entity: entity,
                    workMeter: meter
                )
            } throws: { error in
                error as? FusionExecutionContractError
                    == .missingIdentity(field: "id")
            }
        }

        let duplicateIdentityRows = [
            QueryRow(fields: ["id": .string("a"), "value": .int64(1)]),
            QueryRow(fields: ["id": .string("a"), "value": .int64(2)]),
        ]
        do {
            let meter = makeMeter()
            #expect {
                try FusionCandidateDomain.make(
                    rows: duplicateIdentityRows,
                    entity: entity,
                    workMeter: meter
                )
            } throws: { error in
                error as? FusionExecutionContractError
                    == .duplicateIdentity(.string("a"))
            }
        }

        do {
            let meter = makeMeter()
            let page = try makeScopedRowPage(
                rows: duplicateIdentityRows,
                workMeter: meter
            )
            #expect {
                try FusionCandidateDomain.make(
                    rows: page,
                    entity: entity,
                    workMeter: meter
                )
            } throws: { error in
                error as? FusionExecutionContractError
                    == .duplicateIdentity(.string("a"))
            }
        }
    }

    @Test("Scoped Fusion candidates admit destination ownership before construction")
    func scopedCandidatesAdmitDestinationBeforeConstruction() throws {
        let entity = try FusionExecutionContractItem.schemaEntity
        let rows = [
            QueryRow(
                fields: ["id": .string("candidate"), "value": .int64(1)]
            )
        ]

        let measurementMeter = makeMeter()
        do {
            let page = try makeScopedRowPage(
                rows: rows,
                workMeter: measurementMeter
            )
            _ = try FusionCandidateDomain.make(
                rows: page,
                entity: entity,
                workMeter: measurementMeter
            )
        }
        let oneByteShort = measurementMeter.peakIntermediateBytes - 1
        #expect(measurementMeter.retainedIntermediateBytes == 0)

        let meter = makeMeter(maximumIntermediateBytes: oneByteShort)
        var failure: (any Error)?
        do {
            let page = try makeScopedRowPage(rows: rows, workMeter: meter)
            let sourceBytes = meter.retainedIntermediateBytes
            do {
                _ = try FusionCandidateDomain.make(
                    rows: page,
                    entity: entity,
                    workMeter: meter
                )
                Issue.record("Expected destination admission to fail")
            } catch {
                failure = error
            }
            #expect(meter.retainedIntermediateBytes == sourceBytes)
        }

        guard let workLimit = failure as? DatabaseWorkLimitError,
              case .maximumIntermediateBytes(
            let stage,
            _,
            let requested,
            let maximum
        ) = workLimit else {
            Issue.record(
                "Expected a destination byte-limit failure, got \(String(describing: failure))"
            )
            return
        }
        #expect(stage == .bindingCandidate)
        #expect(requested > 0)
        #expect(maximum == oneByteShort)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Composite Fusion candidate keys admit every decode allocation")
    func compositeCandidateKeysAdmitEveryDecodeAllocation() throws {
        let components: [any TupleElement] = (0..<8).map { index in
            "component-\(index)" as any TupleElement
        }
        let composite = Tuple(
            ByteString([0x41, 0x42, 0x43]),
            Tuple(components)
        )
        let packed = composite.pack()

        // This is the superseded count + 64 admission. It supplies the
        // deliberately insufficient budget used to reproduce the defect.
        let legacyMeter = makeMeter()
        let legacyBudget: UInt64
        do {
            var builder = try DatabaseRetainedArrayBuilder<
                DatabaseRetainedPrimaryKey
            >(
                workMeter: legacyMeter,
                stage: .storageRow,
                layout: try DatabaseRetainedArrayLayout.forElement(
                    DatabaseRetainedPrimaryKey.self
                ),
                expectedCount: 1
            )
            try builder.append(
                footprint: DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(packed.count) + 64
                ),
                at: .storageRow
            ) {
                let reservation = try legacyMeter.reserveIntermediate(
                    at: .storageRow
                )
                return DatabaseRetainedPrimaryKey(
                    value: try Tuple(packed: packed),
                    reservation: reservation
                )
            }
            let retained = try DatabaseRetainedPrimaryKeys(
                buffer: builder.finish()
            )
            legacyBudget = legacyMeter.peakIntermediateBytes
            _ = retained
        }
        #expect(legacyMeter.retainedIntermediateBytes == 0)

        func makeResult(
            key: ByteString,
            sourceMeter: DatabaseWorkMeter
        ) throws -> FusionIndexReadResult {
            FusionIndexReadResult(
                matches: [
                    FusionIndexMatch(
                        primaryKey: key,
                        numericSignal: nil
                    )
                ],
                coverage: .exhausted,
                reservation: try sourceMeter.reserveIntermediate(
                    at: .indexScan
                )
            )
        }

        let constrainedMeter = makeMeter(
            maximumIntermediateBytes: legacyBudget
        )
        let constrainedSourceMeter = makeMeter()
        let constrainedResult = try makeResult(
            key: packed,
            sourceMeter: constrainedSourceMeter
        )
        var failure: (any Error)?
        do {
            _ = try FusionExecution.makeRetainedPrimaryKeys(
                from: constrainedResult,
                workMeter: constrainedMeter
            )
            Issue.record(
                "Expected composite tuple allocation admission to fail"
            )
        } catch {
            failure = error
        }

        guard let workLimit = failure as? DatabaseWorkLimitError,
              case .maximumIntermediateBytes(
            let stage,
            _,
            let requested,
            let maximum
        ) = workLimit else {
            Issue.record(
                "Expected typed composite admission failure, got \(String(describing: failure))"
            )
            return
        }
        #expect(stage == .storageRow)
        #expect(requested > 0)
        #expect(maximum == legacyBudget)
        #expect(constrainedMeter.retainedIntermediateBytes == 0)

        let exactMeter = makeMeter()
        let exactSourceMeter = makeMeter()
        weak var sourceOwner: FusionTestByteOwner?
        var exactResult: FusionIndexReadResult?
        do {
            let owner = FusionTestByteOwner(
                bytes: Array(packed),
                retainedByteCount: packed.count
            )
            sourceOwner = owner
            exactResult = try makeResult(
                key: ByteString(retaining: owner),
                sourceMeter: exactSourceMeter
            )
        }
        var retained: DatabaseRetainedPrimaryKeys? = try
            FusionExecution.makeRetainedPrimaryKeys(
                from: try #require(exactResult),
                workMeter: exactMeter
            )
        exactResult = nil
        #expect(sourceOwner != nil)
        retained?.withRetainedPrimaryKey(at: 0) { key in
            #expect(key == composite)
        }
        #expect(retained?.count == 1)
        retained = nil
        #expect(sourceOwner == nil)
        #expect(exactMeter.retainedIntermediateBytes == 0)
    }

    @Test("Match sink grows from retained matches rather than declared limit")
    func matchSinkGrowsFromRetainedMatches() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 100,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 1_024,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let key = try primaryKey("one")

        do {
            let sink = try FusionMatchSink(
                candidates: nil,
                scoring: .position,
                limit: 1_000_000,
                workMeter: meter
            )
            try sink.submit(primaryKey: key, numericSignal: nil)
            let result = try sink.freeze(coverage: .exhausted)
            #expect(result.matches.map(\.primaryKey) == [key])
            #expect(meter.peakIntermediateBytes <= 1_024)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Match sink retains an exact admitted key owner")
    func matchSinkRetainsExactAdmittedKeyOwner() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 100,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 1_024,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        var result: FusionIndexReadResult?
        do {
            let sink = try FusionMatchSink(
                candidates: nil,
                scoring: .position,
                limit: 1,
                workMeter: meter
            )
            weak var originalOwner: FusionTestByteOwner?
            var submittedKey: ByteString?
            do {
                let canonicalBytes = Array(try primaryKey("owner"))
                let owner = FusionTestByteOwner(
                    bytes: canonicalBytes,
                    retainedByteCount: canonicalBytes.count
                )
                originalOwner = owner
                submittedKey = ByteString(retaining: owner)
                try sink.submit(
                    primaryKey: try #require(submittedKey),
                    numericSignal: nil
                )
            }
            submittedKey = nil
            #expect(originalOwner == nil)
            result = try sink.freeze(coverage: .exhausted)
            let expectedKey = try primaryKey("owner")
            #expect(result?.matches.first?.primaryKey == expectedKey)
            #expect(
                result?.matches.first?.primaryKey.retainedByteCount
                    == expectedKey.count
            )
        }
        result = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Tuple match key is not packed before byte admission")
    func tupleMatchKeyRequiresAdmissionBeforePacking() throws {
        let tuple = try PersistableIdentifierKeyCodec.tuple(
            forPersistedIdentifier: .string("generated")
        )
        let keyByteCount = tuple.packedByteCount
        let measurementMeter = makeMeter()
        let measurementSink = try FusionMatchSink(
            candidates: nil,
            scoring: .position,
            limit: 1,
            workMeter: measurementMeter
        )
        let sinkBytes = measurementMeter.retainedIntermediateBytes
        measurementSink.invalidate()
        #expect(measurementMeter.retainedIntermediateBytes == 0)

        let oneByteShort = try #require(
            sinkBytes.addingReportingOverflow(UInt64(keyByteCount)).overflow
                ? nil
                : sinkBytes + UInt64(keyByteCount) - 1
        )
        let meter = makeMeter(maximumIntermediateBytes: oneByteShort)
        let sink = try FusionMatchSink(
            candidates: nil,
            scoring: .position,
            limit: 1,
            workMeter: meter
        )
        do {
            try sink.submit(
                primaryKeyTuple: tuple,
                numericSignal: nil
            )
            Issue.record("Expected key admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected key admission failure: \(error)")
                return
            }
            #expect(stage == .indexScan)
            #expect(consumed == sinkBytes)
            #expect(requested == UInt64(keyByteCount))
            #expect(maximum == oneByteShort)
        }

        sink.invalidate()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Tuple match key transfers one exact self-contained owner")
    func tupleMatchKeyTransfersExactOwner() throws {
        let tuple = try PersistableIdentifierKeyCodec.tuple(
            forPersistedIdentifier: .string("generated-owner")
        )
        let expectedKey = tuple.pack()
        let meter = makeMeter()
        var result: FusionIndexReadResult?

        do {
            let sink = try FusionMatchSink(
                candidates: nil,
                scoring: .position,
                limit: 1,
                workMeter: meter
            )
            try sink.submit(
                primaryKeyTuple: tuple,
                numericSignal: nil
            )
            result = try sink.freeze(coverage: .exhausted)
            #expect(result?.matches.first?.primaryKey == expectedKey)
            #expect(
                result?.matches.first?.primaryKey.retainedByteCount
                    == expectedKey.count
            )
        }

        result = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("External key owner borrow cannot reenter a lifecycle lock")
    func externalKeyOwnerBorrowReentersWithoutDeadlock() throws {
        let canonicalBytes = Array(try primaryKey("reentrant-invalidation"))
        let meter = makeMeter()
        let sink = try FusionMatchSink(
            candidates: nil,
            scoring: .position,
            limit: 1,
            workMeter: meter
        )
        let owner = FusionTestByteOwner(
            bytes: canonicalBytes,
            retainedByteCount: canonicalBytes.count,
            onBorrow: { [weak sink] in sink?.invalidate() }
        )
        let key = ByteString(retaining: owner)

        #expect {
            try sink.submit(primaryKey: key, numericSignal: nil)
        } throws: { error in
            error as? FusionExecutionContractError == .matchSinkInvalidated
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Exact retained bytes preserve their original owner without copying")
    func retainedBytesLeaseExactOwner() throws {
        let canonicalBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let meter = makeMeter()
        weak var originalOwner: FusionTestByteOwner?
        var retainedBytes: ByteString?

        do {
            let owner = FusionTestByteOwner(
                bytes: canonicalBytes,
                retainedByteCount: canonicalBytes.count
            )
            originalOwner = owner
            var source: ByteString? = ByteString(retaining: owner)
            let reservation = try meter.reserveIntermediate(
                bytes: UInt64(canonicalBytes.count),
                at: .indexScan
            )
            retainedBytes = try DatabaseRetainedByteString.make(
                try #require(source),
                reservation: reservation,
                at: .indexScan
            )
            source = nil
        }

        #expect(originalOwner != nil)
        #expect(retainedBytes == ByteString(canonicalBytes))
        retainedBytes = nil
        #expect(originalOwner == nil)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Exact external bytes detach their enclosing owner once")
    func retainedBytesDetachExactExternalOwner() throws {
        let canonicalBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let meter = makeMeter()
        weak var originalOwner: FusionTestByteOwner?
        var retainedBytes: ByteString?

        do {
            let owner = FusionTestByteOwner(
                bytes: canonicalBytes,
                retainedByteCount: canonicalBytes.count,
                isStorageSelfContained: false
            )
            originalOwner = owner
            var source: ByteString? = ByteString(retaining: owner)
            let reservation = try meter.reserveIntermediate(
                bytes: UInt64(canonicalBytes.count),
                at: .indexScan
            )
            retainedBytes = try DatabaseRetainedByteString.make(
                try #require(source),
                reservation: reservation,
                at: .indexScan
            )
            source = nil
        }

        #expect(originalOwner == nil)
        #expect(retainedBytes == ByteString(canonicalBytes))
        retainedBytes = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Match sink rejects a decodable non-canonical tuple key")
    func matchSinkRejectsNonCanonicalTupleKey() throws {
        let meter = makeMeter()
        do {
            let sink = try FusionMatchSink(
                candidates: nil,
                scoring: .position,
                limit: 1,
                workMeter: meter
            )
            let baselineRows = meter.retainedIntermediateRows
            let baselineBytes = meter.retainedIntermediateBytes
            let nonCanonical: ByteString = [0x16, 0x00, 0x01]

            #expect {
                try sink.submit(
                    primaryKey: nonCanonical,
                    numericSignal: nil
                )
            } throws: { error in
                error as? FusionExecutionContractError
                    == .inconsistentPayload(nonCanonical)
            }
            #expect(meter.retainedIntermediateRows == baselineRows)
            #expect(meter.retainedIntermediateBytes == baselineBytes)
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Match sink charges copied key bytes as bounded work")
    func matchSinkChargesCopiedKeyBytesAsWork() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 3,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 100_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        do {
            let sink = try FusionMatchSink(
                candidates: nil,
                scoring: .position,
                limit: 1,
                workMeter: meter
            )
            let baselineBytes = meter.retainedIntermediateBytes
            let key = ByteString(repeating: 0x41, count: 600)

            #expect {
                try sink.submit(primaryKey: key, numericSignal: nil)
            } throws: { error in
                error as? DatabaseWorkLimitError == .maximumWorkUnits(
                    stage: .indexScan,
                    consumed: 1,
                    requested: 3,
                    maximum: 3
                )
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == baselineBytes)
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Read session confines keys and revokes open cursors")
    func readSessionConfinesAndRevokes() async throws {
        let entity = try FusionExecutionContractItem.schemaEntity
        let descriptor = try #require(entity.indexes.first)
        let indexSubspace = Subspace("fusion", "admitted-index")
        let entries = indexSubspace.subspace("entries")
        let key = entries.pack(Tuple("one"))
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: key)
        }

        try await engine.withTransaction { transaction in
            let session = try FusionIndexReadSession.testing(
                index: ReadableIndex(
                    descriptor: descriptor,
                    physicalLayout: try IndexPhysicalLayout(
                        name: "test.fusion-contract",
                        revision: 1
                    ),
                    subspace: indexSubspace
                ),
                transaction: transaction,
                snapshot: false,
                workMeter: makeMeter()
            )
            #expect(try await session.getValue(key: key)?.bytes == [1])
            await #expect {
                try await session.getValue(
                    key: Subspace("fusion", "other-index")
                        .pack(Tuple("one"))
                )
            } throws: { error in
                error as? FusionExecutionContractError
                    == .indexReadOutsideAdmittedSubspace(
                        index: descriptor.name
                    )
            }

            let cursor = try session.subspaceCursor(entries, reverse: false)
            #expect(try await cursor.next()?.key == key)
            try await session.invalidate()
            await #expect {
                _ = try await cursor.next()
            } throws: { error in
                error as? FusionExecutionContractError
                    == .indexReadSessionInvalidated(index: descriptor.name)
            }
            await #expect {
                _ = try await session.getValue(key: key)
            } throws: { error in
                error as? FusionExecutionContractError
                    == .indexReadSessionInvalidated(index: descriptor.name)
            }
        }
    }

    @Test("Every nested Fusion query is prepared before transaction admission")
    func nestedFusionQueriesArePreparedBeforeTransactionAdmission() async throws {
        let schema = try Schema(
            entities: [try FusionExecutionContractItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionExecutionContractItem.self
        )
        try entityRuntime.register(scalarProvider)
        let engine = TransactionCountingInMemoryEngine()
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-preflight-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                entityRuntimes: [
                    entityRuntime.registration(),
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let baselineTransactions = engine.transactionCount
        let nestedFusion = SelectQuery(
            projection: .all,
            source: .table(
                TableRef(FusionExecutionContractItem.persistableType)
            ),
            accessPath: .fusion(
                FusionSource(stages: [
                    FusionStageSource(inputs: [
                        FusionInput(
                            operation: .index(
                                FusionIndexSource(
                                    selection: .named(
                                        name: "fusion_contract_value",
                                        type: .ordered
                                    )
                                )
                            ),
                            scoring: .position
                        ),
                    ]),
                ])
            )
        )
        let queries = [
            SelectQuery(
                projection: .all,
                source: .subquery(nestedFusion, alias: "nested")
            ),
            SelectQuery(
                projection: .all,
                source: .table(TableRef("nested_cte")),
                subqueries: [
                    NamedSubquery(name: "nested_cte", query: nestedFusion),
                ]
            ),
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .exists(nestedFusion),
                        alias: "has_match"
                    ),
                ]),
                source: .values(
                    [[.int(1)]],
                    columnNames: ["seed"]
                )
            ),
        ]

        let context = container.testBaseContext()
        for query in queries {
            await #expect {
                _ = try await context.query(query)
            } throws: { error in
                error as? FusionExecutionError
                    == .indexExecutorNotRegistered(.ordered)
            }
            #expect(engine.transactionCount == baselineTransactions)
        }
    }

    @Test("Fusion relational inputs reject nested queries before admission")
    func relationalInputsRejectNestedQueries() async throws {
        let schema = try Schema(
            entities: [try FusionExecutionContractItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionExecutionContractItem.self
        )
        try entityRuntime.register(scalarProvider)
        let engine = TransactionCountingInMemoryEngine()
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-relational-subquery-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let baselineTransactions = engine.transactionCount
        let nested = SelectQuery(
            projection: .items([
                ProjectionItem(.literal(.int(1)), alias: "value"),
            ]),
            source: .values([[.int(1)]], columnNames: ["seed"])
        )
        let query = SelectQuery(
            projection: .all,
            source: .table(
                TableRef(FusionExecutionContractItem.persistableType)
            ),
            accessPath: .fusion(
                FusionSource(stages: [
                    FusionStageSource(inputs: [
                        FusionInput(
                            operation: .filter(.exists(nested))
                        ),
                    ]),
                    FusionStageSource(inputs: [
                        FusionInput(
                            operation: .index(
                                FusionIndexSource(
                                    selection: .named(
                                        name: "fusion_contract_value",
                                        type: .ordered
                                    )
                                )
                            ),
                            scoring: .position,
                            requirement: .candidates
                        ),
                    ]),
                ])
            )
        )

        await #expect {
            _ = try await container.testBaseContext().query(query)
        } throws: { error in
            error as? FusionExecutionError
                == .relationalSubqueryNotSupported
        }
        #expect(engine.transactionCount == baselineTransactions)
    }

    @Test("Candidate set algebra preserves one canonical payload per identity")
    func candidateSetAlgebraPreservesPayload() throws {
        let meter = makeMeter()
        let entity = try FusionExecutionContractItem.schemaEntity
        let left = try FusionCandidateDomain.make(
            rows: [
                QueryRow(fields: ["id": .string("c"), "value": .int64(3)]),
                QueryRow(fields: ["id": .string("a"), "value": .int64(1)]),
            ],
            entity: entity,
            workMeter: meter
        )
        let right = try FusionCandidateDomain.make(
            rows: [
                QueryRow(fields: ["id": .string("b"), "value": .int64(2)]),
                QueryRow(fields: ["id": .string("c"), "value": .int64(3)]),
            ],
            entity: entity,
            workMeter: meter
        )

        let union = try left.union(right, workMeter: meter)
        #expect(union.count == 3)
        #expect((0..<union.count).map(union.primaryKey(at:)) == [
            try primaryKey("a"),
            try primaryKey("b"),
            try primaryKey("c"),
        ])

        let intersection = try left.intersection(right, workMeter: meter)
        #expect(intersection.count == 1)
        let keyC = try primaryKey("c")
        #expect(intersection.primaryKey(at: 0) == keyC)

        let inconsistent = try FusionCandidateDomain.make(
            rows: [
                QueryRow(fields: ["id": .string("c"), "value": .int64(30)])
            ],
            entity: entity,
            workMeter: meter
        )
        #expect {
            _ = try left.intersection(inconsistent, workMeter: meter)
        } throws: { error in
            error as? FusionExecutionContractError
                == .inconsistentPayload(keyC)
        }
    }

    @Test("Score composition implements every declared strategy deterministically")
    func scoreCompositionImplementsEveryStrategy() throws {
        let meter = makeMeter()
        let entity = try FusionExecutionContractItem.schemaEntity
        let candidates = try FusionCandidateDomain.make(
            rows: [
                QueryRow(fields: ["id": .string("a"), "value": .int64(1)]),
                QueryRow(fields: ["id": .string("b"), "value": .int64(2)]),
            ],
            entity: entity,
            workMeter: meter
        )
        let keyA = try primaryKey("a")
        let keyB = try primaryKey("b")
        let annotation = try scoredInput(
            candidates: candidates,
            scoring: .annotation(name: "score", order: .higherIsBetter),
            matches: [(keyA, 10), (keyB, 0)],
            meter: meter
        )
        let position = try scoredInput(
            candidates: candidates,
            scoring: .position,
            matches: [(keyB, nil), (keyA, nil)],
            meter: meter
        )

        let sum = try composedScores(
            candidates: candidates,
            inputs: [annotation, position],
            strategy: .sum,
            meter: meter
        )
        #expect(sum.map(\.identity) == ["a", "b"])
        #expect(abs(sum[0].score - 1.5) < 0.000_001)
        #expect(abs(sum[1].score - 1.0) < 0.000_001)

        let maximum = try composedScores(
            candidates: candidates,
            inputs: [annotation, position],
            strategy: .maximum,
            meter: meter
        )
        #expect(maximum.map(\.identity) == ["a", "b"])
        #expect(maximum.map(\.score) == [1, 1])

        let weighted = try composedScores(
            candidates: candidates,
            inputs: [annotation, position],
            strategy: .weighted([0.25, 2]),
            meter: meter
        )
        #expect(weighted.map(\.identity) == ["b", "a"])
        #expect(abs(weighted[0].score - 2.0) < 0.000_001)
        #expect(abs(weighted[1].score - 1.25) < 0.000_001)

        let reciprocalRank = try composedScores(
            candidates: candidates,
            inputs: [annotation, position],
            strategy: .reciprocalRank(rankConstant: 60),
            meter: meter
        )
        #expect(reciprocalRank.map(\.identity) == ["a", "b"])
        #expect(abs(reciprocalRank[0].score - reciprocalRank[1].score)
            < 0.000_001)
    }

    @Test("Score composition bounds Top-K and preserves deterministic ties")
    func scoreCompositionBoundsTopK() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 1_000_000,
                maximumIntermediateRows: 1_000,
                maximumIntermediateBytes: 2_000_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let entity = try FusionExecutionContractItem.schemaEntity
        let payload = String(repeating: "x", count: 4_096)
        let rows = (0..<64).map { index in
            QueryRow(fields: [
                "id": .string(paddedIdentity(index)),
                "value": .int64(Int64(index)),
                "payload": .string(payload),
            ])
        }
        let candidates = try FusionCandidateDomain.make(
            rows: rows,
            entity: entity,
            workMeter: meter
        )
        let matches = try (0..<64).map { index in
            (try primaryKey(paddedIdentity(index)), 1.0 as Double?)
        }
        let input = try scoredInput(
            candidates: candidates,
            scoring: .annotation(
                name: "score",
                order: .higherIsBetter
            ),
            matches: matches,
            meter: meter
        )

        let beforeTopK = meter.peakIntermediateBytes
        let empty = try composedScores(
            candidates: candidates,
            inputs: [input],
            strategy: .sum,
            maximumResultCount: 0,
            meter: meter
        )
        #expect(empty.isEmpty)
        let topThree = try composedScores(
            candidates: candidates,
            inputs: [input],
            strategy: .sum,
            maximumResultCount: 3,
            meter: meter
        )
        #expect(topThree.map(\.identity) == ["000", "001", "002"])
        #expect(meter.peakIntermediateBytes - beforeTopK < 80_000)
    }

    @Test("Score normalization preserves the request work limit")
    func scoreNormalizationPreservesWorkLimit() throws {
        let setupMeter = makeMeter()
        let candidates = try FusionCandidateDomain.make(
            rows: [
                QueryRow(fields: ["id": .string("a")]),
                QueryRow(fields: ["id": .string("b")]),
            ],
            entity: try FusionExecutionContractItem.schemaEntity,
            workMeter: setupMeter
        )
        let input = try scoredInput(
            candidates: candidates,
            scoring: .annotation(
                name: "score",
                order: .higherIsBetter
            ),
            matches: [
                (try primaryKey("a"), 2),
                (try primaryKey("b"), 1),
            ],
            meter: setupMeter
        )
        var builder = try DatabaseRetainedArrayBuilder<FusionScoredInput>(
            workMeter: setupMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionScoredInput.self
            ),
            expectedCount: 1
        )
        try builder.append(
            footprint: DatabaseIntermediateFootprint(rows: 1)
        ) {
            input
        }
        let retainedInputs = try builder.finish().moveToSharedOwnership(
            at: .projection
        )
        let executionMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(maximumWorkUnits: 1),
            monotonicClock: TestProcessMonotonicClock()
        )

        #expect {
            _ = try FusionScoreComposer.compose(
                candidates: candidates,
                scoredInputs: retainedInputs,
                strategy: .sum,
                maximumResultCount: nil,
                workMeter: executionMeter
            )
        } throws: { error in
            guard case .maximumWorkUnits(
                stage: .projection,
                consumed: 1,
                requested: 1,
                maximum: 1
            ) = error as? DatabaseWorkLimitError else {
                return false
            }
            return true
        }
        #expect(executionMeter.retainedIntermediateRows == 0)
        #expect(executionMeter.retainedIntermediateBytes == 0)
    }

    @Test("Transaction-bound execution sanitizes physical contract failures")
    func transactionBoundExecutionSanitizesContractFailure() async throws {
        let schema = try Schema(
            entities: [try FusionExecutionContractItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionExecutionContractItem.self
        )
        try entityRuntime.register(scalarProvider)
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-transaction-sanitization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                fusionIndexReadExecutors: [
                    ContractFailingFusionReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(FusionExecutionContractItem(id: "one", value: 1))
        try await context.save()
        let execution = ReadExecutionContext(
            options: .default,
            monotonicClock: container.monotonicClock
        )
        let query = SelectQuery(
            projection: .all,
            source: .table(
                TableRef(FusionExecutionContractItem.persistableType)
            ),
            accessPath: .fusion(
                FusionSource(stages: [
                    FusionStageSource(inputs: [
                        FusionInput(
                            operation: .index(
                                FusionIndexSource(
                                    selection: .named(
                                        name: "fusion_contract_value",
                                        type: .ordered
                                    )
                                )
                            ),
                            scoring: .position
                        ),
                    ]),
                ])
            )
        )

        await #expect {
            _ = try await context.withReadSnapshot(
                workMeter: execution.workMeter
            ) { snapshot in
                try await context.querySessionBound(
                    query,
                    execution: execution,
                    session: snapshot.session
                )
            }
        } throws: { error in
            error as? FusionExecutionError == .executionContractViolation
        }
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Feature validation occurs once before Fusion execution")
    func featureValidationOccursOnce() async throws {
        let schema = try Schema(
            entities: [try FusionExecutionContractItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionExecutionContractItem.self
        )
        try entityRuntime.register(scalarProvider)
        let executor = ValidationCountingFusionReadExecutor()
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-single-validation-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                fusionIndexReadExecutors: [executor],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(FusionExecutionContractItem(id: "one", value: 1))
        try await context.save()

        await #expect {
            _ = try await context.query(
                SelectQuery(
                    projection: .all,
                    source: .table(
                        TableRef(FusionExecutionContractItem.persistableType)
                    ),
                    accessPath: .fusion(
                        FusionSource(stages: [
                            FusionStageSource(inputs: [
                                FusionInput(
                                    operation: .index(
                                        FusionIndexSource(
                                            selection: .named(
                                                name: "fusion_contract_value",
                                                type: .ordered
                                            )
                                        )
                                    ),
                                    scoring: .position
                                ),
                            ]),
                        ])
                    )
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .executionContractViolation
        }
        #expect(executor.validationCount == 1)
    }

    @Test("Fusion query authorization is decided once")
    func queryAuthorizationOccursOnce() async throws {
        let schema = try Schema(
            entities: [try FusionAuthorizationCountingItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionAuthorizationCountingItem.self
        )
        try entityRuntime.register(scalarProvider)
        let executor = ValidationCountingFusionReadExecutor()
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-single-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                fusionIndexReadExecutors: [executor],
                entityRuntimes: [entityRuntime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        FusionAuthorizationCountingItem.self
                    ),
                ]
            ),
            security: .enabled()
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            FusionAuthorizationCountingItem(id: "one", value: 1)
        )
        try await context.save()
        FusionAuthorizationCountingItem.queryDecisionCount.withLock { $0 = 0 }
        FusionAuthorizationCountingItem.queryLimits.withLock {
            $0.removeAll(keepingCapacity: false)
        }
        FusionAuthorizationCountingItem.readDecisionCount.withLock { $0 = 0 }

        await #expect {
            _ = try await context.query(
                SelectQuery(
                    projection: .all,
                    source: .table(
                        TableRef(
                            FusionAuthorizationCountingItem.persistableType
                        )
                    ),
                    accessPath: .fusion(
                        FusionSource(stages: [
                            FusionStageSource(inputs: [
                                FusionInput(
                                    operation: .index(
                                        FusionIndexSource(
                                            selection: .named(
                                                name: "fusion_authorization_counting_value",
                                                type: .ordered
                                            )
                                        )
                                    ),
                                    scoring: .position
                                ),
                            ]),
                        ])
                    )
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .executionContractViolation
        }
        #expect(
            FusionAuthorizationCountingItem.queryDecisionCount.withLock { $0 }
                == 1
        )
        #expect(executor.validationCount == 1)
    }

    @Test("Fusion materialization preserves the sealed field projection")
    func fusionMaterializationPreservesSealedFieldSet() async throws {
        let schema = try Schema(
            entities: [try FusionProjectedAuthorizationItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionProjectedAuthorizationItem.self
        )
        try entityRuntime.register(scalarProvider)
        let executor = FixedFusionReadExecutor(
            primaryKey: try primaryKey("one")
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-projected-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                fusionIndexReadExecutors: [executor],
                entityRuntimes: [entityRuntime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        FusionProjectedAuthorizationItem.self
                    ),
                ]
            ),
            security: .enabled()
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            FusionProjectedAuthorizationItem(id: "one", value: 42)
        )
        try await context.save()

        let response = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .column(ColumnRef(column: "value"))
                    )
                ]),
                source: .table(
                    TableRef(FusionProjectedAuthorizationItem.persistableType)
                ),
                accessPath: .fusion(
                    FusionSource(stages: [
                        FusionStageSource(inputs: [
                            FusionInput(
                                operation: .index(
                                    FusionIndexSource(
                                        selection: .named(
                                            name:
                                                "fusion_projected_authorization_value",
                                            type: .ordered
                                        )
                                    )
                                ),
                                scoring: .position
                            ),
                        ]),
                    ])
                )
            )
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields == ["value": .int64(42)])
    }

    @Test("Missing Fusion entities are disclosed only after list authorization")
    func missingEntityDisclosureFollowsAuthorization() async throws {
        let schema = try Schema(
            entities: [try FusionAuthorizationCountingItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionAuthorizationCountingItem.self
        )
        try entityRuntime.register(scalarProvider)
        let runtime = try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "fusion-missing-entity-authorization-tests",
                revision: 1
            ),
            indexMaintainerProviderDescriptors: [
                .init(describing: scalarProvider),
            ],
            entityRuntimes: [entityRuntime.registration()],
            authorizationPolicies: [
                AuthorizationPolicyHandler(
                    FusionAuthorizationCountingItem.self
                ),
            ]
        )
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("fusion_missing_entity")),
            accessPath: .fusion(
                FusionSource(stages: [
                    FusionStageSource(inputs: [
                        FusionInput(
                            operation: .index(
                                FusionIndexSource(
                                    selection: .named(
                                        name: "fusion_hidden_index",
                                        type: .ordered
                                    )
                                )
                            ),
                            scoring: .position
                        ),
                    ]),
                ])
            )
        )

        let secured = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: runtime,
            security: .enabled()
        )
        do {
            defer { await secured.shutdown() }
            do {
                _ = try await secured.testBaseContext().query(query)
                Issue.record("Expected list authorization to reject the query")
            } catch let error as SecurityError {
                #expect(error.operation == .list)
                #expect(error.targetType == "fusion_missing_entity")
            } catch {
                Issue.record("Unexpected pre-authorization error: \(error)")
            }
        }

        let unsecured = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: runtime,
            security: .testingDisabled
        )
        defer { await unsecured.shutdown() }
        await #expect {
            _ = try await unsecured.testBaseContext().query(query)
        } throws: { error in
            guard case .unsupportedSource(let message) =
                error as? CanonicalReadError else {
                return false
            }
            return message.contains("fusion_missing_entity")
        }
    }

    @Test("Relational Fusion execution reuses its query authorization")
    func relationalExecutionReusesQueryAuthorization() async throws {
        let schema = try Schema(
            entities: [try FusionAuthorizationCountingItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionAuthorizationCountingItem.self
        )
        try entityRuntime.register(scalarProvider)
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-relational-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                entityRuntimes: [entityRuntime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        FusionAuthorizationCountingItem.self
                    ),
                ]
            ),
            security: .enabled()
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            FusionAuthorizationCountingItem(id: "one", value: 1)
        )
        try await context.save()
        FusionAuthorizationCountingItem.queryDecisionCount.withLock { $0 = 0 }
        FusionAuthorizationCountingItem.queryLimits.withLock {
            $0.removeAll(keepingCapacity: false)
        }
        FusionAuthorizationCountingItem.readDecisionCount.withLock { $0 = 0 }

        let query = SelectQuery(
                projection: .all,
                source: .table(
                    TableRef(FusionAuthorizationCountingItem.persistableType)
                ),
                accessPath: .fusion(
                    FusionSource(stages: [
                        FusionStageSource(inputs: [
                            FusionInput(
                                operation: .filter(
                                    .greaterThan(
                                        .column(ColumnRef(column: "value")),
                                        .literal(.int(0))
                                    )
                                ),
                                limit: 1
                            ),
                        ]),
                        FusionStageSource(inputs: [
                            FusionInput(
                                operation: .order([
                                    SortKey(
                                        .column(ColumnRef(column: "value")),
                                        direction: .descending
                                    ),
                                ]),
                                scoring: .position,
                                requirement: .candidates
                            ),
                        ]),
                    ])
                )
            )
        let analysisExecution = ReadExecutionContext(
            options: .default,
            monotonicClock: container.monotonicClock
        )
        let requirementCount = try await context.withDataOperation {
            try FusionPreflight.resolveGraph(
                query,
                context: context,
                workMeter: analysisExecution.workMeter
            ).listAuthorizationRequirements.count
        }
        #expect(requirementCount == 1)

        let response = try await context.query(query)

        #expect(response.rows.map { $0.fields["id"] } == [.string("one")])
        #expect(
            FusionAuthorizationCountingItem.queryDecisionCount.withLock { $0 }
                == 1
        )
        #expect(
            FusionAuthorizationCountingItem.queryLimits.withLock { $0 }
                == [nil]
        )
        #expect(
            FusionAuthorizationCountingItem.readDecisionCount.withLock { $0 }
                == 1
        )
    }

    @Test("Prepared Fusion lookup binds each exact logical list requirement")
    func preparedLookupBindsExactLogicalListRequirement() async throws {
        let schema = try Schema(
            entities: [try FusionExecutionContractItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionExecutionContractItem.self
        )
        try entityRuntime.register(scalarProvider)
        let fusionExecutor = FixedFusionReadExecutor(
            primaryKey: try primaryKey("one")
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-logical-requirement-binding-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                fusionIndexReadExecutors: [fusionExecutor],
                entityRuntimes: [
                    entityRuntime.registration(),
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            FusionExecutionContractItem(id: "one", value: 1)
        )
        try await context.save()

        let source = FusionSource(stages: [
            FusionStageSource(inputs: [
                FusionInput(
                    operation: .index(
                        FusionIndexSource(
                            selection: .named(
                                name: "fusion_contract_value",
                                type: .ordered
                            )
                        )
                    ),
                    scoring: .position
                ),
            ]),
        ])
        func nestedQuery(limit: UInt64) -> SelectQuery {
            SelectQuery(
                projection: .all,
                source: .table(
                    TableRef(FusionExecutionContractItem.persistableType)
                ),
                accessPath: .fusion(source),
                limit: limit
            )
        }
        let response = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .exists(nestedQuery(limit: 1)),
                        alias: "first"
                    ),
                    ProjectionItem(
                        .exists(nestedQuery(limit: 2)),
                        alias: "second"
                    ),
                ]),
                source: .values(
                    [[.int(1)]],
                    columnNames: ["seed"]
                )
            )
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["first"] == .bool(true))
        #expect(response.rows[0].fields["second"] == .bool(true))
    }

    @Test("Authorization evidence cannot cross principals")
    func authorizationEvidenceIsPrincipalBound() async throws {
        let schema = try Schema(
            entities: [try FusionExecutionContractItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionExecutionContractItem.self
        )
        try entityRuntime.register(scalarProvider)
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-authorization-binding-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let admitted = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "fusion-admitted")
            )
        )

        try await admitted.withDataOperation {
            let policy = try admitted.readPolicy()
            let authorization = try policy.authorizeRead(
                listRequirements: [],
                fields: DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: [:]
                )
            )
            let foreignPolicy = DatabaseReadPolicy(
                schemaLease: container.acquireActiveSchemaLease(),
                authorization: .authenticated(
                    Principal(identifier: "fusion-foreign")
                )
            )
            #expect(
                throws: DatabaseReadSessionError.authorizationMismatch
            ) {
                try foreignPolicy.validate(authorization)
            }
        }
    }

    @Test("Parent session revokes an escaped Fusion index lease")
    func parentSessionRevokesEscapedIndexLease() async throws {
        let schema = try Schema(
            entities: [try FusionExecutionContractItem.schemaEntity]
        )
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FusionExecutionContractItem.self
        )
        try entityRuntime.register(scalarProvider)
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fusion-parent-lease-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(FusionExecutionContractItem(id: "one", value: 1))
        try await context.save()
        let meter = makeMeter()
        let entity = try FusionExecutionContractItem.schemaEntity
        let descriptor = try #require(entity.indexes.first)

        let escaped: any FusionIndexReadAccess = try await context
            .withStorageAccess(requiredAccess: .read) { _ in
                try await DatabaseReadSession.withSession(
                    context: context,
                    workMeter: meter
                ) { session in
                    let index = try #require(
                        await session.readableIndex(
                            named: descriptor.name,
                            indexType: descriptor.type,
                            forEntityName: entity.name,
                            partitions: FieldObject()
                        )
                    )
                    return try await session.withFusionIndexReadLease(
                        index: index,
                        snapshot: false,
                        workMeter: meter
                    ) { lease in
                        lease as any FusionIndexReadAccess
                    }
                }
            }

        await #expect {
            _ = try await escaped.getValue(key: escaped.index.subspace.prefix)
        } throws: { error in
            error as? FusionExecutionContractError
                == .indexReadSessionInvalidated(index: descriptor.name)
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Sanitization preserves nested cleanup failures")
    func sanitizationPreservesNestedCleanupFailures() throws {
        let original = StorageTransactionCleanupError(
            operationError: StorageRangeCleanupError(
                iterationError: FusionExecutionContractError
                    .invalidScoreSignal,
                cleanupError: FusionSanitizationSentinel.rangeCleanup
            ),
            cancellationError: FusionSanitizationSentinel.cancellationOne
        ).addingCancellationError(
            FusionSanitizationSentinel.cancellationTwo
        )
        let sanitized = sanitizedFusionExecutionError(original)
        let transactionCleanup = try #require(
            sanitized as? StorageTransactionCleanupError
        )
        let rangeCleanup = try #require(
            transactionCleanup.operationError as? StorageRangeCleanupError
        )

        #expect(
            rangeCleanup.iterationError as? FusionExecutionError
                == .executionContractViolation
        )
        #expect(
            rangeCleanup.cleanupError as? FusionSanitizationSentinel
                == .rangeCleanup
        )
        #expect(transactionCleanup.cancellationErrors.count == 2)
        #expect(
            transactionCleanup.cancellationErrors[0]
                as? FusionSanitizationSentinel == .cancellationOne
        )
        #expect(
            transactionCleanup.cancellationErrors[1]
                as? FusionSanitizationSentinel == .cancellationTwo
        )
    }

    private func makeMeter(
        maximumIntermediateBytes: UInt64 = 1_000_000
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: 1_000,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func primaryKey(_ identity: String) throws -> ByteString {
        try PersistableIdentifierKeyCodec.tuple(
            forPersistedIdentifier: .string(identity)
        ).pack()
    }

    private func makeScopedRowPage(
        rows: [QueryRow],
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedQueryRowView {
        let ownerRows = [
            QueryRow(fields: ["id": .string("page-prefix")]),
        ] + rows + [
            QueryRow(fields: ["id": .string("page-suffix")]),
        ]
        var builder = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: ownerRows.count
        )
        for row in ownerRows {
            try builder.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                at: .bindingCandidate,
                make: { row }
            )
        }
        let owner = try builder.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
        return CanonicalRetainedQueryRowView(
            owner: owner,
            range: 1..<(owner.count - 1)
        )
    }

    private func paddedIdentity(_ index: Int) -> String {
        let value = String(index)
        return String(repeating: "0", count: 3 - value.count) + value
    }

    private func scoredInput(
        candidates: FusionCandidateDomain,
        scoring: FusionScoring,
        matches: [(ByteString, Double?)],
        meter: DatabaseWorkMeter
    ) throws -> FusionScoredInput {
        let sink = try FusionMatchSink(
            candidates: candidates,
            scoring: scoring,
            limit: matches.count,
            workMeter: meter
        )
        for match in matches {
            try sink.submit(
                primaryKey: match.0,
                numericSignal: match.1
            )
        }
        return FusionScoredInput(
            scoring: scoring,
            result: try sink.freeze(coverage: .exhausted)
        )
    }

    private func composedScores(
        candidates: FusionCandidateDomain,
        inputs: [FusionScoredInput],
        strategy: FusionStrategy,
        maximumResultCount: Int? = nil,
        meter: DatabaseWorkMeter
    ) throws -> [(identity: String, score: Double)] {
        var builder = try DatabaseRetainedArrayBuilder<FusionScoredInput>(
            workMeter: meter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionScoredInput.self
            ),
            expectedCount: inputs.count
        )
        for input in inputs {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1)
            ) {
                input
            }
        }
        let retainedInputs = try builder.finish().moveToSharedOwnership(
            at: .projection
        )
        let result = try FusionScoreComposer.compose(
            candidates: candidates,
            scoredInputs: retainedInputs,
            strategy: strategy,
            maximumResultCount: maximumResultCount,
            workMeter: meter
        )
        var composed: [(identity: String, score: Double)] = []
        composed.reserveCapacity(result.count)
        for index in 0..<result.count {
            try result.withRow(at: index) { row in
                guard case .string(let identity) = row.fields["id"],
                      let score = row.annotations[
                        FusionExecutor.scoreAnnotation
                      ]?.float64Value else {
                    throw FusionExecutionContractError.invalidScoreSignal
                }
                composed.append((identity, score))
            }
        }
        return composed
    }
}

private enum FusionSanitizationSentinel: Error, Equatable, Sendable {
    case rangeCleanup
    case cancellationOne
    case cancellationTwo
}

private struct ContractFailingFusionReadExecutor: FusionIndexReadExecutor {
    let indexType: IndexType = .ordered

    func validate(_ request: FusionIndexValidationRequest) throws {}

    func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        throw FusionExecutionContractError.invalidScoreSignal
    }

    func executeRestricted(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        throw FusionExecutionContractError.invalidScoreSignal
    }
}

private struct FixedFusionReadExecutor: FusionIndexReadExecutor {
    let indexType: IndexType = .ordered
    let primaryKey: ByteString

    func validate(_ request: FusionIndexValidationRequest) throws {}

    func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        try output.submit(primaryKey: primaryKey, numericSignal: nil)
        return .exhausted
    }

    func executeRestricted(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        if try candidates.contains(
            primaryKey: primaryKey,
            workMeter: request.workMeter
        ) {
            try output.submit(primaryKey: primaryKey, numericSignal: nil)
        }
        return .exhausted
    }
}

private final class ValidationCountingFusionReadExecutor:
    FusionIndexReadExecutor,
    Sendable
{
    let indexType: IndexType = .ordered

    private let count = Mutex(0)

    var validationCount: Int {
        count.withLock { $0 }
    }

    func validate(_ request: FusionIndexValidationRequest) throws {
        count.withLock { $0 += 1 }
    }

    func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        throw FusionExecutionContractError.invalidScoreSignal
    }

    func executeRestricted(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        throw FusionExecutionContractError.invalidScoreSignal
    }
}
