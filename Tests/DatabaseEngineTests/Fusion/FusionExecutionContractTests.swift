import DatabaseKit
import DatabaseTypes
import DatabaseRuntime
import ScalarIndex
import StorageKit
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

@Suite("Fusion execution contracts")
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
                    requested: 6,
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
            let session = try FusionIndexReadSession(
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
            _ = try await context.executeCanonicalRead { transaction in
                try await context.executeCanonicalQuery(
                    query,
                    execution: execution,
                    transaction: transaction
                )
            }
        } throws: { error in
            error as? FusionExecutionError == .executionContractViolation
        }
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
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

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: 1_000,
                maximumIntermediateBytes: 1_000_000,
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
        return try result.rows.map { row in
            guard case .string(let identity) = row.fields["id"],
                  let score = row.annotations[
                    FusionExecutor.scoreAnnotation
                  ]?.float64Value else {
                throw FusionExecutionContractError.invalidScoreSignal
            }
            return (identity, score)
        }
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
