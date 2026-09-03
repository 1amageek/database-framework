@_spi(DatabaseExecution) import Database
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import StorageKit
import Synchronization
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Canonical SQL retained ownership", .serialized)
struct CanonicalSQLRetainedOwnershipTests {
    @Test("Canonical group hash slots mask before signed conversion")
    func canonicalGroupHashSlotBoundsSignedHashes() {
        #expect(canonicalHashLookupSlot(hashValue: Int.min, mask: 7) == 0)
        #expect(canonicalHashLookupSlot(hashValue: -1, mask: 7) == 7)
        #expect(canonicalHashLookupSlot(hashValue: Int.max, mask: 7) == 7)
    }

    @Test("Canonical table execution does not decode the application model")
    func canonicalTableExecutionDoesNotDecodeApplicationModel() async throws {
        let schema = try Schema(
            entities: [try DecodeProbeItem.schemaEntity]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "canonical-sql-retained-ownership",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(DecodeProbeItem.self)
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        DecodeProbeItem.resetDecodeCount()
        try context.insert(DecodeProbeItem(id: "one", value: "canonical"))
        try await context.save()
        let decodeCountAfterWrite = DecodeProbeItem.decodeCount

        let response = try await context.executeSQL(
            "SELECT id, value FROM DecodeProbeItem"
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["id"] == .string("one"))
        #expect(response.rows[0].fields["value"] == .string("canonical"))
        #expect(DecodeProbeItem.decodeCount == decodeCountAfterWrite)
    }

    @Test("Safe table window preserves LIMIT and OFFSET including LIMIT 0")
    func safeTableWindowPreservesLimitAndOffset() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(DecodeProbeItem(id: "a", value: "first"))
        try context.insert(DecodeProbeItem(id: "b", value: "second"))
        try context.insert(DecodeProbeItem(id: "c", value: "third"))
        try await context.save()

        let windowed = try await context.query(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(DecodeProbeItem.persistableType)),
                limit: 1,
                offset: 1
            )
        )
        #expect(windowed.rows.map { $0.fields["id"] } == [.string("b")])

        let empty = try await context.query(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(DecodeProbeItem.persistableType)),
                limit: 0
            )
        )
        #expect(empty.rows.isEmpty)
    }

    @Test("Stable initial table page does not fingerprint without continuation")
    func stableInitialTablePageDoesNotFingerprintWithoutContinuation() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(DecodeProbeItem(id: "one", value: "stored"))
        try await context.save()

        let response = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .literal(
                            .langLiteral(
                                value: "Hello",
                                language: "EN-US"
                            )
                        ),
                        alias: "label"
                    )
                ]),
                source: .table(TableRef(DecodeProbeItem.persistableType))
            ),
            options: ReadExecutionOptions(
                pageSize: 10,
                continuationSnapshotIsStable: true
            )
        )

        #expect(response.rows.count == 1)
        #expect(response.continuation == nil)
        guard case .rdfTerm(.literal(let literal))? =
                response.rows[0].fields["label"] else {
            Issue.record("Expected a canonical language literal")
            return
        }
        #expect(literal.languageTag?.rawValue == "en-us")
    }

    @Test("Canonical physical windows are budget bounded and skip stable LIMIT 0")
    func canonicalPhysicalWindowCapsCursorAndSkipsStableLimitZero() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        // A read never creates a directory, so the window this test observes
        // exists only after a write has created the directory it reads. The
        // row is removed again so the scan itself stays empty.
        let seedContext = container.testBaseContext()
        try seedContext.insert(DecodeProbeItem(id: "seed", value: "seed"))
        try await seedContext.save()
        try seedContext.delete(DecodeProbeItem(id: "seed", value: "seed"))
        try await seedContext.save()
        let recordedBefore = control.rangeCursorLimits.count
        let budget = ExecutionBudget(
            maximumRows: 3,
            maximumWorkUnits: 3,
            maximumIntermediateRows: 3,
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            timeoutMilliseconds: 30_000
        )

        let bounded = try await container.testBaseContext().query(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(DecodeProbeItem.persistableType)),
                limit: 1_000_000,
                offset: 1_000_000
            ),
            options: ReadExecutionOptions(budget: budget)
        )

        #expect(bounded.rows.isEmpty)
        let boundedLimits = Array(
            control.rangeCursorLimits.dropFirst(recordedBefore)
        )
        #expect(boundedLimits.count == 1)
        #expect(boundedLimits.allSatisfy { $0 <= 4 })
        let openedBeforeLimitZero = control.openedRangeCursorCount

        let response = try await container.testBaseContext().query(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(DecodeProbeItem.persistableType)),
                limit: 0
            ),
            options: ReadExecutionOptions(
                pageSize: 1,
                continuationSnapshotIsStable: true
            )
        )

        #expect(response.rows.isEmpty)
        #expect(response.continuation == nil)
        #expect(control.openedRangeCursorCount == openedBeforeLimitZero)
        #expect(
            control.rangeCursorLimits.count
                == recordedBefore + boundedLimits.count
        )

        let schema = try Schema(
            entities: [try DecodeProbeItem.schemaEntity]
        )
        let (schemaDriven, schemaControl) = try await makeControlledContainer(
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "canonical-sql-retained-ownership",
                    revision: 1
                ),
                schema: schema
            )
        )
        defer { await schemaDriven.shutdown() }
        let schemaSeedContext = schemaDriven.testBaseContext()
        try schemaSeedContext.insert(DecodeProbeItem(id: "seed", value: "seed"))
        try await schemaSeedContext.save()
        try schemaSeedContext.delete(DecodeProbeItem(id: "seed", value: "seed"))
        try await schemaSeedContext.save()
        let schemaOpenedBefore = schemaControl.openedRangeCursorCount
        let schemaResponse = try await schemaDriven.testBaseContext().query(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(DecodeProbeItem.persistableType)),
                limit: 0
            ),
            options: ReadExecutionOptions(
                pageSize: 1,
                continuationSnapshotIsStable: true
            )
        )
        #expect(schemaResponse.rows.isEmpty)
        #expect(schemaResponse.continuation == nil)
        #expect(schemaControl.openedRangeCursorCount == schemaOpenedBefore)
    }

    @Test("Stable continuation without a storage position keeps one validated proof")
    func stableContinuationWithoutStoragePositionKeepsOneValidatedProof()
        async throws {
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef(DecodeProbeItem.persistableType))
        )
        let continuationScope = ByteString([0x44, 0x46, 0x30, 0x36])
        let firstExecution = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                continuationScope: continuationScope,
                continuationSnapshotIsStable: true
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let first = try CanonicalQueryPagination.window(
            rows: [
                DatabaseEngine.QueryRow(fields: ["id": .string("first")]),
                DatabaseEngine.QueryRow(fields: ["id": .string("second")]),
            ],
            selectQuery: query,
            options: firstExecution
        )
        let continuation = try #require(first.continuation)
        let fingerprintWork = UInt64(
            try QueryIRWireFormat.encode(.select(query)).count
                + continuationScope.count
        )
        let budget = ExecutionBudget(
            maximumRows: 4,
            // The remaining headroom covers fixed query-pipeline work but is
            // smaller than a second fingerprint charge.
            maximumWorkUnits: fingerprintWork * 2 - 1,
            maximumIntermediateRows: 16,
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            timeoutMilliseconds: 30_000
        )
        let schema = try Schema(
            entities: [try DecodeProbeItem.schemaEntity]
        )
        let compiled = try await makeControlledContainer()
        let schemaDriven = try await makeControlledContainer(
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "canonical-sql-retained-ownership",
                    revision: 1
                ),
                schema: schema
            )
        )
        defer {
            await compiled.0.shutdown()
            await schemaDriven.0.shutdown()
        }

        for (container, control) in [compiled, schemaDriven] {
            let response = try await container.testBaseContext().query(
                query,
                options: ReadExecutionOptions(
                    pageSize: 1,
                    continuation: continuation,
                    budget: budget,
                    continuationScope: continuationScope,
                    continuationSnapshotIsStable: true
                )
            )
            #expect(response.rows.isEmpty)
            #expect(response.continuation == nil)

            let cursorCountBeforeMismatch = control.openedRangeCursorCount
            var changedQuery = query
            changedQuery = changedQuery.replacing(
                projection: .items([
                    ProjectionItem(
                        .literal(.string("changed")),
                        alias: "changed"
                    )
                ])
            )
            do {
                _ = try await container.testBaseContext().query(
                    changedQuery,
                    options: ReadExecutionOptions(
                        pageSize: 1,
                        continuation: continuation,
                        continuationScope: continuationScope,
                        continuationSnapshotIsStable: true
                    )
                )
                Issue.record("Expected the changed query to reject the continuation")
            } catch let error as CanonicalReadError {
                guard case .invalidContinuation = error else {
                    Issue.record("Unexpected continuation failure: \(error)")
                    continue
                }
            }
            #expect(control.openedRangeCursorCount == cursorCountBeforeMismatch)
        }
    }

    @Test("Projection rejects before destination admission and releases its ledger")
    func projectionRejectsBeforeDestinationAdmission() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 2,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .column(ColumnRef("value")),
                    alias: "projected"
                )
            ]),
            source: .values(
                [[.string("first")], [.string("second")]],
                columnNames: ["value"]
            )
        )

        do {
            _ = try await container.testBaseContext().queryRetained(
                query,
                execution: execution
            )
            Issue.record("Expected projection destination admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateRows(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected projection failure: \(error)")
                return
            }
            #expect(stage == .projection)
            #expect(consumed == 2)
            #expect(requested == 1)
            #expect(maximum == 2)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Grouping rejects before destination admission and releases its ledger")
    func groupingRejectsBeforeDestinationAdmission() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 2,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .all,
            source: .values(
                [[.string("books")]],
                columnNames: ["category"]
            ),
            groupBy: [.column(ColumnRef("category"))]
        )

        do {
            _ = try await container.testBaseContext().queryRetained(
                query,
                execution: execution
            )
            Issue.record("Expected grouping admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateRows(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected grouping failure: \(error)")
                return
            }
            #expect(stage == .aggregateInput)
            #expect(consumed == 2)
            #expect(requested == 1)
            #expect(maximum == 2)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Join rejects before destination admission and releases its ledger")
    func joinRejectsBeforeDestinationAdmission() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 2,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .all,
            source: .join(
                JoinClause(
                    type: .cross,
                    left: .values(
                        [[.string("left")]],
                        columnNames: ["left_value"]
                    ),
                    right: .values(
                        [[.string("right")]],
                        columnNames: ["right_value"]
                    )
                )
            )
        )

        do {
            _ = try await container.testBaseContext().queryRetained(
                query,
                execution: execution
            )
            Issue.record("Expected join destination admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateRows(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected join failure: \(error)")
                return
            }
            #expect(stage == .joinCandidate)
            #expect(consumed == 2)
            #expect(requested == 1)
            #expect(maximum == 2)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("JOIN ON rejects before merged-row admission and releases its ledger")
    func joinOnRejectsBeforeMergedRowAdmission() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 3,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .all,
            source: .join(
                JoinClause(
                    type: .inner,
                    left: .values(
                        [[.string("left")]],
                        columnNames: ["left_value"]
                    ),
                    right: .values(
                        [[.string("right")]],
                        columnNames: ["right_value"]
                    ),
                    condition: .on(
                        .isNotNull(.column(ColumnRef("left_value")))
                    )
                )
            )
        )

        do {
            _ = try await container.testBaseContext().queryRetained(
                query,
                execution: execution
            )
            Issue.record("Expected JOIN ON merged-row admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateRows(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected JOIN ON failure: \(error)")
                return
            }
            #expect(stage == .joinCandidate)
            #expect(consumed == 3)
            #expect(requested == 1)
            #expect(maximum == 3)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Set alignment rejects before destination admission and releases its ledger")
    func setAlignmentRejectsBeforeDestinationAdmission() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 3,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .all,
            source: .unionAll([
                .values(
                    [[.string("first")]],
                    columnNames: ["first_name"]
                ),
                .values(
                    [[.string("second")]],
                    columnNames: ["second_name"]
                ),
            ])
        )

        do {
            _ = try await container.testBaseContext().queryRetained(
                query,
                execution: execution
            )
            Issue.record("Expected set-alignment destination admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateRows(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected set-alignment failure: \(error)")
                return
            }
            #expect(stage == .bindingCandidate)
            #expect(consumed == 3)
            #expect(requested == 1)
            #expect(maximum == 3)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("LATERAL retains its outer overlay across a nested await")
    func lateralOverlayRetainsNestedAwait() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let nested = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("outer_value")))
            ]),
            source: .values([[]], columnNames: [])
        )
        let outer = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("outer_value")))
            ]),
            source: .values(
                [[.string("outer")]],
                columnNames: ["outer_value"]
            )
        )
        let lateralRight = SelectQuery(
            projection: .items([
                ProjectionItem(.subquery(nested), alias: "nested_value")
            ]),
            source: .values([[]], columnNames: [])
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .column(
                        ColumnRef(table: "outer", column: "outer_value")
                    ),
                    alias: "outer_value"
                ),
                ProjectionItem(
                    .column(
                        ColumnRef(table: "nested", column: "nested_value")
                    ),
                    alias: "nested_value"
                ),
            ]),
            source: .join(
                JoinClause(
                    type: .lateral,
                    left: .subquery(outer, alias: "outer"),
                    right: .subquery(lateralRight, alias: "nested"),
                    condition: .on(.bool(true))
                )
            )
        )

        let retained = try await container.testBaseContext().queryRetained(
            query,
            execution: execution
        )
        #expect(retained.visibleRows.count == 1)
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)

        let response = retained.promoteToPublicResponse()
        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["outer_value"] == .string("outer"))
        #expect(response.rows[0].fields["nested_value"] == .string("outer"))
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("LOWER and UPPER retain derived strings through public promotion")
    func derivedStringFunctionsRetainValuesUntilPromotion() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .function(
                        FunctionCall(
                            name: "LOWER",
                            arguments: [.column(ColumnRef("value"))]
                        )
                    ),
                    alias: "lower_value"
                ),
                ProjectionItem(
                    .function(
                        FunctionCall(
                            name: "UPPER",
                            arguments: [.column(ColumnRef("value"))]
                        )
                    ),
                    alias: "upper_value"
                ),
            ]),
            source: .values(
                [[.string("MiXeD")]],
                columnNames: ["value"]
            )
        )

        let retained = try await container.testBaseContext().queryRetained(
            query,
            execution: execution
        )
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)
        let response = retained.promoteToPublicResponse()
        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["lower_value"] == .string("mixed"))
        #expect(response.rows[0].fields["upper_value"] == .string("MIXED"))
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Allocating expressions require an owned result payload")
    func allocatingExpressionsRequireOwnedPayload() throws {
        let value = Expression.column(ColumnRef("value"))
        let expressions: [Expression] = [
            .function(FunctionCall(name: "LOWER", arguments: [value])),
            .function(FunctionCall(name: "UPPER", arguments: [value])),
            .literal(.langLiteral(value: "Hello", language: "EN-US")),
            .literal(
                .dirLangLiteral(
                    value: "Directed",
                    language: "EN-US",
                    direction: "rtl"
                )
            ),
            .aggregate(
                .groupConcat(value, separator: "|", distinct: false)
            ),
            .aggregate(.arrayAgg(value, orderBy: nil, distinct: false)),
        ]

        for expression in expressions {
            #expect(canonicalExpressionResultRequiresOwnedPayload(expression))
        }
    }

    @Test("Derived value producer is rejected before exact payload allocation")
    func derivedValueProducerRequiresExactAdmission() throws {
        let expectedBytes: UInt64 = 257
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: expectedBytes - 1
        )
        var producerWasInvoked = false

        do {
            _ = try DatabaseQueryScopedFieldValue.producing(
                maximumFootprint: DatabaseIntermediateFootprint(
                    bytes: expectedBytes
                ),
                workMeter: meter,
                stage: .aggregateInput
            ) {
                producerWasInvoked = true
                return .string("must-not-be-built")
            }
            Issue.record("Expected exact derived payload admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected derived payload failure: \(error)")
                return
            }
            #expect(stage == .aggregateInput)
            #expect(consumed == 0)
            #expect(requested == expectedBytes)
            #expect(maximum == expectedBytes - 1)
        }

        #expect(!producerWasInvoked)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Query-scoped value keeps its exact claim across suspension")
    func queryScopedValueRetainsClaimAcrossSuspension() async throws {
        let producedValue = FieldValue.string("candidate")
        let measurementMeter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024
        )
        let actualFootprint = try CanonicalRelationalFootprintMeter
            .valueFootprint(
                of: producedValue,
                workMeter: measurementMeter,
                stage: .expressionEvaluation
            )
        let maximumFootprint = try actualFootprint.adding(
            DatabaseIntermediateFootprint(bytes: 257)
        )
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: maximumFootprint.bytes + 1_024
        )
        var value: DatabaseQueryScopedFieldValue? = try .producing(
            maximumFootprint: maximumFootprint,
            workMeter: meter,
            stage: .expressionEvaluation
        ) {
            producedValue
        }

        #expect(meter.retainedIntermediateRows == actualFootprint.rows)
        #expect(meter.retainedIntermediateBytes == actualFootprint.bytes)
        await Task.yield()
        var observed: FieldValue?
        try #require(value).withValue { observed = $0 }
        #expect(observed == producedValue)
        #expect(meter.retainedIntermediateRows == actualFootprint.rows)
        #expect(meter.retainedIntermediateBytes == actualFootprint.bytes)

        value = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Derived value rejects payload beyond its admitted maximum")
    func derivedValueRejectsActualFootprintBeyondMaximum() throws {
        let producedValue = FieldValue.string("underdeclared")
        let measurementMeter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024
        )
        let actualFootprint = try CanonicalRelationalFootprintMeter
            .valueFootprint(
                of: producedValue,
                workMeter: measurementMeter,
                stage: .expressionEvaluation
            )
        let maximumFootprint = DatabaseIntermediateFootprint(
            rows: actualFootprint.rows,
            bytes: try #require(
                actualFootprint.bytes > 0 ? actualFootprint.bytes - 1 : nil
            )
        )
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: actualFootprint.bytes * 2
        )
        var producerWasInvoked = false

        #expect {
            try DatabaseQueryScopedFieldValue.producing(
                maximumFootprint: maximumFootprint,
                workMeter: meter,
                stage: .expressionEvaluation
            ) {
                producerWasInvoked = true
                return producedValue
            }
        } throws: { error in
            error as? DatabaseQueryScopedFieldValueError
                == .payloadFootprintExceeded(
                    maximumRows: maximumFootprint.rows,
                    maximumBytes: maximumFootprint.bytes,
                    actualRows: actualFootprint.rows,
                    actualBytes: actualFootprint.bytes
                )
        }

        #expect(producerWasInvoked)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Derived value producer failure releases its maximum claim")
    func derivedValueProducerFailureReleasesClaim() throws {
        let maximumFootprint = DatabaseIntermediateFootprint(bytes: 257)
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: maximumFootprint.bytes
        )

        #expect {
            try DatabaseQueryScopedFieldValue.producing(
                maximumFootprint: maximumFootprint,
                workMeter: meter,
                stage: .expressionEvaluation
            ) {
                throw CanonicalReadError.unsupportedExpression
            }
        } throws: { error in
            error is CanonicalReadError
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Uppercase language tags canonicalize and retain through promotion")
    func uppercaseLanguageLiteralRetainsCanonicalLanguage() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .literal(
                        .langLiteral(
                            value: "Hello",
                            language: "EN-US"
                        )
                    ),
                    alias: "label"
                ),
                ProjectionItem(
                    .literal(
                        .dirLangLiteral(
                            value: "Directed",
                            language: "EN-US",
                            direction: "rtl"
                        )
                    ),
                    alias: "directed_label"
                ),
            ]),
            source: .values(
                [[.int(1)]],
                columnNames: ["source"]
            )
        )

        let retained = try await container.testBaseContext().queryRetained(
            query,
            execution: execution
        )
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)
        let response = retained.promoteToPublicResponse()
        #expect(response.rows.count == 1)
        guard let value = response.rows[0].fields["label"] else {
            Issue.record("Expected a canonical language literal")
            return
        }
        guard case .rdfTerm(.literal(let literal)) = value else {
            Issue.record("Expected an RDF language literal, got \(value)")
            return
        }
        #expect(literal.lexicalForm == "Hello")
        #expect(literal.languageTag?.rawValue == "en-us")
        guard let directedValue = response.rows[0].fields["directed_label"],
              case .rdfTerm(.literal(let directedLiteral)) = directedValue else {
            Issue.record("Expected a directed RDF language literal")
            return
        }
        #expect(directedLiteral.lexicalForm == "Directed")
        #expect(directedLiteral.languageTag?.rawValue == "en-us")
        #expect(directedLiteral.baseDirection == .rightToLeft)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("GROUP_CONCAT and ARRAY_AGG retain exact results until promotion")
    func collectionAggregatesRetainResultsUntilPromotion() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let value = Expression.column(ColumnRef("value"))
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .aggregate(
                        .arrayAgg(
                            value,
                            orderBy: nil,
                            distinct: false
                        )
                    ),
                    alias: "values"
                ),
                ProjectionItem(
                    .aggregate(
                        .groupConcat(
                            value,
                            separator: "|",
                            distinct: false
                        )
                    ),
                    alias: "joined"
                ),
            ]),
            source: .values(
                [[.string("first")], [.string("second")]],
                columnNames: ["value"]
            )
        )

        let retained = try await container.testBaseContext().queryRetained(
            query,
            execution: execution
        )
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)
        let response = retained.promoteToPublicResponse()
        #expect(response.rows.count == 1)
        #expect(
            response.rows[0].fields["values"]
                == .array([.string("first"), .string("second")])
        )
        #expect(response.rows[0].fields["joined"] == .string("first|second"))
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("IN subquery keeps its candidate across a nested await")
    func membershipSubqueryRetainsCandidateAcrossAwait() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 128,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let membership = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("candidate")))
            ]),
            source: .values(
                [[.int(1)], [.int(2)]],
                columnNames: ["candidate"]
            )
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("value")), alias: "value")
            ]),
            source: .values(
                [[.int(2)], [.int(3)]],
                columnNames: ["value"]
            ),
            filter: .inSubquery(
                .column(ColumnRef("value")),
                subquery: membership
            )
        )

        let retained = try await container.testBaseContext().queryRetained(
            query,
            execution: execution
        )
        #expect(retained.visibleRows.count == 1)
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)
        let response = retained.promoteToPublicResponse()
        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["value"] == .int64(2))
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("VALUES rejects one byte short before field allocation")
    func valuesRejectsOneByteShortBeforeFieldAllocation() async throws {
        let fieldName = "value"
        let stringValue = String(repeating: "x", count: 1_024)
        let fieldValue = FieldValue.string(stringValue)
        let measurementMeter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: UInt64.max
        )
        var valuesFootprint = try CanonicalRelationalFootprintMeter
            .sourceRowFootprint(
                fields: [:],
                sourceName: nil,
                annotations: [:],
                version: nil,
                workMeter: measurementMeter,
                stage: .bindingCandidate
            )
        valuesFootprint = try valuesFootprint.adding(
            CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                nameUTF8Count: fieldName.utf8.count,
                value: fieldValue,
                workMeter: measurementMeter,
                stage: .bindingCandidate
            )
        )
        let layout = try DatabaseRetainedArrayLayout.forElement(
            CanonicalSourceRow.self
        )
        let initialGrowth = try layout.growth(from: 0, toFit: 1)
        let initialBuilderBytes = layout.containerByteCount
            + initialGrowth.additionalByteCount
        let retainedBeforeAppendBytes = DatabaseReadSession
            .scopeCursorRegistryContainerByteCount
            + initialBuilderBytes
        let appendRequestBytes = valuesFootprint.bytes
            + layout.appendAdmissionByteCount
        let maximumBytes = retainedBeforeAppendBytes + appendRequestBytes - 1

        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: maximumBytes
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let query = SelectQuery(
            projection: .all,
            source: .values(
                [[Literal.string(stringValue)]],
                columnNames: [fieldName]
            )
        )

        do {
            _ = try await container.testBaseContext().queryRetained(
                query,
                execution: execution
            )
            Issue.record("Expected one-byte-short VALUES admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected VALUES failure: \(error)")
                return
            }
            #expect(stage == .bindingCandidate)
            #expect(consumed == retainedBeforeAppendBytes)
            #expect(requested == appendRequestBytes)
            #expect(maximum == maximumBytes)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Multiple scalar subqueries retain values until public promotion")
    func scalarSubqueriesRetainValuesUntilPromotion() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let first = scalarSubquery(value: "first")
        let second = scalarSubquery(value: "second")
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.subquery(first), alias: "first_value"),
                ProjectionItem(.subquery(second), alias: "second_value"),
            ]),
            source: .values(
                [[.string("outer")]],
                columnNames: ["outer_value"]
            )
        )

        let retained = try await container.testBaseContext().queryRetained(
            query,
            execution: execution
        )
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)

        let response = retained.promoteToPublicResponse()
        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["first_value"] == .string("first"))
        #expect(response.rows[0].fields["second_value"] == .string("second"))
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("A failed scalar subquery releases earlier scalar owners")
    func failedScalarSubqueryReleasesEarlierOwners() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let execution = makeExecution(
            in: container,
            meter: meter
        )
        let first = scalarSubquery(value: "first")
        let invalid = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("value")))
            ]),
            source: .values(
                [[.string("one")], [.string("two")]],
                columnNames: ["value"]
            )
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.subquery(first), alias: "first_value"),
                ProjectionItem(.subquery(invalid), alias: "invalid_value"),
            ]),
            source: .values(
                [[.string("outer")]],
                columnNames: ["outer_value"]
            )
        )

        do {
            _ = try await container.testBaseContext().queryRetained(
                query,
                execution: execution
            )
            Issue.record("Expected a multi-row scalar subquery to fail")
        } catch let error as CanonicalReadError {
            guard case .invalidScalarSubquery(
                rowCount: 2,
                columnCount: nil
            ) = error else {
                Issue.record("Unexpected scalar subquery failure: \(error)")
                return
            }
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Index metadata remains charged until public promotion")
    func indexMetadataRemainsChargedUntilPromotion() throws {
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: 1_024 * 1_024
        )
        let expectedMetadata: [String: FieldValue] = [
            "total": .int64(2),
            "facet": .string("books"),
        ]
        let metadataFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: DatabaseEngine.QueryRow(
                fields: [:],
                annotations: expectedMetadata
            ),
            workMeter: meter,
            stage: .indexScan
        )
        var metadataBuildInvoked = false
        let metadataOwner = try DatabaseRetainedIndexMetadata.build(
            workMeter: meter,
            footprint: metadataFootprint,
            at: .indexScan
        ) {
            metadataBuildInvoked = true
            return [
                "total": .int64(2),
                "facet": .string("books"),
            ]
        }
        #expect(metadataBuildInvoked)
        var indexResult: IndexReadResult? = try IndexReadResult.build(
            workMeter: meter,
            metadata: consume metadataOwner
        ) { _ in }
        guard indexResult?.retainedMetadataReservation != nil else {
            Issue.record("Expected an index result with retained metadata")
            return
        }
        let retained = CanonicalRetainedQueryResponse(
            rows: try makeEmptyCanonicalRows(workMeter: meter),
            visibleRange: 0..<0,
            continuation: nil,
            metadata: expectedMetadata,
            affectedRows: nil,
            metadataReservation: indexResult?.retainedMetadataReservation
        )
        indexResult = nil

        #expect(meter.retainedIntermediateBytes > 0)
        let ready = try finalizePostClosureResult(
            consume retained,
            ownsProducingTransaction: false
        )
        let response = ready.promoteToPublicResponse()
        #expect(response.metadata == expectedMetadata)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Index metadata rejects one byte short without retaining ledger state")
    func indexMetadataRejectsOneByteShort() throws {
        let metadata: [String: FieldValue] = [
            "total": .int64(2),
            "facet": .string("books"),
        ]
        let measurementMeter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: UInt64.max
        )
        let metadataFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: DatabaseEngine.QueryRow(fields: [:], annotations: metadata),
            workMeter: measurementMeter,
            stage: .indexScan
        )
        let maximumBytes = metadataFootprint.bytes - 1
        let meter = makeMeter(
            maximumIntermediateRows: 64,
            maximumIntermediateBytes: maximumBytes
        )

        var metadataBuildInvoked = false
        do {
            _ = try DatabaseRetainedIndexMetadata.build(
                workMeter: meter,
                footprint: metadataFootprint,
                at: .indexScan
            ) {
                metadataBuildInvoked = true
                return metadata
            }
            Issue.record("Expected one-byte-short metadata admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(
                let stage,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected metadata failure: \(error)")
                return
            }
            #expect(stage == .indexScan)
            #expect(consumed == 0)
            #expect(requested == metadataFootprint.bytes)
            #expect(maximum == maximumBytes)
        }

        #expect(!metadataBuildInvoked)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Complete staging outlives its read session and pages in bounds")
    func completeStagingOutlivesReadSession() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        for index in 0..<3 {
            try context.insert(
                DecodeProbeItem(id: "id-\(index)", value: "value-\(index)")
            )
        }
        try await context.save()

        let options = ReadExecutionOptions()
        let workMeter = DatabaseWorkMeter(
            budget: options.budget,
            monotonicClock: container.monotonicClock
        )
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock,
            workMeter: workMeter
        )
        let prepared = DatabasePreparedSQLSelect(
            query: SelectQuery(
                projection: .all,
                source: .table(TableRef("DecodeProbeItem"))
            ),
            workMeter: workMeter
        )

        do {
            let staged = try await context.indexQueryContext.withSession(
                workMeter: workMeter
            ) { session in
                try await prepared.stageCompleteRows(
                    in: session,
                    execution: execution
                )
            }

            #expect(staged.count == 3)
            #expect(workMeter.retainedIntermediateRows >= 3)

            let leadingPage = staged.materializePage(0..<2)
            let trailingPage = staged.materializePage(2..<3)
            #expect(leadingPage.count == 2)
            #expect(trailingPage.count == 1)

            var identifiers: [String] = []
            for row in leadingPage + trailingPage {
                guard case .string(let identifier) = row.fields["id"] else {
                    Issue.record("Staged row is missing its identifier")
                    continue
                }
                identifiers.append(identifier)
            }
            #expect(identifiers.sorted() == ["id-0", "id-1", "id-2"])
            #expect(workMeter.retainedIntermediateRows >= 3)
        }

        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Complete staging rejects a foreign request meter")
    func completeStagingRejectsForeignRequestMeter() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let options = ReadExecutionOptions()
        let preparationMeter = DatabaseWorkMeter(
            budget: options.budget,
            monotonicClock: container.monotonicClock
        )
        let sessionMeter = DatabaseWorkMeter(
            budget: options.budget,
            monotonicClock: container.monotonicClock
        )
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock,
            workMeter: sessionMeter
        )
        let prepared = DatabasePreparedSQLSelect(
            query: SelectQuery(
                projection: .all,
                source: .table(TableRef("DecodeProbeItem"))
            ),
            workMeter: preparationMeter
        )
        let context = container.testBaseContext()

        await #expect(
            throws: DatabasePreparedSQLSelectError.workMeterMismatch
        ) {
            try await context.indexQueryContext.withSession(
                workMeter: sessionMeter
            ) { session in
                _ = try await prepared.stageCompleteRows(
                    in: session,
                    execution: execution
                )
            }
        }

        #expect(preparationMeter.retainedIntermediateRows == 0)
        #expect(preparationMeter.retainedIntermediateBytes == 0)
        #expect(sessionMeter.retainedIntermediateRows == 0)
        #expect(sessionMeter.retainedIntermediateBytes == 0)
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [try DecodeProbeItem.schemaEntity]
        )
        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "canonical-sql-retained-ownership",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(DecodeProbeItem.self)
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeControlledContainer(
        runtimeConfiguration: DatabaseRuntimeConfiguration? = nil
    ) async throws -> (
        DBContainer,
        StorageTransactionControl
    ) {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let schema = try Schema(
            entities: [try DecodeProbeItem.schemaEntity]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try runtimeConfiguration
                ?? DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "canonical-sql-retained-ownership",
                        revision: 1
                    ),
                    entityRuntimes: [
                        try DatabaseFrameworkRuntime.entity(
                            DecodeProbeItem.self
                        )
                    ]
                ),
            security: .testingDisabled
        )
        return (container, storage.control)
    }

    private func makeMeter(
        maximumIntermediateRows: UInt32,
        maximumIntermediateBytes: UInt64
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: maximumIntermediateBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func makeExecution(
        in container: DBContainer,
        meter: DatabaseWorkMeter
    ) -> ReadExecutionContext {
        ReadExecutionContext(
            options: ReadExecutionOptions(budget: meter.budget),
            monotonicClock: container.monotonicClock,
            workMeter: meter
        )
    }

    private func scalarSubquery(value: String) -> SelectQuery {
        SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("value")))
            ]),
            source: .values(
                [[.string(value)]],
                columnNames: ["value"]
            )
        )
    }

    private func makeEmptyCanonicalRows(
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseSharedRetainedArray<DatabaseEngine.QueryRow> {
        let builder = try DatabaseRetainedArrayBuilder<DatabaseEngine.QueryRow>(
            workMeter: workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseEngine.QueryRow.self
            )
        )
        return try builder.finish().moveToSharedOwnership(at: .projection)
    }
}

private struct DecodeProbeItem: Persistable {
    typealias ID = String

    let id: String
    let value: String

    private static let decodeCountState = Mutex(0)

    static var decodeCount: Int {
        decodeCountState.withLock { $0 }
    }

    static func resetDecodeCount() {
        decodeCountState.withLock { $0 = 0 }
    }

    static var persistableType: String { "DecodeProbeItem" }
    static var allFields: [String] { ["id", "value"] }
    static var fieldSchemas: [FieldSchema] {
        [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "value", fieldNumber: 2, type: .string),
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? {
        switch fieldName {
        case "id": 1
        case "value": 2
        default: nil
        }
    }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    func encodePersistedFields<Output: PersistedFieldOutput>(
        to output: inout Output
    ) throws(PersistableEncodingFailure<Output.Failure>) {
        try output.write(
            FieldIdentity(name: "id", number: 1),
            value: id,
            entity: Self.persistableType
        )
        try output.write(
            FieldIdentity(name: "value", number: 2),
            value: value,
            entity: Self.persistableType
        )
    }

    static func decodePersistedFields<Input: PersistedFieldInput>(
        from input: inout Input
    ) throws(PersistableDecodingFailure<Input.Failure>) -> Self {
        decodeCountState.withLock { $0 += 1 }
        let id = try input.decode(
            String.self,
            for: FieldIdentity(name: "id", number: 1),
            entity: persistableType
        )
        let value = try input.decode(
            String.self,
            for: FieldIdentity(name: "value", number: 2),
            entity: persistableType
        )
        try input.finish(entity: persistableType)
        return Self(id: id, value: value)
    }

    static func decodePersistedFields(
        _ fields: consuming [PersistableField]
    ) throws(PersistableDecodingError) -> Self {
        var input = try PersistedFieldCollectionInput(
            entity: persistableType,
            fields: fields,
            schemas: fieldSchemas
        )
        return try input.decode { (
            input: inout PersistedFieldCollectionInput
        ) throws(PersistableDecodingFailure<Never>) -> Self in
            try Self.decodePersistedFields(from: &input)
        }
    }

    static func decodePersistedObject(
        _ object: FieldObject
    ) throws(PersistableDecodingError) -> Self {
        var input = try PersistedObjectInput(
            entity: persistableType,
            object: object,
            schemas: fieldSchemas
        )
        return try input.decode { (
            input: inout PersistedObjectInput
        ) throws(PersistableDecodingFailure<Never>) -> Self in
            try Self.decodePersistedFields(from: &input)
        }
    }

    func persistedFieldValue(
        for field: FieldIdentity
    ) throws(PersistableEncodingError) -> FieldValue? {
        switch (field.name, field.number) {
        case ("id", 1): .string(id)
        case ("value", 2): .string(value)
        default: nil
        }
    }
}
