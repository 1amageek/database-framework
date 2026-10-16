import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Compiled statement assignment conversion")
struct DatabaseStatementAssignmentTests {
    @Test("INSERT consumes compiled character lengths and assignment space rules")
    func insertCharacterAssignments() async throws {
        for target: DataType in [.char(length: 3), .varchar(length: 3)] {
            let container = try await makeContainer(type: target)
            defer { await container.shutdown() }
            _ = try await execute(insert("short", .string("é")), container: container)
            #expect(try await title("short", container: container) == .string(target == .char(length: 3) ? "é  " : "é"))
            _ = try await execute(insert("spaces", .string("é😀   ")), container: container)
            #expect(try await title("spaces", container: container) == .string("é😀 "))
            _ = try await execute(insert("combining", .string("e\u{0301}😀")), container: container)
            #expect(try await title("combining", container: container) == .string("e\u{0301}😀"))
            await #expect(throws: CanonicalReadError.self) {
                try await execute(insert("invalid", .string("é😀xy")), container: container)
            }
            #expect(try await title("invalid", container: container) == nil)
        }
    }

    @Test("UPDATE shares nested explicit CAST, F04 and lazy CASE evaluation")
    func updateSelectedExpressions() async throws {
        let container = try await makeContainer(type: .varchar(length: 3))
        defer { await container.shutdown() }
        _ = try await execute(insert("one", .string("old")), container: container)
        let explicit = Expression.cast(.string("ABCDE"), targetType: .varchar(length: 2))
        let lower = Expression.function(FunctionCall(name: "lower", arguments: [explicit]))
        _ = try await execute(update(lower), container: container)
        #expect(try await title("one", container: container) == .string("ab"))
        let lazy = Expression.caseWhen(operand: nil, cases: [
            CaseWhenPair(condition: .literal(.bool(true)), result: .string("ok"))
        ], elseResult: .cast(.divide(.int(1), .int(0)), targetType: .text))
        _ = try await execute(update(lazy), container: container)
        #expect(try await title("one", container: container) == .string("ok"))
        await #expect(throws: CanonicalReadError.self) {
            try await execute(update(.string("ABCDE")), container: container)
        }
        #expect(try await title("one", container: container) == .string("ok"))
        _ = try await execute(insert("unicode", .string("e\u{0301}😀")), container: container)
        let codepoints = Expression.equal(.function(FunctionCall(name: "length", arguments: [.col("title")])), .int(3))
        _ = try await execute(.update(UpdateQuery(target: TableRef("Assignment"),
            assignments: [Assignment(column: "title", value: .string("hit"))], filter: codepoints)), container: container)
        #expect(try await title("unicode", container: container) == .string("hit"))
        _ = try await execute(insert("case", .string("ÉA")), container: container)
        let posix = Expression.equal(.function(FunctionCall(name: "lower", arguments: [.col("title")])), .string("Éa"))
        _ = try await execute(.insert(InsertQuery(target: TableRef("Assignment"), columns: ["id", "title"],
            source: .values([[.string("case"), .string("new")]]),
            onConflict: .doUpdate(assignments: [Assignment(column: "title", value: .string("yes"))], where: posix))), container: container)
        #expect(try await title("case", container: container) == .string("yes"))
        _ = try await execute(insert("delete", .string("e\u{0301}😀")), container: container)
        _ = try await execute(.delete(DeleteQuery(target: TableRef("Assignment"),
            filter: .and(.equal(.col("id"), .string("delete")), codepoints))), container: container)
        #expect(try await title("delete", container: container) == nil)
        #expect(try await title("one", container: container) == .string("ok"))

        let temporal = try await makeContainer(fieldType: .object, queryType: .sql(.interval()))
        defer { await temporal.shutdown() }
        let initial = Expression.cast(.string("1 month"), targetType: .interval())
        _ = try await execute(insert("interval", initial), container: temporal)
        let equalMonth = Expression.equal(.col("title"), .cast(.string("30 days"), targetType: .interval()))
        _ = try await execute(.update(UpdateQuery(target: TableRef("Assignment"),
            assignments: [Assignment(column: "title", value: .add(.col("title"), .cast(.string("2 seconds"), targetType: .interval())))],
            filter: equalMonth)), container: temporal)
        let payload = try #require(try await title("interval", container: temporal))
        let limits = try QueryFieldStorageAdapter.inferredLimits(for: payload, type: .sql(.interval()))
        let decoded = try QueryFieldStorageAdapter.decode(payload, type: .sql(.interval()), limits: limits)
        let expected = try QueryIntervalValue(period: CalendarPeriod(months: 1), time: TimeSpan(seconds: 2))
        #expect(decoded == .interval(expected))
        let failedMeter = meter()
        await #expect(throws: CanonicalReadError.self) {
            try await execute(update(.cast(.string("invalid interval"), targetType: .interval())), container: temporal, workMeter: failedMeter)
        }
        #expect(failedMeter.retainedIntermediateBytes == 0)
        #expect(try await title("interval", container: temporal) == payload)
        _ = try await execute(update(.cast(.string("infinity"), targetType: .interval())), container: temporal)
        let infinitePayload = try #require(try await title("interval", container: temporal))
        let infiniteLimits = try QueryFieldStorageAdapter.inferredLimits(for: infinitePayload, type: .sql(.interval()))
        #expect(try QueryFieldStorageAdapter.decode(infinitePayload, type: .sql(.interval()), limits: infiniteLimits) == .intervalInfinity(.positive))
    }

    @Test("A late assignment failure leaves all rows unchanged and permits the next mutation")
    func failureIsAtomic() async throws {
        let container = try await makeContainer(type: .varchar(length: 3))
        defer { await container.shutdown() }
        _ = try await execute(insert("a", .string("old")), container: container)
        _ = try await execute(insert("b", .string("old")), container: container)
        let value = Expression.caseWhen(operand: nil, cases: [
            CaseWhenPair(condition: .equal(.col("id"), .string("a")), result: .string("new"))
        ], elseResult: .string("too long"))
        let workMeter = meter()
        await #expect(throws: CanonicalReadError.self) {
            try await execute(update(value), container: container, workMeter: workMeter)
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
        #expect(try await title("a", container: container) == .string("old"))
        #expect(try await title("b", container: container) == .string("old"))
        _ = try await execute(update(.string("yes")), container: container)
        #expect(try await title("a", container: container) == .string("yes"))
        #expect(try await title("b", container: container) == .string("yes"))
        let constrained = meter(bytes: 1)
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await execute(update(.string("no")), container: container, workMeter: constrained)
        }
        #expect(constrained.retainedIntermediateBytes == 0)
        #expect(try await title("a", container: container) == .string("yes"))
        let cancelledMeter = meter()
        let cancelled = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await execute(update(.string("no")), container: container, workMeter: cancelledMeter)
        }
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        #expect(cancelledMeter.retainedIntermediateBytes == 0)
        #expect(try await title("a", container: container) == .string("yes"))
        _ = try await execute(update(.string("end")), container: container)
        #expect(try await title("b", container: container) == .string("end"))
    }

    @Test("Compiled assignment signatures bind even when the source is empty")
    func bindsBeforeEmptyScanAndPreservesNumericEnvelope() async throws {
        let container = try await makeContainer(type: .decimal(precision: nil, scale: nil))
        defer { await container.shutdown() }
        await #expect(throws: DatabaseExpressionEvaluationError.self) {
            try await execute(update(.literal(.bool(true))), container: container)
        }
        let lexical = Expression.literal(.queryValue(.primitive(.string("1.2300")), type: .sql(.text)))
        _ = try await execute(insert("numeric", .cast(lexical, targetType: .decimal(precision: nil, scale: nil))), container: container)
        let encoded = try #require(try await title("numeric", container: container))
        let decoded = try QueryFieldStorageAdapter.decode(encoded,
            type: .sql(.decimal(precision: nil, scale: nil)),
            limits: QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 1_000,
                maximumNestingDepth: 16, maximumCollectionElements: 1_000))
        let expected = try QueryNumericValue(value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 4)
        #expect(decoded == .numeric(expected))
        let unsignedCases: [(FieldSchemaType, FieldValue, UInt64)] = [
            (.uint8, .uint8(.max), UInt64(UInt8.max)),
            (.uint16, .uint16(.max), UInt64(UInt16.max)),
            (.uint32, .uint32(.max), UInt64(UInt32.max)),
            (.uint64, .uint64(.max), UInt64.max)
        ]
        for (kind, maximumValue, maximum) in unsignedCases {
            let unsigned = try await makeContainer(fieldType: kind, queryType: nil)
            defer { await unsigned.shutdown() }
            let exact = Expression.literal(.queryValue(.primitive(maximumValue), type: .primitive(kind, isArray: false)))
            _ = try await execute(insert("maximum", exact), container: unsigned)
            #expect(try await title("maximum", container: unsigned) == maximumValue)
            _ = try await execute(update(.col("title")), container: unsigned)
            #expect(try await title("maximum", container: unsigned) == maximumValue)
            let onePast = try QueryNumericValue(value: ExactDecimal(coefficient: Int128(maximum) + 1, scale: 0), displayScale: 0)
            for invalid in [Expression.int(-1), .literal(.queryValue(.numeric(onePast), type: .sql(.decimal(precision: nil, scale: nil))))] {
                let rejectedMeter = meter()
                do {
                    _ = try await execute(update(invalid), container: unsigned, workMeter: rejectedMeter)
                    Issue.record("Accepted an unsigned assignment outside its declared range")
                } catch let error as CanonicalReadError {
                    guard case .expressionEvaluation(let cause, _) = error else { throw error }
                    #expect(cause == .numericOverflow)
                }
                #expect(rejectedMeter.retainedIntermediateBytes == 0)
                #expect(try await title("maximum", container: unsigned) == maximumValue)
            }
            _ = try await execute(update(exact), container: unsigned)
            #expect(try await title("maximum", container: unsigned) == maximumValue)
        }
    }

    @Test("Generated payload claims survive until the destination admits the change")
    func payloadHandoffRetainsClaims() throws {
        let workMeter = meter()
        var prepared: DatabasePreparedStatementMutationChange? = try preparedPayload(workMeter)
        #expect(workMeter.retainedIntermediateBytes > 0)
        let change = try #require(prepared?.change)
        let footprint = try DatabaseEntityMutationFootprintMeter.footprint(of: change, workMeter: workMeter)
        let destination = try withExtendedLifetime(prepared) {
            try workMeter.reserveIntermediate(rows: footprint.rows, bytes: footprint.bytes, at: .mutationPlanning)
        }
        prepared = nil
        #expect(workMeter.retainedIntermediateBytes == footprint.bytes)
        destination.release()
        #expect(workMeter.retainedIntermediateBytes == 0)
        let exactBytes = workMeter.peakIntermediateBytes
        for maximum in [exactBytes, exactBytes - 1] {
            let bounded = meter(bytes: maximum)
            do {
                let admitted = try preparedPayload(bounded)
                let required = try DatabaseEntityMutationFootprintMeter.footprint(of: admitted.change, workMeter: bounded)
                let claim = try withExtendedLifetime(admitted) {
                    try bounded.reserveIntermediate(rows: required.rows, bytes: required.bytes, at: .mutationPlanning)
                }
                claim.release()
                #expect(maximum == exactBytes)
            } catch let error as DatabaseWorkLimitError {
                guard case .maximumIntermediateBytes = error else { throw error }
                #expect(maximum == exactBytes - 1)
            }
            #expect(bounded.retainedIntermediateBytes == 0)
        }
        let other = meter()
        let scoped = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.string("a")), from: .sql(.text),
            to: .sql(.char(length: 3)), context: .assignment, workMeter: other)
        let reservation = try #require(scoped.retainedReservation)
        var builder = try DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>(workMeter: other,
            stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self))
        try builder.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: reservation) })
        let payloads = try builder.finish().moveToSharedOwnership(at: .mutationPlanning)
        #expect(throws: DatabaseIntermediateReservationError.workMeterMismatch) {
            try DatabasePreparedStatementMutationChange(change: change, payloads: payloads, workMeter: workMeter)
        }
    }

    private func preparedPayload(_ workMeter: DatabaseWorkMeter) throws -> DatabasePreparedStatementMutationChange {
        let scoped = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.string("a")), from: .sql(.text),
            to: .sql(.char(length: 3)), context: .assignment, workMeter: workMeter)
        let reservation = try #require(scoped.retainedReservation)
        var builder = try DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>(workMeter: workMeter,
            stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self))
        try builder.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: reservation) })
        let change = try scoped.withValue { value in
            EntityMutationChange(kind: .insert, identity: try EntityReference(entity: "Assignment", id: .string("one")),
                fields: try FieldObject([("title", copy value)]))
        }
        return try DatabasePreparedStatementMutationChange(change: change,
            payloads: builder.finish().moveToSharedOwnership(at: .mutationPlanning), workMeter: workMeter)
    }

    private func meter(bytes: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: 1_000_000,
            maximumIntermediateRows: 100, maximumIntermediateBytes: bytes, timeoutMilliseconds: 30_000),
            monotonicClock: SystemStorageClock())
    }

    private func insert(_ id: String, _ value: Expression) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("Assignment"), columns: ["id", "title"],
            source: .values([[.string(id), value]])))
    }

    private func update(_ value: Expression) -> QueryStatement {
        .update(UpdateQuery(target: TableRef("Assignment"), assignments: [Assignment(column: "title", value: value)]))
    }

    private func execute(_ statement: QueryStatement, container: DBContainer,
                         workMeter: DatabaseWorkMeter? = nil) async throws -> [EntityMutationEffect] {
        let executor = DatabaseEntityStatementMutationExecutor(container: container,
            limits: try DatabaseEntityMutationLimits(maximumChanges: 16, maximumPreconditions: 16))
        let activeMeter = workMeter ?? meter()
        return try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
            try await executor.execute(statement, transaction: transaction, workMeter: activeMeter)
        }
    }

    private func title(_ id: String, container: DBContainer) async throws -> FieldValue? {
        try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
            guard let model = try await transaction.loadPersistedModel(entity: "Assignment", id: Tuple(id), partition: nil) else { return nil }
            return try DatabaseEntityProjection.fieldObject(for: model)["title"]
        }
    }

    private func makeContainer(type: DataType) async throws -> DBContainer {
        let fieldType: FieldSchemaType
        if case .decimal = type { fieldType = .object } else { fieldType = .string }
        return try await makeContainer(fieldType: fieldType, queryType: .sql(type))
    }

    private func makeContainer(fieldType: FieldSchemaType, queryType: QueryColumnType?) async throws -> DBContainer {
        let entity = try Schema.Entity(name: "Assignment", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "title", fieldNumber: 2, type: fieldType, queryType: queryType)
        ], directoryComponents: [.staticPath("assignment")])
        let schema = try Schema(entities: [entity], version: Schema.Version(1, 0, 0))
        return try await DBContainer.open(testing: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "assignment-tests", revision: 1), schema: schema),
            security: .testingDisabled)
    }
}
