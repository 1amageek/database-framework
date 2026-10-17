import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S13 generated AGE review regressions", .timeLimit(.minutes(1)))
struct S13SQLReviewScalarAdmissionTests {
    @Test("Unary AGE is rejected as a generated expression for either timestamp overload", arguments: [false, true])
    func unaryAgeIsNotImmutable(withTimeZone: Bool) throws {
        let expression = Expression.function(FunctionCall(name: "age", arguments: [.col("created")]))
        let schema = try schema(withTimeZone: withTimeZone)
        let relation = try relation(expression)
        let catalog = try SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [relation])
        #expect(throws: SQLConstraintError.invalidDeclaration(relation: "items", reason: "generated expression must be immutable")) {
            try SQLConstraintCompiler.validate(catalog: catalog, physicalSchema: schema)
        }
    }

    @Test("Binary timestamp AGE remains admitted and generates the same interval on different transaction dates")
    func binaryAgeRetainsImmutableGeneration() async throws {
        let expression = Expression.function(FunctionCall(name: "age", arguments: [
            .cast(.string("2024-06-01"), targetType: .timestamp(withTimeZone: false)), .col("created")]))
        let schema = try schema(withTimeZone: false)
        let relation = try relation(expression)
        try SQLConstraintCompiler.validate(catalog: SQLCatalog(
            namespaces: [SQLNamespace(id: "public", name: "public")], relations: [relation]), physicalSchema: schema)
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: 100_000,
            maximumIntermediateRows: 100, maximumIntermediateBytes: 1_000_000), monotonicClock: TestProcessMonotonicClock())
        let first = environment(seconds: 1_735_689_600)
        let created = try CanonicalSQLTemporalEvaluator.cast(.primitive(.string("2024-04-30")),
            from: .sql(.text), to: .sql(.timestamp(withTimeZone: false)), context: .explicit,
            environment: first, workMeter: meter)
        let stored = try QueryFieldStorageAdapter.encode(created, type: .sql(.timestamp(withTimeZone: false)),
            limits: SQLConstraintRows.limits(meter))
        let fields = try FieldObject([("id", .string("one")), ("created", stored)])
        let expected = try CanonicalSQLTemporalEvaluator.cast(.primitive(.string("1 month 1 day")),
            from: .sql(.text), to: .sql(.interval()), context: .explicit, environment: first, workMeter: meter)
        for environment in [first, environment(seconds: 1_735_776_000)] {
            let row = try await SQLConstraintRows.row(fields: fields, relation: relation,
                physicalSchema: schema, environment: environment, workMeter: meter)
            #expect(row.values[2] == expected)
        }
    }

    private func schema(withTimeZone: Bool) throws -> Schema {
        try Schema(entities: [Schema.Entity(name: "ReviewItem", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "created", fieldNumber: 2, type: withTimeZone ? .timestamp : .dateTime,
                queryType: .sql(.timestamp(withTimeZone: withTimeZone)))
        ], directoryComponents: [.staticPath("s13-review-age")])])
    }

    private func relation(_ expression: Expression) throws -> SQLRelation {
        try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "ReviewItem", columns: [
            SQLColumn(id: "id", name: "id", fieldNumber: 1),
            SQLColumn(id: "created", name: "created", fieldNumber: 2),
            SQLColumn(id: "elapsed", name: "elapsed", backing: .virtual(type: .sql(.interval())), policy: .generated(expression))
        ])
    }

    private func environment(seconds: Int64) -> SQLTemporalEnvironment {
        let instant = Timestamp(secondsSinceUnixEpoch: seconds)
        return SQLTemporalEnvironment(transactionInstant: instant, statementInstant: instant, wallClock: FixedTestWallClock())
    }
}
