import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("SQL binding execution")
struct SQLBindingExecutionTests {
    @Persistable
    struct BindingItem {
        #Directory<BindingItem>("sql_binding_items")

        var id: String
        var i: Int32
        var t: String
    }

    @Test("Unknown SQL strings adopt their expression context")
    func unknownStringsAdoptContext() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let result = try await context.executeSQL(
            "SELECT '7' = 7 AS equal, CASE WHEN TRUE THEN '7' ELSE 8 END AS choice, COALESCE('7', 8) AS fallback"
        )
        #expect(result.columns.map(\.type) == [.sql(.boolean), .sql(.integer), .sql(.integer)])
        #expect(result.rows.first?.values == [.primitive(.bool(true)), .primitive(.int32(7)), .primitive(.int32(7))])
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL("SELECT TEXT '7' = 7")
        }
        await container.shutdown()
    }

    @Test("SQL function identities preserve qualification and quoted case")
    func exactFunctionIdentities() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let result = try await context.executeSQL("SELECT pg_catalog.lower('ABC') AS value, length('ABC') AS size")
        #expect(result.columns.map(\.type) == [.sql(.text), .sql(.integer)])
        #expect(result.rows.first?.values == [.primitive(.string("abc")), .primitive(.int32(3))])
        for sql in ["SELECT \"LOWER\"('ABC')", "SELECT \"pg_catalog.lower\"('ABC')", "SELECT other.lower('ABC')"] {
            await #expect(throws: SQLBindingSourceFailure.self) {
                _ = try await context.executeSQL(sql)
            }
        }
        await container.shutdown()
    }

    @Test("SQL parameters keep identity across incompatible column contexts")
    func parameterIdentitySurvivesAdmission() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        for sql in [
            "SELECT i FROM \"BindingItem\" WHERE i = $1 AND t = $1",
            "SELECT i FROM \"BindingItem\" WHERE t = $1 AND i = $1"
        ] {
            await #expect(throws: SQLBindingSourceFailure.self) {
                _ = try await context.executeSQL(sql, parameters: [
                    QueryParameter(position: 1, value: .primitive(.null))
                ])
            }
        }
        let result = try await context.executeSQL("SELECT $1::integer AS number, $1::text AS label", parameters: [
            QueryParameter(position: 1, value: .primitive(.null))
        ])
        #expect(result.columns.map(\.type) == [.sql(.integer), .sql(.text)])
        #expect(result.rows.first?.values == [.primitive(.null), .primitive(.null)])
        await container.shutdown()
    }

    @Test("Unconstrained SQL parameter contexts reject unresolved inference")
    func unresolvedParametersRejectExecution() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let parameters = [QueryParameter(position: 1, value: .primitive(.null))]
        let bare = try await context.executeSQL("SELECT $1 AS value", parameters: parameters)
        #expect(bare.columns.first?.type == .sql(.text))
        for sql in ["SELECT $1 IS NULL", "SELECT $1 AS value, $1 IS NULL", "SELECT $1 IS NULL, $1::integer"] {
            await #expect(throws: SQLBindingSourceFailure.self) {
                _ = try await context.executeSQL(sql, parameters: parameters)
            }
        }
        await container.shutdown()
    }

    @Test("Unused and forward CTE references are validated before execution")
    func allCTEsAreAdmitted() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        for sql in [
            "WITH bad AS (SELECT absent FROM \"BindingItem\") SELECT 1 AS value",
            "WITH a AS (SELECT * FROM b), b AS (SELECT 7 AS value) SELECT * FROM a"
        ] {
            await #expect(throws: SQLBindingSourceFailure.self) {
                _ = try await context.executeSQL(sql)
            }
        }
        let valid = try await context.executeSQL("WITH a AS (SELECT 7 AS value), b AS (SELECT value FROM a) SELECT value FROM b")
        #expect(valid.rows.first?.values == [.primitive(.int32(7))])
        await container.shutdown()
    }

    @Test("NULL casts validate the complete declared type path")
    func nullCastPathsAreValidated() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        for sql in ["SELECT NULL::uuid::integer", "SELECT NULL::integer::timestamp(3)"] {
            await #expect(throws: SQLBindingSourceFailure.self) {
                _ = try await context.executeSQL(sql)
            }
        }
        let result = try await context.executeSQL("SELECT NULL::uuid AS id, NULL::numeric(8,4)::numeric(6,2) AS value")
        #expect(result.rows.first?.values == [.primitive(.null), .primitive(.null)])
        #expect(result.columns.first?.type == .sql(.uuid))

        let parameters = [
            QueryParameter(position: 1, value: .primitive(.null), type: .sql(.timestamp(withTimeZone: false, precision: 6))),
            QueryParameter(position: 2, value: .primitive(.null), type: .sql(.time(withTimeZone: false, precision: 6))),
            QueryParameter(position: 3, value: .primitive(.null), type: .sql(.decimal(precision: 8, scale: 4)))
        ]
        let precisionSQL = "SELECT $1::timestamp(3) AS stamp, $2::time(3) AS clock, $3::numeric(6,2) AS amount"
        let expectedTypes: [QueryColumnType] = [
            .sql(.timestamp(withTimeZone: false, precision: 3)),
            .sql(.time(withTimeZone: false, precision: 3)),
            .sql(.decimal(precision: 6, scale: 2))
        ]
        let singleton = try await context.executeSQL(precisionSQL, parameters: parameters)
        #expect(singleton.columns.map(\.type) == expectedTypes)
        #expect(singleton.columns.allSatisfy { $0.isNullable })
        #expect(singleton.rows.first?.values == Array(repeating: .primitive(.null), count: 3))
        let tableSQL = precisionSQL + " FROM \"BindingItem\""
        let empty = try await context.executeSQL(tableSQL, parameters: parameters)
        #expect(empty.rows.isEmpty)
        #expect(empty.columns == singleton.columns)
        try context.insert(BindingItem(id: "precision", i: 1, t: "precision"))
        try await context.save()
        let populated = try await context.executeSQL(tableSQL, parameters: parameters)
        #expect(populated.columns == empty.columns)
        #expect(populated.rows.first?.values == Array(repeating: .primitive(.null), count: 3))

        for (sql, type) in [
            ("SELECT $1::integer", DataType.uuid),
            ("SELECT $1::timestamp(3)", DataType.integer)
        ] {
            do {
                _ = try await context.executeSQL(sql, parameters: [
                    QueryParameter(position: 1, value: .primitive(.null), type: .sql(type))
                ])
                Issue.record("Expected the declared NULL source type to reject an illegal cast")
            } catch let failure as SQLBindingSourceFailure {
                guard case .expression(.invalidCast("unsupported source-to-target conversion")) = failure.cause else {
                    Issue.record("Expected the typed illegal NULL cast signature")
                    return
                }
            }
        }
        let time = try CivilTime(hour: 12, minute: 0, second: 0, nanoseconds: 123_456_000)
        let dateTime = CivilDateTime(date: try CivilDate(year: 2026, month: 10, day: 15), time: time)
        let nonNullParameters = [
            QueryParameter(position: 1, value: .primitive(.dateTime(dateTime)), type: .sql(.timestamp(withTimeZone: false, precision: 6))),
            QueryParameter(position: 1, value: .primitive(.time(time)), type: .sql(.time(withTimeZone: false, precision: 6)))
        ]
        let roundedTime = try CivilTime(hour: 12, minute: 0, second: 0, nanoseconds: 123_000_000)
        let roundedDateTime = CivilDateTime(date: dateTime.date, time: roundedTime)
        let roundedSQLTime = try QueryTimeValue(microsecondsSinceMidnight: 43_200_123_000)
        let roundedTypes: [QueryColumnType] = [.sql(.timestamp(withTimeZone: false, precision: 3)), .sql(.time(withTimeZone: false, precision: 3))]
        let expectedValues: [QueryValue] = [.primitive(.dateTime(roundedDateTime)), .time(roundedSQLTime)]
        for (index, pair) in zip(["SELECT $1::timestamp(3)", "SELECT $1::time(3)"], nonNullParameters).enumerated() {
            let result = try await context.executeSQL(pair.0, parameters: [pair.1])
            #expect(result.columns.map(\.type) == [roundedTypes[index]])
            #expect(result.rows[0].values == [expectedValues[index]])
        }
        await container.shutdown()
    }

    @Test("SQL binding source positions identify the exact UTF-8 occurrence")
    func exactBindingSourcePosition() async throws {
        let container = try await makeContainer()
        let sql = "SELECT 'あ' AS label, lower('ABC') AS valid, \"LOWER\"('ABC') AS invalid"
        let marker = Array("\"LOWER\"".utf8)
        let bytes = Array(sql.utf8)
        let expected = bytes.indices.first { index in
            index + marker.count <= bytes.count && Array(bytes[index..<(index + marker.count)]) == marker
        }
        do {
            _ = try await container.testBaseContext().executeSQL(sql)
            Issue.record("Expected exact quoted function binding to fail")
        } catch let failure as SQLBindingSourceFailure {
            #expect(failure.position == expected)
            #expect(failure.occurrence != nil)
            guard case .expression(.unsupportedFunction("LOWER")) = failure.cause else {
                Issue.record("Expected the exact quoted function rejection")
                return
            }
        }
        let context = container.testBaseContext()
        let duplicateAliasSQL = "SELECT 'あ' AS label, 1 AS x, 2 AS x\r\nORDER BY x"
        do {
            _ = try await context.executeSQL(duplicateAliasSQL)
            Issue.record("Expected distinct duplicate output aliases to reject ORDER BY")
        } catch let failure as SQLBindingSourceFailure {
            guard case .read(.expressionEvaluation(.ambiguousColumn("x"), _)) = failure.cause else {
                Issue.record("Expected the typed duplicate output alias ambiguity")
                return
            }
            #expect(failure.position == duplicateAliasSQL.utf8.count - 1)
            #expect(failure.occurrence != nil)
        }
        let sameAlias = try await context.executeSQL("SELECT 1 AS x, 1 AS x ORDER BY x")
        #expect(sameAlias.rows.first?.values == [.primitive(.int32(1)), .primitive(.int32(1))])

        await container.shutdown()
    }

    @Test("SQL output ordinals preserve syntax meaning and decisive source positions")
    func outputOrdinalsKeepSyntaxMeaning() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()

        func executeSuccessfulSQL(_ sql: String) async throws -> QueryResponse {
            do {
                return try await context.executeSQL(sql)
            } catch {
                Issue.record("Unexpected execution failure for SQL: \(sql); error: \(error)")
                throw error
            }
        }
        for populated in [false, true] {
            if populated {
                try context.insert(BindingItem(id: "a", i: 2, t: "same"))
                try context.insert(BindingItem(id: "b", i: 1, t: "same"))
                try context.insert(BindingItem(id: "c", i: 3, t: "other"))
                try await context.save()
            }
            for clause in ["ORDER BY", "GROUP BY"] {
                for ordinal in ["0", "-1", "3"] {
                    let sql = "SELECT 'あ' AS label, i AS value FROM \"BindingItem\"\r\n\(clause) \(ordinal)"
                    do {
                        _ = try await context.executeSQL(sql)
                        Issue.record("Expected an out-of-range SQL output ordinal to reject binding")
                    } catch let failure as SQLBindingSourceFailure {
                        guard case .read(.invalidQueryStructure(.columnReference, "Output ordinal is outside the projection")) = failure.cause else {
                            Issue.record("Expected the typed invalid output ordinal failure")
                            return
                        }
                        #expect(failure.position == sql.utf8.count - ordinal.utf8.count)
                        #expect(failure.occurrence != nil)
                    }
                }
            }
        }
        let ordered = try await executeSuccessfulSQL("SELECT i, id FROM \"BindingItem\" ORDER BY 1 DESC")
        #expect(ordered.rows.map { $0.values[0] } == [.primitive(.int32(3)), .primitive(.int32(2)), .primitive(.int32(1))])
        let wildcard = try await executeSuccessfulSQL("SELECT * FROM \"BindingItem\" ORDER BY 2 DESC")
        #expect(wildcard.columns.map(\.name) == ["id", "i", "t"])
        #expect(wildcard.rows.map { $0.values[1] } == [.primitive(.int32(3)), .primitive(.int32(2)), .primitive(.int32(1))])
        let constantOrder = try await executeSuccessfulSQL("SELECT i, id FROM \"BindingItem\" ORDER BY 1::integer, id")
        #expect(constantOrder.rows.map { $0.values[0] } == [.primitive(.int32(2)), .primitive(.int32(1)), .primitive(.int32(3))])
        let grouped = try await executeSuccessfulSQL("SELECT t AS category, COUNT(*) AS total FROM \"BindingItem\" GROUP BY 1 ORDER BY 1")
        #expect(grouped.rows.map(\.values) == [
            [.primitive(.string("other")), .primitive(.int64(1))],
            [.primitive(.string("same")), .primitive(.int64(2))]
        ])
        let constantGroup = try await executeSuccessfulSQL("SELECT COUNT(*) AS total FROM \"BindingItem\" GROUP BY 1::integer")
        #expect(constantGroup.rows.first?.values == [.primitive(.int64(3))])
        let aggregateOrdinalSQL = "SELECT COUNT(*) AS total FROM \"BindingItem\"\r\nGROUP BY 1"
        do {
            _ = try await context.executeSQL(aggregateOrdinalSQL)
            Issue.record("Expected GROUP BY ordinal selecting an aggregate to reject binding")
        } catch let failure as SQLBindingSourceFailure {
            guard case .read(.aggregateEvaluation(.invalidGroupedExpression("GROUP BY cannot contain aggregates"))) = failure.cause else {
                Issue.record("Expected the typed aggregate GROUP BY rejection")
                return
            }
            #expect(failure.position == aggregateOrdinalSQL.utf8.count - 1)
        }
        await container.shutdown()
    }

    @Test("Prepared SQL parameter metadata remains accounted through execution")
    func preparedParametersRetainTheirReservation() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let workMeter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let execution = ReadExecutionContext(monotonicClock: container.monotonicClock, workMeter: workMeter)
        try await context.indexQueryContext.withSession(workMeter: workMeter) { session in
            let baseline = workMeter.retainedIntermediateBytes
            let prepared = try await context.prepareSQLSelectForCanonicalExecution(
                SelectQuery(projection: [ProjectionItem(.parameter(.position(1)), alias: "value")],
            source: .values([[]])),
                workMeter: workMeter,
                session: session,
                structuralLimits: .default,
                parameters: [QueryParameter(position: 1, value: .primitive(.null), type: .sql(.integer))]
            )
            #expect(workMeter.retainedIntermediateBytes > baseline)
            let response = try await prepared.execute(in: session, execution: execution)
            #expect(response.columns.first?.type == .sql(.integer))
            #expect(response.rows.first?.values == [.primitive(.null)])
            withExtendedLifetime(prepared) {
                #expect(workMeter.retainedIntermediateBytes > baseline)
            }
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
        #expect(workMeter.retainedIntermediateRows == 0)
        await container.shutdown()
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: Schema(entities: [try BindingItem.schemaEntity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "sql-binding-tests", revision: 1),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(BindingItem.self)]
            ),
            security: .testingDisabled
        )
    }
}
