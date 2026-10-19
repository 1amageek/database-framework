import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import Foundation
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

/// Replays the PostgreSQL 18.6 oracle recorded by `scripts/sql-differential-oracle`
/// through the public Framework SQL entry and compares observable results.
@Suite("SQL PostgreSQL differential matrix", .timeLimit(.minutes(10)))
struct SQLDifferentialTests {
    @Test("PostgreSQL differential case", arguments: SQLDifferentialOracle.caseIdentifiers)
    func replay(_ identifier: String) async throws {
        let oracle = try SQLDifferentialOracle.load()
        let differential = try #require(oracle.cases.first { $0.id == identifier })
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        if !differential.setup.isEmpty {
            _ = try await context.executeSQLBlock(differential.setup.joined(separator: ";\n"), options: options())
        }
        let expected = differential.expected
        let outcome: Result<SQLBlockResult, SQLExecutionFailure>
        do {
            outcome = .success(try await context.executeSQLBlock(differential.query.text, options: options()))
        } catch let failure as SQLExecutionFailure {
            outcome = .failure(failure)
        }
        switch outcome {
        case .failure(let failure):
            // The profile compares the SQLSTATE class; subclass codes need not match.
            #expect(failure.diagnostic.sqlState.prefix(2) == expected.state.prefix(2),
                "\(identifier): Framework failed with \(failure.diagnostic.sqlState) (PostgreSQL \(expected.state)): \(failure.diagnostic.message) cause \(String(reflecting: failure.underlyingError))")
        case .success(let result):
            #expect(expected.state == "00000", "\(identifier): Framework succeeded where PostgreSQL failed with \(expected.state)")
            if expected.state == "00000", differential.mode == .query {
                try compareQuery(result, expected: expected, differential: differential, container: container)
            } else if expected.state == "00000", let names = expected.columnNames {
                // Block and script cases compare the final statement by name and value.
                guard case .rows(let response) = try #require(result.results.last) else {
                    Issue.record("\(identifier): final statement returned no rows")
                    return
                }
                #expect(response.columns.map(\.name) == names, "\(identifier): final columns \(response.columns.map(\.name)) != \(names)")
                let actual = try rendered(response, container: container)
                let rows = expected.rows ?? []
                if differential.shape {
                    #expect(actual.count == rows.count, "\(identifier): final row count differs")
                } else if differential.ordered {
                    #expect(actual == rows, "\(identifier): final ordered rows \(actual) != \(rows)")
                } else {
                    #expect(Self.multiset(actual) == Self.multiset(rows), "\(identifier): final rows \(actual) != \(rows)")
                }
            }
        }
        if let post = differential.post {
            let readback = try await context.executeSQLBlock(post, options: options())
            guard case .rows(let response) = try #require(readback.results.last) else {
                Issue.record("\(identifier): post readback returned no rows")
                return
            }
            #expect(response.columns.map(\.name) == expected.postColumns,
                "\(identifier): post columns \(response.columns.map(\.name)) != \(expected.postColumns ?? [])")
            let state = try rendered(response, container: container)
            #expect(state == expected.post, "\(identifier): post state \(state) != \(expected.post ?? [])")
        }
    }

    private func compareQuery(_ result: SQLBlockResult, expected: SQLDifferentialOracle.Expected,
        differential: SQLDifferentialOracle.Case, container: DBContainer) throws {
        let identifier = differential.id
        let columns = expected.columns ?? []
        switch try #require(result.results.last) {
        case .rows(let response):
            #expect(response.columns.map(\.name) == columns.map(\.name),
                "\(identifier): column names \(response.columns.map(\.name)) != \(columns.map(\.name))")
            #expect(response.columns.map { Self.postgreSQLTypeName($0.type) } == columns.map { Self.productType($0.type) },
                "\(identifier): column types \(response.columns.map { Self.postgreSQLTypeName($0.type) }) != \(columns.map(\.type))")
            let actual = try rendered(response, container: container)
            let rows = expected.rows ?? []
            if differential.shape {
                // Environment-dependent values compare only metadata and cardinality.
                #expect(actual.count == rows.count, "\(identifier): row count differs")
            } else if differential.ordered {
                #expect(actual == rows, "\(identifier): ordered rows \(actual) != \(rows)")
            } else {
                #expect(Self.multiset(actual) == Self.multiset(rows), "\(identifier): rows \(actual) != \(rows)")
            }
            #expect((response.affectedRows ?? response.rows.count) == expected.rowCount,
                "\(identifier): row count \(response.affectedRows ?? response.rows.count) != \(expected.rowCount ?? -1)")
        case .command(_, let affectedRows):
            #expect(columns.isEmpty, "\(identifier): Framework returned a command where PostgreSQL returned columns")
            #expect(affectedRows.map(Int.init) == expected.rowCount, "\(identifier): affected row count differs")
        case .copy:
            Issue.record("\(identifier): Framework returned COPY output for a query case")
        }
    }

    private func rendered(_ response: QueryResponse, container: DBContainer) throws -> [[String?]] {
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: 50_000_000),
            monotonicClock: container.monotonicClock)
        let environment = SQLTemporalEnvironment(transactionInstant: Timestamp(secondsSinceUnixEpoch: 0),
            statementInstant: Timestamp(secondsSinceUnixEpoch: 0), wallClock: FixedTestWallClock())
        return try response.rows.map { row in
            try zip(row.values, response.columns).map { value, column in
                if value.isNull { return nil }
                // PostgreSQL's boolean output function differs from its text cast.
                if case .primitive(.bool(let flag)) = value { return flag ? "t" : "f" }
                return try CanonicalSQLScalarText.withText(of: value, type: column.type,
                    preservingCharacterPadding: true, temporalEnvironment: environment,
                    workMeter: meter) { String($0) }
            }
        }
    }

    private static func multiset(_ rows: [[String?]]) -> [[String?]: Int] {
        rows.reduce(into: [:]) { counts, row in counts[row, default: 0] += 1 }
    }

    /// The product has no PostgreSQL `name` type or information_schema domains;
    /// those relations and identifier functions return their base types.
    static func productType(_ postgreSQLType: String) -> String {
        switch postgreSQLType {
        case "name", "information_schema.sql_identifier", "information_schema.character_data",
             "information_schema.yes_or_no": "text"
        case "name[]": "text[]"
        case "information_schema.cardinal_number": "integer"
        default: postgreSQLType
        }
    }

    static func postgreSQLTypeName(_ type: QueryColumnType) -> String {
        switch type {
        case .sql(let type): return postgreSQLTypeName(type)
        case .primitive(let kind, let isArray):
            let element: String
            switch kind {
            case .bool: element = "boolean"
            case .int16: element = "smallint"
            case .int32: element = "integer"
            case .int64: element = "bigint"
            case .float32: element = "real"
            case .float64: element = "double precision"
            case .decimal: element = "numeric"
            case .string: element = "text"
            case .uuid: element = "uuid"
            case .bytes: element = "bytea"
            default: element = "unmapped:\(kind)"
            }
            return isArray ? element + "[]" : element
        }
    }

    static func postgreSQLTypeName(_ type: DataType) -> String {
        switch type {
        case .boolean: "boolean"
        case .smallint: "smallint"
        case .integer: "integer"
        case .bigint: "bigint"
        case .real: "real"
        case .doublePrecision: "double precision"
        case .decimal: "numeric"
        case .char: "character"
        case .varchar: "character varying"
        case .text: "text"
        case .date: "date"
        case .time(let withTimeZone, _): withTimeZone ? "time with time zone" : "time without time zone"
        case .timestamp(let withTimeZone, _): withTimeZone ? "timestamp with time zone" : "timestamp without time zone"
        case .interval: "interval"
        case .binary, .varbinary, .blob: "bytea"
        case .json: "json"
        case .jsonb: "jsonb"
        case .uuid: "uuid"
        case .array(let element): postgreSQLTypeName(element) + "[]"
        case .custom(let name, _): name
        case .composite: "record"
        }
    }

    /// The matrix compares semantics, so work admission is explicit and generous;
    /// default-budget admission is owned by the execution budget tests.
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(
            maximumEntries: 10_000, maximumDepth: 64, maximumRetainedBytes: 64_000_000,
            maximumWorkUnits: 10_000_000, maximumReadPageRows: 10_000),
            budget: ExecutionBudget(maximumWorkUnits: 50_000_000))
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [])
        let runtime = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "sql-differential", revision: 1), entityRuntimes: [],
            schemaDrivenAuthorizationPolicyFactory: { entity in
                AuthorizationPolicyHandler(schemaDriven: entity) { _, authorization in
                    authorization.principal?.roles.contains("test-runner") == true
                }
            })
        #if SQLITE
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        #else
        let engine = InMemoryEngine()
        #endif
        return try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: engine, sqlTemporal: try temporalConfiguration(),
                sqlDatabaseName: "differential"),
            runtimeConfiguration: runtime, security: .enabled())
    }

    /// IANA zones require explicitly supplied rules; the corpus zones come from host TZif data.
    private func temporalConfiguration() throws -> SQLTemporalConfiguration {
        let zones = ["America/New_York", "Asia/Tokyo"]
        let definitions = try zones.map { name in
            SQLTimeZoneDatabase.Definition(name: name, tzif: ByteString(Array(
                try Data(contentsOf: URL(fileURLWithPath: "/usr/share/zoneinfo/" + name)))))
        }
        let provider = try SQLTimeZoneDatabase(definitions: definitions,
            provenance: ByteString(utf8: "host /usr/share/zoneinfo for the SQL differential corpus"),
            limits: .init(maximumInputBytes: 65_536, maximumRetainedBytes: 262_144, maximumTransitions: 4_096))
        return try SQLTemporalConfiguration(rulesProvider: provider,
            limits: .init(maximumAbbreviations: 64, maximumRetainedBytes: 4_096))
    }
}

/// The checked-in PostgreSQL oracle; its corpus is parsed only by the oracle script.
struct SQLDifferentialOracle: Decodable, Sendable {
    enum Mode: String, Decodable, Sendable { case query, block, script }

    enum Statement: Decodable, Sendable {
        case one(String)
        case many([String])

        init(from decoder: any Decoder) throws {
            let container = try decoder.singleValueContainer()
            do {
                self = .one(try container.decode(String.self))
            } catch DecodingError.typeMismatch {
                self = .many(try container.decode([String].self))
            }
        }

        var text: String {
            switch self {
            case .one(let text): text
            case .many(let statements): statements.joined(separator: ";\n")
            }
        }
    }

    struct Column: Decodable, Sendable { let name: String; let type: String }

    struct Expected: Decodable, Sendable {
        let state: String
        let columns: [Column]?
        let columnNames: [String]?
        let rows: [[String?]]?
        let rowCount: Int?
        let post: [[String?]]?
        let postColumns: [String]?
    }

    struct Case: Decodable, Sendable {
        let id: String
        let inventory: String
        let kind: String
        let mode: Mode
        let ordered: Bool
        let shape: Bool
        let setup: [String]
        let query: Statement
        let post: String?
        let expected: Expected
    }

    let formatVersion: Int
    let postgresqlVersion: String
    let cases: [Case]

    static let location = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        .appendingPathComponent("SQLDifferential/oracle.json")

    static func load() throws -> Self {
        let oracle = try JSONDecoder().decode(Self.self, from: Data(contentsOf: location))
        guard oracle.formatVersion == 1, oracle.postgresqlVersion == "18.6" else {
            throw SQLDifferentialOracleError.unsupportedOracle
        }
        return oracle
    }

    /// A load failure becomes one failing case instead of silently selecting none.
    static let caseIdentifiers: [String] = {
        do { return try load().cases.map(\.id) } catch { return ["oracle-load-failure"] }
    }()
}

enum SQLDifferentialOracleError: Error { case unsupportedOracle }
