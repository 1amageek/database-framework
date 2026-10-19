import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S17 encoded SQL facade", .timeLimit(.minutes(1)))
struct S17EncodedSQLFacadeTests {
    @Test("Text, semantic block and statement entries share the complete projection lifecycle")
    func allEntriesProject() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let encoder = S17FacadeEncoder()
        let statement = QueryStatement.select(SelectQuery(projection: [.expression(.int(7))], source: .values([[]])))
        let text = try await context.executeEncodedSQLBlock("SELECT 7", options: options(), encoder: encoder)
        let semantic = try await context.executeEncodedSQLBlock(SQLBlock(steps: [.statement(statement)]),
            options: options(), encoder: encoder)
        let single = try await context.executeEncodedStatement(statement, options: options(), encoder: encoder)
        #expect(try text.promoteToOutput() == ByteString([1]))
        #expect(try semantic.promoteToOutput() == ByteString([1]))
        #expect(try single.promoteToOutput() == ByteString([1]))
    }

    @Test("Encoded text preserves absolute source position and original binding failure")
    func sourceDiagnostic() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        do {
            _ = try await container.testBaseContext().executeEncodedSQLBlock("SELECT '猫';\r\nSELECT missing_column",
                options: options(), encoder: S17FacadeEncoder())
            Issue.record("Expected binding failure before projection")
        } catch let failure as SQLExecutionFailure {
            #expect(failure.diagnostic.sqlState == "42703")
            #expect(failure.diagnostic.position == 21)
            #expect(failure.underlyingError is CanonicalSQLBindingFailure)
        }
    }

    @Test("Facade retains projection failure and rolls back earlier SQL mutation")
    func rejectionKeepsTypedCause() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("CREATE TABLE items (id integer)", options: options())
        do {
            _ = try await context.executeEncodedSQLBlock("INSERT INTO items VALUES (1); SELECT id FROM items",
                options: options(), encoder: S17FacadeEncoder(refuseEncoding: true))
            Issue.record("Expected projection refusal")
        } catch let failure as SQLExecutionFailure {
            #expect(failure.underlyingError is S17FacadeRefusal)
        }
        #expect(try await context.executeSQL("SELECT id FROM items").rows.isEmpty)
        let diagnostic = try SQLDiagnostic(sqlState: "54000", category: .resource,
            message: "Complete response exceeds the configured limit")
        let classified = SQLExecutionFailure(underlyingError: S17FacadeRefusal.rejected, diagnostic: diagnostic)
        let encoder = S17FacadeEncoder(classifiedFailure: classified)
        let statement = QueryStatement.select(SelectQuery(projection: [.expression(.int(1))], source: .values([[]])))
        for entry in 0..<3 {
            do {
                switch entry {
                case 0:
                    _ = try await context.executeEncodedSQLBlock("INSERT INTO items VALUES (2); SELECT id FROM items",
                        options: options(), encoder: encoder)
                case 1:
                    _ = try await context.executeEncodedSQLBlock(SQLBlock(steps: [.statement(statement)]),
                        options: options(), encoder: encoder)
                default:
                    _ = try await context.executeEncodedStatement(statement, options: options(), encoder: encoder)
                }
                Issue.record("Expected preclassified projection refusal")
            } catch let failure as SQLExecutionFailure {
                #expect(failure.diagnostic == diagnostic)
                #expect(failure.underlyingError is S17FacadeRefusal)
                #expect(!(failure.underlyingError is SQLExecutionFailure))
            }
        }
        #expect(try await context.executeSQL("SELECT id FROM items").rows.isEmpty)
    }

    private func makeContainer() async throws -> DBContainer {
        let runtime = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "s17-facade", revision: 1), entityRuntimes: [])
        return try await DBContainer.open(for: Schema(entities: []), configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: runtime, security: .testingDisabled)
    }
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 100,
            maximumDepth: 32, maximumRetainedBytes: 8_000_000, maximumWorkUnits: 1_000_000, maximumReadPageRows: 1000))
    }
}

private enum S17FacadeRefusal: Error { case rejected }

private struct S17FacadeEncoder: SQLBlockResponseEncoder {
    let refuseEncoding: Bool
    let classifiedFailure: SQLExecutionFailure?
    init(refuseEncoding: Bool = false, classifiedFailure: SQLExecutionFailure? = nil) {
        self.refuseEncoding = refuseEncoding; self.classifiedFailure = classifiedFailure
    }
    func measure(_ output: borrowing SQLBlockOutputView) throws -> UInt64 { 1 }
    func encode(_ output: borrowing SQLBlockOutputView, into destination: inout MutableSpan<UInt8>) throws {
        if refuseEncoding { throw S17FacadeRefusal.rejected }
        if let classifiedFailure { throw classifiedFailure }
        destination[0] = UInt8(output.statementCount)
    }
}
