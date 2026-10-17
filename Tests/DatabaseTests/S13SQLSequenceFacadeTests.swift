import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S13 text sequence execution", .timeLimit(.minutes(1)))
struct S13SQLSequenceFacadeTests {
    @Test("Text SELECT owns request history and publishes independently committed allocation")
    func textSequenceHistory() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let response = try await context.executeSQL("SELECT NEXTVAL('counter'), currval('counter'), lastval()")
        #expect(response.rows.map(\.values) == [[.primitive(.int64(1)), .primitive(.int64(1)), .primitive(.int64(1))]])
        await #expect(throws: SQLSequenceExecutionError.currentValueUnavailable("counter")) {
            try await context.executeSQL("SELECT currval('counter')")
        }
        await #expect(throws: SQLBindingSourceFailure.self) {
            try await context.executeSQL("SELECT setval('counter', 1.5)")
        }
        #expect(try await context.executeSQL("SELECT nextval('counter')").rows.map(\.values)
            == [[.primitive(.int64(2))]])
        #expect(try await context.executeSQL("SELECT setval('counter', 1.5::bigint)").rows.map(\.values)
            == [[.primitive(.int64(2))]])
        #expect(try await context.executeSQL("SELECT nextval('counter')").rows.map(\.values)
            == [[.primitive(.int64(3))]])
    }

    @Test("An expression failure rolls back data while preserving consumed sequence values")
    func expressionFailureConsumesValue() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let bindingSQL = #"/* あ */ SELECT nextval('counter'), "LOWER"('ABC')"#
        do {
            _ = try await context.executeSQL(bindingSQL)
            Issue.record("A quoted unknown function must fail binding")
        } catch let failure as SQLBindingSourceFailure {
            guard case .expression(.unsupportedFunction("LOWER")) = failure.cause else {
                Issue.record("Expected the original quoted function failure"); return
            }
            #expect(failure.position == bindingSQL.range(of: #""LOWER""#).map {
                bindingSQL.utf8.distance(from: bindingSQL.utf8.startIndex, to: $0.lowerBound)
            })
        }
        let runtimeSQL = "SELECT nextval('counter'), 1 / 0"
        do {
            _ = try await context.executeSQL(runtimeSQL)
            Issue.record("The selected division must fail")
        } catch let failure as SQLBindingSourceFailure {
            guard case .expression(.divisionByZero) = failure.cause else {
                Issue.record("Expected the original division failure"); return
            }
            #expect(failure.position == runtimeSQL.range(of: "1 / 0").map {
                runtimeSQL.utf8.distance(from: runtimeSQL.utf8.startIndex, to: $0.lowerBound)
            })
        }
        #expect(try await context.executeSQL("SELECT nextval('counter')").rows.map(\.values)
            == [[.primitive(.int64(2))]])
        #expect(try await context.executeSQL("SELECT setval(NULL, 4), nextval('counter')").rows.map(\.values)
            == [[.primitive(.null), .primitive(.int64(3))]])
    }

    @Test("External paging is rejected before consuming a sequence value")
    func pagingCannotReplayEffects() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        await #expect(throws: SQLExecutionError.self) {
            try await context.executeSQL("SELECT nextval('counter')", options: ReadExecutionOptions(pageSize: 1))
        }
        #expect(try await context.executeSQL("SELECT nextval('counter')").rows.map(\.values)
            == [[.primitive(.int64(1))]])
    }

    private func makeContainer() async throws -> DBContainer {
        let entity = try Schema.Entity(name: "SequenceFacadeItem", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)],
            directoryComponents: [.staticPath("s13-sequence-facade")])
        let schema = try Schema(entities: [entity])
        let container = try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-sequence-facade", revision: 1), schema: schema),
            security: .testingDisabled)
        do {
            let old = container.sqlCatalog
            let sequence = try SQLSequence(id: "counter", namespaceID: old.namespaces[0].id,
                name: "counter", start: 1, increment: 1, minimum: 1, maximum: 100)
            _ = try await container.publishSQLCatalog(
                SQLCatalog(namespaces: old.namespaces, relations: old.relations, sequences: [sequence]),
                expectedGeneration: container.schemaGeneration, idempotencyKey: "sequence-facade",
                authorization: TestBaseEnvironment.authorization)
            return container
        } catch {
            await container.shutdown()
            throw error
        }
    }
}
