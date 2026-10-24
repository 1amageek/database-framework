import Database
import DatabaseEmbedded
@_spi(Testing) import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
#if canImport(WASILibc)
import WASILibc
#elseif canImport(Darwin)
import Darwin
#else
import Glibc
#endif

@main
struct EmbeddedRuntimeContract {
    enum Failure: Error { case assertion }

    static func main() async {
        #if DATABASE_EMBEDDED_MULTI_BASE
        // This fixture owns the ordinary-root portable adapter contract only.
        print("EMBEDDED_ACCESS_UNSUPPORTED MultiBase")
        exit(2)
        #else
        var phase = "container-open"
        do {
            let container = try await DBContainer.open(for: Schema(entities: []),
                configuration: DBConfiguration(storageEngine: InMemoryEngine(),
                    monotonicClock: FrozenMonotonicClock(), wallClock: FrozenWallClock()),
                runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                    executionIdentity: .init(identifier: "embedded-access-runtime", revision: 1), entityRuntimes: []),
                security: .disabledForTesting)
            defer { await container.shutdown() }
            phase = "context"
            let context = container.newContext(authorization: .authenticated(.init(identifier: "runtime", roles: ["admin"])))
            phase = "borrowed-open"
            let access = try await DatabaseConfiguration.borrowingEmbedded(context: context, maximumConcurrentExecutions: 2).open()
            defer { await access.close() }
            for text in ["SELECT 7 AS n", "WITH c AS (SELECT 2 AS n) SELECT sum(n)::bigint AS n FROM c LIMIT 1"] {
                phase = "representations:\(text)"
                let statement = try SQLParser(preservesCastOutputNames: true).parse(text)
                let textResult = try await execute(access, .sql(text))
                let ir = try await execute(access, .statement(statement))
                let block = try await execute(access, .block(SQLBlock(steps: [.statement(statement)])))
                try require(textResult.columns == ir.columns && ir.columns == block.columns)
                try require(try await values(textResult.rows) == values(ir.rows))
                try require(try await values(ir.rows) == values(block.rows))
            }
            print("EMBEDDED_ACCESS_CASE representations PASS")
            _ = try await execute(access, .sql("CREATE TABLE items(id integer); INSERT INTO items VALUES(1)"))
            var rejected = false
            do { _ = try await execute(access, .sql("INSERT INTO items VALUES(2); SELECT missing; INSERT INTO items VALUES(3)")) }
            catch { rejected = true }
            try require(rejected)
            rejected = false
            do {
                _ = try await access.execute(.sql("INSERT INTO items VALUES(4)"), parameters: [], options: .init(budget: .init(maximumWorkUnits: 0)), metadata: .init())
            } catch { rejected = true }
            try require(rejected)
            try require(try await values(execute(access, .sql("SELECT id FROM items")).rows) == [[.primitive(.int32(1))]])
            print("EMBEDDED_ACCESS_CASE budget-rollback PASS")
            phase = "named-access"
            try await namedAccess(container: container, context: context)
            let retained = try await execute(access, .sql("SELECT id FROM items"))
            await access.close()
            try require(try await values(retained.rows) == [[.primitive(.int32(1))]])
            let direct = try await context.openConnection()
            await direct.close()
            let owned = try await DatabaseConfiguration.owningEmbedded(maximumConcurrentExecutions: 1,
                makeContainer: { container }, context: { _ in context }).open()
            await owned.close()
            let closed = try await context.openConnection()
            defer { await closed.close() }
            rejected = false
            do { _ = try await closed.execute("SELECT 1") } catch { rejected = true }
            try require(rejected)
            print("EMBEDDED_ACCESS_CASE lifecycle PASS")
            print("EMBEDDED_ACCESS_PASS cases=4")
        } catch {
            print("EMBEDDED_ACCESS_FAIL phase=\(phase)")
            #if !hasFeature(Embedded)
            print(error)
            #endif
            exit(1)
        }
        #endif
    }

    static func require(_ condition: Bool) throws(Failure) {
        guard condition else { throw .assertion }
    }
    #if !DATABASE_EMBEDDED_MULTI_BASE
    static func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: []),
            configuration: DBConfiguration(storageEngine: InMemoryEngine(),
                monotonicClock: FrozenMonotonicClock(), wallClock: FrozenWallClock()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "embedded-access-runtime", revision: 1), entityRuntimes: []),
            security: .disabledForTesting)
    }
    static func namedAccess(container: DBContainer, context: DatabaseContext) async throws {
        let second = try await makeContainer()
        defer { await second.shutdown() }
        let secondContext = second.newContext(authorization: .authenticated(.init(identifier: "runtime", roles: ["admin"])))
        let directory = DatabaseDirectory()
        try await directory.configure(databases: [
            "primary": .borrowingEmbedded(context: context, maximumConcurrentExecutions: 2),
            "analytics": .borrowingEmbedded(context: secondContext, maximumConcurrentExecutions: 2)])
        defer { await directory.shutdown() }
        let primary = try directory.database("primary"), analytics = try directory.database("analytics")
        for database in [primary, analytics] {
            _ = try await database.execute("CREATE TABLE \"EmbeddedRuntimePerson\"(id text, name text, age integer, \"isActive\" boolean)")
        }
        let tuples = (1...100).map { "('\($0)', 'value\($0)', \($0), true)" }.joined(separator: ",")
        _ = try await primary.execute(SQL(text: "INSERT INTO \"EmbeddedRuntimePerson\" VALUES " + tuples))
        _ = try await analytics.execute("INSERT INTO \"EmbeddedRuntimePerson\" VALUES('a','analytics',20,true)")
        let pure = try EmbeddedRuntimePerson.select(EmbeddedRuntimePerson.fields.name, EmbeddedRuntimePerson.fields.age)
            .where(EmbeddedRuntimePerson.fields.isActive == true).orderBy(EmbeddedRuntimePerson.fields.age).limit(2)
        let bound = try primary.entity(EmbeddedRuntimePerson.self).select(EmbeddedRuntimePerson.fields.name, EmbeddedRuntimePerson.fields.age)
            .where(EmbeddedRuntimePerson.fields.isActive == true).orderBy(EmbeddedRuntimePerson.fields.age).limit(2)
        try require(pure.definition == bound.definition)
        let result = try await bound.execute()
        try require(try await values(result.rows) == values(primary.execute(pure).rows))
        try require(try await values(result.rows) == [[.primitive(.string("value1")), .primitive(.int32(1))], [.primitive(.string("value2")), .primitive(.int32(2))]])
        try require(try await values(analytics.execute(pure).rows) == [[.primitive(.string("analytics")), .primitive(.int32(20))]])
        await directory.shutdown()
        try await directory.configure(databases: ["primary": .borrowingEmbedded(context: secondContext, maximumConcurrentExecutions: 2)])
        var rejected = false
        do { _ = try await bound.execute() } catch { rejected = true }
        try require(rejected)
        print("EMBEDDED_ACCESS_CASE named-access PASS")
    }
    #endif
    static func execute(_ access: any DatabaseExecuting, _ input: QueryInput) async throws -> AccessResult {
        try await access.execute(input, parameters: [], options: .init(), metadata: .init())
    }
    static func values(_ rows: RowStream) async throws -> [[QueryValue]] {
        var output: [[QueryValue]] = []
        for try await row in rows { output.append(row.values) }
        return output
    }
}
