import Testing
import Core
import QueryIR
import DatabaseClientProtocol
import DatabaseEngine
import DatabaseRuntime

@Suite("Canonical Read Registry")
struct CanonicalReadRegistryTests {
    private struct TestPolymorphicExecutor: PolymorphicIndexReadExecutor {
        let kindIdentifier = "test.polymorphic.runtime"

        func execute(
            context: FDBContext,
            selectQuery: SelectQuery,
            indexScan: IndexScanSource,
            group: PolymorphicGroup,
            options: ReadExecutionOptions,
            partitionValues: [String : String]?
        ) async throws -> QueryResponse {
            QueryResponse()
        }
    }

    @Test("Unknown kindIdentifier is not resolved")
    func unknownKindIdentifierReturnsNil() {
        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "com.example.unknown") == nil)
    }

    @Test("Polymorphic executors register independently from typed executors")
    func polymorphicExecutorsRegisterSeparately() {
        let executor = TestPolymorphicExecutor()
        ReadExecutorRegistry.shared.registerPolymorphic(executor)

        #expect(ReadExecutorRegistry.shared.indexExecutor(for: executor.kindIdentifier) == nil)
        #expect(ReadExecutorRegistry.shared.polymorphicIndexExecutor(for: executor.kindIdentifier) != nil)
    }

    @Test("Builtin runtime registers canonical read executors")
    func builtinRuntimeRegistersExecutors() {
        BuiltinReadRuntime.registerBuiltins()

        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "vector") != nil)
        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "fulltext") != nil)
        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "rank") != nil)
        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "bitmap") != nil)
        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "version") != nil)
        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "permuted") != nil)
        #expect(ReadExecutorRegistry.shared.polymorphicIndexExecutor(for: "vector") != nil)
        #expect(ReadExecutorRegistry.shared.polymorphicIndexExecutor(for: "fulltext") != nil)
        #expect(ReadExecutorRegistry.shared.polymorphicIndexExecutor(for: "rank") != nil)
        #expect(ReadExecutorRegistry.shared.polymorphicIndexExecutor(for: "bitmap") != nil)
        #expect(LogicalSourceExecutorRegistry.shared.graphTableExecutor != nil)
        #expect(LogicalSourceExecutorRegistry.shared.sparqlExecutor != nil)
    }
}
