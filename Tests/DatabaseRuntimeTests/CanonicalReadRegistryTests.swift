import Testing
import DatabaseEngine
import DatabaseRuntime

@Suite("Canonical Read Registry")
struct CanonicalReadRegistryTests {
    @Test("Unknown kindIdentifier is not resolved")
    func unknownKindIdentifierReturnsNil() {
        #expect(ReadExecutorRegistry.shared.indexExecutor(for: "com.example.unknown") == nil)
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
        #expect(LogicalSourceExecutorRegistry.shared.graphTableExecutor != nil)
        #expect(LogicalSourceExecutorRegistry.shared.sparqlExecutor != nil)
    }
}
