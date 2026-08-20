import Testing

struct FoundationDBBenchmarkTrait: TestTrait, SuiteTrait, TestScoping {
    var isRecursive: Bool { true }

    func provideScope(
        for test: Test,
        testCase: Test.Case?,
        performing function: @Sendable () async throws -> Void
    ) async throws {
        guard !test.isSuite else {
            try await function()
            return
        }
        try await FoundationDBBenchmarkEnvironment.shared
            .withExclusiveAccess(function)
    }
}

extension SuiteTrait where Self == FoundationDBBenchmarkTrait {
    static var foundationDBBenchmark: FoundationDBBenchmarkTrait {
        FoundationDBBenchmarkTrait()
    }
}

extension TestTrait where Self == FoundationDBBenchmarkTrait {
    static var foundationDBBenchmark: FoundationDBBenchmarkTrait {
        FoundationDBBenchmarkTrait()
    }
}
