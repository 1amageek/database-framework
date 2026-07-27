#if FOUNDATION_DB
import Testing

/// Serializes every test in an annotated suite against the shared
/// FoundationDB consistency domain.
public struct FoundationDBScenarioTrait: TestTrait, SuiteTrait, TestScoping {
    public var isRecursive: Bool { true }

    public init() {}

    public func provideScope(
        for test: Test,
        testCase: Test.Case?,
        performing function: @Sendable () async throws -> Void
    ) async throws {
        guard !test.isSuite else {
            try await function()
            return
        }

        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess(
            function
        )
    }
}

extension SuiteTrait where Self == FoundationDBScenarioTrait {
    public static var foundationDBScenario: FoundationDBScenarioTrait {
        FoundationDBScenarioTrait()
    }
}

extension TestTrait where Self == FoundationDBScenarioTrait {
    public static var foundationDBScenario: FoundationDBScenarioTrait {
        FoundationDBScenarioTrait()
    }
}
#endif
