import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import DatabaseWire
import TestHeartbeat
import TestSupport
import Testing
@testable import GraphIndex

@Suite("SPARQL expression machine", .heartbeat)
struct SPARQLExpressionMachineTests {
    private actor ResolutionLog {
        private var entries: [String] = []

        func append(_ entry: String) {
            entries.append(entry)
        }

        func snapshot() -> [String] {
            entries
        }
    }

    @Test("An admitted deeply nested expression executes without recursion")
    func deeplyNestedExpressionUsesTheFlatMachine() throws {
        var expression = Expression.literal(.bool(true))
        for _ in 0..<511 {
            expression = .not(expression)
        }
        let limits = SPARQLExpressionCompilationLimits(
            structuralLimits: QueryStructuralLimits(
                maximumNestingDepth: 512,
                maximumTotalNodes: 513
            )
        )
        let plan = try SPARQLExpressionPlan(expression, limits: limits)

        let result = try ExpressionEvaluator.evaluate(
            plan,
            binding: VariableBinding()
        )

        #expect(try ExpressionEvaluator.effectiveBooleanValue(result) == false)
    }

    @Test("The first expression beyond the admitted bound is rejected")
    func expressionDepthBoundaryIsExact() {
        var expression = Expression.literal(.bool(true))
        for _ in 0..<512 {
            expression = .not(expression)
        }
        let limits = SPARQLExpressionCompilationLimits(
            structuralLimits: QueryStructuralLimits(
                maximumNestingDepth: 512,
                maximumTotalNodes: 513
            )
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            _ = try SPARQLExpressionPlan(expression, limits: limits)
        }
    }

    @Test("IN preserves a recoverable error until a later value matches")
    func membershipContinuesAfterRecoverableError() throws {
        let result = try ExpressionEvaluator.evaluate(
            .inList(
                .literal(.int(2)),
                values: [
                    .variable(Variable("missing")),
                    .literal(.int(2)),
                ]
            ),
            binding: VariableBinding()
        )

        #expect(try ExpressionEvaluator.effectiveBooleanValue(result))
    }

    @Test("IF resolves only the selected branch exactly once")
    func conditionalResolvesOnlySelectedBranch() async throws {
        let selected = "did:example:selected"
        let unreachable = "did:example:unreachable"
        let log = ResolutionLog()
        let plan = try SPARQLExpressionPlan(
            .function(
                FunctionCall(
                    name: "IF",
                    arguments: [
                        .literal(.bool(true)),
                        Self.extensionCall(selected),
                        Self.extensionCall(unreachable),
                    ]
                )
            )
        )
        let workMeter = Self.workMeter()

        let outcome = try await SPARQLRuntimeExpressionEvaluator.evaluate(
            plan,
            binding: VariableBinding(),
            workMeter: workMeter,
            resolver: Self.resolver(log: log)
        )

        guard case .value(.string(let value)) = outcome else {
            Issue.record("IF did not return the selected resolver value")
            return
        }
        #expect(value == selected)
        #expect(await log.snapshot() == [selected])
        #expect(workMeter.consumedWorkUnits == 3)
    }

    @Test("Work exhaustion stops before an unresolved branch is invoked")
    func workExhaustionPrecedesRuntimeResolution() async throws {
        let selected = "did:example:selected"
        let unreachable = "did:example:unreachable"
        let log = ResolutionLog()
        let plan = try SPARQLExpressionPlan(
            .function(
                FunctionCall(
                    name: "IF",
                    arguments: [
                        .literal(.bool(true)),
                        Self.extensionCall(selected),
                        Self.extensionCall(unreachable),
                    ]
                )
            )
        )

        await #expect(
            throws: DatabaseWorkLimitError.maximumWorkUnits(
                stage: .expressionEvaluation,
                consumed: 2,
                requested: 1,
                maximum: 2
            )
        ) {
            _ = try await SPARQLRuntimeExpressionEvaluator.evaluateAsBoolean(
                plan,
                binding: VariableBinding(),
                workMeter: Self.workMeter(maximumWorkUnits: 2),
                resolver: Self.resolver(log: log)
            )
        }
        #expect(await log.snapshot().isEmpty)
    }

    @Test("COALESCE resolves each required branch once and then stops")
    func coalesceStopsAfterFirstSuccessfulResolution() async throws {
        let failing = "did:example:failing"
        let selected = "did:example:selected"
        let unreachable = "did:example:unreachable"
        let log = ResolutionLog()
        let plan = try SPARQLExpressionPlan(
            .function(
                FunctionCall(
                    name: "COALESCE",
                    arguments: [
                        Self.extensionCall(failing),
                        Self.extensionCall(selected),
                        Self.extensionCall(unreachable),
                    ]
                )
            )
        )

        let outcome = try await SPARQLRuntimeExpressionEvaluator.evaluate(
            plan,
            binding: VariableBinding(),
            workMeter: Self.workMeter(),
            resolver: Self.resolver(log: log, failingFunction: failing)
        )

        guard case .value(.string(let value)) = outcome else {
            Issue.record("COALESCE did not return the first successful value")
            return
        }
        #expect(value == selected)
        #expect(await log.snapshot() == [failing, selected])
    }

    @Test("A nonrecoverable right failure is not hidden by a left value error")
    func nonrecoverableFailureIsNotMasked() async throws {
        let failure = "did:example:runtime-failure"
        let log = ResolutionLog()
        let plan = try SPARQLExpressionPlan(
            .and(
                .variable(Variable("missing")),
                Self.extensionCall(failure)
            )
        )

        let outcome = try await SPARQLRuntimeExpressionEvaluator.evaluate(
            plan,
            binding: VariableBinding(),
            workMeter: Self.workMeter(),
            resolver: Self.resolver(log: log, fatalFunction: failure)
        )

        guard case .expressionError(.runtimeInvariant(let detail)) = outcome else {
            Issue.record("The runtime failure was masked")
            return
        }
        #expect(detail == failure)
        #expect(await log.snapshot() == [failure])
    }

    @Test("IN returns its first recoverable error when no value matches")
    func membershipReturnsFirstRecoverableErrorWithoutMatch() {
        #expect(
            throws: SPARQLExpressionEvaluationError
                .unboundVariable("?missing")
        ) {
            _ = try ExpressionEvaluator.evaluate(
                .inList(
                    .literal(.int(2)),
                    values: [
                        .variable(Variable("missing")),
                        .literal(.int(3)),
                    ]
                ),
                binding: VariableBinding()
            )
        }
    }

    @Test("IN stops immediately after a nonrecoverable failure")
    func membershipStopsAfterNonrecoverableFailure() async throws {
        let failure = "did:example:runtime-failure"
        let unreachable = "did:example:unreachable"
        let log = ResolutionLog()
        let plan = try SPARQLExpressionPlan(
            .inList(
                .literal(.int(2)),
                values: [
                    Self.extensionCall(failure),
                    Self.extensionCall(unreachable),
                ]
            )
        )

        let outcome = try await SPARQLRuntimeExpressionEvaluator.evaluate(
            plan,
            binding: VariableBinding(),
            workMeter: Self.workMeter(),
            resolver: Self.resolver(log: log, fatalFunction: failure)
        )

        guard case .expressionError(.runtimeInvariant(let detail)) = outcome else {
            Issue.record("IN masked the nonrecoverable failure")
            return
        }
        #expect(detail == failure)
        #expect(await log.snapshot() == [failure])
    }

    @Test("EXISTS requests its compiled handle exactly once")
    func existsResolvesItsHandleExactlyOnce() async throws {
        let log = ResolutionLog()
        let plan = try SPARQLExpressionPlan(
            .exists(
                SelectQuery(
                    projection: .all,
                    source: .graphPattern(.basic([]))
                )
            )
        )

        let outcome = try await SPARQLRuntimeExpressionEvaluator.evaluate(
            plan,
            binding: VariableBinding(),
            workMeter: Self.workMeter(),
            resolver: Self.resolver(log: log)
        )

        guard case .value(let value) = outcome else {
            Issue.record("EXISTS did not produce a value")
            return
        }
        #expect(try ExpressionEvaluator.effectiveBooleanValue(value))
        #expect(await log.snapshot() == ["exists:0"])
    }

    @Test("Malformed lazy functions report argument errors")
    func malformedLazyFunctionsAreNotRuntimeInvariants() {
        let malformed = Expression.function(
            FunctionCall(
                name: "BOUND",
                arguments: [.literal(.bool(true))]
            )
        )

        #expect(
            throws: SPARQLExpressionEvaluationError
                .invalidFunctionArguments("BOUND")
        ) {
            _ = try ExpressionEvaluator.evaluate(
                malformed,
                binding: VariableBinding()
            )
        }
    }

    private static func extensionCall(_ name: String) -> Expression {
        .function(FunctionCall(name: name, arguments: []))
    }

    private static func workMeter(
        maximumWorkUnits: UInt64 = 10_000
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: maximumWorkUnits,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private static func resolver(
        log: ResolutionLog,
        failingFunction: String? = nil,
        fatalFunction: String? = nil
    ) -> SPARQLRuntimeExpressionResolver {
        SPARQLRuntimeExpressionResolver(
            exists: { handle, _ in
                await log.append("exists:\(handle)")
                return .value(true)
            },
            function: { name, _, _ in
                await log.append(name)
                if name == failingFunction {
                    return .expressionError(
                        .typeError("recoverable function failure")
                    )
                }
                if name == fatalFunction {
                    return .expressionError(.runtimeInvariant(name))
                }
                return .value(.string(name))
            }
        )
    }
}
