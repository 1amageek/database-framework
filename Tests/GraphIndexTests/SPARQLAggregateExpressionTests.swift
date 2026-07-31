import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import DatabaseWire
@testable import GraphIndex
import Synchronization
import TestHeartbeat
import TestSupport
import Testing

@Suite("SPARQL aggregate expression semantics", .heartbeat)
struct SPARQLAggregateExpressionTests {
    @Test("SUM evaluates an arbitrary expression once per solution")
    func sumEvaluatesExpressionOncePerSolution() async throws {
        let evaluations = Mutex(0)
        let aggregate = AggregateExpression.sum(
            expression: try SPARQLExpressionPlan(
                .add(.variable(Variable("value")), .literal(.int(1)))
            ),
            distinct: false,
            alias: "?sum"
        )
        let result = try await evaluate(
            aggregate,
            bindings: [
                VariableBinding(["?value": .int64(1)]),
                VariableBinding(["?value": .int64(2)]),
            ],
            evaluateExpression: { plan, binding in
                evaluations.withLock { $0 += 1 }
                return Self.expressionOutcome(plan, binding: binding)
            }
        )

        #expect(evaluations.withLock { $0 } == 2)
        #expect(integerLexicalForm(result) == "5")
    }

    @Test("SUM DISTINCT deduplicates evaluated values")
    func sumDistinctUsesEvaluatedValues() async throws {
        let aggregate = AggregateExpression.sum(
            expression: try SPARQLExpressionPlan(
                .variable(Variable("value"))
            ),
            distinct: true,
            alias: "?sum"
        )
        let result = try await evaluate(
            aggregate,
            bindings: [
                VariableBinding(["?value": .int64(2)]),
                VariableBinding(["?value": .int64(2)]),
                VariableBinding(["?value": .int64(3)]),
            ]
        )

        #expect(integerLexicalForm(result) == "5")
    }

    @Test("Empty SUM and AVG produce canonical integer zero")
    func emptySumAndAverageAreZero() async throws {
        let expression = try SPARQLExpressionPlan(
            .variable(Variable("value"))
        )
        let sum = AggregateExpression.sum(
            expression: expression,
            distinct: false,
            alias: "?sum"
        )
        let average = AggregateExpression.avg(
            expression: expression,
            distinct: false,
            alias: "?average"
        )

        #expect(integerLexicalForm(try await evaluate(sum, bindings: [])) == "0")
        #expect(integerLexicalForm(try await evaluate(average, bindings: [])) == "0")
    }

    @Test("COUNT ignores local expression errors")
    func countIgnoresExpressionErrors() async throws {
        let aggregate = AggregateExpression.count(
            expression: try SPARQLExpressionPlan(
                .variable(Variable("value"))
            ),
            distinct: false,
            alias: "?count"
        )
        let result = try await evaluate(
            aggregate,
            bindings: [
                VariableBinding(),
                VariableBinding(["?value": .int64(1)]),
            ]
        )

        #expect(integerLexicalForm(result) == "1")
    }

    @Test("Non-COUNT aggregates preserve local expression errors")
    func nonCountAggregateDoesNotDiscardExpressionErrors() async throws {
        let aggregate = AggregateExpression.sum(
            expression: try SPARQLExpressionPlan(
                .variable(Variable("value"))
            ),
            distinct: false,
            alias: "?sum"
        )

        await #expect(throws: SPARQLExpressionEvaluationError.self) {
            _ = try await evaluate(
                aggregate,
                bindings: [
                    VariableBinding(["?value": .int64(1)]),
                    VariableBinding(),
                ]
            )
        }
    }

    @Test("GROUP_CONCAT rejects values without an RDF lexical form")
    func groupConcatRejectsNonLexicalValues() async throws {
        let aggregate = AggregateExpression.groupConcat(
            expression: try SPARQLExpressionPlan(
                .variable(Variable("value"))
            ),
            separator: ",",
            distinct: false,
            alias: "?joined"
        )

        await #expect(throws: SPARQLExpressionEvaluationError.self) {
            _ = try await evaluate(
                aggregate,
                bindings: [
                    VariableBinding(["?value": .array([.int64(1)])])
                ]
            )
        }
    }

    @Test("GROUP_CONCAT returns canonical xsd:string")
    func groupConcatReturnsCanonicalString() async throws {
        let aggregate = AggregateExpression.groupConcat(
            expression: try SPARQLExpressionPlan(
                .variable(Variable("value"))
            ),
            separator: ",",
            distinct: false,
            alias: "?joined"
        )
        let result = try await evaluate(
            aggregate,
            bindings: [
                VariableBinding(["?value": .string("a")]),
                VariableBinding(["?value": .string("b")]),
            ]
        )

        guard case .rdfTerm(.literal(let literal)) = result else {
            Issue.record("GROUP_CONCAT did not return an RDF literal")
            return
        }
        #expect(literal.datatypeIRI == .xsdString)
        #expect(literal.lexicalForm == "a,b")
    }

    @Test("Runtime failures abort aggregate evaluation")
    func runtimeFailurePropagates() async throws {
        let aggregate = AggregateExpression.count(
            expression: try SPARQLExpressionPlan(
                .variable(Variable("value"))
            ),
            distinct: false,
            alias: "?count"
        )

        await #expect(throws: SPARQLExpressionEvaluationError.self) {
            _ = try await evaluate(
                aggregate,
                bindings: [VariableBinding(["?value": .int64(1)])],
                evaluateExpression: { _, _ in
                    return .expressionError(
                        .resourceLimitExceeded(
                            stage: "test",
                            required: 2,
                            maximum: 1
                        )
                    )
                }
            )
        }
    }

    private func evaluate(
        _ aggregate: AggregateExpression,
        bindings: [VariableBinding]
    ) async throws -> FieldValue? {
        try await evaluate(
            aggregate,
            bindings: bindings,
            evaluateExpression: { plan, binding in
                Self.expressionOutcome(plan, binding: binding)
            }
        )
    }

    private func evaluate(
        _ aggregate: AggregateExpression,
        bindings: [VariableBinding],
        evaluateExpression: @escaping @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> FieldValue? {
        let workMeter = meter()
        var builder = try SPARQLRetainedBindingBuilder.make(
            workMeter: workMeter,
            stage: .aggregateInput,
            expectedCount: bindings.count
        )
        for binding in bindings {
            try builder.append(binding, at: .aggregateInput)
        }
        let expressionContext = try SPARQLQueryExpressionContext(
            now: Timestamp(secondsSinceUnixEpoch: 0),
            functionRegistry: .empty,
            workMeter: workMeter
        )
        let partition = try await SPARQLGroupPartitionBuilder.build(
            source: builder.finish(),
            grouping: .implicitSingleGroup,
            expressionContext: expressionContext,
            workMeter: workMeter,
            evaluateKey: evaluateExpression
        )
        let outcome = try await aggregate.evaluate(
            groupIndex: 0,
            in: partition,
            workMeter: workMeter,
            evaluateExpression: evaluateExpression
        )
        switch outcome {
        case .value(let value):
            return value
        case .expressionError(let error):
            throw error
        }
    }

    private func meter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func integerLexicalForm(_ value: FieldValue?) -> String? {
        guard case .rdfTerm(.literal(let literal)) = value,
              literal.datatypeIRI.rawValue
                == "http://www.w3.org/2001/XMLSchema#integer" else {
            return nil
        }
        return literal.lexicalForm
    }

    private static func expressionOutcome(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding
    ) -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        do throws(SPARQLExpressionEvaluationError) {
            return .value(
                try ExpressionEvaluator.evaluate(
                    plan,
                    binding: binding
                )
            )
        } catch let error {
            return .expressionError(error)
        }
    }
}
