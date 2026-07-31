import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import DatabaseWire
@testable import GraphIndex
import Testing
import TestSupport

@Suite("Aggregate expression work budget")
struct AggregateExpressionWorkBudgetTests {
    @Test("GROUP_CONCAT charges output bytes before materialization")
    func groupConcatChargesOutputBytes() async throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 4,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let aggregate = try AggregateExpression.groupConcat(
            "?value",
            separator: ",",
            as: "?joined"
        )
        let partition = try await makePartition(
            [VariableBinding(["?value": .string("payload")])]
        )

        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await aggregate.evaluate(
                groupIndex: 0,
                in: partition,
                workMeter: meter,
                evaluateExpression: { plan, binding in
                    Self.expressionOutcome(plan, binding: binding)
                }
            )
        }
    }

    @Test("Each aggregate input and DISTINCT insertion is charged")
    func countDistinctChargesEachInput() async throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 4,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let aggregate = try AggregateExpression.countDistinct(
            "?value",
            as: "?count"
        )
        let partition = try await makePartition([
            VariableBinding(["?value": .string("same")]),
            VariableBinding(["?value": .string("same")]),
        ])
        let outcome = try await aggregate.evaluate(
            groupIndex: 0,
            in: partition,
            workMeter: meter,
            evaluateExpression: { plan, binding in
                Self.expressionOutcome(plan, binding: binding)
            }
        )

        let expected = try RDFLiteral(
            lexicalForm: "1",
            datatype: "http://www.w3.org/2001/XMLSchema#integer"
        )
        guard case .value(let value) = outcome else {
            Issue.record("COUNT DISTINCT returned an expression error")
            return
        }
        #expect(value == .rdfTerm(.literal(expected)))
        #expect(meter.consumedWorkUnits == 4)
    }

    private func makePartition(
        _ bindings: [VariableBinding]
    ) async throws -> SPARQLGroupPartition {
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        var builder = try SPARQLRetainedBindingBuilder.make(
            workMeter: workMeter,
            stage: .aggregateInput,
            expectedCount: bindings.count
        )
        for binding in bindings {
            try builder.append(binding, at: .aggregateInput)
        }
        return try await SPARQLGroupPartitionBuilder.build(
            source: builder.finish(),
            grouping: .implicitSingleGroup,
            expressionContext: try SPARQLQueryExpressionContext(
                now: Timestamp(secondsSinceUnixEpoch: 0),
                functionRegistry: .empty,
                workMeter: workMeter
            ),
            workMeter: workMeter,
            evaluateKey: { plan, binding in
                Self.expressionOutcome(plan, binding: binding)
            }
        )
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
