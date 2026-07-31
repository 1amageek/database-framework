import DatabaseEngine
import DatabaseTypes

/// Dataset-dependent expression resolution supplied by the query executor.
/// Thrown failures retain their original execution-domain type. SPARQL
/// expression failures travel through the outcome value.
struct SPARQLRuntimeExpressionResolver: Sendable {
    let exists: @Sendable (
        Int,
        VariableBinding
    ) async throws -> SPARQLExpressionEvaluationOutcome<Bool>
    let function: @Sendable (
        String,
        [FieldValue],
        VariableBinding
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
}

struct SPARQLRuntimeExpressionEvaluator: Sendable {
    static func evaluateAsBoolean(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding,
        workMeter: DatabaseWorkMeter,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> Bool {
        switch try await evaluate(
            plan,
            binding: binding,
            workMeter: workMeter,
            resolver: resolver
        ) {
        case .value(let value):
            do throws(SPARQLExpressionEvaluationError) {
                return try ExpressionEvaluator.effectiveBooleanValue(value)
            } catch let error {
                if error.isSPARQLEvaluationError {
                    return false
                }
                throw error
            }
        case .expressionError(let error):
            if error.isSPARQLEvaluationError {
                return false
            }
            throw error
        }
    }

    static func evaluate(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding,
        workMeter: DatabaseWorkMeter,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        var machine = SPARQLExpressionEvaluationMachine(
            program: plan.program,
            binding: binding
        )

        while true {
            switch try machine.advance(workMeter: workMeter) {
            case .finished(let outcome):
                return outcome

            case .action(.exists(let handle)):
                let resolved = try await resolver.exists(handle, binding)
                switch resolved {
                case .value(let value):
                    do throws(SPARQLExpressionEvaluationError) {
                        machine.resume(
                            with: .value(
                                try ExpressionEvaluator
                                    .canonicalBoolean(value)
                            )
                        )
                    } catch let error {
                        machine.resume(with: .expressionError(error))
                    }
                case .expressionError(let error):
                    machine.resume(with: .expressionError(error))
                }

            case .action(.function(let name, let arguments)):
                machine.resume(
                    with: try await resolver.function(
                        name,
                        arguments,
                        binding
                    )
                )
            }
        }
    }
}
