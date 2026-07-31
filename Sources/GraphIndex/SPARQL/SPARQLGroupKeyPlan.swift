import DatabaseKit

public struct SPARQLGroupKeyPlan: Sendable {
    public let outputVariable: String
    public let expression: SPARQLExpressionPlan

    package init(
        outputVariable: String,
        expression: SPARQLExpressionPlan
    ) {
        self.outputVariable = outputVariable
        self.expression = expression
    }

    package static func executionVariable(
        _ variable: String
    ) throws -> SPARQLGroupKeyPlan {
        guard variable.first == "?" else {
            throw SPARQLQueryError.invalidGroupBy(
                "GROUP BY variables must use SPARQL '?name' syntax"
            )
        }
        let rawName = String(variable.dropFirst())
        do {
            _ = try SPARQLVariableName(rawName)
        } catch {
            throw SPARQLQueryError.invalidGroupBy(
                "Invalid GROUP BY variable '\(variable)': \(error)"
            )
        }
        return SPARQLGroupKeyPlan(
            outputVariable: variable,
            expression: try SPARQLExpressionPlan(
                .variable(Variable(rawName))
            )
        )
    }
}
