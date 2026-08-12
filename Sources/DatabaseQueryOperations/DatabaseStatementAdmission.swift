import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import QueryAST
import DatabaseKit

/// Performs the non-replaceable admission path for statement execution.
package struct DatabaseStatementAdmission: Sendable {
    package let structuralLimits: QueryStructuralLimits

    package init(structuralLimits: QueryStructuralLimits) {
        self.structuralLimits = structuralLimits
    }

    package func admit(
        _ input: QueryExecuteOperation.Input,
        parameters: [QueryParameter]
    ) throws -> ValidatedDatabaseStatement {
        let parsed = try statement(from: input)
        let binder = try QueryParameterBinder(
            parameters: parameters,
            structuralLimits: structuralLimits
        )
        let bound = try binder.bind(parsed)
        try SPARQLSemanticValidator.validate(
            bound,
            limits: structuralLimits
        )
        return ValidatedDatabaseStatement(
            statement: bound,
            structuralLimits: structuralLimits
        )
    }

    private func statement(
        from input: QueryExecuteOperation.Input
    ) throws -> QueryStatement {
        switch input {
        case .ir(let statement):
            return statement
        case .text(let language, let statement):
            switch language {
            case .sql:
                return try SQLParser(structuralLimits: structuralLimits)
                    .parse(statement)
            case .sparql:
                #if DATABASE_QUERY_OPERATIONS_GRAPH_INDEXES
                return try SPARQLParser(structuralLimits: structuralLimits)
                    .parse(statement)
                #else
                _ = statement
                throw DatabaseStatementAdmissionError.featureUnavailable(
                    "SPARQL text requires the GraphIndexes package trait"
                )
                #endif
            }
        }
    }
}
