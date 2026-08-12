import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_MULTIPLE_BASES
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import DatabaseKit

/// Admits only SPARQL algebra that can be evaluated independently in every
/// Base and then combined without weakening the statement's meaning.
package enum DatabaseCompositionSPARQLPlanValidator {
    package static func validate(_ query: AskQuery) throws {
        guard query.modifiers == .none else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "ASK solution modifiers require a global federated solution planner"
            )
        }
        try validateBaseLocal(query.pattern, statement: "ASK")
    }

    package static func validate(_ query: ConstructQuery) throws {
        guard query.modifiers == .none else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "CONSTRUCT solution modifiers require a global federated solution planner"
            )
        }
        try validateBaseLocal(query.pattern, statement: "CONSTRUCT")
    }

    package static func validate(_ query: DescribeQuery) throws {
        guard query.modifiers == .none else {
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "DESCRIBE solution modifiers require a global federated solution planner"
            )
        }
        if let pattern = query.pattern {
            try validateBaseLocal(pattern, statement: "DESCRIBE")
        }
    }

    private static func validateBaseLocal(
        _ pattern: GraphPattern,
        statement: String
    ) throws {
        switch pattern {
        case .basic, .values:
            return
        case .join(let left, let right),
             .optional(let left, let right),
             .union(let left, let right),
             .minus(let left, let right),
             .lateral(let left, let right):
            try validateBaseLocal(left, statement: statement)
            try validateBaseLocal(right, statement: statement)
        case .filter(let inner, _),
             .graph(_, let inner),
             .bind(let inner, _, _):
            try validateBaseLocal(inner, statement: statement)
        case .service:
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "SPARQL SERVICE is not a Base-local Composition source"
            )
        case .subquery, .groupBy:
            throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                "\(statement) subqueries and grouping require a global federated solution planner"
            )
        }
    }
}
#endif

#endif
