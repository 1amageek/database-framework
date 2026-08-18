#if DATABASE_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit

enum CompositionSPARQLPlanValidator {
    static func validate(_ query: SelectQuery) throws {
        guard query.accessPath == nil else {
            throw CompositionQueryError.unsupportedPlan(
                "SPARQL Composition SELECT does not accept an index access path"
            )
        }
        guard query.subqueries == nil else {
            throw CompositionQueryError.unsupportedPlan(
                "SPARQL Composition SELECT does not accept WITH bindings"
            )
        }
        guard query.groupBy == nil, query.having == nil else {
            throw CompositionQueryError.unsupportedPlan(
                "SPARQL grouping requires a global federated solution planner"
            )
        }
        guard query.reduced == false else {
            throw CompositionQueryError.unsupportedPlan(
                "SPARQL REDUCED semantics are not advertised for Composition targets"
            )
        }
        switch query.source {
        case .graphPattern(let pattern):
            try validateBaseLocal(pattern, statement: "SELECT")
        case .namedGraph(_, let pattern):
            try validateBaseLocal(pattern, statement: "SELECT")
        case .service:
            throw CompositionQueryError.unsupportedPlan(
                "SPARQL SERVICE is not a Base-local Composition source"
            )
        default:
            throw CompositionQueryError.unsupportedPlan(
                "SPARQL Composition SELECT requires a Base-local graph pattern"
            )
        }
    }

    static func validate(_ statement: CompositionRDFStatement) throws {
        switch statement {
        case .construct(let query):
            guard query.modifiers == .none else {
                throw CompositionQueryError.unsupportedPlan(
                    "CONSTRUCT solution modifiers require a global federated solution planner"
                )
            }
            try validateBaseLocal(query.pattern, statement: "CONSTRUCT")
        case .describe(let query):
            guard query.modifiers == .none else {
                throw CompositionQueryError.unsupportedPlan(
                    "DESCRIBE solution modifiers require a global federated solution planner"
                )
            }
            if let pattern = query.pattern {
                try validateBaseLocal(pattern, statement: "DESCRIBE")
            }
        }
    }

    static func validate(_ query: AskQuery) throws {
        guard query.modifiers == .none else {
            throw CompositionQueryError.unsupportedPlan(
                "ASK solution modifiers require a global federated solution planner"
            )
        }
        try validateBaseLocal(query.pattern, statement: "ASK")
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
            throw CompositionQueryError.unsupportedPlan(
                "SPARQL SERVICE is not a Base-local Composition source"
            )
        case .subquery, .groupBy:
            throw CompositionQueryError.unsupportedPlan(
                "\(statement) subqueries and grouping require a global federated solution planner"
            )
        }
    }
}
#endif
