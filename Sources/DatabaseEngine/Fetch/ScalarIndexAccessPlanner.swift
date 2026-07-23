import Core
import DatabaseValue

struct ScalarIndexClause: Sendable {
    let fieldName: String
    let comparison: ComparisonOperator
    let value: FieldValue
}

struct ScalarIndexAccessSelection: Sendable {
    let descriptor: IndexDescriptor
    let clauses: [ScalarIndexClause]
    let requiresPostFilter: Bool
}

enum ScalarIndexAccessPlanner {
    static func select<T: Persistable>(
        for predicate: Predicate<T>,
        descriptors: [IndexDescriptor],
        forcedIndexName: String?
    ) throws -> ScalarIndexAccessSelection? {
        let candidates = try candidates(
            descriptors: descriptors,
            forcedIndexName: forcedIndexName,
            typeName: T.persistableType
        )
        let clauses = scalarClauses(in: predicate)
        guard !clauses.isEmpty else {
            return nil
        }

        var clausesByField: [String: ScalarIndexClause] = [:]
        for clause in clauses {
            if let existing = clausesByField[clause.fieldName] {
                if clause.comparison == .equal,
                   existing.comparison != .equal {
                    clausesByField[clause.fieldName] = clause
                }
            } else {
                clausesByField[clause.fieldName] = clause
            }
        }

        for descriptor in candidates {
            guard descriptor.kindIdentifier == "scalar",
                  descriptor.fieldNames.count > 1 else {
                continue
            }
            var equalityPrefix: [ScalarIndexClause] = []
            for fieldName in descriptor.fieldNames {
                guard let clause = clausesByField[fieldName],
                      clause.comparison == .equal else {
                    break
                }
                equalityPrefix.append(clause)
            }
            if equalityPrefix.count >= 2 {
                return ScalarIndexAccessSelection(
                    descriptor: descriptor,
                    clauses: equalityPrefix,
                    requiresPostFilter: !isFullySatisfied(
                        predicate,
                        by: equalityPrefix
                    )
                )
            }
        }

        for clause in clauses where clause.comparison == .equal {
            if let descriptor = candidates.first(where: {
                $0.kindIdentifier == "scalar"
                    && $0.fieldNames.first == clause.fieldName
            }) {
                return ScalarIndexAccessSelection(
                    descriptor: descriptor,
                    clauses: [clause],
                    requiresPostFilter: !isFullySatisfied(
                        predicate,
                        by: [clause]
                    )
                )
            }
        }

        for clause in clauses {
            if let descriptor = candidates.first(where: {
                $0.kindIdentifier == "scalar"
                    && $0.fieldNames.first == clause.fieldName
            }) {
                return ScalarIndexAccessSelection(
                    descriptor: descriptor,
                    clauses: [clause],
                    requiresPostFilter: !isFullySatisfied(
                        predicate,
                        by: [clause]
                    )
                )
            }
        }

        return nil
    }

    private static func candidates(
        descriptors: [IndexDescriptor],
        forcedIndexName: String?,
        typeName: String
    ) throws -> [IndexDescriptor] {
        guard let forcedIndexName else {
            return descriptors
        }
        guard let descriptor = descriptors.first(where: {
            $0.name == forcedIndexName
        }) else {
            throw CanonicalReadError.indexHintNotFound(
                "Forced index '\(forcedIndexName)' not found on type '\(typeName)'"
            )
        }
        guard descriptor.kindIdentifier == "scalar" else {
            throw CanonicalReadError.unsupportedAccessPath(
                "Forced index '\(forcedIndexName)' has kind '\(descriptor.kindIdentifier)'; typed model queries require a scalar index"
            )
        }
        return [descriptor]
    }

    private static func scalarClauses<T: Persistable>(
        in predicate: Predicate<T>
    ) -> [ScalarIndexClause] {
        switch predicate {
        case .comparison(let comparison):
            switch comparison.op {
            case .equal, .lessThan, .lessThanOrEqual,
                 .greaterThan, .greaterThanOrEqual:
                return [
                    ScalarIndexClause(
                        fieldName: comparison.fieldName,
                        comparison: comparison.op,
                        value: comparison.value
                    )
                ]
            default:
                return []
            }

        case .and(let predicates):
            return predicates.flatMap { scalarClauses(in: $0) }

        default:
            return []
        }
    }

    private static func isFullySatisfied<T: Persistable>(
        _ predicate: Predicate<T>,
        by clauses: [ScalarIndexClause]
    ) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return clauses.contains { clause in
                clause.fieldName == comparison.fieldName
                    && clause.comparison == comparison.op
                    && clause.value == comparison.value
            }

        case .and(let predicates):
            return predicates.allSatisfy {
                isFullySatisfied($0, by: clauses)
            }

        case .true:
            return true

        case .or, .not, .false:
            return false
        }
    }
}
