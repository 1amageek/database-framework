import DatabaseKit

/// Selects the physical property-graph ordering with the longest bound prefix.
package enum GraphIndexScanPlanner {
    package static func ordering(
        strategy: GraphIndexStrategy,
        subjectBound: Bool,
        predicateBound: Bool,
        objectBound: Bool,
        graphBound: Bool
    ) -> GraphIndexOrdering {
        if strategy == .namedGraphStore {
            if subjectBound { return .gspo }
            if predicateBound { return .gpos }
            if objectBound { return .gosp }
            return .gspo
        }

        if graphBound {
            switch strategy {
            case .quadStore:
                if subjectBound { return .gspo }
                if predicateBound { return .gpos }
                if objectBound { return .gosp }
                return .gspo
            case .adjacency, .tripleStore, .hexastore, .namedGraphStore:
                break
            }
        }

        switch strategy {
        case .adjacency:
            return subjectBound || !objectBound ? .out : .in

        case .tripleStore, .namedGraphStore, .quadStore:
            if subjectBound { return .spo }
            if predicateBound { return .pos }
            if objectBound { return .osp }
            return .spo

        case .hexastore:
            switch (subjectBound, predicateBound, objectBound) {
            case (true, true, _):
                return .spo
            case (true, false, true):
                return .sop
            case (false, true, true):
                return .pos
            case (true, false, false):
                return .spo
            case (false, true, false):
                return .pso
            case (false, false, true):
                return .osp
            case (false, false, false):
                return .spo
            }
        }
    }
}
