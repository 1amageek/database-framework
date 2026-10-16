import DatabaseKit

/// Construction-order provenance avoids matching equal semantic values after parsing.
struct SQLSourceCaptureCollector {
    enum CaptureError: Error {
        case inconsistentConstruction
        case identifierOverflow
    }

    private enum Kind { case expression, query, source, dataType, statement }
    private struct Child {
        let field: QueryOccurrenceField
        let index: UInt32?
        let kind: Kind
    }
    private struct Edge {
        let field: QueryOccurrenceField
        let index: UInt32?
        let child: Int
    }
    private struct Node {
        let kind: Kind
        let annotation: SQLSourceAnnotation
        let edges: Range<Int>
    }

    private var nodes: [Node] = []
    private var edges: [Edge] = []
    private var pending: [Int] = []
    private var occurrences: QueryOccurrenceArena
    private var annotations: [QueryOccurrenceID: SQLSourceAnnotation] = [:]

    init(limits: QueryStructuralLimits) {
        occurrences = QueryOccurrenceArena(limits: limits)
    }

    var snapshot: SQLSourceCaptureSnapshot {
        SQLSourceCaptureSnapshot(occurrences: occurrences, sourceMap: SQLSourceMap(annotations: annotations))
    }

    var pendingCheckpoint: Int { pending.count }

    mutating func discardPending(after checkpoint: Int) throws {
        guard checkpoint >= 0, checkpoint <= pending.count else { throw CaptureError.inconsistentConstruction }
        pending.removeLast(pending.count - checkpoint)
    }

    mutating func discardLastExpression() -> SQLSourceRange? {
        guard let index = pending.lastIndex(where: { nodes[$0].kind == .expression }) else { return nil }
        let node = pending.remove(at: index)
        return nodes[node].annotation.range
    }

    /// Reorders normalized function arguments without changing their source ranges.
    mutating func reorderLastExpressions(_ order: [Int]) throws {
        guard pending.count >= order.count, Set(order) == Set(order.indices) else {
            throw CaptureError.inconsistentConstruction
        }
        let base = pending.count - order.count
        let original = Array(pending[base...])
        guard original.allSatisfy({ nodes[$0].kind == .expression }) else {
            throw CaptureError.inconsistentConstruction
        }
        for index in order.indices { pending[base + index] = original[order[index]] }
    }

    mutating func discardLastType() {
        guard let index = pending.lastIndex(where: { nodes[$0].kind == .dataType }) else { return }
        pending.remove(at: index)
    }

    private func child(
        _ field: QueryOccurrenceField, index: Int? = nil, kind: Kind = .expression,
        into children: inout [Child], ledger: inout QueryStructuralResourceLedger
    ) throws {
        let ordinal: UInt32?
        if let index {
            guard let value = UInt32(exactly: index) else { throw CaptureError.identifierOverflow }
            ordinal = value
        } else { ordinal = nil }
        try ledger.consume(.collectionElements)
        try ledger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Child>.stride + MemoryLayout<Edge>.stride))
        children.append(Child(field: field, index: ordinal, kind: kind))
    }

    mutating func recordProjectionQualifier(
        start: Int, end: Int, identifierEnd: Int,
        ledger: inout QueryStructuralResourceLedger
    ) throws {
        try recordNode(kind: .expression, children: [], start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
    }

    mutating func record(
        _ expression: Expression, start: Int? = nil, end: Int, identifierEnd: Int? = nil,
        ledger: inout QueryStructuralResourceLedger
    ) throws {
        var children: [Child] = []
        switch expression {
        case .literal, .currentValue, .column, .variable, .parameter, .bound: break
        case .add, .subtract, .multiply, .divide, .modulo, .equal, .notEqual,
             .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual, .and, .or, .nullIf, .isDistinctFrom, .concatenate:
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            try child(.operand, index: 1, into: &children, ledger: &ledger)
        case .negate, .not, .isNull, .isNotNull, .truthTest, .regex, .isTriple, .subject, .predicate, .object:
            try child(.operand, index: 0, into: &children, ledger: &ledger)
        case .patternMatch(_, _, let escape, _):
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            try child(.argument, index: 0, into: &children, ledger: &ledger)
            if escape != nil { try child(.argument, index: 1, into: &children, ledger: &ledger) }
        case .between, .triple:
            for index in 0..<3 { try child(.operand, index: index, into: &children, ledger: &ledger) }
        case .array(let values), .row(let values), .coalesce(let values), .greatest(let values), .least(let values):
            for index in values.indices { try child(.argument, index: index, into: &children, ledger: &ledger) }
        case .inList(_, let values), .notInList(_, let values):
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            for index in values.indices { try child(.argument, index: index, into: &children, ledger: &ledger) }
        case .quantifiedComparison(_, _, _, let source):
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            switch source {
            case .array: try child(.operand, index: 1, into: &children, ledger: &ledger)
            case .subquery: try child(.subquery, kind: .query, into: &children, ledger: &ledger)
            }
        case .inSubquery:
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            try child(.subquery, kind: .query, into: &children, ledger: &ledger)
        case .subquery, .exists:
            try child(.subquery, kind: .query, into: &children, ledger: &ledger)
        case .function(let function):
            for index in function.arguments.indices { try child(.argument, index: index, into: &children, ledger: &ledger) }
        case .caseWhen(let operand, let cases, let elseResult):
            if operand != nil { try child(.operand, index: 0, into: &children, ledger: &ledger) }
            for index in cases.indices {
                try child(.caseCondition, index: index, into: &children, ledger: &ledger)
                try child(.caseResult, index: index, into: &children, ledger: &ledger)
            }
            if elseResult != nil { try child(.caseElse, into: &children, ledger: &ledger) }
        case .cast:
            // Prefix typed literals construct the type before their operand;
            // CAST and postfix casts construct the operand first.
            if pending.count >= 2,
               nodes[pending[pending.count - 2]].kind == .dataType,
               nodes[pending[pending.count - 1]].kind == .expression {
                pending.swapAt(pending.count - 2, pending.count - 1)
            }
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            try child(.castType, kind: .dataType, into: &children, ledger: &ledger)
        case .aggregate(let aggregate):
            switch aggregate {
            case .count(let value, _):
                if value != nil { try child(.aggregateArgument, into: &children, ledger: &ledger) }
            case .arrayAgg(_, let orderBy, _):
                try child(.aggregateArgument, into: &children, ledger: &ledger)
                for index in (orderBy ?? []).indices { try child(.aggregateOrderBy, index: index, into: &children, ledger: &ledger) }
            default:
                try child(.aggregateArgument, into: &children, ledger: &ledger)
            }
        }
        try recordNode(kind: .expression, children: children, start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
    }

    mutating func record(
        _ query: SelectQuery, start: Int? = nil, end: Int, identifierEnd: Int? = nil,
        ledger: inout QueryStructuralResourceLedger
    ) throws {
        var children: [Child] = []
        for index in (query.subqueries ?? []).indices { try child(.cte, index: index, kind: .query, into: &children, ledger: &ledger) }
        switch query.projection {
        case .items(let items), .distinctItems(let items):
            for index in items.indices { try child(.projection, index: index, into: &children, ledger: &ledger) }
        case .allFrom:
            try child(.projection, index: 0, into: &children, ledger: &ledger)
        case .all: break
        }
        try child(.source, kind: .source, into: &children, ledger: &ledger)
        if query.filter != nil { try child(.filter, into: &children, ledger: &ledger) }
        for index in (query.groupBy ?? []).indices { try child(.groupBy, index: index, into: &children, ledger: &ledger) }
        if query.having != nil { try child(.having, into: &children, ledger: &ledger) }
        for index in (query.orderBy ?? []).indices { try child(.orderBy, index: index, into: &children, ledger: &ledger) }
        try recordNode(kind: .query, children: children, start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
    }

    mutating func record(
        _ source: DataSource, start: Int? = nil, end: Int, identifierEnd: Int? = nil,
        ledger: inout QueryStructuralResourceLedger
    ) throws {
        var children: [Child] = []
        switch source {
        case .subquery:
            try child(.subquery, kind: .query, into: &children, ledger: &ledger)
        case .join(let join):
            try child(.joinLeft, kind: .source, into: &children, ledger: &ledger)
            try child(.joinRight, kind: .source, into: &children, ledger: &ledger)
            if case .on = join.condition { try child(.joinCondition, into: &children, ledger: &ledger) }
        default: break
        }
        try recordNode(kind: .source, children: children, start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
    }

    mutating func record(
        _ type: DataType, start: Int? = nil, end: Int, identifierEnd: Int? = nil,
        ledger: inout QueryStructuralResourceLedger
    ) throws {
        var children: [Child] = []
        if case .array = type { try child(.elementType, kind: .dataType, into: &children, ledger: &ledger) }
        try recordNode(kind: .dataType, children: children, start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
    }

    private mutating func recordNode(
        kind: Kind, children: [Child], start: Int?, end: Int, identifierEnd: Int?,
        ledger: inout QueryStructuralResourceLedger
    ) throws {
        guard pending.count >= children.count else { throw CaptureError.inconsistentConstruction }
        let base = pending.count - children.count
        var rangeStart = start ?? end
        for (offset, child) in children.enumerated() {
            let node = nodes[pending[base + offset]]
            guard node.kind == child.kind else { throw CaptureError.inconsistentConstruction }
            rangeStart = min(rangeStart, node.annotation.range.start)
        }
        try ledger.consume(.totalNodes)
        try ledger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Node>.stride + MemoryLayout<Int>.stride))
        let edgeStart = edges.count
        for (offset, child) in children.enumerated() {
            edges.append(Edge(field: child.field, index: child.index, child: pending[base + offset]))
        }
        pending.removeLast(children.count)
        let range = SQLSourceRange(start: rangeStart, end: max(rangeStart, end))
        let identifierRange: SQLSourceRange?
        if let start, let identifierEnd { identifierRange = SQLSourceRange(start: start, end: identifierEnd) }
        else { identifierRange = nil }
        let index = nodes.count
        nodes.append(Node(kind: kind, annotation: SQLSourceAnnotation(range: range, identifierRange: identifierRange), edges: edgeStart..<edges.count))
        pending.append(index)
    }

    /// Projects the constructed tree once into a shared prefix arena in linear space.
    mutating func finishStatement(
        index: UInt32? = nil, statement: QueryStatement? = nil, start: Int? = nil, end: Int? = nil,
        ledger: inout QueryStructuralResourceLedger
    ) throws -> QueryOccurrenceID {
        if let statement {
            if case .select = statement {} else {
                pending.removeAll(keepingCapacity: true)
                try recordNode(kind: .statement, children: [], start: start, end: end ?? start ?? 0, identifierEnd: nil, ledger: &ledger)
            }
        }
        guard pending.count == 1, let rootNode = pending.last else { throw CaptureError.inconsistentConstruction }
        let root = try appendOccurrence(node: rootNode, parent: nil, field: .statement, index: index, ledger: &ledger)
        try appendDescendants(node: rootNode, parent: root, ledger: &ledger)
        pending.removeAll(keepingCapacity: true)
        return root
    }

    private mutating func appendDescendants(
        node: Int, parent: QueryOccurrenceID, ledger: inout QueryStructuralResourceLedger
    ) throws {
        guard !nodes[node].edges.isEmpty else { return }
        try ledger.enterNesting()
        do {
            for edgeIndex in nodes[node].edges {
                let edge = edges[edgeIndex]
                let occurrence = try appendOccurrence(node: edge.child, parent: parent, field: edge.field, index: edge.index, ledger: &ledger)
                try appendDescendants(node: edge.child, parent: occurrence, ledger: &ledger)
            }
        } catch {
            try ledger.leaveNesting()
            throw error
        }
        try ledger.leaveNesting()
    }

    private mutating func appendOccurrence(
        node: Int, parent: QueryOccurrenceID?, field: QueryOccurrenceField, index: UInt32?,
        ledger: inout QueryStructuralResourceLedger
    ) throws -> QueryOccurrenceID {
        try ledger.consume(.totalNodes)
        try ledger.consume(.collectionElements, amount: parent == nil ? 0 : 1)
        // Charge both the arena lookup and sparse source-map lookup before either grows.
        let bytes = MemoryLayout<QueryOccurrence>.stride + MemoryLayout<QueryOccurrenceID>.stride * 3
            + MemoryLayout<QueryOccurrenceField>.stride + MemoryLayout<UInt32?>.stride
            + MemoryLayout<SQLSourceAnnotation>.stride
        try ledger.consume(.retainedBytes, amount: UInt64(bytes))
        let occurrence = try occurrences.append(parent: parent, field: field, index: index)
        annotations[occurrence] = nodes[node].annotation
        return occurrence
    }
}
