import DatabaseKit

/// Construction-order provenance avoids matching equal semantic values after parsing.
struct SQLSourceCaptureCollector {
    enum CaptureError: Error {
        case inconsistentConstruction
        case identifierOverflow
    }

    private enum Kind { case expression, literal, query, source, dataType, statement, valuesRow, grouping, groupingComponent, groupingSet, windowFunction, windowReference, namedWindow, mutationTarget, assignment, conflictTarget, conflictAction, returningAliases, namedStatement, mergeClause, mergeAction, ddlColumn, ddlConstraint, ddlAction, ddlIndexKey }
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
        let annotation: SQLSourceAnnotation?
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

    mutating func unwrapLastQuery() throws {
        guard let node = pending.popLast(), nodes[node].kind == .query else { throw CaptureError.inconsistentConstruction }
        for edge in nodes[node].edges { pending.append(edges[edge].child) }
    }

    mutating func unwrapLastExpression() throws -> SQLSourceAnnotation? {
        guard let node = pending.popLast(), nodes[node].kind == .expression else { throw CaptureError.inconsistentConstruction }
        for edge in nodes[node].edges { pending.append(edges[edge].child) }
        return nodes[node].annotation
    }

    mutating func convertLastExpressionToLiteral() throws {
        guard let index = pending.last, nodes[index].kind == .expression,
              nodes[index].edges.isEmpty else { throw CaptureError.inconsistentConstruction }
        let node = nodes[index]
        nodes[index] = Node(kind: .literal, annotation: node.annotation, edges: node.edges)
    }

    mutating func movePendingToEnd(from start: Int, count: Int) throws {
        guard start >= 0, count >= 0, start <= pending.count, count <= pending.count - start else {
            throw CaptureError.inconsistentConstruction
        }
        let middle = start + count
        for range in [start..<middle, middle..<pending.count, start..<pending.count] {
            var lower = range.lowerBound
            var upper = range.upperBound
            while lower < upper {
                upper -= 1
                if lower < upper { pending.swapAt(lower, upper) }
                lower += 1
            }
        }
    }

    mutating func swapLastPending() throws {
        guard pending.count >= 2 else { throw CaptureError.inconsistentConstruction }
        pending.swapAt(pending.count - 1, pending.count - 2)
    }

    mutating func recordSyntheticProjection(ledger: inout QueryStructuralResourceLedger) throws {
        try ledger.consume(.totalNodes)
        try ledger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Node>.stride + MemoryLayout<Int>.stride))
        let index = nodes.count
        nodes.append(Node(kind: .expression, annotation: nil, edges: edges.count..<edges.count))
        pending.append(index)
    }

    mutating func recordValuesRow(count: Int, start: Int?, end: Int,
                                  ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        for index in 0..<count { try child(.valuesCell, index: index, into: &children, ledger: &ledger) }
        try recordNode(kind: .valuesRow, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func discardPending(after checkpoint: Int) throws {
        guard checkpoint >= 0, checkpoint <= pending.count else { throw CaptureError.inconsistentConstruction }
        pending.removeLast(pending.count - checkpoint)
    }

    mutating func discardLastExpression() -> SQLSourceRange? {
        guard let index = pending.lastIndex(where: { nodes[$0].kind == .expression }) else { return nil }
        let node = pending.remove(at: index)
        return nodes[node].annotation?.range
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
        start: Int, end: Int, identifierEnd: Int?,
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
        case .defaultValue, .literal, .currentValue, .column, .variable, .parameter, .bound, .wholeRow: break
        case .collate, .field, .fieldExpansion:
            try child(.operand, index: 0, into: &children, ledger: &ledger)
        case .add, .subtract, .multiply, .divide, .modulo, .equal, .notEqual,
             .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual, .and, .or, .nullIf, .isDistinctFrom, .concatenate:
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            try child(.operand, index: 1, into: &children, ledger: &ledger)
        case .collectionOperation:
            try child(.left, into: &children, ledger: &ledger)
            try child(.right, into: &children, ledger: &ledger)
        case .arrayAccess(_, let subscripts):
            try child(.operand, into: &children, ledger: &ledger)
            for (dimension, subscriptValue) in subscripts.enumerated() {
                switch subscriptValue {
                case .index:
                    try child(.argument, index: 2 * dimension, into: &children, ledger: &ledger)
                case .slice(let lower, let upper):
                    if lower != nil { try child(.argument, index: 2 * dimension, into: &children, ledger: &ledger) }
                    if upper != nil { try child(.argument, index: 2 * dimension + 1, into: &children, ledger: &ledger) }
                }
            }
        case .negate, .not, .isNull, .isNotNull, .truthTest, .regex, .isTriple, .subject, .predicate, .object:
            try child(.operand, index: 0, into: &children, ledger: &ledger)
        case .patternMatch(_, _, let escape, _):
            try child(.operand, index: 0, into: &children, ledger: &ledger)
            try child(.argument, index: 0, into: &children, ledger: &ledger)
            if escape != nil { try child(.argument, index: 1, into: &children, ledger: &ledger) }
        case .between, .triple:
            for index in 0..<3 { try child(.operand, index: index, into: &children, ledger: &ledger) }
        case .array(let values), .row(let values), .coalesce(let values), .greatest(let values), .least(let values), .grouping(let values):
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
        case .subquery, .exists, .arraySubquery:
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
        case .window:
            try child(.windowFunction, kind: .windowFunction, into: &children, ledger: &ledger)
            try child(.windowReference, kind: .windowReference, into: &children, ledger: &ledger)
        case .aggregate(let aggregate):
            switch aggregate {
            case .sql(let call):
                try aggregateChildren(call, into: &children, ledger: &ledger)
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
        for (index, cte) in (query.subqueries ?? []).enumerated() {
            try child(.cte, index: index, kind: .query, into: &children, ledger: &ledger)
            if cte.cycle?.markValue != nil { try child(.cteCycleMark, index: index, kind: .literal, into: &children, ledger: &ledger) }
            if cte.cycle?.defaultValue != nil { try child(.cteCycleDefault, index: index, kind: .literal, into: &children, ledger: &ledger) }
        }
        for index in query.projection.indices {
            try child(.projection, index: index, into: &children, ledger: &ledger)
        }
        try child(.source, kind: .source, into: &children, ledger: &ledger)
        if query.filter != nil { try child(.filter, into: &children, ledger: &ledger) }
        for index in (query.groupBy ?? []).indices { try child(.groupBy, index: index, kind: .grouping, into: &children, ledger: &ledger) }
        if query.having != nil { try child(.having, into: &children, ledger: &ledger) }
        for index in query.windows.indices { try child(.namedWindow, index: index, kind: .namedWindow, into: &children, ledger: &ledger) }
        if case .distinctOn(let keys) = query.quantifier {
            for index in keys.indices { try child(.distinctOn, index: index, into: &children, ledger: &ledger) }
        }
        for index in (query.orderBy ?? []).indices { try child(.orderBy, index: index, into: &children, ledger: &ledger) }
        if case .expression = query.pagination.offset { try child(.offset, into: &children, ledger: &ledger) }
        if case .count(.expression, _) = query.pagination.limit { try child(.limit, into: &children, ledger: &ledger) }
        try recordNode(kind: .query, children: children, start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
    }

    mutating func record(
        _ source: DataSource, start: Int? = nil, end: Int, identifierEnd: Int? = nil,
        ledger: inout QueryStructuralResourceLedger
    ) throws {
        var children: [Child] = []
        switch source {
        case .tableFunction(let source):
            for index in source.function.arguments.indices { try child(.argument, index: index, into: &children, ledger: &ledger) }
        case .subquery:
            try child(.subquery, kind: .query, into: &children, ledger: &ledger)
        case .aliased:
            try child(.source, kind: .source, into: &children, ledger: &ledger)
        case .setOperation:
            try child(.setLeft, kind: .query, into: &children, ledger: &ledger)
            try child(.setRight, kind: .query, into: &children, ledger: &ledger)
        case .values(let rows):
            for index in rows.indices { try child(.valuesRow, index: index, kind: .valuesRow, into: &children, ledger: &ledger) }
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

    private mutating func aggregateChildren(_ call: SQLAggregateCall, into children: inout [Child],
                                   ledger: inout QueryStructuralResourceLedger) throws {
        for index in call.arguments.indices { try child(.aggregateArgument, index: index, into: &children, ledger: &ledger) }
        for index in call.orderBy.indices { try child(.aggregateOrderBy, index: index, into: &children, ledger: &ledger) }
        if call.filter != nil, let withinGroup = call.withinGroup {
            try movePendingToEnd(from: pending.count - withinGroup.count - 1, count: withinGroup.count)
        }
        if call.filter != nil { try child(.aggregateFilter, into: &children, ledger: &ledger) }
        for index in (call.withinGroup ?? []).indices { try child(.aggregateWithinGroup, index: index, into: &children, ledger: &ledger) }
    }

    mutating func unwrapLastRow(count: Int) throws {
        guard let row = pending.popLast(), nodes[row].kind == .expression,
              nodes[row].edges.count == count else { throw CaptureError.inconsistentConstruction }
        for edgeIndex in nodes[row].edges { pending.append(edges[edgeIndex].child) }
    }

    mutating func recordExpressionGroupingSet(count: Int, start: Int, end: Int,
                                             ledger: inout QueryStructuralResourceLedger) throws {
        guard pending.count >= count else { throw CaptureError.inconsistentConstruction }
        let base = pending.count - count
        for _ in 0..<count {
            try movePendingToEnd(from: base, count: 1)
            guard let expression = pending.last, nodes[expression].kind == .expression else {
                throw CaptureError.inconsistentConstruction
            }
            var children: [Child] = []
            try child(.operand, into: &children, ledger: &ledger)
            let range = nodes[expression].annotation?.range
            try recordNode(kind: .grouping, children: children, start: range?.start,
                end: range?.end ?? end, identifierEnd: nil, ledger: &ledger)
        }
        try recordGroupingSet(count: count, start: start, end: end, ledger: &ledger)
    }

    mutating func recordGroupingComponent(count: Int, start: Int, end: Int,
                                         ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        for index in 0..<count { try child(.argument, index: index, into: &children, ledger: &ledger) }
        try recordNode(kind: .groupingComponent, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordGroupingSet(count: Int, start: Int, end: Int,
                                   ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        for index in 0..<count { try child(.groupingElement, index: index, kind: .grouping, into: &children, ledger: &ledger) }
        try recordNode(kind: .groupingSet, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordGrouping(_ element: GroupingElement, start: Int, end: Int,
                                ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        switch element {
        case .expression: try child(.operand, into: &children, ledger: &ledger)
        case .rollup(let components), .cube(let components):
            for index in components.indices { try child(.groupingComponent, index: index, kind: .groupingComponent, into: &children, ledger: &ledger) }
        case .groupingSets(let sets):
            for index in sets.indices { try child(.groupingSet, index: index, kind: .groupingSet, into: &children, ledger: &ledger) }
        }
        try recordNode(kind: .grouping, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordWindowFunction(_ function: WindowFunction, start: Int, end: Int, identifierEnd: Int,
                                      ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        switch function {
        case .aggregate(let call): try aggregateChildren(call, into: &children, ledger: &ledger)
        case .analytic(_, let arguments):
            for index in arguments.indices { try child(.argument, index: index, into: &children, ledger: &ledger) }
        }
        try recordNode(kind: .windowFunction, children: children, start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
    }

    mutating func recordNamedWindowReference(start: Int, end: Int,
                                            ledger: inout QueryStructuralResourceLedger) throws {
        try recordNode(kind: .windowReference, children: [], start: start, end: end, identifierEnd: end, ledger: &ledger)
    }

    mutating func recordWindowSpecification(_ specification: WindowSpecification, start: Int, end: Int,
                                           ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        for index in specification.partitionBy.indices { try child(.windowPartitionBy, index: index, into: &children, ledger: &ledger) }
        for index in specification.orderBy.indices { try child(.windowOrderBy, index: index, into: &children, ledger: &ledger) }
        if let frame = specification.frame {
            switch frame.start {
            case .preceding, .following: try child(.windowFrameStart, into: &children, ledger: &ledger)
            default: break
            }
            switch frame.end {
            case .preceding, .following: try child(.windowFrameEnd, into: &children, ledger: &ledger)
            default: break
            }
        }
        try recordNode(kind: .windowReference, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordNamedWindow(start: Int, end: Int, identifierEnd: Int,
                                   ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        try child(.windowReference, kind: .windowReference, into: &children, ledger: &ledger)
        try recordNode(kind: .namedWindow, children: children, start: start, end: end, identifierEnd: identifierEnd, ledger: &ledger)
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
            if let range = node.annotation?.range { rangeStart = min(rangeStart, range.start) }
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
        let synthetic = start == nil && edges[edgeStart...].allSatisfy { nodes[$0.child].annotation == nil }
        let annotation = synthetic ? nil : SQLSourceAnnotation(range: range, identifierRange: identifierRange)
        nodes.append(Node(kind: kind, annotation: annotation, edges: edgeStart..<edges.count))
        pending.append(index)
    }

    /// Projects the constructed tree once into a shared prefix arena in linear space.
    mutating func finishStatement(
        index: UInt32? = nil, statement: QueryStatement? = nil, start: Int? = nil, end: Int? = nil,
        ledger: inout QueryStructuralResourceLedger
    ) throws -> QueryOccurrenceID {
        if let statement {
            switch statement {
            case .select, .insert, .update, .delete, .with, .merge, .truncate, .ddl: break
            default:
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
        struct Frame { let node: Int; let parent: QueryOccurrenceID; var next: Int }
        guard !nodes[node].edges.isEmpty else { return }
        var frames: [Frame] = []
        do {
            try ledger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Frame>.stride))
            try ledger.enterNesting()
            frames.append(Frame(node: node, parent: parent, next: nodes[node].edges.lowerBound))
            while let frame = frames.last {
                if frame.next == nodes[frame.node].edges.upperBound {
                    frames.removeLast()
                    try ledger.leaveNesting()
                    continue
                }
                let edge = edges[frame.next]
                frames[frames.count - 1].next += 1
                let occurrence = try appendOccurrence(node: edge.child, parent: frame.parent,
                    field: edge.field, index: edge.index, ledger: &ledger)
                if !nodes[edge.child].edges.isEmpty {
                    try ledger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Frame>.stride))
                    try ledger.enterNesting()
                    frames.append(Frame(node: edge.child, parent: occurrence, next: nodes[edge.child].edges.lowerBound))
                }
            }
        } catch {
            for _ in frames { try ledger.leaveNesting() }
            throw error
        }
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
        if let annotation = nodes[node].annotation { annotations[occurrence] = annotation }
        return occurrence
    }
}

extension SQLSourceCaptureCollector {
    mutating func normalizeSelectDefinitions(count: Int, ledger: inout QueryStructuralResourceLedger) throws {
        guard count >= 0, pending.count >= count else { throw CaptureError.inconsistentConstruction }
        let base = pending.count - count
        let originals = pending[base...]
        var replacements: [Int] = []
        for node in originals {
            guard nodes[node].kind == .namedStatement else { throw CaptureError.inconsistentConstruction }
            for edge in nodes[node].edges {
                try ledger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Int>.stride))
                replacements.append(edges[edge].child)
            }
        }
        pending.removeLast(count)
        pending.append(contentsOf: replacements)
    }

    mutating func recordMutationTarget(start: Int, end: Int, ledger: inout QueryStructuralResourceLedger) throws {
        try recordNode(kind: .mutationTarget, children: [], start: start, end: end, identifierEnd: end, ledger: &ledger)
    }

    mutating func recordAssignment(_ assignment: Assignment, start: Int, end: Int,
                                   ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        try child(.assignmentValue, into: &children, ledger: &ledger)
        try recordNode(kind: .assignment, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordInsertSource(_ source: InsertSource, end: Int,
                                     ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        switch source {
        case .values(let rows):
            for index in rows.indices { try child(.valuesRow, index: index, kind: .valuesRow, into: &children, ledger: &ledger) }
        case .select: try child(.subquery, kind: .query, into: &children, ledger: &ledger)
        case .defaultValues: break
        }
        try recordNode(kind: .source, children: children, start: nil, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordConflictTarget(_ target: SQLConflictTarget, start: Int, end: Int,
                                       ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        if case .columns(_, .some) = target { try child(.conflictPredicate, into: &children, ledger: &ledger) }
        try recordNode(kind: .conflictTarget, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    private func assignmentChildren(_ assignments: [Assignment], into children: inout [Child],
                                    ledger: inout QueryStructuralResourceLedger) throws {
        for index in assignments.indices { try child(.assignment, index: index, kind: .assignment, into: &children, ledger: &ledger) }
    }

    mutating func recordConflictAction(_ action: OnConflictAction, start: Int, end: Int,
                                       ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        if case .doUpdate(let assignments, let predicate) = action {
            try assignmentChildren(assignments, into: &children, ledger: &ledger)
            if predicate != nil { try child(.filter, into: &children, ledger: &ledger) }
        }
        try recordNode(kind: .conflictAction, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordReturningAliases(start: Int, end: Int, ledger: inout QueryStructuralResourceLedger) throws {
        try recordNode(kind: .returningAliases, children: [], start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    private func returningChildren(_ returning: [ProjectionItem]?, aliases: SQLReturningAliases?,
                                   into children: inout [Child], ledger: inout QueryStructuralResourceLedger) throws {
        if aliases != nil { try child(.returningAlias, kind: .returningAliases, into: &children, ledger: &ledger) }
        for index in (returning ?? []).indices { try child(.returning, index: index, into: &children, ledger: &ledger) }
    }

    mutating func recordNamedStatement(_ value: SQLNamedStatement, start: Int, end: Int,
                                       ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        let kind: Kind
        if case .select = value.statement { kind = .query } else { kind = .statement }
        try child(.cteStatement, kind: kind, into: &children, ledger: &ledger)
        if value.cycle?.markValue != nil { try child(.cteCycleMark, kind: .literal, into: &children, ledger: &ledger) }
        if value.cycle?.defaultValue != nil { try child(.cteCycleDefault, kind: .literal, into: &children, ledger: &ledger) }
        try recordNode(kind: .namedStatement, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordMergeAction(_ value: SQLMergeAction, start: Int, end: Int,
                                    ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        switch value {
        case .insert(_, let values, _):
            for index in (values ?? []).indices { try child(.valuesCell, index: index, into: &children, ledger: &ledger) }
        case .update(let assignments): try assignmentChildren(assignments, into: &children, ledger: &ledger)
        case .delete, .doNothing: break
        }
        try recordNode(kind: .mergeAction, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordMergeClause(_ value: SQLMergeClause, start: Int, end: Int,
                                    ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        if value.condition != nil { try child(.filter, into: &children, ledger: &ledger) }
        try child(.mergeAction, kind: .mergeAction, into: &children, ledger: &ledger)
        try recordNode(kind: .mergeClause, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordMutation(_ statement: QueryStatement, start: Int, end: Int,
                                 ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        switch statement {
        case .insert(let value):
            try child(.mutationTarget, kind: .mutationTarget, into: &children, ledger: &ledger)
            try child(.source, kind: .source, into: &children, ledger: &ledger)
            if value.conflictTarget != nil { try child(.conflictTarget, kind: .conflictTarget, into: &children, ledger: &ledger) }
            if value.onConflict != nil { try child(.conflictAction, kind: .conflictAction, into: &children, ledger: &ledger) }
            try returningChildren(value.returning, aliases: value.returningAliases, into: &children, ledger: &ledger)
        case .update(let value):
            try child(.mutationTarget, kind: .mutationTarget, into: &children, ledger: &ledger)
            try assignmentChildren(value.assignments, into: &children, ledger: &ledger)
            if value.from != nil { try child(.source, kind: .source, into: &children, ledger: &ledger) }
            if value.filter != nil { try child(.filter, into: &children, ledger: &ledger) }
            try returningChildren(value.returning, aliases: value.returningAliases, into: &children, ledger: &ledger)
        case .delete(let value):
            try child(.mutationTarget, kind: .mutationTarget, into: &children, ledger: &ledger)
            if value.using != nil { try child(.source, kind: .source, into: &children, ledger: &ledger) }
            if value.filter != nil { try child(.filter, into: &children, ledger: &ledger) }
            try returningChildren(value.returning, aliases: value.returningAliases, into: &children, ledger: &ledger)
        case .with(let value):
            for index in value.statements.indices { try child(.cte, index: index, kind: .namedStatement, into: &children, ledger: &ledger) }
            let kind: Kind
            if case .select = value.body { kind = .query } else { kind = .statement }
            try child(.withBody, kind: kind, into: &children, ledger: &ledger)
        case .merge(let value):
            try child(.mutationTarget, kind: .mutationTarget, into: &children, ledger: &ledger)
            try child(.source, kind: .source, into: &children, ledger: &ledger)
            try child(.mergeCondition, into: &children, ledger: &ledger)
            for index in value.clauses.indices { try child(.mergeClause, index: index, kind: .mergeClause, into: &children, ledger: &ledger) }
            try returningChildren(value.returning, aliases: value.returningAliases, into: &children, ledger: &ledger)
        case .truncate(let value):
            for index in value.tables.indices { try child(.truncateRelation, index: index, kind: .mutationTarget, into: &children, ledger: &ledger) }
        default: throw CaptureError.inconsistentConstruction
        }
        try recordNode(kind: .statement, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }
}

extension SQLSourceCaptureCollector {
    mutating func recordDDLColumn(_ value: SQLColumnDefinition, checkpoint: Int, start: Int, end: Int,
                                  ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        var constraint = 0
        var type = 0
        for index in checkpoint..<pending.count {
            switch nodes[pending[index]].kind {
            case .dataType:
                try child(type == 0 ? .castType : .elementType, kind: .dataType, into: &children, ledger: &ledger)
                type += 1
            case .expression:
                let field: QueryOccurrenceField
                if case .generated = value.policy { field = .ddlGenerated } else { field = .ddlDefault }
                try child(field, into: &children, ledger: &ledger)
            case .ddlConstraint:
                try child(.ddlConstraint, index: constraint, kind: .ddlConstraint, into: &children, ledger: &ledger)
                constraint += 1
            default: throw CaptureError.inconsistentConstruction
            }
        }
        try recordNode(kind: .ddlColumn, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordDDLConstraint(_ value: SQLTableConstraint, checkpoint: Int, start: Int, end: Int,
                                      ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        for index in checkpoint..<pending.count {
            switch nodes[pending[index]].kind {
            case .expression: try child(.ddlCheck, into: &children, ledger: &ledger)
            case .mutationTarget: try child(.source, kind: .mutationTarget, into: &children, ledger: &ledger)
            default: throw CaptureError.inconsistentConstruction
            }
        }
        try recordNode(kind: .ddlConstraint, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordDDLAction(_ value: SQLAlterTableAction, checkpoint: Int, start: Int, end: Int,
                                 ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        for index in checkpoint..<pending.count {
            switch nodes[pending[index]].kind {
            case .ddlColumn: try child(.ddlColumn, kind: .ddlColumn, into: &children, ledger: &ledger)
            case .ddlConstraint: try child(.ddlConstraint, kind: .ddlConstraint, into: &children, ledger: &ledger)
            case .dataType: try child(.castType, kind: .dataType, into: &children, ledger: &ledger)
            case .expression:
                let field: QueryOccurrenceField
                if case .setDefault = value { field = .ddlDefault } else { field = .ddlUsing }
                try child(field, into: &children, ledger: &ledger)
            default: throw CaptureError.inconsistentConstruction
            }
        }
        try recordNode(kind: .ddlAction, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }

    mutating func recordDDLIndexKeys(count: Int, ledger: inout QueryStructuralResourceLedger) throws {
        guard count >= 0, count <= pending.count else { throw CaptureError.inconsistentConstruction }
        for index in (pending.count - count)..<pending.count {
            let childNode = pending[index]
            guard nodes[childNode].kind == .expression else { throw CaptureError.inconsistentConstruction }
            try ledger.consume(.totalNodes)
            try ledger.consume(.collectionElements)
            try ledger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Node>.stride + MemoryLayout<Edge>.stride))
            let edgeStart = edges.count
            edges.append(Edge(field: .operand, index: nil, child: childNode))
            let node = nodes.count
            nodes.append(Node(kind: .ddlIndexKey, annotation: nodes[childNode].annotation, edges: edgeStart..<edges.count))
            pending[index] = node
        }
    }

    mutating func recordDDL(_ value: SQLDDLStatement, start: Int, end: Int,
                           ledger: inout QueryStructuralResourceLedger) throws {
        var children: [Child] = []
        var column = 0, constraint = 0, action = 0, key = 0, target = 0
        let multipleTargets: Bool
        switch value {
        case .dropSchema, .dropTable, .dropIndex, .dropView, .dropMaterializedView, .dropSequence: multipleTargets = true
        default: multipleTargets = false
        }
        for index in pending.indices {
            switch nodes[pending[index]].kind {
            case .mutationTarget:
                if case .createIndex = value, target == 1 {
                    try child(.source, kind: .mutationTarget, into: &children, ledger: &ledger)
                } else {
                    try child(.ddlTarget, index: multipleTargets ? target : nil, kind: .mutationTarget, into: &children, ledger: &ledger)
                }
                target += 1
            case .ddlColumn:
                try child(.ddlColumn, index: column, kind: .ddlColumn, into: &children, ledger: &ledger); column += 1
            case .ddlConstraint:
                try child(.ddlConstraint, index: constraint, kind: .ddlConstraint, into: &children, ledger: &ledger); constraint += 1
            case .ddlAction:
                try child(.ddlAction, index: action, kind: .ddlAction, into: &children, ledger: &ledger); action += 1
            case .ddlIndexKey:
                try child(.ddlIndexKey, index: key, kind: .ddlIndexKey, into: &children, ledger: &ledger); key += 1
            case .query: try child(.ddlQuery, kind: .query, into: &children, ledger: &ledger)
            case .expression: try child(.ddlIndexPredicate, into: &children, ledger: &ledger)
            case .dataType: try child(.castType, kind: .dataType, into: &children, ledger: &ledger)
            default: throw CaptureError.inconsistentConstruction
            }
        }
        try recordNode(kind: .statement, children: children, start: start, end: end, identifierEnd: nil, ledger: &ledger)
    }
}
