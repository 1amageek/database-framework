import DatabaseKit

/// Logical Fusion nodes resolved from one bounded traversal of the query.
struct FusionResolvedQueryGraph: Sendable {
    struct Node: Sendable {
        enum Resolution: Sendable {
            case resolved(FusionResolvedPlan)
            case missingEntity(String)
        }

        let source: FusionSource
        let resolution: Resolution
    }

    let nodes: DatabaseSharedRetainedArray<Node>?
    let listAuthorizationRequirements: [
        DatabaseListReadAuthorizationRequirement
    ]
    let authorizationPlan: DatabaseFieldReadAuthorizationPlan
    let schemaGeneration: UInt64
}

/// Fully resolved Fusion plans retained after one authorization decision.
struct FusionPreparedQueryGraph: Sendable {
    struct EntryKey: Sendable, Hashable {
        let tableRef: TableRef
        static func == (left: Self, right: Self) -> Bool {
            CanonicalSourceName.equal(left.tableRef.schema, right.tableRef.schema)
                && CanonicalSourceName.equal(left.tableRef.table, right.tableRef.table)
                && CanonicalSourceName.equal(left.tableRef.alias, right.tableRef.alias)
                && left.tableRef.partitions == right.tableRef.partitions
                && left.source == right.source
                && left.listAuthorizationRequirement == right.listAuthorizationRequirement
        }
        func hash(into hasher: inout Hasher) {
            hasher.combine(tableRef.schema.map(CanonicalSourceName.init))
            hasher.combine(CanonicalSourceName(tableRef.table)); hasher.combine(tableRef.alias.map(CanonicalSourceName.init))
            hasher.combine(tableRef.partitions); hasher.combine(source); hasher.combine(listAuthorizationRequirement)
        }
        let source: FusionSource
        let listAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement
    }

    struct Entry: Sendable {
        let source: FusionSource
        let plan: FusionPreparedPlan
    }

    static let empty = FusionPreparedQueryGraph(
        entries: nil,
        entryIndices: [:],
        lookupReservation: nil,
        authorization: nil
    )

    let entries: DatabaseSharedRetainedArray<Entry>?
    private let entryIndices: [EntryKey: Int]
    private let lookupReservation: DatabaseIntermediateReservation?
    let authorization: DatabaseReadAuthorization?

    init(
        entries: DatabaseSharedRetainedArray<Entry>?,
        entryIndices: [EntryKey: Int],
        lookupReservation: DatabaseIntermediateReservation?,
        authorization: DatabaseReadAuthorization?
    ) {
        self.entries = entries
        self.entryIndices = entryIndices
        self.lookupReservation = lookupReservation
        self.authorization = authorization
    }

    func entry(
        tableRef: TableRef,
        source: FusionSource,
        listAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement,
        workMeter: DatabaseWorkMeter
    ) throws -> Entry {
        try workMeter.consume(at: .validation)
        guard let entries,
              lookupReservation != nil,
              let index = entryIndices[
                EntryKey(
                    tableRef: tableRef,
                    source: source,
                    listAuthorizationRequirement:
                        listAuthorizationRequirement
                )
              ] else {
            throw FusionExecutionError.executionContractViolation
        }
        return entries[index]
    }
}

enum FusionSelectQueryGraphWalker {
    static func containsNestedQuery(
        in expression: Expression,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        var stack = try WorkStack(
            root: .expression(expression),
            workMeter: workMeter
        )
        while let item = stack.popLast() {
            try workMeter.consume(at: .validation)
            switch item {
            case .query:
                return true
            case .source(let source, let query):
                try appendChildren(
                    of: source,
                    ownedBy: query,
                    to: &stack
                )
            case .path(let path):
                try appendChildren(of: path, to: &stack)
            case .pattern(let pattern):
                try appendChildren(of: pattern, to: &stack)
            case .expression(let nested):
                try appendChildren(of: nested, to: &stack)
            case .aggregate(let aggregate):
                try appendChildren(of: aggregate, to: &stack)
            case .grouping(let grouping):
                try appendChildren(of: grouping, to: &stack)
            }
        }
        return false
    }

    static func forEachQuery(
        in root: SelectQuery,
        workMeter: DatabaseWorkMeter,
        _ visit: (SelectQuery) throws -> Void,
        visitTable: (TableRef, SelectQuery) throws -> Void,
        visitPolymorphic: (LogicalSourceRef, SelectQuery) throws -> Void
    ) throws {
        var stack = try WorkStack(
            root: .query(root),
            workMeter: workMeter
        )
        while let item = stack.popLast() {
            try workMeter.consume(at: .validation)
            switch item {
            case .query(let query):
                try visit(query)
                try appendChildren(of: query, to: &stack)
            case .source(let source, let query):
                if case .table(let tableRef) = source {
                    let isCommonTable = try tableRef.schema == nil
                        && stack.scope?.contains(tableRef.table, workMeter: workMeter) == true
                    if !isCommonTable { try visitTable(tableRef, query) }
                } else if case .logical(let logicalSource) = source,
                    logicalSource.kindIdentifier
                        == LogicalSourceKind.polymorphic
                {
                    try visitPolymorphic(logicalSource, query)
                }
                try appendChildren(
                    of: source,
                    ownedBy: query,
                    to: &stack
                )
            case .path(let path):
                try appendChildren(of: path, to: &stack)
            case .pattern(let pattern):
                try appendChildren(of: pattern, to: &stack)
            case .expression(let expression):
                try appendChildren(of: expression, to: &stack)
            case .aggregate(let aggregate):
                try appendChildren(of: aggregate, to: &stack)
            case .grouping(let grouping):
                try appendChildren(of: grouping, to: &stack)
            }
        }
    }

    private enum WorkItem: Sendable {
        case query(SelectQuery)
        case source(DataSource, owner: SelectQuery)
        case path(PathPattern)
        case pattern(GraphPattern)
        case expression(Expression)
        case aggregate(AggregateFunction)
        case grouping(GroupingElement)
    }

    private final class CommonTableScope: Sendable {
        private struct Contents: Sendable {
            let parent: CommonTableScope?
            let definitions: [NamedSubquery]
            let visibleCount: Int
        }

        private let contents: Contents
        private let reservation: DatabaseIntermediateReservation

        init(parent: CommonTableScope?, definitions: [NamedSubquery], visibleCount: Int,
             workMeter: DatabaseWorkMeter) throws {
            let layout = try DatabaseRetainedArrayLayout.forElement(Contents.self)
            let footprint = try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                .adding(DatabaseIntermediateFootprint(bytes: layout.sharedOwnerByteCount))
                .adding(DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<DatabaseIntermediateReservation>.stride)))
            self.reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .validation)
            self.contents = Contents(parent: parent, definitions: definitions, visibleCount: visibleCount)
        }

        func contains(_ name: String, workMeter: DatabaseWorkMeter) throws -> Bool {
            var current: CommonTableScope? = self
            while let scope = current {
                for definition in scope.contents.definitions.prefix(scope.contents.visibleCount) {
                    try workMeter.consume(at: .validation)
                    if CanonicalSourceName.equal(definition.name, name) { return true }
                }
                current = scope.contents.parent
            }
            return false
        }
    }

    private struct WorkStack {
        private struct Entry {
            let item: WorkItem
            let scope: CommonTableScope?
        }

        private var values: [Entry] = []
        private let reservation: DatabaseIntermediateReservation
        private let layout: DatabaseRetainedArrayLayout
        private var accountedCapacity = 0
        var scope: CommonTableScope?

        var workMeter: DatabaseWorkMeter { reservation.workMeter }

        init(
            root: WorkItem,
            workMeter: DatabaseWorkMeter
        ) throws {
            let layout = try DatabaseRetainedArrayLayout.forElement(
                Entry.self
            )
            self.layout = layout
            self.reservation = try workMeter.reserveIntermediate(
                bytes: layout.containerByteCount,
                at: .validation
            )
            try append(root)
        }

        mutating func append(_ item: WorkItem) throws {
            let (requiredCount, overflow) = values.count
                .addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: accountedCapacity
                )
            }
            let growth = try layout.growth(
                from: accountedCapacity,
                toFit: requiredCount
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: growth.additionalByteCount,
                at: .validation
            )
            if growth.capacity != accountedCapacity {
                values.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
            }
            values.append(Entry(item: item, scope: scope))
        }

        mutating func popLast() -> WorkItem? {
            guard let value = values.popLast() else { return nil }
            reservation.releaseGuaranteedPartial(rows: 1)
            scope = value.scope
            return value.item
        }
    }

    private static func appendChildren(
        of query: SelectQuery,
        to stack: inout WorkStack
    ) throws {
        if let definitions = query.subqueries, !definitions.isEmpty {
            let inheritedScope = stack.scope
            for (index, definition) in definitions.enumerated() {
                stack.scope = try CommonTableScope(parent: inheritedScope,
                    definitions: definitions, visibleCount: query.withRecursive ? definitions.count : index,
                    workMeter: stack.workMeter)
                try stack.append(.query(definition.query))
            }
            stack.scope = try CommonTableScope(parent: inheritedScope,
                definitions: definitions, visibleCount: definitions.count, workMeter: stack.workMeter)
        }
        try stack.append(.source(query.source, owner: query))
        for item in query.projection {
            if case .expression(let expression, _) = item { try stack.append(.expression(expression)) }
        }
        if case .distinctOn(let expressions) = query.quantifier {
            for expression in expressions { try stack.append(.expression(expression)) }
        }
        if case .expression(let expression) = query.pagination.offset {
            try stack.append(.expression(expression))
        }
        if case .count(.expression(let expression), _) = query.pagination.limit {
            try stack.append(.expression(expression))
        }
        if let filter = query.filter {
            try stack.append(.expression(filter))
        }
        for grouping in query.groupBy ?? [] {
            try stack.append(.grouping(grouping))
        }
        for window in query.windows { try appendChildren(of: window.specification, to: &stack) }
        if let having = query.having {
            try stack.append(.expression(having))
        }
        for key in query.orderBy ?? [] {
            try stack.append(.expression(key.expression))
        }
        if case .fusion(let source) = query.accessPath {
            for stage in source.stages {
                for input in stage.inputs {
                    switch input.operation {
                    case .filter(let expression):
                        try stack.append(.expression(expression))
                    case .order(let keys):
                        for key in keys {
                            try stack.append(.expression(key.expression))
                        }
                    case .index, .connected:
                        break
                    }
                }
            }
        }
    }

    private static func appendChildren(
        of source: DataSource,
        ownedBy query: SelectQuery,
        to stack: inout WorkStack
    ) throws {
        switch source {
        case .tableFunction(let source):
            for expression in source.function.arguments { try stack.append(.expression(expression)) }
        case .table, .logical:
            break
        case .values(let rows):
            for row in rows { for expression in row { try stack.append(.expression(expression)) } }
        case .aliased(let inner, _, _):
            try stack.append(.source(inner, owner: query))
        case .subquery(let query):
            try stack.append(.query(query))
        case .join(let join):
            try stack.append(.source(join.left, owner: query))
            try stack.append(.source(join.right, owner: query))
            if case .on(let expression) = join.condition {
                try stack.append(.expression(expression))
            }
        #if DATABASE_MULTI_BASE
        case .base(_, let source):
            try stack.append(.source(source, owner: query))
        #endif
        case .graphTable(let source):
            if let expression = source.matchPattern.where {
                try stack.append(.expression(expression))
            }
            for column in source.columns ?? [] {
                try stack.append(.expression(column.expression))
            }
            for path in source.matchPattern.paths {
                try stack.append(.path(path))
            }
        case .graphPattern(let pattern),
             .namedGraph(_, let pattern),
             .service(_, let pattern, _):
            try stack.append(.pattern(pattern))
        case .setOperation(_, _, let left, let right):
            try stack.append(.query(left))
            try stack.append(.query(right))
        }
    }

    private static func appendChildren(
        of path: PathPattern,
        to stack: inout WorkStack
    ) throws {
        for element in path.elements {
            switch element {
            case .node(let node):
                for property in node.properties ?? [] {
                    try stack.append(.expression(property.value))
                }
            case .edge(let edge):
                for property in edge.properties ?? [] {
                    try stack.append(.expression(property.value))
                }
            case .quantified(let nested, _):
                try stack.append(.path(nested))
            case .alternation(let alternatives):
                for nested in alternatives {
                    try stack.append(.path(nested))
                }
            }
        }
    }

    private static func appendChildren(
        of pattern: GraphPattern,
        to stack: inout WorkStack
    ) throws {
        switch pattern {
        case .basic, .values:
            break
        case .join(let lhs, let rhs), .optional(let lhs, let rhs),
             .union(let lhs, let rhs), .minus(let lhs, let rhs),
             .lateral(let lhs, let rhs):
            try stack.append(.pattern(lhs))
            try stack.append(.pattern(rhs))
        case .filter(let pattern, let expression),
             .bind(let pattern, _, let expression):
            try stack.append(.pattern(pattern))
            try stack.append(.expression(expression))
        case .graph(_, let pattern), .service(_, let pattern, _):
            try stack.append(.pattern(pattern))
        case .subquery(let query):
            try stack.append(.query(query))
        case .groupBy(let pattern, let expressions, let aggregates):
            try stack.append(.pattern(pattern))
            for expression in expressions {
                try stack.append(.expression(expression))
            }
            for binding in aggregates {
                try stack.append(.aggregate(binding.aggregate))
            }
        }
    }

    private static func appendChildren(
        of expression: Expression,
        to stack: inout WorkStack
    ) throws {
        switch expression {
        case .literal, .currentValue, .column, .variable, .parameter, .bound, .wholeRow:
            break
        case .collectionOperation(_, let lhs, let rhs),
             .add(let lhs, let rhs), .subtract(let lhs, let rhs),
             .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
             .modulo(let lhs, let rhs), .concatenate(let lhs, let rhs), .equal(let lhs, let rhs),
             .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
             .lessThanOrEqual(let lhs, let rhs),
             .greaterThan(let lhs, let rhs),
             .greaterThanOrEqual(let lhs, let rhs), .and(let lhs, let rhs),
             .or(let lhs, let rhs), .nullIf(let lhs, let rhs), .isDistinctFrom(let lhs, let rhs, _):
            try stack.append(.expression(lhs))
            try stack.append(.expression(rhs))
        case .collate(let nested, _), .field(let nested, _), .fieldExpansion(let nested), .negate(let nested), .not(let nested), .isNull(let nested), .truthTest(let nested, _, _),
             .isNotNull(let nested),
             .regex(let nested, _, _), .cast(let nested, _),
             .isTriple(let nested), .subject(let nested),
             .predicate(let nested), .object(let nested):
            try stack.append(.expression(nested))
        case .arrayAccess(let base, let subscripts):
            try stack.append(.expression(base))
            for subscriptValue in subscripts {
                switch subscriptValue {
                case .index(let value): try stack.append(.expression(value))
                case .slice(let lower, let upper):
                    if let lower { try stack.append(.expression(lower)) }
                    if let upper { try stack.append(.expression(upper)) }
                }
            }
        case .patternMatch(let subject, let pattern, let escape, _):
            try stack.append(.expression(subject))
            try stack.append(.expression(pattern))
            if let escape { try stack.append(.expression(escape)) }
        case .between(let value, let lower, let upper):
            try stack.append(.expression(value))
            try stack.append(.expression(lower))
            try stack.append(.expression(upper))
        case .inList(let value, let values),
             .notInList(let value, let values):
            try stack.append(.expression(value))
            for nested in values {
                try stack.append(.expression(nested))
            }
        case .quantifiedComparison(let lhs, _, _, let source):
            try stack.append(.expression(lhs))
            switch source {
            case .array(let value): try stack.append(.expression(value))
            case .subquery(let query): try stack.append(.query(query))
            }
        case .inSubquery(let value, let query):
            try stack.append(.expression(value))
            try stack.append(.query(query))
        case .window(let call):
            switch call.function {
            case .aggregate(let aggregate): try appendChildren(of: aggregate, to: &stack)
            case .analytic(_, let arguments):
                for argument in arguments { try stack.append(.expression(argument)) }
            }
            if case .specification(let specification) = call.reference {
                try appendChildren(of: specification, to: &stack)
            }
        case .aggregate(let aggregate):
            try stack.append(.aggregate(aggregate))
        case .function(let call):
            for argument in call.arguments {
                try stack.append(.expression(argument))
            }
        case .caseWhen(let operand, let cases, let fallback):
            if let operand { try stack.append(.expression(operand)) }
            for pair in cases {
                try stack.append(.expression(pair.condition))
                try stack.append(.expression(pair.result))
            }
            if let fallback {
                try stack.append(.expression(fallback))
            }
        case .coalesce(let expressions), .array(let expressions), .row(let expressions), .greatest(let expressions), .least(let expressions), .grouping(let expressions):
            for nested in expressions {
                try stack.append(.expression(nested))
            }
        case .triple(let subject, let predicate, let object):
            try stack.append(.expression(subject))
            try stack.append(.expression(predicate))
            try stack.append(.expression(object))
        case .subquery(let query), .exists(let query), .arraySubquery(let query):
            try stack.append(.query(query))
        }
    }

    private static func appendChildren(
        of aggregate: AggregateFunction,
        to stack: inout WorkStack
    ) throws {
        switch aggregate {
        case .sql(let call): try appendChildren(of: call, to: &stack)
        case .count(let expression, _):
            if let expression {
                try stack.append(.expression(expression))
            }
        case .sum(let expression, _), .avg(let expression, _),
             .min(let expression), .max(let expression),
             .sample(let expression):
            try stack.append(.expression(expression))
        case .groupConcat(let expression, _, _):
            try stack.append(.expression(expression))
        case .arrayAgg(let expression, let orderBy, _):
            try stack.append(.expression(expression))
            for key in orderBy ?? [] {
                try stack.append(.expression(key.expression))
            }
        }
    }

    private static func appendChildren(of grouping: GroupingElement, to stack: inout WorkStack) throws {
        switch grouping {
        case .expression(let expression): try stack.append(.expression(expression))
        case .rollup(let components), .cube(let components):
            for component in components { for expression in component { try stack.append(.expression(expression)) } }
        case .groupingSets(let sets):
            for set in sets { for element in set { try stack.append(.grouping(element)) } }
        }
    }

    private static func appendChildren(of call: SQLAggregateCall, to stack: inout WorkStack) throws {
        for argument in call.arguments { try stack.append(.expression(argument)) }
        for key in call.orderBy { try stack.append(.expression(key.expression)) }
        if let filter = call.filter { try stack.append(.expression(filter)) }
        for key in call.withinGroup ?? [] { try stack.append(.expression(key.expression)) }
    }

    private static func appendChildren(of specification: WindowSpecification, to stack: inout WorkStack) throws {
        for expression in specification.partitionBy { try stack.append(.expression(expression)) }
        for key in specification.orderBy { try stack.append(.expression(key.expression)) }
        if let frame = specification.frame {
            for bound in [frame.start, frame.end] {
                switch bound {
                case .preceding(let expression), .following(let expression): try stack.append(.expression(expression))
                default: break
                }
            }
        }
    }

}
