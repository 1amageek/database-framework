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
                    try visitTable(tableRef, query)
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
    }

    private struct WorkStack {
        private var values: [WorkItem] = []
        private let reservation: DatabaseIntermediateReservation
        private let layout: DatabaseRetainedArrayLayout
        private var accountedCapacity = 0

        init(
            root: WorkItem,
            workMeter: DatabaseWorkMeter
        ) throws {
            let layout = try DatabaseRetainedArrayLayout.forElement(
                WorkItem.self
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
            values.append(item)
        }

        mutating func popLast() -> WorkItem? {
            guard let value = values.popLast() else { return nil }
            reservation.releaseGuaranteedPartial(rows: 1)
            return value
        }
    }

    private static func appendChildren(
        of query: SelectQuery,
        to stack: inout WorkStack
    ) throws {
        for nested in query.subqueries ?? [] {
            try stack.append(.query(nested.query))
        }
        try stack.append(.source(query.source, owner: query))
        switch query.projection {
        case .all, .allFrom:
            break
        case .items(let items), .distinctItems(let items):
            for item in items {
                try stack.append(.expression(item.expression))
            }
        }
        if let filter = query.filter {
            try stack.append(.expression(filter))
        }
        for expression in query.groupBy ?? [] {
            try stack.append(.expression(expression))
        }
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
        case .table, .logical, .values:
            break
        case .subquery(let query, _):
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
        case .union(let sources), .unionAll(let sources),
             .intersect(let sources):
            for source in sources {
                try stack.append(.source(source, owner: query))
            }
        case .except(let lhs, let rhs):
            try stack.append(.source(lhs, owner: query))
            try stack.append(.source(rhs, owner: query))
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
        case .literal, .column, .variable, .parameter, .bound:
            break
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
             .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
             .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
             .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
             .lessThanOrEqual(let lhs, let rhs),
             .greaterThan(let lhs, let rhs),
             .greaterThanOrEqual(let lhs, let rhs), .and(let lhs, let rhs),
             .or(let lhs, let rhs), .nullIf(let lhs, let rhs):
            try stack.append(.expression(lhs))
            try stack.append(.expression(rhs))
        case .negate(let nested), .not(let nested), .isNull(let nested),
             .isNotNull(let nested), .like(let nested, _),
             .regex(let nested, _, _), .cast(let nested, _),
             .isTriple(let nested), .subject(let nested),
             .predicate(let nested), .object(let nested):
            try stack.append(.expression(nested))
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
        case .inSubquery(let value, let query):
            try stack.append(.expression(value))
            try stack.append(.query(query))
        case .aggregate(let aggregate):
            try stack.append(.aggregate(aggregate))
        case .function(let call):
            for argument in call.arguments {
                try stack.append(.expression(argument))
            }
        case .caseWhen(let cases, let fallback):
            for pair in cases {
                try stack.append(.expression(pair.condition))
                try stack.append(.expression(pair.result))
            }
            if let fallback {
                try stack.append(.expression(fallback))
            }
        case .coalesce(let expressions):
            for nested in expressions {
                try stack.append(.expression(nested))
            }
        case .triple(let subject, let predicate, let object):
            try stack.append(.expression(subject))
            try stack.append(.expression(predicate))
            try stack.append(.expression(object))
        case .subquery(let query), .exists(let query):
            try stack.append(.query(query))
        }
    }

    private static func appendChildren(
        of aggregate: AggregateFunction,
        to stack: inout WorkStack
    ) throws {
        switch aggregate {
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
}
