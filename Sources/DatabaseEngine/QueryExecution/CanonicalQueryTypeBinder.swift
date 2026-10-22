import DatabaseTypes
import DatabaseKit
import QueryAST

/// SQL-only typing for the currently executable canonical expression forms.
/// The relation owner supplies name lookup and subquery binding before reads.
struct CanonicalQueryTypeBinder {
    let columns: [QueryColumn]
    private let parameterState: CanonicalParameterBindingState
    private let ownsParameterState: Bool
    private let allowingSPARQLFunctions: Bool
    private let occurrences: QueryOccurrenceArena?
    private let root: QueryOccurrenceID?
    private var currentOccurrence: QueryOccurrenceID?
    private var isNested = false
    private var allowsRowSubquery = false
    private var permitsAggregates = true
    private var permitsWindows = true
    private(set) var windowDefinitions: [CanonicalBoundWindowDefinition] = []
    private let resolveColumn: (ColumnRef) throws -> Int
    private let wildcardSlots: (String?) throws -> [Int]
    private let resolveWholeRow: ((String) throws -> CanonicalBoundWholeRow)?
    private let bindSubquery: ((SelectQuery) throws -> [QueryColumn])?
    private let bindLocatedSubquery: ((SelectQuery, QueryOccurrenceID?) throws -> CanonicalBoundSelect)?
    private let workMeter: DatabaseWorkMeter?
    private let bindingMetadata = CanonicalBindingMetadata()

    init(
        columns: [QueryColumn],
        parameters: [QueryParameter] = [],
        parameterState: CanonicalParameterBindingState? = nil,
        allowingSPARQLFunctions: Bool = false,
        occurrences: QueryOccurrenceArena? = nil,
        root: QueryOccurrenceID? = nil,
        resolveColumn: @escaping (ColumnRef) throws -> Int,
        wildcardSlots: @escaping (String?) throws -> [Int],
        resolveWholeRow: ((String) throws -> CanonicalBoundWholeRow)? = nil,
        bindSubquery: ((SelectQuery) throws -> [QueryColumn])? = nil,
        bindLocatedSubquery: ((SelectQuery, QueryOccurrenceID?) throws -> CanonicalBoundSelect)? = nil,
        workMeter: DatabaseWorkMeter? = nil
    ) {
        self.columns = columns
        self.parameterState = parameterState ?? CanonicalParameterBindingState(parameters: parameters)
        self.ownsParameterState = parameterState == nil
        self.allowingSPARQLFunctions = allowingSPARQLFunctions
        self.occurrences = occurrences
        self.root = root
        self.currentOccurrence = root
        self.resolveColumn = resolveColumn
        self.wildcardSlots = wildcardSlots
        self.resolveWholeRow = resolveWholeRow
        self.bindSubquery = bindSubquery
        self.bindLocatedSubquery = bindLocatedSubquery
        self.workMeter = workMeter
    }

    /// The supplied schema is the already-bound FROM scope. Source recursion,
    /// including ON clauses, remains owned by the relation binder.
    func bind(_ query: SelectQuery) throws -> [QueryColumn] {
        try bindSelect(query).columns
    }

    func bindSelect(_ query: SelectQuery, occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundSelect {
        if let occurrence {
            var scoped = self
            scoped.currentOccurrence = occurrence
            return try scoped.bindSelect(query)
        }
        if !query.windows.isEmpty {
            return try preparingWindows(query.windows).bindSelect(query.replacing(windows: []))
        }
        let bytes = UInt64(MemoryLayout<[CanonicalBoundExpression]>.stride
            + MemoryLayout<[CanonicalBoundKey]>.stride * 2)
        let reservation = try workMeter?.reserveIntermediate(bytes: bytes, at: .bindingCandidate)
        var successful = false
        defer { if !successful { reservation?.release() } }
        let selected = try bindProjection(query.projection)
        let output = selected.columns
        let projection = selected.projection
        let filter = try query.filter.map { try childBinder(.filter).bindPredicate($0) }
        @AdmittedArray var groupBy: [CanonicalBoundExpression] = []
        func groupingLeaves(_ elements: [GroupingElement], binder: Self, field: QueryOccurrenceField = .groupBy) throws {
            for (index, element) in elements.enumerated() {
                let selected = binder.childBinder(field, index)
                switch element {
                case .expression(let expression):
                    let child = try selected.childBinder(.operand).bindGroupingExpression(expression)
                    try CanonicalBindingStorage.append(child, to: &$groupBy, reservation: reservation)
                case .rollup(let units), .cube(let units):
                    for (unit, expressions) in units.enumerated() {
                        for (position, expression) in expressions.enumerated() {
                            let child = try selected.childBinder(.groupingComponent, unit)
                                .childBinder(.argument, position).bindGroupingExpression(expression)
                            try CanonicalBindingStorage.append(child, to: &$groupBy, reservation: reservation)
                        }
                    }
                case .groupingSets(let sets):
                    for (index, set) in sets.enumerated() {
                        try groupingLeaves(set, binder: selected.childBinder(.groupingSet, index), field: .groupingElement)
                    }
                }
            }
        }
        try groupingLeaves(query.groupBy ?? [], binder: self)
        let having = try query.having.map { try childBinder(.having).bindHaving($0) }
        @AdmittedArray var orderBy: [CanonicalBoundKey] = []
        for (index, key) in (query.orderBy ?? []).enumerated() {
            try CanonicalBindingStorage.append(childBinder(.orderBy, index).bindOrderKey(key,
                projection: projection, output: output), to: &$orderBy, reservation: reservation)
        }
        @AdmittedArray var distinctOn: [CanonicalBoundKey] = []
        switch query.quantifier {
        case .all: break
        case .distinct:
            for column in output { try requireSQLComparison(column.type, operation: "DISTINCT") }
        case .distinctOn(let expressions):
            for (index, expression) in expressions.enumerated() {
                let key = try childBinder(.distinctOn, index).bindOrderKey(SortKey(expression),
                    projection: projection, output: output)
                try CanonicalBindingStorage.append(CanonicalBoundKey(expression: key.expression,
                    projectionIndex: key.projectionIndex, occurrence: key.occurrence), to: &$distinctOn,
                    reservation: reservation)
            }
        }
        if ownsParameterState { try parameterState.finalize() }
        successful = true
        return CanonicalBoundSelect(columns: output, projection: projection, filter: filter,
            groupBy: groupBy, having: having, orderBy: orderBy, distinctOn: distinctOn,
            metadataLease: reservation.map { DatabaseQueryPayloadLease(retaining: $0) }, bindingMetadata: bindingMetadata,
            windows: windowDefinitions)
    }

    /// Binds only ordered projection items; the relation owner supplies clause binding.
    func bindProjection(_ items: [ProjectionItem], occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundSelect {
        var scoped = self
        if let occurrence { scoped.currentOccurrence = occurrence }
        let bytes = UInt64(MemoryLayout<[QueryColumn]>.stride + MemoryLayout<[CanonicalBoundExpression]>.stride)
        let reservation = try workMeter?.reserveIntermediate(bytes: bytes, at: .bindingCandidate)
        var successful = false
        defer { if !successful { reservation?.release() } }
        var columns: [QueryColumn] = []
        var projection: [CanonicalBoundExpression] = []
        for (index, item) in items.enumerated() {
            try scoped.childBinder(.projection, index).appendProjectionItem(item, index: index,
                columns: &columns, projection: &projection, reservation: reservation)
        }
        try bindingMetadata.retain(reservation)
        successful = true
        return CanonicalBoundSelect(columns: columns, projection: projection, filter: nil,
            groupBy: [], having: nil, orderBy: [], bindingMetadata: bindingMetadata, windows: windowDefinitions)
    }

    private func appendProjectionItem(_ item: ProjectionItem, index: Int,
        columns: inout [QueryColumn], projection: inout [CanonicalBoundExpression],
        reservation: DatabaseIntermediateReservation?) throws {
        switch item {
        case .wildcard(let qualifier):
            try appendProjectionWildcard(qualifier, columns: &columns, projection: &projection, reservation: reservation)
        case .expression(let expression, let alias):
            if case .fieldExpansion(let operand) = expression {
                try appendProjectionExpansion(operand, alias: alias, columns: &columns,
                    projection: &projection, reservation: reservation)
            } else {
                let bound = try bind(expression, parameterContext: .output)
                try appendProjection(bound, name: Self.projectionName(expression, alias: alias, index: index, bound: bound),
                    columns: &columns, projection: &projection, reservation: reservation)
            }
        }
    }

    /// Names one bound projection item. Nested scalar subqueries recurse
    /// through `appendProjectionItem`, so the naming closure that captures
    /// `bound` lives in this out-of-line frame instead of every level's.
    @inline(never)
    private static func projectionName(_ expression: Expression, alias: String?, index: Int,
        bound: CanonicalBoundExpression) -> String {
        if let alias { return alias }
        return outputName(expression, index: index, subqueryName: { boundSubqueryName($0, in: [bound]) })
    }

    private func appendProjection(_ bound: CanonicalBoundExpression, name: String,
        columns: inout [QueryColumn], projection: inout [CanonicalBoundExpression],
        reservation: DatabaseIntermediateReservation?) throws {
        let column = try QueryColumn(number: ordinal(columns.count), name: name,
            type: bound.type, isNullable: bound.isNullable)
        try CanonicalBindingStorage.appendGrowing(bound, to: &projection, reservation: reservation)
        try CanonicalBindingStorage.appendGrowing(column, to: &columns, reservation: reservation)
    }

    private func appendProjectionWildcard(_ qualifier: String?, columns: inout [QueryColumn],
        projection: inout [CanonicalBoundExpression], reservation: DatabaseIntermediateReservation?) throws {
        for slot in try wildcardSlots(qualifier) {
            let column = try self.column(at: slot)
            let bound = CanonicalBoundExpression(expression: .column(ColumnRef(column: column.name)),
                type: column.type, isNullable: column.isNullable, columnSlot: slot, occurrence: currentOccurrence)
            try appendProjection(bound, name: column.name, columns: &columns, projection: &projection, reservation: reservation)
        }
    }

    private func appendProjectionExpansion(_ operand: Expression, alias: String?, columns: inout [QueryColumn],
        projection: inout [CanonicalBoundExpression], reservation: DatabaseIntermediateReservation?) throws {
        let source = try bind(operand, parameterContext: .output)
        guard case .sql(.composite(let descriptor)) = source.type else { throw mismatch("field expansion requires a composite") }
        guard alias == nil || descriptor.columns.count == 1 else { throw mismatch("alias on multiple expanded fields") }
        for field in descriptor.columns.indices {
            let column = descriptor.columns[field]
            let bound = CanonicalBoundExpression(expression: .field(source.expression, name: column.name),
                type: column.type, isNullable: source.isNullable || column.isNullable,
                children: [source], occurrence: currentOccurrence, fieldOrdinal: field)
            try appendProjection(bound, name: alias ?? column.name, columns: &columns,
                projection: &projection, reservation: reservation)
        }
    }

    func bindOrderKey(_ key: SortKey, projection: [CanonicalBoundExpression], output: [QueryColumn],
        allowSourceExpressions: Bool = true, occurrence: QueryOccurrenceID? = nil,
        isVolatileSubquery: (CanonicalBoundSelect) -> Bool = { _ in true }) throws -> CanonicalBoundKey {
        var scoped = self
        if let occurrence { scoped.currentOccurrence = occurrence }
        do {
            return try scoped.bindSelectedOrderKey(key, projection: projection, output: output,
                allowSourceExpressions: allowSourceExpressions, isVolatileSubquery: isVolatileSubquery)
        } catch let error as DatabaseExpressionEvaluationError {
            throw scoped.located(error, at: scoped.currentOccurrence)
        } catch let error as QueryValueError {
            throw scoped.located(.queryValue(error), at: scoped.currentOccurrence)
        }
    }

    private func bindSelectedOrderKey(_ key: SortKey, projection: [CanonicalBoundExpression], output: [QueryColumn],
        allowSourceExpressions: Bool, isVolatileSubquery: (CanonicalBoundSelect) -> Bool) throws -> CanonicalBoundKey {
        let index: Int?
        switch key.expression {
        case .literal(.int(let number)):
            guard number > 0, let selected = Int(exactly: number - 1), projection.indices.contains(selected) else {
                throw mismatch("ORDER BY position outside projection")
            }
            index = selected
        case .literal(.uint(let number)):
            guard number > 0, let selected = Int(exactly: number - 1), projection.indices.contains(selected) else {
                throw mismatch("ORDER BY position outside projection")
            }
            index = selected
        case .column(let column) where column.table == nil:
            var selected: Int?
            for position in output.indices where output[position].name.utf8.elementsEqual(column.column.utf8) {
                if let selected {
                    let candidate = CanonicalBoundKey(expression: projection[selected], projectionIndex: selected)
                    let duplicate = CanonicalBoundKey(expression: projection[position], projectionIndex: position)
                    guard try candidate.isSemanticallyEquivalent(to: duplicate, workMeter: workMeter,
                        isVolatileSubquery: isVolatileSubquery) else {
                        let cause = CanonicalReadError.expressionEvaluation(.ambiguousColumn(column.displayName))
                        guard occurrences != nil else { throw cause }
                        throw CanonicalSQLBindingFailure(cause: .read(cause),
                            occurrence: currentOccurrence, root: root)
                    }
                } else { selected = position }
            }
            index = selected
        default: index = nil
        }
        let expression: CanonicalBoundExpression
        if let index { expression = projection[index] }
        else {
            guard allowSourceExpressions else { throw mismatch("set ordering requires an output alias or position") }
            expression = try bind(key.expression)
        }
        try requireSQLComparison(expression.type, operation: "ORDER BY")
        return CanonicalBoundKey(expression: expression, projectionIndex: index, sortKey: key,
            occurrence: currentOccurrence)
    }

    func bind(
        _ expression: DatabaseKit.Expression,
        contextualType: QueryColumnType? = nil,
        parameterContext: CanonicalParameterBindingState.Context = .unconstrained,
        occurrence: QueryOccurrenceID? = nil
    ) throws -> CanonicalBoundExpression {
        var scoped = self
        if let occurrence { scoped.currentOccurrence = occurrence }
        do {
            return try finalizing(scoped.bindSelectedExpression(expression,
                contextualType: contextualType, parameterContext: parameterContext))
        }
        catch let cause as QueryValueError {
            throw scoped.located(cause, at: scoped.currentOccurrence)
        }
        catch let cause as DatabaseExpressionEvaluationError {
            throw scoped.located(cause, at: scoped.currentOccurrence)
        }
    }

    private func finalizing(_ result: CanonicalBoundExpression) throws -> CanonicalBoundExpression {
        if ownsParameterState && !isNested { try parameterState.finalize() }
        return result
    }

    func bindPredicate(_ expression: DatabaseKit.Expression, occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundExpression {
        var scoped = self
        scoped.permitsAggregates = false
        scoped.permitsWindows = false
        if let occurrence { scoped.currentOccurrence = occurrence }
        return try scoped.boolean(expression)
    }

    func bindHaving(_ expression: Expression, occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundExpression {
        var scoped = self
        scoped.permitsWindows = false
        if let occurrence { scoped.currentOccurrence = occurrence }
        return try scoped.boolean(expression)
    }

    func bindGroupingExpression(_ expression: Expression, occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundExpression {
        var scoped = self
        scoped.permitsAggregates = false
        scoped.permitsWindows = false
        let selected = try scoped.bind(expression, occurrence: occurrence)
        try requireSQLComparison(selected.type, operation: "GROUP BY")
        return selected
    }

    func bindScalarExpression(_ expression: Expression, contextualType: QueryColumnType? = nil,
        occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundExpression {
        var scoped = self
        scoped.permitsAggregates = false
        scoped.permitsWindows = false
        return try scoped.bind(expression, contextualType: contextualType, occurrence: occurrence)
    }

    func bindScalarAssignment(_ expression: Expression, to type: QueryColumnType,
        occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundExpression {
        var scoped = self
        scoped.permitsAggregates = false
        scoped.permitsWindows = false
        if case .sql(.composite) = type { scoped.allowsRowSubquery = true }
        return try scoped.bindAssignment(expression, to: type, occurrence: occurrence)
    }

    func bindAssignment(_ expression: DatabaseKit.Expression, to type: QueryColumnType,
        occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundExpression {
        try validate(type)
        let context: QueryColumnType
        if case .sql(.decimal) = type { context = .sql(.decimal(precision: nil, scale: nil)) }
        else { context = Self.isText(type) ? .sql(.text) : type }
        let child = try bind(expression, contextualType: context, occurrence: occurrence)
        return try bindAssignment(child, to: type)
    }

    func bindAssignment(_ child: CanonicalBoundExpression, to type: QueryColumnType) throws -> CanonicalBoundExpression {
        try validate(type)
        do { try validateCast(from: child.type, to: type, context: .assignment) }
        catch let error as DatabaseExpressionEvaluationError { throw located(error, at: child.occurrence) }
        let target: DataType
        switch type {
        case .sql(let scalar): target = scalar
        case .primitive(.string, false): target = .text
        case .primitive(.bytes, false): target = .blob
        case .primitive(.uuid, false): target = .uuid
        default:
            guard let scalar = Self.sqlScalar(type) else {
                guard child.type == type else { throw mismatch("assignment target") }
                return child
            }
            target = scalar
        }
        return CanonicalBoundExpression(expression: .cast(child.expression, targetType: target), type: type,
            isNullable: child.isNullable, children: [child], occurrence: child.occurrence, castContext: .assignment, bindingMetadata: bindingMetadata)
    }

    /// Common-source conversion preserves relation identity independently of SQL CAST policy.
    func bindSourceValue(_ expression: Expression, to type: QueryColumnType,
        occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundExpression {
        let context = Self.isText(type) ? QueryColumnType.sql(.text) : type
        return try bindSourceValue(bind(expression, contextualType: context, occurrence: occurrence), to: type)
    }

    func bindSourceValue(_ child: CanonicalBoundExpression, to type: QueryColumnType) throws -> CanonicalBoundExpression {
        try validate(type)
        if child.type == type { return child }
        let exactNumeric: Bool
        if case .sql(.decimal(precision: nil, scale: nil)) = type {
            exactNumeric = Self.numericKind(child.type) != nil
        } else { exactNumeric = false }
        if !exactNumeric { try validateCast(from: child.type, to: type, context: .implicit) }
        guard case .sql(let target) = type else {
            return try bindAssignment(child, to: type)
        }
        return CanonicalBoundExpression(expression: .cast(child.expression, targetType: target), type: type,
            isNullable: child.isNullable, children: [child], occurrence: child.occurrence,
            castContext: .implicit, sourceAlignment: true, bindingMetadata: bindingMetadata)
    }

    func bindPagination(_ pagination: QueryPagination, localColumnCount: Int,
                        sql: Bool = true) throws -> CanonicalBoundPagination {
        func count(_ count: QueryCount, field: QueryOccurrenceField) throws -> CanonicalBoundCount {
            switch count {
            case .fixed(let value):
                guard !sql || value <= UInt64(Int64.max) else {
                    throw DatabaseExpressionEvaluationError.numericOverflow
                }
                return .fixed(value)
            case .expression(let expression):
                let selected = try childBinder(field).bindAssignment(expression, to: .sql(.bigint))
                let layout = try DatabaseRetainedArrayLayout.forElement(CanonicalBoundExpression.self)
                let scratch = try workMeter?.reserveIntermediate(bytes: layout.containerByteCount, at: .bindingCandidate)
                defer { scratch?.release() }
                var pending: [CanonicalBoundExpression] = []
                var capacity = 0
                func append(_ child: CanonicalBoundExpression) throws {
                    let next = pending.count.addingReportingOverflow(1)
                    guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                    let growth = try layout.growth(from: capacity, toFit: next.partialValue)
                    if growth.capacity > capacity {
                        try scratch?.reserveAdditional(bytes: growth.additionalByteCount, at: .bindingCandidate)
                        pending.reserveCapacity(growth.capacity)
                        capacity = growth.capacity
                    }
                    pending.append(child)
                }
                try append(selected)
                while let child = pending.popLast() {
                    try workMeter?.consume(at: .bindingCandidate)
                    if let slot = child.columnSlot, slot < localColumnCount {
                        throw mismatch("pagination cannot reference its source row")
                    }
                    if case .aggregate = child.expression { throw mismatch("pagination aggregate") }
                    if case .window = child.expression { throw mismatch("pagination window") }
                    if case .grouping = child.expression { throw mismatch("pagination grouping") }
                    for nested in child.children.reversed() { try append(nested) }
                }
                return .expression(selected)
            }
        }
        let offset = try pagination.offset.map { try count($0, field: .offset) }
        let limit: CanonicalBoundCount?
        let withTies: Bool
        var rejectsUnknownNullLimit = false
        switch pagination.limit {
        case .none, .some(.all): limit = nil; withTies = false
        case .some(.count(let selected, let ties)):
            limit = try count(selected, field: .limit)
            withTies = ties
            if case .expression(let expression) = selected {
                switch expression {
                case .literal(.null), .literal(.queryValue(.primitive(.null), type: nil)):
                    rejectsUnknownNullLimit = ties
                default: break
                }
            }
        }
        return CanonicalBoundPagination(offset: offset, limit: limit, withTies: withTies,
            rejectsUnknownNullLimit: rejectsUnknownNullLimit)
    }

    // Query callbacks must not retain unrelated dispatch and result-construction frames.
    private func bindSelectedExpression(_ expression: Expression, contextualType: QueryColumnType?,
        parameterContext: CanonicalParameterBindingState.Context) throws -> CanonicalBoundExpression {
        if let query = Self.selectedSubquery(expression) {
            return try finishSelectedSubquery(query, expression: expression,
                binding: bindLocatedSubquery?(query, subqueryOccurrence()))
        }
        return try bindOtherSelectedExpression(expression, contextualType: contextualType, parameterContext: parameterContext)
    }

    private static func selectedSubquery(_ expression: Expression) -> SelectQuery? {
        switch expression {
        case .subquery(let query), .arraySubquery(let query), .exists(let query): return query
        default: return nil
        }
    }

    private func subqueryOccurrence() -> QueryOccurrenceID? {
        childBinder(.subquery).currentOccurrence
    }

    private func bindOtherSelectedExpression(_ expression: Expression, contextualType: QueryColumnType?,
        parameterContext: CanonicalParameterBindingState.Context) throws -> CanonicalBoundExpression {
        switch expression {
        case .inSubquery(let operand, let query): return try bindSubqueryComparison(operand, query: query, expression: expression)
        case .quantifiedComparison(let operand, _, _, .subquery(let query)):
            return try bindSubqueryComparison(operand, query: query, expression: expression)
        default: return try bindExpression(expression, contextualType: contextualType, parameterContext: parameterContext)
        }
    }

    private func finishSelectedSubquery(_ query: SelectQuery, expression: Expression,
        binding: CanonicalBoundSelect?) throws -> CanonicalBoundExpression {
        switch expression {
        case .arraySubquery:
            let column = try scalarColumn(query, binding: binding)
            let element: DataType
            if let nested = Self.arrayElement(column.type), case .sql(let selected) = nested { element = selected }
            else if case .sql(let selected) = column.type { element = selected }
            else if let selected = Self.sqlScalar(column.type) { element = selected }
            else { throw mismatch("ARRAY subquery element type") }
            return node(expression, .sql(.array(element)), false, subqueryBinding: binding)
        case .subquery:
            let output = try binding?.columns ?? subqueryColumns(query)
            guard !output.isEmpty else { throw mismatch("scalar subquery column count") }
            if output.count == 1 { return node(expression, output[0].type, true, subqueryBinding: binding) }
            guard allowsRowSubquery else { throw mismatch("scalar subquery column count") }
            let descriptor = try CanonicalBindingStorage.composite(columns: output, workMeter: workMeter)
            return node(expression, .sql(.composite(descriptor.type)), true, subqueryBinding: binding,
                metadataLease: descriptor.lease)
        case .exists:
            if binding == nil { _ = try subqueryColumns(query) }
            return node(expression, .sql(.boolean), false, subqueryBinding: binding)
        default: throw DatabaseExpressionEvaluationError.unsupportedExpression("selected subquery expression")
        }
    }

    private func bindExpression(
        _ expression: DatabaseKit.Expression,
        contextualType: QueryColumnType?,
        parameterContext: CanonicalParameterBindingState.Context
    ) throws -> CanonicalBoundExpression {
        switch expression {
        case .currentValue(let kind, let precision):
            guard precision == nil || (0...6).contains(precision!), kind != .date || precision == nil else {
                throw DatabaseExpressionEvaluationError.invalidTemporalInput
            }
            let type: DataType
            switch kind {
            case .date: type = .date
            case .time: type = .time(withTimeZone: true, precision: precision)
            case .timestamp: type = .timestamp(withTimeZone: true, precision: precision)
            case .localTime: type = .time(withTimeZone: false, precision: precision)
            case .localTimestamp: type = .timestamp(withTimeZone: false, precision: precision)
            }
            return node(expression, .sql(type), false)
        case .greatest(let expressions), .least(let expressions):
            guard !expressions.isEmpty else { throw mismatch("GREATEST/LEAST") }
            let children = try bindCommon(expressions, defaultType: contextualType ?? .sql(.text), field: .argument)
            try requireComparable(children, operation: "GREATEST/LEAST")
            return node(expression, children[0].type, children.allSatisfy { $0.isNullable }, children)
        case .truthTest(let operand, _, _):
            return node(expression, .sql(.boolean), false, [try childBinder(.operand, 0).boolean(operand)])
        case .isDistinctFrom(let lhs, let rhs, _):
            let children = try bindComparison([lhs, rhs])
            return node(expression, .sql(.boolean), false, children)
        case .quantifiedComparison(let lhs, _, _, let source):
            return try bindQuantified(lhs, source: source, expression: expression)

        case .array(let elements):
            return try bindArrayConstructor(elements, expression: expression, contextualType: contextualType)
        case .arrayAccess(let base, let subscripts):
            return try bindArrayAccess(base, subscripts: subscripts, expression: expression)
        case .collectionOperation(let operation, let left, let right):
            return try bindCollectionOperation(operation, left: left, right: right, expression: expression)
        case .row(let expressions):
            let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[CanonicalBoundExpression]>.stride
                + MemoryLayout<[QueryColumn]>.stride), at: .bindingCandidate)
            var successful = false
            defer { if !successful { claim?.release() } }
            var children: [CanonicalBoundExpression] = []
            var fields: [QueryColumn] = []
            let context: QueryCompositeType?
            if case .sql(.composite(let type))? = contextualType { context = type } else { context = nil }
            func append(_ child: CanonicalBoundExpression) throws {
                let index = fields.count
                let field = try QueryColumn(number: ordinal(index), name: "f\(index + 1)",
                    type: child.type, isNullable: child.isNullable)
                try CanonicalBindingStorage.appendGrowing(child, to: &children, reservation: claim)
                try CanonicalBindingStorage.appendGrowing(field, to: &fields, reservation: claim)
            }
            for (index, value) in expressions.enumerated() {
                let scoped = childBinder(.argument, index)
                if case .fieldExpansion(let operand) = value {
                    let child = try scoped.bind(operand)
                    guard case .sql(.composite(let descriptor)) = child.type else { throw mismatch("ROW field expansion") }
                    for position in descriptor.columns.indices {
                        let field = descriptor.columns[position]
                        try append(scoped.node(.field(child.expression, name: field.name), field.type,
                            child.isNullable || field.isNullable, [child], fieldOrdinal: position))
                    }
                } else {
                    let type = context.flatMap { $0.columns.indices.contains(fields.count) ? $0.columns[fields.count].type : nil }
                    try append(scoped.bind(value, contextualType: type))
                }
            }
            let descriptor = try CanonicalBindingStorage.composite(columns: fields, workMeter: workMeter)
            let temporary = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<[QueryColumn]>.stride))
                .adding(try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryColumn>.stride))
                    .multiplied(by: UInt64(fields.count)))
            try claim?.releasePartial(bytes: temporary.bytes)
            // The descriptor lease and the selected children remain live together.
            let result = node(expression, .sql(.composite(descriptor.type)), false, children,
                comparisonMode: .constructor, metadataLease: descriptor.lease)
            successful = true
            return CanonicalBoundExpression(expression: result.expression, type: result.type, isNullable: false,
                children: result.children, occurrence: result.occurrence, comparisonMode: .constructor,
                metadataLease: descriptor.lease, constructionLease: claim.map { DatabaseQueryPayloadLease(retaining: $0) },
                bindingMetadata: bindingMetadata)
        case .wholeRow(let name):
            guard let selected = try resolveWholeRow?(name) else { throw mismatch("unknown whole-row source") }
            return node(expression, .sql(.composite(selected.type)), true, wholeRow: selected)
        case .field(let operand, let name):
            let child = try childBinder(.operand, 0).bind(operand)
            guard case .sql(.composite(let descriptor)) = child.type else { throw mismatch("field selection requires a composite") }
            var selected: Int?
            for index in descriptor.columns.indices where descriptor.columns[index].name.utf8.elementsEqual(name.utf8) {
                guard selected == nil else { throw mismatch("ambiguous composite field") }
                selected = index
            }
            guard let selected else { throw mismatch("unknown composite field") }
            let field = descriptor.columns[selected]
            return node(expression, field.type, child.isNullable || field.isNullable, [child], fieldOrdinal: selected)
        case .fieldExpansion(let operand):
            let child = try childBinder(.operand, 0).bind(operand)
            guard case .sql(.composite) = child.type else { throw mismatch("field expansion requires a composite") }
            return node(expression, child.type, child.isNullable, [child])
        case .collate(let operand, let collation):
            let child = try childBinder(.operand, 0).bind(operand, contextualType: contextualType)
            guard Self.isText(child.type) else { throw mismatch("COLLATE requires character input") }
            return node(expression, child.type, child.isNullable, [child], collation: collation)
        case .arraySubquery: return try bindSelectedExpression(expression, contextualType: contextualType, parameterContext: parameterContext)
        case .literal(let literal):
            let type = try literalType(literal, contextualType: contextualType)
            if case .sqlUnknownString(let text) = literal {
                if Self.isCollection(type), case .sql(let target) = type {
                    try validateCollectionText(text, to: type)
                    let input = node(.literal(.queryValue(.primitive(.string(text)), type: .sql(.text))), .sql(.text), false)
                    return CanonicalBoundExpression(expression: .cast(input.expression, targetType: target), type: type,
                        isNullable: false, children: [input], occurrence: currentOccurrence, castContext: .implicit)
                }
                if let target = Self.temporalType(type) {
                    let input = node(.literal(.queryValue(.primitive(.string(text)), type: .sql(.text))), .sql(.text), false)
                    return CanonicalBoundExpression(expression: .cast(input.expression, targetType: target), type: type,
                        isNullable: false, children: [input], occurrence: currentOccurrence, castContext: .implicit)
                }
                return node(.literal(.queryValue(try unknownString(text, to: type), type: type)), type, false)
            }
            return node(expression, type, literal.isNull)
        case .column(let reference):
            do {
                let slot = try resolveColumn(reference)
                let column = try column(at: slot)
                return CanonicalBoundExpression(
                    expression: expression, type: column.type,
                    isNullable: column.isNullable, columnSlot: slot, occurrence: currentOccurrence, bindingMetadata: bindingMetadata
                )
            } catch let error as DatabaseExpressionEvaluationError {
                guard case .missingColumn = error, reference.table == nil, let resolveWholeRow else { throw error }
                let selected = try resolveWholeRow(reference.column)
                return node(expression, .sql(.composite(selected.type)), true, wholeRow: selected)
            }
        case .parameter(let reference):
            let parameter = try parameterState.resolve(reference, contextualType: contextualType,
                context: contextualType == nil ? parameterContext : .required,
                occurrence: currentOccurrence, root: root)
            let type = try literalType(
                .queryValue(parameter.value, type: parameter.type),
                contextualType: contextualType
            )
            return node(.literal(.queryValue(parameter.value, type: type)), type, parameter.value.isNull)
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
             .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
             .modulo(let lhs, let rhs):
            if case .subtract = expression, let selected = try bindCollectionBinary(expression, left: lhs, right: rhs) { return selected }
            if Self.isUntypedLiteral(lhs), Self.isUntypedLiteral(rhs) {
                throw DatabaseExpressionEvaluationError.ambiguousOperator("arithmetic on two untyped literals")
            }
            var known: [CanonicalBoundExpression?] = []
            if let temporal = try bindTemporalArithmetic(expression, operands: [lhs, rhs], inputs: &known) { return temporal }
            let inputs = try bindInputs([lhs, rhs], defaultType: .sql(.bigint), known: known)
            let type = try arithmeticType(inputs.map { $0.type })
            if case .modulo = expression, Self.numericKind(type) == .floating { throw mismatch("floating MOD") }
            let children: [CanonicalBoundExpression]
            if inputs.allSatisfy({ Self.numericKind($0.type) == .signed }) {
                children = try inputs.map { input in
                    guard let scalar = Self.sqlScalar(input.type) else { throw mismatch("integer arithmetic") }
                    return try coerce(input, to: .sql(scalar))
                }
            } else { children = try inputs.map { try coerce($0, to: type) } }
            try requirePrimitiveOperation(children, operation: "arithmetic")
            return node(expression, type, children.contains { $0.isNullable }, children)
        case .negate(let operand):
            var known: [CanonicalBoundExpression?] = []
            if let temporal = try bindTemporalArithmetic(expression, operands: [operand], inputs: &known) { return temporal }
            let child = try known[0] ?? childBinder(.operand, 0).bind(operand, contextualType: .sql(.bigint))
            try requirePrimitiveOperation([child], operation: "negation")
            let type = try arithmeticType([child.type])
            return node(expression, type, child.isNullable, [try coerce(child, to: type)])
        case .equal(let lhs, let rhs), .notEqual(let lhs, let rhs),
             .lessThan(let lhs, let rhs), .lessThanOrEqual(let lhs, let rhs),
             .greaterThan(let lhs, let rhs), .greaterThanOrEqual(let lhs, let rhs):
            let children = try bindComparison([lhs, rhs])
            let mode = Self.comparisonMode([lhs, rhs])
            return node(expression, .sql(.boolean), try comparisonNullable(children, mode: mode), children,
                comparisonMode: mode)
        case .and(let lhs, let rhs), .or(let lhs, let rhs):
            let children = try [childBinder(.operand, 0).boolean(lhs), childBinder(.operand, 1).boolean(rhs)]
            return node(expression, .sql(.boolean), children.contains { $0.isNullable }, children)
        case .not(let operand):
            let child = try childBinder(.operand, 0).boolean(operand)
            return node(expression, .sql(.boolean), child.isNullable, [child])
        case .isNull(let operand), .isNotNull(let operand):
            return node(expression, .sql(.boolean), false, [try childBinder(.operand, 0).bind(operand)])
        case .patternMatch(let subject, let pattern, let escape, let kind):
            if escape != nil, kind == .regularExpression || kind == .caseInsensitiveRegularExpression {
                throw mismatch("regular expression ESCAPE")
            }
            let subjectInput = try childBinder(.operand, 0).bind(subject, contextualType: .sql(.text))
            guard Self.isText(subjectInput.type) else { throw mismatch("pattern subject") }
            let selectedSubject: CanonicalBoundExpression
            if case .sql(.char) = subjectInput.type { selectedSubject = subjectInput }
            else { selectedSubject = try coerce(subjectInput, to: .sql(.text)) }
            var children = [selectedSubject, try selectedText(pattern, binder: childBinder(.argument, 0))]
            if let escape { children.append(try selectedText(escape, binder: childBinder(.argument, 1))) }
            return node(expression, .sql(.boolean), children.contains { $0.isNullable }, children)
        case .concatenate(let lhs, let rhs):
            if let selected = try bindCollectionBinary(expression, left: lhs, right: rhs) { return selected }
            let operands = [lhs, rhs]
            let known: [CanonicalBoundExpression?] = try operands.indices.map { index in
                try isContextualUnknown(operands[index]) ? nil : childBinder(.operand, index).bind(operands[index])
            }
            // bytea || bytea: an unknown side takes the known bytea type.
            if known.contains(where: { $0 != nil }), known.allSatisfy({ $0.map { Self.isBytes($0.type) } ?? true }) {
                let children = try operands.indices.map { index -> CanonicalBoundExpression in
                    guard let input = known[index] else {
                        return try childBinder(.operand, index).bind(operands[index], contextualType: .sql(.blob))
                    }
                    return try coerce(input, to: .sql(.blob))
                }
                return node(expression, .sql(.blob), children.contains { $0.isNullable }, children)
            }
            // text || anynonarray: an unknown side is text and the other side's
            // value is rendered as text; two non-text operands have no operator.
            guard known.contains(where: { $0.map { Self.isText($0.type) } ?? true }) else {
                throw DatabaseExpressionEvaluationError.unsupportedFunction("||")
            }
            let children = try operands.indices.map { index -> CanonicalBoundExpression in
                guard let input = known[index] else {
                    return try childBinder(.operand, index).bind(operands[index], contextualType: .sql(.text))
                }
                if Self.isText(input.type) { return try coerce(input, to: .sql(.text)) }
                guard Self.arrayElement(input.type) == nil else { throw mismatch("concatenation") }
                return try textOutput(input)
            }
            return node(expression, .sql(.text), children.contains { $0.isNullable }, children)
        case .between(let operand, let low, let high):
            let children = try bindComparison([operand, low, high])
            let mode = Self.comparisonMode([operand, low, high])
            return node(expression, .sql(.boolean), try comparisonNullable(children, mode: mode), children,
                comparisonMode: mode)
        case .inList(let operand, let values), .notInList(let operand, let values):
            if allowingSPARQLFunctions, values.contains(where: Self.isSPARQLProducer) {
                let child = try childBinder(.operand, 0).bind(operand)
                var others: [CanonicalBoundExpression] = []
                var common = child.type
                for (index, value) in values.enumerated() {
                    let scoped = childBinder(.argument, index)
                    if Self.isSPARQLProducer(value) { try scoped.bindSPARQLProducer(value) }
                    else {
                        let bound = try scoped.bind(value, contextualType: child.type)
                        do { common = try commonType([common, bound.type]) }
                        catch let cause as DatabaseExpressionEvaluationError { throw located(cause, at: bound.occurrence) }
                        others.append(bound)
                    }
                }
                try requirePrimitiveOperation([child] + others, operation: "IN")
                return node(expression, .sql(.boolean), true)
            }
            let addresses = [childBinder(.operand, 0).currentOccurrence] + values.indices.map { childBinder(.argument, $0).currentOccurrence }
            let children = try bindComparison([operand] + values, addresses: addresses)
            let mode = Self.comparisonMode([operand] + values)
            return node(expression, .sql(.boolean), try !values.isEmpty && comparisonNullable(children, mode: mode), children,
                comparisonMode: mode)
        case .caseWhen(let operand, let pairs, let fallback):
            let conditions: [CanonicalBoundExpression]
            if let operand {
                var inferredMatches: [Int: CanonicalBoundExpression] = [:]
                var operandContext = QueryColumnType.sql(.text)
                if try isContextualUnknown(operand) {
                    for (index, pair) in pairs.enumerated() where try !isContextualUnknown(pair.condition) {
                        let match = try childBinder(.caseCondition, index).bind(pair.condition)
                        inferredMatches[index] = match
                        operandContext = match.type
                        break
                    }
                }
                let original = try childBinder(.operand, 0).bind(operand, contextualType: operandContext)
                let comparisons = try pairs.enumerated().map { index, pair in
                    let scoped = childBinder(.caseCondition, index)
                    let match = try inferredMatches[index] ?? scoped.bind(pair.condition, contextualType: original.type)
                    do {
                        let selected = try selectComparisonInputs([original, match])
                        let mode = Self.comparisonMode([original.expression, match.expression])
                        return scoped.node(.equal(original.expression, match.expression), .sql(.boolean),
                            try scoped.comparisonNullable(selected, mode: mode), selected, comparisonMode: mode)
                    } catch let cause as DatabaseExpressionEvaluationError {
                        throw located(cause, at: match.occurrence)
                    }
                }
                conditions = [original] + comparisons
            } else {
                conditions = try pairs.enumerated().map { try childBinder(.caseCondition, $0.offset).boolean($0.element.condition) }
            }
            // PostgreSQL considers ELSE first when choosing a conditional common type.
            let results = [fallback ?? .literal(.null)] + pairs.map { $0.result }
            let addresses = [childBinder(.caseElse).currentOccurrence] + pairs.indices.map { childBinder(.caseResult, $0).currentOccurrence }
            let ordered = try bindCommon(results, defaultType: contextualType ?? .sql(.text), addresses: addresses)
            let branches = Array(ordered.dropFirst()) + [ordered[0]]
            return node(expression, ordered[0].type, branches.contains { $0.isNullable }, conditions + branches)
        case .coalesce(let expressions):
            guard !expressions.isEmpty else { throw mismatch("COALESCE") }
            let children = try bindCommon(expressions, defaultType: contextualType ?? .sql(.text), field: .argument)
            return node(expression, try commonType(children.map { $0.type }), children.allSatisfy { $0.isNullable }, children)
        case .nullIf(let lhs, let rhs):
            let children = try bindComparison([lhs, rhs])
            return node(expression, children[0].type, true, children)
        case .cast(let operand, let target):
            let type = QueryColumnType.sql(target)
            do { try validate(type) }
            catch let cause as DatabaseExpressionEvaluationError { throw located(cause, at: childBinder(.castType).currentOccurrence) }
            let inputType: QueryColumnType
            if case .decimal = target { inputType = .sql(.decimal(precision: nil, scale: nil)) }
            else if Self.arrayElement(type) != nil, case .array = operand { inputType = type }
            else if Self.compositeType(type) != nil { inputType = type }
            else if Self.isText(type) || Self.temporalType(type) != nil || Self.isCollection(type) { inputType = .sql(.text) }
            else { inputType = type }
            let child = try childBinder(.operand, 0).bind(operand, contextualType: inputType)
            do { try validateCast(from: child.type, to: type) }
            catch let cause as DatabaseExpressionEvaluationError { throw located(cause, at: childBinder(.castType).currentOccurrence) }
            if Self.isCollection(type), let text = Self.constantText(child.expression) { try validateCollectionText(text, to: type) }
            if Self.isStaticallyNull(child.expression) {
                return node(.literal(.queryValue(.primitive(.null), type: type)), type, true)
            }
            if child.type == type, type != .sql(.char(length: nil)), Self.temporalType(type) == nil { return child }
            return node(expression, type, child.isNullable, [child])
        case .defaultValue:
            throw DatabaseExpressionEvaluationError.unsupportedExpression("DEFAULT requires an admitted mutation position")
        case .function(let function):
            return try bindFunction(function, expression: expression)
        case .aggregate(let aggregate):
            guard permitsAggregates else { throw DatabaseExpressionEvaluationError.groupingViolation }
            return try bindAggregate(aggregate, expression: expression)
        case .window(let call):
            guard permitsWindows else { throw DatabaseExpressionEvaluationError.windowingViolation }
            return try bindWindow(call, expression: expression)
        case .grouping(let arguments):
            guard permitsAggregates, !arguments.isEmpty, arguments.count <= 31 else {
                throw DatabaseExpressionEvaluationError.groupingViolation
            }
            var scoped = self
            scoped.permitsAggregates = false
            scoped.permitsWindows = false
            let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[CanonicalBoundExpression]>.stride), at: .bindingCandidate)
            var successful = false
            defer { if !successful { claim?.release() } }
            var children: [CanonicalBoundExpression] = []
            for (index, argument) in arguments.enumerated() {
                try CanonicalBindingStorage.appendGrowing(scoped.childBinder(.argument, index).bind(argument),
                    to: &children, reservation: claim)
            }
            try bindingMetadata.retain(claim)
            successful = true
            return node(expression, .sql(.integer), false, children)
        case .subquery: return try bindSelectedExpression(expression, contextualType: contextualType, parameterContext: parameterContext)
        case .exists: return try bindSelectedExpression(expression, contextualType: contextualType, parameterContext: parameterContext)
        case .inSubquery(let operand, let query):
            return try bindSubqueryComparison(operand, query: query, expression: expression)
        case .variable, .bound, .regex, .triple, .isTriple, .subject, .predicate, .object:
            throw DatabaseExpressionEvaluationError.unsupportedExpression("SQL binding does not admit this expression")
        }
    }

    private func comparisonNullable(_ children: [CanonicalBoundExpression],
        mode: CanonicalSQLCompositeComparisonMode) throws -> Bool {
        if children.contains(where: { $0.isNullable }) { return true }
        guard mode == .constructor else { return false }
        // A present ROW can yield UNKNOWN through one of its direct NULL fields.
        // Nested records retain their separate null-safe record comparison policy.
        for child in children {
            guard let descriptor = Self.compositeType(child.type) else { continue }
            for field in descriptor.columns {
                try workMeter?.consume(at: .bindingCandidate)
                if field.isNullable { return true }
            }
        }
        return false
    }

    private static func comparisonMode(_ expressions: [Expression]) -> CanonicalSQLCompositeComparisonMode {
        let constructor = expressions.contains { if case .row = $0 { return true }; return false }
        return constructor && expressions.allSatisfy {
            switch $0 { case .row, .subquery: return true; default: return false }
        } ? .constructor : .record
    }

    private static func constantText(_ expression: Expression) -> String? {
        switch expression {
        case .literal(.string(let text)), .literal(.sqlUnknownString(let text)),
             .literal(.queryValue(.primitive(.string(let text)), _)): return text
        default: return nil
        }
    }

    private func validateCollectionText(_ text: String, to type: QueryColumnType) throws {
        let structural = QueryStructuralLimits.default
        let limits = workMeter.map(CanonicalSQLScalarCast.collectionLimits) ?? QueryValueLimits(
            maximumRetainedBytes: structural.maximumRetainedBytes, maximumTotalNodes: structural.maximumTotalNodes,
            maximumNestingDepth: structural.maximumNestingDepth, maximumCollectionElements: structural.maximumCollectionElements)
        if Self.isJSON(type) {
            if let workMeter {
                let result = try CanonicalSQLJSONEvaluator.cast(.primitive(.string(text)), from: .sql(.text), to: type,
                    context: .explicit, temporalEnvironment: nil, workMeter: workMeter, limits: limits)
                withExtendedLifetime(result) {}
            } else {
                let json = try QueryJSONValue(bytes: ByteString(utf8: text), limits: limits)
                if type == .sql(.jsonb) { _ = try QueryJSONBValue(json: json, limits: limits) }
            }
            return
        }
        guard let element = Self.arrayElement(type) else { return }
        func validate(_ input: borrowing QueryArrayInput) throws {
            for index in 0..<input.count {
                // The public requested-cell API can materialize at most the complete input.
                let claim = try workMeter?.reserveIntermediate(bytes: UInt64(text.utf8.count), at: .bindingCandidate)
                defer { claim?.release() }
                guard let value = try input.elementText(at: index) else { continue }
                if Self.temporalType(element) != nil { continue }
                if Self.isCollection(element) { try validateCollectionText(value, to: element) }
                else { _ = try unknownString(value, to: element) }
            }
        }
        do {
            if let workMeter {
                try SQLArrayInputWorkspace.withInput(sqlLexicalForm: text, limits: limits, workMeter: workMeter, validate)
            } else { try validate(QueryArrayInput(sqlLexicalForm: text, limits: limits)) }
        } catch QueryValueError.malformedArrayInput(let offset, let reason) {
            throw DatabaseExpressionEvaluationError.collection(.invalidArrayInput(offset: offset, reason: reason))
        }
    }

    static func arrayElement(_ type: QueryColumnType) -> QueryColumnType? {
        switch type {
        case .sql(.array(let element)): return .sql(element)
        case .primitive(let kind, true):
            guard let scalar = sqlScalar(.primitive(kind)) else { return nil }
            return .sql(scalar)
        default: return nil
        }
    }

    static func isJSON(_ type: QueryColumnType) -> Bool { type == .sql(.json) || type == .sql(.jsonb) }
    static func compositeType(_ type: QueryColumnType) -> QueryCompositeType? {
        if case .sql(.composite(let descriptor)) = type { return descriptor }
        return nil
    }

    static func isCollection(_ type: QueryColumnType) -> Bool { arrayElement(type) != nil || isJSON(type) }

    private func sqlElement(_ type: QueryColumnType) throws -> DataType {
        if case .sql(let type) = type { return type }
        guard let scalar = Self.sqlScalar(type) else { throw mismatch("SQL array element type") }
        return scalar
    }

    private func bindArrayConstructor(_ elements: [Expression], expression: Expression,
        contextualType: QueryColumnType?) throws -> CanonicalBoundExpression {
        var known: [[Int]: CanonicalBoundExpression] = [:]
        var leaves: [QueryColumnType] = []
        func scan(_ items: [Expression], scope: Self, path: [Int]) throws {
            for (index, item) in items.enumerated() {
                let child = scope.childBinder(.argument, index), address = path + [index]
                if case .array(let nested) = item { try scan(nested, scope: child, path: address) }
                else if try !child.isContextualUnknown(item) {
                    let bound = try child.bind(item)
                    known[address] = bound
                    leaves.append(Self.arrayElement(bound.type) ?? bound.type)
                }
            }
        }
        try scan(elements, scope: self, path: [])
        let contextualElement = contextualType.flatMap(Self.arrayElement)
        guard !elements.isEmpty || contextualElement != nil else { throw mismatch("empty ARRAY requires an element declaration") }
        let common = try leaves.isEmpty ? (contextualElement ?? .sql(.text)) : commonType(leaves)
        let element = try sqlElement(common), scalar = QueryColumnType.sql(element), array = QueryColumnType.sql(.array(element))
        func build(_ items: [Expression], scope: Self, path: [Int], expression: Expression) throws -> CanonicalBoundExpression {
            let nested = items.enumerated().contains { index, item in
                if case .array = item { return true }
                return known[path + [index]].flatMap { Self.arrayElement($0.type) } != nil
            }
            let children = try items.enumerated().map { index, item in
                let child = scope.childBinder(.argument, index), address = path + [index]
                if case .array(let values) = item { return try build(values, scope: child, path: address, expression: item) }
                let target = nested ? array : scalar
                let selected = try known[address] ?? child.bind(item, contextualType: target)
                return try scope.coerce(selected, to: target)
            }
            return scope.node(expression, array, false, children)
        }
        return try build(elements, scope: self, path: [], expression: expression)
    }

    private func bindArrayAccess(_ base: Expression, subscripts: [ArraySubscript],
        expression: Expression) throws -> CanonicalBoundExpression {
        let boundArray = try childBinder(.operand).bind(base)
        guard let element = Self.arrayElement(boundArray.type), !subscripts.isEmpty else { throw mismatch("array access") }
        let array = try coerce(boundArray, to: .sql(.array(try sqlElement(element))))
        var children = [array]
        var slicing = false
        func append(_ value: Expression, address: Int) throws {
            let child = try childBinder(.argument, address).bind(value, contextualType: .sql(.integer))
            children.append(try coerce(child, to: .sql(.integer)))
        }
        for (index, dimension) in subscripts.enumerated() {
            switch dimension {
            case .index(let value): try append(value, address: 2 * index)
            case .slice(let lower, let upper):
                slicing = true
                if let lower { try append(lower, address: 2 * index) }
                if let upper { try append(upper, address: 2 * index + 1) }
            }
        }
        return node(expression, slicing ? .sql(.array(try sqlElement(element))) : element, true, children)
    }

    private func collectionArguments(_ arguments: [Expression], targets: [QueryColumnType]) throws -> [CanonicalBoundExpression] {
        guard arguments.count == targets.count else { throw mismatch("collection arity") }
        return try arguments.indices.map { index in
            let child = try childBinder(.argument, index).bind(arguments[index], contextualType: targets[index])
            return try coerce(child, to: targets[index])
        }
    }

    private func polymorphicArrayArguments(_ arguments: [Expression], arraySlots: Set<Int>,
        elementSlots: Set<Int>, textSlots: Set<Int> = []) throws -> (children: [CanonicalBoundExpression], element: DataType) {
        var known: [Int: CanonicalBoundExpression] = [:], types: [QueryColumnType] = []
        for index in arraySlots.union(elementSlots).sorted() {
            let value = arguments[index]
            if case .array(let elements) = value, elements.isEmpty { continue }
            if try isContextualUnknown(value) { continue }
            let child = try childBinder(.argument, index).bind(value)
            known[index] = child
            if arraySlots.contains(index) {
                guard let element = Self.arrayElement(child.type) else { throw mismatch("array argument") }
                types.append(element)
            } else { types.append(child.type) }
        }
        let element = try sqlElement(commonType(types)), scalar = QueryColumnType.sql(element), array = QueryColumnType.sql(.array(element))
        let children = try arguments.indices.map { index in
            let target: QueryColumnType
            if arraySlots.contains(index) { target = array }
            else if elementSlots.contains(index) { target = scalar }
            else if textSlots.contains(index) { target = .sql(.text) }
            else { target = .sql(.integer) }
            let child = try known[index] ?? childBinder(.argument, index).bind(arguments[index], contextualType: target)
            return try coerce(child, to: target)
        }
        return (children, element)
    }

    private func bindCollectionBinary(_ expression: Expression, left: Expression, right: Expression) throws -> CanonicalBoundExpression? {
        func emptyArray(_ expression: Expression) -> Bool { if case .array(let values) = expression { return values.isEmpty }; return false }
        let lhs = try isContextualUnknown(left) || emptyArray(left) ? nil : childBinder(.operand, 0).bind(left)
        let rhs = try isContextualUnknown(right) || emptyArray(right) ? nil : childBinder(.operand, 1).bind(right)
        let jsonConcatenation: Bool
        if case .concatenate = expression { jsonConcatenation = lhs?.type == .sql(.jsonb) || rhs?.type == .sql(.jsonb) }
        else { jsonConcatenation = false }
        if lhs?.type == .sql(.jsonb) || jsonConcatenation {
            let target: QueryColumnType
            if case .concatenate = expression { target = .sql(.jsonb) }
            else {
                let signatures: [[DataType]] = [[.text], [.integer], [.array(.text)]]
                target = .sql(try selectFunction(signatures, inputs: [rhs], name: "JSONB deletion")[0])
            }
            let children = try [coerce(lhs ?? childBinder(.operand, 0).bind(left, contextualType: .sql(.jsonb)), to: .sql(.jsonb)), coerce(rhs ?? childBinder(.operand, 1).bind(right, contextualType: target), to: target)]
            return node(expression, .sql(.jsonb), children.contains { $0.isNullable }, children)
        }
        guard case .concatenate = expression else { return nil }
        guard lhs.map({ Self.arrayElement($0.type) != nil }) == true || rhs.map({ Self.arrayElement($0.type) != nil }) == true || emptyArray(left) || emptyArray(right) else { return nil }
        guard lhs != nil || rhs != nil else { throw mismatch("empty ARRAY requires an element declaration") }
        let leftArray = lhs.map { Self.arrayElement($0.type) != nil } ?? true
        let rightArray = rhs.map { Self.arrayElement($0.type) != nil } ?? true
        let types = [lhs, rhs].compactMap { $0 }.map { Self.arrayElement($0.type) ?? $0.type }
        let element = try sqlElement(commonType(types)), array = QueryColumnType.sql(.array(element))
        let targets: [QueryColumnType] = [leftArray ? array : .sql(element), rightArray ? array : .sql(element)]
        let children = try [lhs, rhs].enumerated().map { index, known in
            try coerce(known ?? childBinder(.operand, index).bind(index == 0 ? left : right, contextualType: targets[index]), to: targets[index])
        }
        return node(expression, array, leftArray && rightArray && children.allSatisfy { $0.isNullable }, children)
    }

    private func bindCollectionOperation(_ operation: CollectionOperator, left: Expression,
        right: Expression, expression: Expression) throws -> CanonicalBoundExpression {
        let lhs = try isContextualUnknown(left) ? nil : childBinder(.left).bind(left)
        let rhs = try isContextualUnknown(right) ? nil : childBinder(.right).bind(right)
        let targets: [QueryColumnType], result: QueryColumnType
        switch operation {
        case .contains, .containedBy, .overlaps:
            if operation != .overlaps, lhs?.type == .sql(.jsonb) || rhs?.type == .sql(.jsonb) {
                targets = [.sql(.jsonb), .sql(.jsonb)]
            } else {
                let known = [lhs, rhs].compactMap { $0 }
                guard !known.isEmpty, known.allSatisfy({ Self.arrayElement($0.type) != nil }) else { throw mismatch("array collection predicate") }
                let element = try sqlElement(commonType(known.compactMap { Self.arrayElement($0.type) }))
                try requireSQLComparison(.sql(element), operation: "array collection predicate")
                targets = [.sql(.array(element)), .sql(.array(element))]
            }
            result = .sql(.boolean)
        case .jsonField, .jsonFieldText, .jsonPath, .jsonPathText:
            guard let json = lhs?.type, Self.isJSON(json) else { throw mismatch("JSON extraction source") }
            let field: QueryColumnType
            if operation == .jsonField || operation == .jsonFieldText {
                field = .sql(try selectFunction([[.text], [.integer]], inputs: [rhs], name: "JSON field")[0])
            } else { field = .sql(.array(.text)) }
            targets = [json, field]
            result = operation == .jsonFieldText || operation == .jsonPathText ? .sql(.text) : json
        case .jsonHasKey: targets = [.sql(.jsonb), .sql(.text)]; result = .sql(.boolean)
        case .jsonHasAnyKey, .jsonHasAllKeys: targets = [.sql(.jsonb), .sql(.array(.text))]; result = .sql(.boolean)
        case .jsonDeletePath: targets = [.sql(.jsonb), .sql(.array(.text))]; result = .sql(.jsonb)
        }
        let children = try [lhs, rhs].enumerated().map { index, known in
            try coerce(known ?? childBinder(index == 0 ? .left : .right).bind(index == 0 ? left : right, contextualType: targets[index]), to: targets[index])
        }
        return node(expression, result, true, children)
    }

    private func bindCollectionFunction(_ call: FunctionCall, expression: Expression) throws -> CanonicalBoundExpression? {
        let name = call.name, args = call.arguments
        let arrayFunctions = ["array_length", "array_lower", "array_upper", "array_ndims", "array_dims", "cardinality", "array_append", "array_prepend", "array_cat", "array_position", "array_positions", "array_remove", "array_replace", "array_to_string", "string_to_array"]
        let jsonFunctions = ["row_to_json", "to_json", "to_jsonb", "array_to_json", "json_build_array", "jsonb_build_array", "json_build_object", "jsonb_build_object", "json_typeof", "jsonb_typeof", "json_array_length", "jsonb_array_length", "json_extract_path", "jsonb_extract_path", "json_extract_path_text", "jsonb_extract_path_text", "jsonb_set", "jsonb_insert", "jsonb_strip_nulls"]
        if ["generate_series", "unnest", "generate_subscripts", "json_each", "jsonb_each", "json_each_text", "jsonb_each_text", "json_array_elements", "jsonb_array_elements", "json_array_elements_text", "jsonb_array_elements_text", "json_object_keys", "jsonb_object_keys"].contains(name) {
            throw DatabaseExpressionEvaluationError.unsupportedExpression("set-returning function requires a FROM source")
        }
        guard arrayFunctions.contains(name) || jsonFunctions.contains(name) else { return nil }
        let builders = ["json_build_array", "jsonb_build_array", "json_build_object", "jsonb_build_object"]
        let path = ["json_extract_path", "jsonb_extract_path", "json_extract_path_text", "jsonb_extract_path_text"].contains(name)
        guard !call.finalArrayIsVariadic || builders.contains(name) || path else { throw mismatch("unsupported VARIADIC signature") }
        if builders.contains(name) {
            guard !call.finalArrayIsVariadic || !args.isEmpty else { throw mismatch("VARIADIC array argument") }
            var children: [CanonicalBoundExpression] = []
            for (index, value) in args.enumerated() {
                let target: QueryColumnType = call.finalArrayIsVariadic && index == args.count - 1 ? .sql(.array(.text)) : .sql(.text)
                var child = try childBinder(.argument, index).bind(value, contextualType: target)
                if call.finalArrayIsVariadic && index == args.count - 1 {
                    guard let element = Self.arrayElement(child.type) else { throw mismatch("VARIADIC array argument") }
                    child = try coerce(child, to: .sql(.array(try sqlElement(element))))
                } else if !Self.isCollection(child.type) && Self.compositeType(child.type) == nil && !Self.permitsScalarText(child.type) { throw mismatch("JSON value conversion") }
                children.append(child)
            }
            if name.hasSuffix("object"), !call.finalArrayIsVariadic {
                guard children.count.isMultiple(of: 2) else { throw DatabaseExpressionEvaluationError.collection(.oddJSONObjectArgumentCount(actual: children.count)) }
                for index in stride(from: 0, to: children.count, by: 2) {
                    guard !Self.isCollection(children[index].type), Self.compositeType(children[index].type) == nil, Self.permitsScalarText(children[index].type) else {
                        throw DatabaseExpressionEvaluationError.collection(.invalidJSONObjectKey(argument: index + 1))
                    }
                }
            }
            return node(expression, .sql(name.hasPrefix("jsonb") ? .jsonb : .json), call.finalArrayIsVariadic && children.last!.isNullable, children)
        }
        if path {
            guard !args.isEmpty, !call.finalArrayIsVariadic || args.count >= 2 else { throw mismatch(name) }
            let family: QueryColumnType = .sql(name.hasPrefix("jsonb") ? .jsonb : .json)
            let targets = args.indices.map { index in index == 0 ? family : (call.finalArrayIsVariadic && index == args.count - 1 ? .sql(.array(.text)) : .sql(.text)) }
            return node(expression, name.hasSuffix("text") ? .sql(.text) : family, true, try collectionArguments(args, targets: targets))
        }
        if arrayFunctions.contains(name) {
            if name == "string_to_array" {
                guard (2...3).contains(args.count) else { throw mismatch(name) }
                let children = try collectionArguments(args, targets: Array(repeating: .sql(.text), count: args.count))
                return node(expression, .sql(.array(.text)), children[0].isNullable, children)
            }
            let arraySlots: Set<Int>, elementSlots: Set<Int>, counts: ClosedRange<Int>
            switch name {
            case "array_prepend": arraySlots = [1]; elementSlots = [0]; counts = 2...2
            case "array_cat": arraySlots = [0, 1]; elementSlots = []; counts = 2...2
            case "array_append", "array_positions", "array_remove": arraySlots = [0]; elementSlots = [1]; counts = 2...2
            case "array_position": arraySlots = [0]; elementSlots = [1]; counts = 2...3
            case "array_replace": arraySlots = [0]; elementSlots = [1, 2]; counts = 3...3
            case "array_length", "array_lower", "array_upper": arraySlots = [0]; elementSlots = []; counts = 2...2
            case "array_to_string": arraySlots = [0]; elementSlots = []; counts = 2...3
            default: arraySlots = [0]; elementSlots = []; counts = 1...1
            }
            guard counts.contains(args.count) else { throw mismatch(name) }
            if elementSlots.isEmpty, name != "array_cat", try isContextualUnknown(args[0]) { throw mismatch("unresolved polymorphic array") }
            let selected = try polymorphicArrayArguments(args, arraySlots: arraySlots, elementSlots: elementSlots,
                textSlots: name == "array_to_string" ? Set(args.indices.dropFirst()) : [])
            if ["array_position", "array_positions", "array_remove", "array_replace"].contains(name) { try requireSQLComparison(.sql(selected.element), operation: name) }
            let result: DataType
            switch name {
            case "array_dims", "array_to_string": result = .text
            case "array_positions": result = .array(.integer)
            case "array_append", "array_prepend", "array_cat", "array_remove", "array_replace": result = .array(selected.element)
            default: result = .integer
            }
            let nullable: Bool
            switch name {
            case "array_append", "array_prepend": nullable = false
            case "array_cat": nullable = selected.children.allSatisfy { $0.isNullable }
            case "array_remove", "array_replace", "array_positions", "cardinality": nullable = selected.children[0].isNullable
            case "array_to_string": nullable = selected.children[0].isNullable || selected.children[1].isNullable
            default: nullable = true
            }
            return node(expression, .sql(result), nullable, selected.children)
        }
        let binaryJSON = name.hasPrefix("jsonb") || name == "to_jsonb"
        let family: QueryColumnType = .sql(binaryJSON ? .jsonb : .json)
        if name == "to_json" || name == "to_jsonb" || name == "array_to_json" || name == "row_to_json" {
            guard args.count == 1 || (name == "array_to_json" || name == "row_to_json") && args.count == 2 else { throw mismatch(name) }
            guard try !isContextualUnknown(args[0]) else { throw mismatch("unresolved polymorphic JSON conversion") }
            let input = try childBinder(.argument, 0).bind(args[0], contextualType: .sql(.text))
            guard Self.isCollection(input.type) || Self.compositeType(input.type) != nil || Self.permitsScalarText(input.type) else { throw mismatch(name) }
            if name == "array_to_json", Self.arrayElement(input.type) == nil { throw mismatch(name) }
            if name == "row_to_json", Self.compositeType(input.type) == nil { throw mismatch(name) }
            var children = [input]
            if args.count == 2 { children.append(try coerce(childBinder(.argument, 1).bind(args[1], contextualType: .sql(.boolean)), to: .sql(.boolean))) }
            return node(expression, family, children.contains { $0.isNullable }, children)
        }
        var arguments = args
        if name == "jsonb_set" || name == "jsonb_insert", arguments.count == 3 { arguments.append(.literal(.bool(name == "jsonb_set"))) }
        if name == "jsonb_strip_nulls", arguments.count == 1 { arguments.append(.literal(.bool(false))) }
        let targets: [QueryColumnType]
        if name == "jsonb_set" || name == "jsonb_insert" { targets = [.sql(.jsonb), .sql(.array(.text)), .sql(.jsonb), .sql(.boolean)] }
        else if name == "jsonb_strip_nulls" { targets = [.sql(.jsonb), .sql(.boolean)] }
        else { targets = [family] }
        let children = try collectionArguments(arguments, targets: targets)
        let result: QueryColumnType = name.hasSuffix("typeof") ? .sql(.text) : (name.hasSuffix("array_length") ? .sql(.integer) : family)
        return node(expression, result, children.contains { $0.isNullable }, children)
    }

    static func temporalType(_ type: QueryColumnType) -> DataType? {
        switch type {
        case .sql(.date), .primitive(.date, false): return .date
        case .sql(.time(let zone, _)): return .time(withTimeZone: zone)
        case .primitive(.time, false): return .time(withTimeZone: false)
        case .sql(.timestamp(let zone, _)): return .timestamp(withTimeZone: zone)
        case .primitive(.dateTime, false): return .timestamp(withTimeZone: false)
        case .primitive(.timestamp, false): return .timestamp(withTimeZone: true)
        case .sql(.interval): return .interval()
        default: return nil
        }
    }

    private static func temporalCastAllowed(from source: DataType, to target: DataType, context: CastContext) -> Bool {
        if source == target { return true }
        let implicit: Bool
        switch (source, target) {
        case (.date, .timestamp), (.time(false, _), .time(true, _)),
             (.time(false, _), .interval), (.timestamp(false, _), .timestamp(true, _)): implicit = true
        default: implicit = false
        }
        if implicit { return true }
        guard context != .implicit else { return false }
        switch (source, target) {
        case (.timestamp(false, _), .date), (.timestamp(false, _), .time(false, _)),
             (.timestamp(true, _), .date), (.timestamp(true, _), .time),
             (.timestamp(true, _), .timestamp(false, _)), (.interval, .time(false, _)),
             (.time(true, _), .time(false, _)): return true
        default: return false
        }
    }

    private func bindTemporalFunction(_ function: FunctionCall, expression: Expression) throws -> CanonicalBoundExpression? {
        let text = DataType.text, integer = DataType.integer, double = DataType.doublePrecision
        let time = DataType.time(withTimeZone: false), timetz = DataType.time(withTimeZone: true)
        let timestamp = DataType.timestamp(withTimeZone: false), instant = DataType.timestamp(withTimeZone: true)
        let interval = DataType.interval()
        let name = function.name
        var arguments = function.arguments
        let signatures: [[DataType]]
        switch name {
        case "now", "transaction_timestamp", "statement_timestamp", "clock_timestamp": signatures = [[]]
        case "extract", "date_part": signatures = [.date, time, timetz, timestamp, instant, interval].map { [text, $0] }
        case "date_trunc": signatures = [[text, timestamp], [text, instant], [text, interval], [text, instant, text]]
        case "date_bin": signatures = [[interval, timestamp, timestamp], [interval, instant, instant]]
        case "age": signatures = [[timestamp], [instant], [timestamp, timestamp], [instant, instant]]
        case "make_date": signatures = [[integer, integer, integer]]
        case "make_time": signatures = [[integer, integer, double]]
        case "make_timestamp": signatures = [[integer, integer, integer, integer, integer, double]]
        case "make_timestamptz": signatures = [[integer, integer, integer, integer, integer, double], [integer, integer, integer, integer, integer, double, text]]
        case "make_interval":
            guard arguments.count <= 7 else { throw mismatch(name) }
            while arguments.count < 7 {
                arguments.append(.literal(.queryValue(arguments.count == 6 ? .primitive(.float64(0)) : .primitive(.int32(0)),
                    type: .sql(arguments.count == 6 ? double : integer))))
            }
            signatures = [[integer, integer, integer, integer, integer, integer, double]]
        case "timezone": signatures = [text, interval].flatMap { zone in [timestamp, instant, timetz].map { [zone, $0] } }
        case "to_char": signatures = [[timestamp, text], [instant, text], [interval, text]]
        case "to_date": signatures = [[text, text]]
        case "to_timestamp": signatures = [[text, text], [double]]
        default: return nil
        }
        let candidates = signatures.filter { $0.count == arguments.count }
        guard !candidates.isEmpty else { throw DatabaseExpressionEvaluationError.invalidFunctionArity(function: name) }
        let inputs: [CanonicalBoundExpression?] = try arguments.enumerated().map { index, argument in
            try isContextualUnknown(argument) ? nil : childBinder(.argument, index).bind(argument)
        }
        let selected = try selectFunction(candidates, inputs: inputs, name: name)
        let children = try arguments.indices.map { index in
            let target = QueryColumnType.sql(selected[index])
            let input = try inputs[index] ?? childBinder(.argument, index).bind(arguments[index], contextualType: target)
            return try coerce(input, to: target)
        }
        let result: DataType
        switch name {
        case "extract": result = .decimal(precision: nil, scale: nil)
        case "date_part": result = double
        case "date_trunc": result = selected[1]
        case "date_bin": result = selected[1]
        case "age", "make_interval": result = interval
        case "make_date", "to_date": result = .date
        case "make_time": result = time
        case "make_timestamp": result = timestamp
        case "timezone": result = selected[1] == timestamp ? instant : (selected[1] == instant ? timestamp : timetz)
        case "to_char": result = text
        default: result = instant
        }
        let nullable = ["extract", "date_part", "to_char"].contains(name) || children.contains { $0.isNullable }
        return node(expression, .sql(result), nullable, children)
    }

    private func bindTemporalArithmetic(_ expression: Expression, operands: [Expression],
        inputs: inout [CanonicalBoundExpression?]) throws -> CanonicalBoundExpression? {
        let date = DataType.date, integer = DataType.integer, double = DataType.doublePrecision
        let time = DataType.time(withTimeZone: false), timetz = DataType.time(withTimeZone: true)
        let timestamp = DataType.timestamp(withTimeZone: false), instant = DataType.timestamp(withTimeZone: true)
        let interval = DataType.interval()
        inputs = try operands.enumerated().map { index, operand in
            try isContextualUnknown(operand) ? nil : childBinder(.operand, index).bind(operand)
        }
        guard inputs.contains(where: { $0.map { Self.temporalType($0.type) != nil } ?? false }) else { return nil }
        var signatures: [([DataType], DataType)] = []
        switch expression {
        case .add:
            signatures = [([date, integer], date), ([date, interval], timestamp), ([date, time], timestamp), ([date, timetz], instant),
                ([timestamp, interval], timestamp), ([instant, interval], instant), ([time, interval], time), ([timetz, interval], timetz)]
            signatures += signatures.map { (Array($0.0.reversed()), $0.1) }
            signatures.append(([interval, interval], interval))
        case .subtract:
            signatures = [([date, integer], date), ([date, date], integer), ([date, interval], timestamp),
                ([timestamp, interval], timestamp), ([instant, interval], instant), ([time, interval], time), ([timetz, interval], timetz),
                ([timestamp, timestamp], interval), ([instant, instant], interval), ([time, time], interval), ([interval, interval], interval)]
        case .multiply: signatures = [([interval, double], interval), ([double, interval], interval)]
        case .divide: signatures = [([interval, double], interval)]
        case .negate: signatures = [([interval], interval)]
        default: throw mismatch("temporal arithmetic")
        }
        let selected = try selectFunction(signatures.map { $0.0 }, inputs: inputs, name: "temporal arithmetic")
        guard let result = signatures.first(where: { $0.0 == selected })?.1 else { throw mismatch("temporal arithmetic") }
        let children = try operands.indices.map { index in
            let target = QueryColumnType.sql(selected[index])
            let input = try inputs[index] ?? childBinder(.operand, index).bind(operands[index], contextualType: target)
            return try coerce(input, to: target)
        }
        return node(expression, .sql(result), children.contains { $0.isNullable }, children)
    }

    private func bindFunction(
        _ function: FunctionCall, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression {
        let name = function.name
        guard function.schema == nil || function.schema == "pg_catalog", !function.distinct else {
            throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
        }
        if let selected = try bindCollectionFunction(function, expression: expression) { return selected }
        guard !function.finalArrayIsVariadic else { throw mismatch("unsupported VARIADIC signature") }
        if name == "merge_action" {
            guard ActiveSQLStatementReads.mergeReturning, function.arguments.isEmpty else {
                throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
            }
            return node(expression, .sql(.text), false, [])
        }
        if let selected = try bindSessionFunction(function, expression: expression) { return selected }
        if let selected = try bindTemporalFunction(function, expression: expression) { return selected }
        if let selected = try bindScalarFunction(function, expression: expression) { return selected }
        if name == "width_bucket", function.arguments.count == 2 {
            let children = try bindArrayComparison(function.arguments[0], array: function.arguments[1], field: .argument, commonSelection: true)
            return node(expression, .sql(.integer), children.contains { $0.isNullable }, children)
        }
        let signatures: [[DataType]]
        let numeric = DataType.decimal(precision: nil, scale: nil)
        let six: [DataType] = [.smallint, .integer, .bigint, numeric, .real, .doublePrecision]
        switch name {
        case "abs": signatures = six.map { [$0] }
        case "sign", "ceil", "ceiling", "floor", "sqrt", "exp", "ln", "log10":
            signatures = [[numeric], [.doublePrecision]]
        case "round", "trunc":
            signatures = function.arguments.count == 2 ? [[numeric, .integer]] : [[numeric], [.doublePrecision]]
        case "log":
            signatures = function.arguments.count == 2 ? [[numeric, numeric]] : [[numeric], [.doublePrecision]]
        case "div": signatures = [[numeric, numeric]]
        case "mod": signatures = [.smallint, .integer, .bigint, numeric].map { [$0, $0] }
        case "power": signatures = [[numeric, numeric], [.doublePrecision, .doublePrecision]]
        case "gcd", "lcm": signatures = [.integer, .bigint, numeric].map { [$0, $0] }
        case "scale", "min_scale", "trim_scale": signatures = [[numeric]]
        case "cbrt", "degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan":
            signatures = [[.doublePrecision]]
        case "atan2": signatures = [[.doublePrecision, .doublePrecision]]
        case "pi": signatures = [[]]
        case "width_bucket": signatures = [[numeric, numeric, numeric, .integer], [.doublePrecision, .doublePrecision, .doublePrecision, .integer]]
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
        }
        let candidates = signatures.filter { $0.count == function.arguments.count }
        guard !candidates.isEmpty else { throw DatabaseExpressionEvaluationError.invalidFunctionArity(function: name) }
        let inputs: [CanonicalBoundExpression?] = try function.arguments.enumerated().map { index, argument in
            if try isContextualUnknown(argument) { return nil }
            return try childBinder(.argument, index).bind(argument)
        }
        let selected = try selectFunction(candidates, inputs: inputs, name: name)
        let children = try function.arguments.indices.map { index in
            let target = QueryColumnType.sql(selected[index])
            let input = try inputs[index] ?? childBinder(.argument, index).bind(function.arguments[index], contextualType: target)
            return try coerce(input, to: target)
        }
        try requirePrimitiveOperation(children, operation: name)
        let result: QueryColumnType
        if ["scale", "min_scale", "width_bucket"].contains(name) { result = .sql(.integer) }
        else { result = .sql(selected.first ?? .doublePrecision) }
        return node(expression, result, children.contains { $0.isNullable }, children)
    }

    private func bindSessionFunction(
        _ function: FunctionCall, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression? {
        let signatures: [[DataType]]
        let result: QueryColumnType
        switch function.name {
        case "nextval", "currval": signatures = [[.text]]; result = .sql(.bigint)
        case "lastval": signatures = [[]]; result = .sql(.bigint)
        case "setval": signatures = [[.text, .bigint], [.text, .bigint, .boolean]]; result = .sql(.bigint)
        case "current_schema", "current_database", "current_user", "session_user":
            signatures = [[]]; result = .sql(.text)
        case "current_schemas": signatures = [[.boolean]]; result = .sql(.array(.text))
        default: return nil
        }
        let candidates = signatures.filter { $0.count == function.arguments.count }
        guard !candidates.isEmpty else { throw DatabaseExpressionEvaluationError.invalidFunctionArity(function: function.name) }
        let inputs: [CanonicalBoundExpression?] = try function.arguments.enumerated().map { index, argument in
            if try isContextualUnknown(argument) { return nil }
            return try childBinder(.argument, index).bind(argument)
        }
        let signature = try selectFunction(candidates, inputs: inputs, name: function.name)
        let children = try function.arguments.indices.map { index in
            let target = QueryColumnType.sql(signature[index])
            let input = try inputs[index] ?? childBinder(.argument, index).bind(function.arguments[index], contextualType: target)
            return try coerce(input, to: target)
        }
        return node(expression, result, function.name == "current_schema" || children.contains { $0.isNullable }, children)
    }

    private func selectedText(_ expression: DatabaseKit.Expression, binder: Self) throws -> CanonicalBoundExpression {
        let input = try binder.bind(expression, contextualType: .sql(.text))
        guard Self.isText(input.type) else { throw mismatch("text expression") }
        return try coerce(input, to: .sql(.text))
    }

    private static func permitsScalarText(_ type: QueryColumnType) -> Bool {
        if isCollection(type) { return true }
        if temporalType(type) != nil { return true }
        if numericKind(type) != nil || isText(type) || isBytes(type) { return true }
        return type == .sql(.boolean) || type == .primitive(.bool)
            || type == .sql(.uuid) || type == .primitive(.uuid)
    }

    /// Catalog selection is completed using declarations, including typed NULL.
    /// The selected children remain authoritative when no source rows exist.
    private func bindScalarFunction(
        _ function: FunctionCall, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression? {
        let name = function.name
        if ["regexp_matches", "regexp_split_to_table"].contains(name) {
            throw DatabaseExpressionEvaluationError.unsupportedExpression("set-returning function requires a FROM source")
        }
        if name == "concat" || name == "concat_ws" {
            guard !function.arguments.isEmpty else { throw mismatch(name) }
            var children: [CanonicalBoundExpression] = []
            for (index, argument) in function.arguments.enumerated() {
                let input = try childBinder(.argument, index).bind(argument, contextualType: .sql(.text))
                guard Self.permitsScalarText(input.type) else {
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar text output for CONCAT")
                }
                if name == "concat_ws", index == 0 {
                    guard Self.isText(input.type) else { throw mismatch(name) }
                    children.append(try coerce(input, to: .sql(.text)))
                } else { children.append(input) }
            }
            return node(expression, .sql(.text), name == "concat_ws" && children[0].isNullable, children)
        }
        let text = DataType.text, integer = DataType.integer, bytes = DataType.blob
        let signatures: [[DataType]]
        let result: QueryColumnType
        switch name {
        case "lower", "upper", "reverse": signatures = [[text]]; result = .sql(.text)
        case "ascii": signatures = [[text]]; result = .sql(.integer)
        case "chr": signatures = [[integer]]; result = .sql(.text)
        case "char_length", "character_length": signatures = [[text]]; result = .sql(.integer)
        case "length", "octet_length", "bit_length": signatures = [[text], [bytes]]; result = .sql(.integer)
        case "substr", "substring":
            signatures = [[text, integer], [text, integer, integer], [bytes, integer], [bytes, integer, integer]]
                + (name == "substring" ? [[text, text], [text, text, text]] : [])
            result = .sql(.text)
        case "strpos", "position": signatures = [[text, text]]; result = .sql(.integer)
        case "btrim", "ltrim", "rtrim": signatures = [[text], [text, text]]; result = .sql(.text)
        case "replace", "translate": signatures = [[text, text, text]]; result = .sql(.text)
        case "split_part": signatures = [[text, text, integer]]; result = .sql(.text)
        case "left", "right", "repeat": signatures = [[text, integer]]; result = .sql(.text)
        case "lpad", "rpad": signatures = [[text, integer], [text, integer, text]]; result = .sql(.text)
        case "encode": signatures = [[bytes, text]]; result = .sql(.text)
        case "decode": signatures = [[text, text]]; result = .sql(.blob)
        case "get_byte": signatures = [[bytes, integer]]; result = .sql(.integer)
        case "get_bit": signatures = [[bytes, .bigint]]; result = .sql(.integer)
        case "set_byte": signatures = [[bytes, integer, integer]]; result = .sql(.blob)
        case "set_bit": signatures = [[bytes, .bigint, integer]]; result = .sql(.blob)
        case "regexp_like": signatures = [[text, text], [text, text, text]]; result = .sql(.boolean)
        case "regexp_match", "regexp_split_to_array":
            signatures = [[text, text], [text, text, text]]; result = .sql(.array(.text))
        case "regexp_replace":
            signatures = [[text, text, text], [text, text, text, text],
                          [text, text, text, integer], [text, text, text, integer, integer],
                          [text, text, text, integer, integer, text]]
            result = .sql(.text)
        default: return nil
        }
        let candidates = signatures.filter { $0.count == function.arguments.count }
        guard !candidates.isEmpty else { throw DatabaseExpressionEvaluationError.invalidFunctionArity(function: name) }
        let inputs: [CanonicalBoundExpression?] = try function.arguments.enumerated().map { index, argument in
            if try isContextualUnknown(argument) { return nil }
            return try childBinder(.argument, index).bind(argument)
        }
        let selected = try selectFunction(candidates, inputs: inputs, name: name)
        let preservesCharacterType = ["length", "char_length", "character_length", "octet_length", "bit_length"].contains(name)
        let children = try function.arguments.indices.map { index in
            let target = QueryColumnType.sql(selected[index])
            let input = try inputs[index] ?? childBinder(.argument, index).bind(function.arguments[index], contextualType: target)
            if preservesCharacterType, selected[index] == .text, case .sql(.char) = input.type { return input }
            return try coerce(input, to: target)
        }
        let type: QueryColumnType
        if (name == "substr" || name == "substring"), selected.first == bytes { type = .sql(.blob) }
        else { type = result }
        let nullable = name == "regexp_match" || (name == "substring" && selected.count > 1 && selected[1] == text)
            || children.contains { $0.isNullable }
        return node(expression, type, nullable, children)
    }

    private func selectFunction(_ signatures: [[DataType]], inputs: [CanonicalBoundExpression?], name: String) throws -> [DataType] {
        let admitted = signatures.filter { signature in
            zip(inputs, signature).allSatisfy { input, target in
                input.map { Self.permitsImplicit(from: $0.type, to: .sql(target)) } ?? true
            }
        }
        guard !admitted.isEmpty else { throw mismatch(name) }
        func exact(_ signature: [DataType]) -> Int {
            zip(inputs, signature).filter { input, target in input.map { Self.sqlScalar($0.type) == target } ?? false }.count
        }
        let maximumExact = admitted.map(exact).max() ?? 0
        let exactCandidates = admitted.filter { exact($0) == maximumExact }
        func preferred(_ signature: [DataType]) -> Int {
            zip(inputs, signature).filter { input, target in
                (target == .doublePrecision || target == .text || target == .timestamp(withTimeZone: true)) && (input.map { Self.sqlScalar($0.type) != target } ?? true)
            }.count
        }
        let maximumPreferred = exactCandidates.map(preferred).max() ?? 0
        let preferredCandidates = exactCandidates.filter { preferred($0) == maximumPreferred }
        guard preferredCandidates.count == 1, let selected = preferredCandidates.first else {
            throw mismatch("ambiguous function \(name)")
        }
        return selected
    }

    private func bindAggregate(
        _ aggregate: AggregateFunction, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression {
        if case .sql(let call) = aggregate { return try bindSQLAggregate(call) }
        if !allowingSPARQLFunctions {
            let call: SQLAggregateCall?
            switch aggregate {
            case .count(let operand, let distinct): call = SQLAggregateCall(function: .count, arguments: operand.map { [$0] } ?? [], distinct: distinct)
            case .sum(let operand, let distinct): call = SQLAggregateCall(function: .sum, arguments: [operand], distinct: distinct)
            case .avg(let operand, let distinct): call = SQLAggregateCall(function: .avg, arguments: [operand], distinct: distinct)
            case .min(let operand): call = SQLAggregateCall(function: .min, arguments: [operand])
            case .max(let operand): call = SQLAggregateCall(function: .max, arguments: [operand])
            case .arrayAgg(let operand, let ordering, let distinct): call = SQLAggregateCall(function: .arrayAgg, arguments: [operand], distinct: distinct, orderBy: ordering ?? [])
            case .groupConcat, .sample, .sql: call = nil
            }
            if let call { return try bindSQLAggregate(call, indexedArgumentOccurrences: false) }
        }
        var scoped = self
        scoped.permitsAggregates = false
        scoped.permitsWindows = false
        return try scoped.bindGenericAggregate(aggregate, expression: expression)
    }

    private func bindGenericAggregate(_ aggregate: AggregateFunction, expression: Expression) throws -> CanonicalBoundExpression {
        switch aggregate {
        case .sql: throw mismatch("SQL aggregate on generic evaluator")
        case .count(let operand, let distinct):
            let children = try operand.map { [try childBinder(.aggregateArgument).bind($0)] } ?? []
            if distinct { for child in children { try requireSQLComparison(child.type, operation: "COUNT DISTINCT") } }
            return node(expression, .sql(.bigint), false, children)
        case .sum(let operand, _), .avg(let operand, _):
            let input = try childBinder(.aggregateArgument).bind(operand, contextualType: .sql(.bigint))
            guard let selected = Self.numericAggregateInputType(input.type) else { throw mismatch("numeric expression") }
            let child = try coerce(input, to: selected)
            let type: QueryColumnType
            if case .avg = aggregate {
                type = Self.numericKind(selected) == .floating ? .sql(.doublePrecision) : .sql(.decimal(precision: nil, scale: nil))
            } else {
                switch selected {
                case .sql(.smallint), .sql(.integer): type = .sql(.bigint)
                case .sql(.real): type = .sql(.real)
                case .sql(.doublePrecision): type = .sql(.doublePrecision)
                default: type = .sql(.decimal(precision: nil, scale: nil))
                }
            }
            return node(expression, type, true, [child])
        case .min(let operand), .max(let operand), .sample(let operand):
            let child = try childBinder(.aggregateArgument).bind(operand)
            if case .sample = aggregate {} else { try requireComparable([child], operation: "aggregate") }
            return node(expression, child.type, true, [child])
        case .groupConcat(let operand, _, _):
            let child = try childBinder(.aggregateArgument).bind(operand, contextualType: .sql(.text))
            guard Self.isText(child.type) else { throw mismatch("GROUP_CONCAT") }
            return node(expression, .sql(.text), true, [child])
        case .arrayAgg(let operand, let ordering, _):
            let child = try childBinder(.aggregateArgument).bind(operand)
            try requirePrimitiveOperation([child], operation: "ARRAY_AGG")
            guard let kind = Self.primitiveKind(child.type), !Self.isPrimitiveArray(child.type) else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("ARRAY_AGG of rich or nested values")
            }
            let keys = try (ordering ?? []).enumerated().map { try childBinder(.aggregateOrderBy, $0.offset).bind($0.element.expression) }
            return node(expression, .primitive(kind, isArray: true), true, [child] + keys)
        }
    }

    private func boolean(_ expression: DatabaseKit.Expression) throws -> CanonicalBoundExpression {
        let bound = try bind(expression, contextualType: .sql(.boolean))
        guard bound.type == .sql(.boolean) || bound.type == .primitive(.bool) else { throw mismatch("boolean expression") }
        return bound
    }

    private func bindInputs(
        _ expressions: [DatabaseKit.Expression], defaultType: QueryColumnType = .sql(.text),
        field: QueryOccurrenceField = .operand, addresses: [QueryOccurrenceID?]? = nil,
        known: [CanonicalBoundExpression?]? = nil
    ) throws -> [CanonicalBoundExpression] {
        var bound: [Int: CanonicalBoundExpression] = [:]
        for (index, expression) in expressions.enumerated() where try !isContextualUnknown(expression) {
            var scoped = childBinder(field, index)
            if let addresses { scoped.currentOccurrence = addresses[index] }
            bound[index] = try known?[index] ?? scoped.bind(expression)
        }
        var context: QueryColumnType?
        for index in expressions.indices {
            guard let child = bound[index] else { continue }
            do {
                if let existing = context, Self.compositeType(existing) != nil, Self.compositeType(child.type) != nil { continue }
                context = try context.map { try commonType([$0, child.type]) } ?? child.type
            }
            catch let cause as DatabaseExpressionEvaluationError { throw located(cause, at: child.occurrence) }
        }
        let selectedContext: QueryColumnType
        switch context ?? defaultType {
        case .sql(.decimal): selectedContext = .sql(.decimal(precision: nil, scale: nil))
        case .sql(.char): selectedContext = .sql(.char(length: nil))
        default: selectedContext = context ?? defaultType
        }
        return try expressions.enumerated().map { index, expression in
            var scoped = childBinder(field, index)
            if let addresses { scoped.currentOccurrence = addresses[index] }
            return try bound[index] ?? scoped.bind(expression, contextualType: selectedContext)
        }
    }

    private func bindCommon(
        _ expressions: [DatabaseKit.Expression], defaultType: QueryColumnType = .sql(.text),
        field: QueryOccurrenceField = .operand, addresses: [QueryOccurrenceID?]? = nil
    ) throws -> [CanonicalBoundExpression] {
        let inputs = try bindInputs(expressions, defaultType: defaultType, field: field, addresses: addresses)
        if inputs.allSatisfy({ Self.compositeType($0.type) != nil }) { return try selectCompositeInputs(inputs) }
        let type = try commonType(inputs.map { $0.type })
        return try inputs.map { try coerce($0, to: type) }
    }

    private func bindComparison(
        _ expressions: [DatabaseKit.Expression], addresses: [QueryOccurrenceID?]? = nil
    ) throws -> [CanonicalBoundExpression] {
        var selected = self
        selected.allowsRowSubquery = expressions.contains { if case .row = $0 { return true }; return false }
        return try selected.selectComparisonInputs(selected.bindInputs(expressions, addresses: addresses))
    }

    private func selectComparisonInputs(_ inputs: [CanonicalBoundExpression]) throws -> [CanonicalBoundExpression] {
        let children: [CanonicalBoundExpression]
        if inputs.allSatisfy({ Self.compositeType($0.type) != nil }) {
            children = try selectCompositeInputs(inputs)
        } else if let first = inputs.first, case .primitive = first.type,
           inputs.allSatisfy({ $0.type == first.type }) {
            children = inputs
        } else if inputs.allSatisfy({ if case .sql(.char) = $0.type { return true }; return false }) {
            children = inputs
        } else if inputs.allSatisfy({ Self.numericKind($0.type) == .signed }) {
            // Integer comparison operators have exact cross-width signatures.
            children = try inputs.map { input in
                guard let scalar = Self.sqlScalar(input.type) else { throw mismatch("integer comparison") }
                return try coerce(input, to: .sql(scalar))
            }
        } else if inputs.allSatisfy({ Self.numericKind($0.type) != nil }) {
            let type = try arithmeticType(inputs.map { $0.type })
            children = try inputs.map { try coerce($0, to: type) }
        } else {
            let type = try commonType(inputs.map { $0.type })
            children = try inputs.map { try coerce($0, to: type) }
        }
        try requireComparable(children, operation: "comparison")
        return children
    }

    private func constructorField(_ input: CanonicalBoundExpression, at index: Int) throws -> Expression? {
        guard case .row(let arguments) = input.expression else { return nil }
        var start = 0
        for argument in arguments {
            if case .fieldExpansion(let operand) = argument {
                var scoped = self
                scoped.isNested = true
                let expanded = try scoped.bind(operand)
                guard let descriptor = Self.compositeType(expanded.type) else { throw mismatch("ROW expansion") }
                let end = start.addingReportingOverflow(descriptor.columns.count)
                guard !end.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                if index < end.partialValue { return nil }
                start = end.partialValue
            } else {
                if start == index { return argument }
                start += 1
            }
        }
        return nil
    }

    private func selectCompositeInputs(_ inputs: [CanonicalBoundExpression]) throws -> [CanonicalBoundExpression] {
        guard let first = inputs.first, let descriptor = Self.compositeType(first.type) else { throw mismatch("row comparison") }
        let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[QueryColumn]>.stride
            + MemoryLayout<[CanonicalBoundExpression]>.stride + MemoryLayout<[QueryColumnType]>.stride), at: .bindingCandidate)
        var successful = false
        defer { if !successful { claim?.release() } }
        @AdmittedArray var fields: [QueryColumn] = []
        @AdmittedArray var selected: [CanonicalBoundExpression] = []
        @AdmittedArray var types: [QueryColumnType] = []
        for input in inputs {
            guard Self.compositeType(input.type)?.columns.count == descriptor.columns.count else { throw mismatch("row comparison arity") }
            if allowsRowSubquery, case .subquery(let query) = input.expression {
                let arity = try input.subqueryBinding?.columns.count ?? subqueryColumns(query).count
                guard arity == descriptor.columns.count else { throw mismatch("row subquery column count") }
            }
        }
        for index in descriptor.columns.indices {
            types.removeAll(keepingCapacity: true)
            var nullable = false
            for input in inputs {
                let field = Self.compositeType(input.type)!.columns[index]
                nullable = nullable || field.isNullable
                if let original = try constructorField(input, at: index), try isContextualUnknown(original) { continue }
                try CanonicalBindingStorage.append(field.type, to: &$types, reservation: claim)
            }
            let type: QueryColumnType
            if !types.isEmpty, types.allSatisfy({ Self.numericKind($0) != nil }) { type = try arithmeticType(types) }
            else { type = try commonType(types) }
            try CanonicalBindingStorage.append(try QueryColumn(number: ordinal(index), name: descriptor.columns[index].name,
                type: type, isNullable: nullable), to: &$fields, reservation: claim)
        }
        let destination = try CanonicalBindingStorage.composite(columns: fields, workMeter: workMeter)
        try bindingMetadata.retain(destination.reservation)
        let target = QueryColumnType.sql(.composite(destination.type))
        for input in inputs {
            if case .row = input.expression {
                let childrenClaim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[CanonicalBoundExpression]>.stride), at: .bindingCandidate)
                var completed = false
                defer { if !completed { childrenClaim?.release() } }
                @AdmittedArray var children: [CanonicalBoundExpression] = []
                for index in descriptor.columns.indices {
                    let child: CanonicalBoundExpression
                    if let original = try constructorField(input, at: index), try isContextualUnknown(original) {
                        var scoped = self
                        scoped.isNested = true
                        scoped.currentOccurrence = input.children[index].occurrence
                        child = try scoped.bind(original, contextualType: destination.type.columns[index].type)
                    } else { child = try coerce(input.children[index], to: destination.type.columns[index].type) }
                    try CanonicalBindingStorage.append(child, to: &$children, reservation: childrenClaim)
                }
                let replacement = CanonicalBoundExpression(expression: input.expression, type: target, isNullable: false,
                    children: children, occurrence: input.occurrence, comparisonMode: .constructor,
                    constructionLease: childrenClaim.map { DatabaseQueryPayloadLease(retaining: $0) }, bindingMetadata: bindingMetadata)
                try CanonicalBindingStorage.append(replacement, to: &$selected, reservation: claim)
                completed = true
            } else { try CanonicalBindingStorage.append(coerce(input, to: target), to: &$selected, reservation: claim) }
        }
        // Only the selected array escapes; descriptor/temporary field buffers do not.
        let temporary = UInt64(MemoryLayout<[QueryColumn]>.stride + MemoryLayout<[QueryColumnType]>.stride)
        let slots = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryColumn>.stride)).multiplied(by: UInt64(fields.count))
            .adding(try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryColumnType>.stride)).multiplied(by: UInt64(types.count))).bytes
        try claim?.releasePartial(bytes: try DatabaseIntermediateFootprint(bytes: temporary).adding(.init(bytes: slots)).bytes)
        try bindingMetadata.retain(claim)
        successful = true
        return selected
    }

    private func bindArrayComparison(
        _ lhs: DatabaseKit.Expression, array: DatabaseKit.Expression,
        field: QueryOccurrenceField = .operand, commonSelection: Bool = false
    ) throws -> [CanonicalBoundExpression] {
        let source = try childBinder(field, 1).bind(array)
        let element: QueryColumnType
        switch source.type {
        case .sql(.array(let type)): element = .sql(type)
        case .primitive(let kind, true): element = .primitive(kind)
        default: throw mismatch("quantified comparison array")
        }
        let input = try childBinder(field, 0).bind(lhs, contextualType: element)
        let type: QueryColumnType
        if !commonSelection, Self.numericKind(input.type) != nil, Self.numericKind(element) != nil {
            type = try arithmeticType([input.type, element])
        } else { type = try commonType([input.type, element]) }
        let child = try coerce(input, to: type)
        let declaration = node(.literal(.queryValue(.primitive(.null), type: element)), element, true)
        try requireComparable([child, declaration], operation: "quantified comparison")
        guard Self.permitsImplicit(from: element, to: type) else { throw mismatch("quantified comparison element") }
        return [child, source]
    }

    private func bindQuantified(
        _ lhs: DatabaseKit.Expression, source: QuantifiedComparisonSource,
        expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression {
        switch source {
        case .array(let array):
            let children = try bindArrayComparison(lhs, array: array)
            // A nonempty array may contain NULL even when the array itself is nonnullable.
            return node(expression, .sql(.boolean), true, children,
                comparisonMode: Self.comparisonMode([lhs, .row([])]))
        case .subquery(let query):
            return try bindSubqueryComparison(lhs, query: query, expression: expression)
        }
    }

    private func bindSubqueryComparison(_ operand: Expression, query: SelectQuery,
        expression: Expression) throws -> CanonicalBoundExpression {
        let binding = try bindLocatedSubquery?(query, childBinder(.subquery).currentOccurrence)
        let output = try binding?.columns ?? subqueryColumns(query)
        guard !output.isEmpty else { throw mismatch("subquery comparison arity") }
        let inputType: QueryColumnType
        if output.count == 1 { inputType = output[0].type }
        else {
            let descriptor = try CanonicalBindingStorage.composite(columns: output, workMeter: workMeter)
            try bindingMetadata.retain(descriptor.reservation)
            inputType = .sql(.composite(descriptor.type))
        }
        let child = try childBinder(.operand, 0).bind(operand, contextualType: inputType)
        let selected: QueryColumnType
        if Self.numericKind(child.type) != nil, Self.numericKind(inputType) != nil {
            selected = try arithmeticType([child.type, inputType])
        } else { selected = try commonType([child.type, inputType]) }
        try requireSQLComparison(selected, operation: "subquery comparison")
        let converted = try coerce(child, to: selected)
        let memberTypes: [QueryColumnType]
        if output.count > 1, case .sql(.composite(let descriptor)) = selected {
            memberTypes = descriptor.columns.map { $0.type }
        } else { memberTypes = [selected] }
        return node(expression, .sql(.boolean), true, [converted], subqueryBinding: binding,
            comparisonMode: Self.comparisonMode([operand, .row([])]), subqueryComparisonTypes: memberTypes)
    }

    func requireSQLComparison(_ type: QueryColumnType, operation: String) throws {
        try CanonicalSQLValueSemantics.requireComparable(type, operation: operation,
            workMeter: workMeter, stage: .bindingCandidate)
    }

    private func requireComparable(_ children: [CanonicalBoundExpression], operation: String) throws {
        for child in children {
            try requireSQLComparison(child.type, operation: operation)
            if Self.isCollection(child.type) || Self.compositeType(child.type) != nil { continue }
            guard !Self.isPrimitiveArray(child.type), Self.primitiveKind(child.type) != nil || Self.temporalType(child.type) != nil else { throw mismatch(operation) }
        }
    }

    private func isContextualUnknown(_ expression: DatabaseKit.Expression) throws -> Bool {
        switch expression {
        case .literal(.null), .literal(.sqlUnknownString), .literal(.queryValue(.primitive(.null), type: nil)): return true
        case .array(let values) where values.isEmpty: return true
        case .parameter(let reference):
            return try parameterState.declaredType(reference) == nil
        case .coalesce(let children), .greatest(let children), .least(let children): return try children.allSatisfy(isContextualUnknown)
        case .caseWhen(_, let pairs, let fallback):
            return try pairs.allSatisfy { try isContextualUnknown($0.result) } && (fallback.map(isContextualUnknown) ?? true)
        default: return false
        }
    }

    func commonType(_ types: [QueryColumnType]) throws -> QueryColumnType {
        guard let first = types.first else { return .sql(.text) }
        if types.allSatisfy({ $0 == first }) { return first }
        if types.allSatisfy({ Self.compositeType($0) != nil }) {
            guard let descriptor = Self.compositeType(first) else { throw mismatch("composite common arity") }
            let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[QueryColumn]>.stride
                + MemoryLayout<[QueryColumnType]>.stride), at: .bindingCandidate)
            defer { claim?.release() }
            for type in types {
                guard Self.compositeType(type)?.columns.count == descriptor.columns.count else { throw mismatch("composite common arity") }
            }
            @AdmittedArray var fields: [QueryColumn] = []
            @AdmittedArray var candidates: [QueryColumnType] = []
            for index in descriptor.columns.indices {
                candidates.removeAll(keepingCapacity: true)
                var nullable = false
                for type in types {
                    guard let source = Self.compositeType(type) else { throw mismatch("composite common type") }
                    try workMeter?.consume(at: .bindingCandidate)
                    nullable = nullable || source.columns[index].isNullable
                    try CanonicalBindingStorage.append(source.columns[index].type, to: &$candidates, reservation: claim)
                }
                let type = try commonType(candidates)
                let field = try QueryColumn(number: ordinal(index), name: descriptor.columns[index].name, type: type, isNullable: nullable)
                try CanonicalBindingStorage.append(field, to: &$fields, reservation: claim)
            }
            let result = try CanonicalBindingStorage.composite(columns: fields, workMeter: workMeter)
            try bindingMetadata.retain(result.reservation)
            return .sql(.composite(result.type))
        }
        if types.allSatisfy({ Self.arrayElement($0) != nil }) {
            let element = try commonType(types.compactMap(Self.arrayElement))
            guard case .sql(let scalar) = element else { throw mismatch("array element common type") }
            return .sql(.array(scalar))
        }
        if types.allSatisfy({ Self.temporalType($0) != nil }) {
            let bases = types.compactMap(Self.temporalType)
            let interval = DataType.interval()
            guard bases.allSatisfy({ ($0 == interval) == (bases[0] == interval) }) else { throw mismatch("temporal common type") }
            let candidates = bases.filter { target in
                bases.allSatisfy { Self.temporalCastAllowed(from: $0, to: target, context: .implicit) }
            }
            guard let selected = candidates.first(where: { $0 == .timestamp(withTimeZone: true) }) ?? candidates.last else {
                throw mismatch("temporal common type")
            }
            return .sql(selected)
        }
        if types.allSatisfy({ if case .sql(.char) = $0 { return true }; return false }) { return .sql(.char(length: nil)) }
        if types.allSatisfy(Self.isText) { return .sql(.text) }
        if types.allSatisfy(Self.isBytes) { return .sql(.blob) }
        if types.allSatisfy({ Self.numericKind($0) != nil }) {
            let kinds = types.compactMap(Self.numericKind)
            if kinds.contains(.floating) {
                return types.contains(where: { Self.primitiveKind($0) == .float64 })
                    ? .sql(.doublePrecision) : .sql(.real)
            }
            if kinds.contains(.decimal) { return .sql(.decimal(precision: nil, scale: nil)) }
            if kinds.contains(.signed) && kinds.contains(.unsigned) {
                return .sql(.decimal(precision: nil, scale: nil))
            }
            let ranked = types.compactMap { type -> (QueryColumnType, Int)? in
                let rank: Int
                switch Self.primitiveKind(type) {
                case .int8?, .uint8?: rank = 8
                case .int16?, .uint16?: rank = 16
                case .int32?, .uint32?: rank = 32
                case .int64?, .uint64?: rank = 64
                default: return nil
                }
                return (type, rank)
            }
            guard let selected = ranked.max(by: { $0.1 < $1.1 }) else { throw mismatch("numeric common type") }
            return selected.0
        }
        if !types.contains(where: Self.isPrimitiveArray),
           let kind = Self.primitiveKind(first), types.allSatisfy({ Self.primitiveKind($0) == kind }) {
            return first
        }
        throw mismatch("common result type")
    }

    /// Recursive columns preserve seed meaning and carrier, independent of native/SQL metadata spelling.
    static func recursiveColumnTypesAgree(_ seed: QueryColumnType, _ term: QueryColumnType) -> Bool {
        if seed == term { return true }
        func matches(_ native: QueryColumnType, _ sql: QueryColumnType) -> Bool {
            guard case .primitive(let kind, false) = native, case .sql(let type) = sql else { return false }
            switch type {
            case .boolean, .smallint, .integer, .bigint, .real, .doublePrecision, .text, .blob, .uuid:
                return primitiveKind(sql) == kind
            default: return false
            }
        }
        return matches(seed, term) || matches(term, seed)
    }

    static func numericAggregateInputType(_ type: QueryColumnType) -> QueryColumnType? {
        guard let scalar = sqlScalar(type), numericKind(type) != nil else { return nil }
        if case .decimal = scalar { return .sql(.decimal(precision: nil, scale: nil)) }
        return .sql(scalar)
    }

    private func arithmeticType(_ types: [QueryColumnType]) throws -> QueryColumnType {
        let selected = try types.map { type -> DataType in
            guard let scalar = Self.sqlScalar(type), Self.numericKind(type) != nil else { throw mismatch("numeric expression") }
            return scalar
        }
        if selected.contains(.doublePrecision) { return .sql(.doublePrecision) }
        if selected.contains(.real) {
            return .sql(selected.allSatisfy { $0 == .real } ? .real : .doublePrecision)
        }
        if selected.contains(where: { if case .decimal = $0 { return true }; return false }) {
            return .sql(.decimal(precision: nil, scale: nil))
        }
        if selected.contains(.bigint) { return .sql(.bigint) }
        if selected.contains(.integer) { return .sql(.integer) }
        return .sql(.smallint)
    }

    private static func sqlScalar(_ type: QueryColumnType) -> DataType? {
        guard !isPrimitiveArray(type) else { return nil }
        if let temporal = temporalType(type) { return temporal }
        switch primitiveKind(type) {
        case .int8?, .int64?: return .bigint
        case .int16?: return .smallint
        case .int32?: return .integer
        case .uint8?, .uint16?, .uint32?, .uint64?, .decimal?: return .decimal(precision: nil, scale: nil)
        case .float32?: return .real
        case .float64?: return .doublePrecision
        case .bool?: return .boolean
        case .string?: return .text
        case .bytes?: return .blob
        case .uuid?: return .uuid
        default: return nil
        }
    }

    private static func permitsImplicit(from source: QueryColumnType, to target: QueryColumnType) -> Bool {
        if source == target { return true }
        if let left = compositeType(source), let right = compositeType(target) {
            return left.columns.count == right.columns.count && left.columns.indices.allSatisfy {
                permitsImplicit(from: left.columns[$0].type, to: right.columns[$0].type)
            }
        }
        if let source = arrayElement(source), let target = arrayElement(target) {
            return permitsImplicit(from: source, to: target)
        }
        if let source = temporalType(source), let target = temporalType(target) {
            return temporalCastAllowed(from: source, to: target, context: .implicit)
        }
        if numericKind(source) != nil, numericKind(target) != nil {
            func rank(_ type: DataType?) -> Int {
                switch type {
                case .smallint?: return 0
                case .integer?: return 1
                case .bigint?: return 2
                case .decimal?: return 3
                case .real?: return 4
                case .doublePrecision?: return 5
                default: return -1
                }
            }
            return rank(sqlScalar(source)) <= rank(sqlScalar(target))
        }
        return !isPrimitiveArray(source) && !isPrimitiveArray(target)
            && primitiveKind(source) != nil && primitiveKind(source) == primitiveKind(target)
    }

    private func literalType(_ literal: Literal, contextualType: QueryColumnType?) throws -> QueryColumnType {
        switch literal {
        case .null: return contextualType ?? .sql(.text)
        case .sqlUnknownString: return contextualType ?? .sql(.text)
        case .bool: return .sql(.boolean)
        case .int: return .sql(.bigint)
        case .uint: return .primitive(.uint64)
        case .decimal(let value):
            _ = try QueryNumericValue(value: value, displayScale: max(0, value.scale))
            return .sql(.decimal(precision: nil, scale: nil))
        case .double(let value):
            guard value.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
            return .sql(.doublePrecision)
        case .string: return .sql(.text)
        case .date: return .sql(.date)
        case .timestamp: return .sql(.timestamp(withTimeZone: true))
        case .binary: return .sql(.blob)
        case .uuid: return .sql(.uuid)
        case .array(let elements):
            let children = try elements.filter { !$0.isNull }.map { try literalType($0, contextualType: nil) }
            if children.isEmpty, let contextualType, Self.isPrimitiveArray(contextualType) { return contextualType }
            guard let first = children.first, let kind = Self.primitiveKind(first),
                  children.allSatisfy({ Self.primitiveKind($0) == kind && !Self.isPrimitiveArray($0) }) else {
                throw mismatch("primitive literal array requires one declared element type")
            }
            return .primitive(kind, isArray: true)
        case .queryValue(let value, let explicitType):
            if let explicitType {
                try validate(explicitType)
                guard QueryValueValidator.accepts(value, type: explicitType) else { throw mismatch("typed literal") }
                return explicitType
            }
            if value.isNull { return contextualType ?? .sql(.text) }
            let type = try Self.type(of: value)
            guard QueryValueValidator.accepts(value, type: type) else { throw mismatch("literal value") }
            return type
        case .iri, .blankNode, .typedLiteral, .langLiteral, .dirLangLiteral, .rdfTerm:
            return .primitive(.rdfTerm)
        }
    }

    static func type(of value: QueryValue) throws -> QueryColumnType {
        switch value {
        case .numeric: return .sql(.decimal(precision: nil, scale: nil))
        case .json: return .sql(.json)
        case .jsonb: return .sql(.jsonb)
        case .interval, .intervalInfinity: return .sql(.interval())
        case .time: return .sql(.time(withTimeZone: false))
        case .timeWithTimeZone: return .sql(.time(withTimeZone: true))
        case .dateInfinity: return .sql(.date)
        case .timestampInfinity(_, let zone): return .sql(.timestamp(withTimeZone: zone))
        case .array(let value): return .sql(.array(value.elementType))
        case .composite(let value): return .sql(.composite(value.type))
        case .primitive(let value):
            switch value {
            case .null: return .sql(.text)
            case .bool: return .sql(.boolean)
            case .int8: return .primitive(.int8)
            case .int16: return .sql(.smallint)
            case .int32: return .sql(.integer)
            case .int64: return .sql(.bigint)
            case .uint8: return .primitive(.uint8)
            case .uint16: return .primitive(.uint16)
            case .uint32: return .primitive(.uint32)
            case .uint64: return .primitive(.uint64)
            case .float32(let value):
                guard value.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
                return .sql(.real)
            case .float64(let value):
                guard value.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
                return .sql(.doublePrecision)
            case .decimal: return .sql(.decimal(precision: nil, scale: nil))
            case .string: return .sql(.text)
            case .bytes: return .sql(.blob)
            case .date: return .sql(.date)
            case .time: return .sql(.time(withTimeZone: false))
            case .dateTime: return .sql(.timestamp(withTimeZone: false))
            case .timestamp: return .sql(.timestamp(withTimeZone: true))
            case .timeSpan: return .primitive(.timeSpan)
            case .calendarPeriod: return .primitive(.calendarPeriod)
            case .geographicPoint: return .primitive(.geographicPoint)
            case .geographicPosition: return .primitive(.geographicPosition)
            case .vector: return .primitive(.vector)
            case .uuid: return .sql(.uuid)
            case .object: return .primitive(.object)
            case .reference: return .primitive(.reference)
            case .rdfTerm: return .primitive(.rdfTerm)
            case .array(let values):
                var kind: FieldSchemaType?
                for element in values where !element.isNull {
                    if case .array = element {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")
                    }
                    guard let elementKind = primitiveKind(try type(of: .primitive(element))),
                          kind == nil || kind == elementKind else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")
                    }
                    kind = elementKind
                }
                guard let kind else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")
                }
                return .primitive(kind, isArray: true)
            }
        }
    }

    enum CastContext: Sendable { case implicit, assignment, explicit }

    func validateCast(from source: QueryColumnType, to target: QueryColumnType, context: CastContext = .explicit) throws {
        if source == target { return }
        if let left = Self.compositeType(source), let right = Self.compositeType(target) {
            guard left.columns.count == right.columns.count else { throw mismatch("composite cast arity") }
            for index in left.columns.indices {
                try validateCast(from: left.columns[index].type, to: right.columns[index].type, context: context)
            }
            return
        }
        if let sourceElement = Self.arrayElement(source), let targetElement = Self.arrayElement(target) {
            try validateCast(from: sourceElement, to: targetElement, context: context)
            return
        }
        if context == .explicit, Self.isText(source), Self.isCollection(target) { return }
        if context != .implicit, Self.isCollection(source), Self.isText(target) { return }
        if context != .implicit, Self.isJSON(source), Self.isJSON(target) { return }
        if context == .explicit, source == .sql(.jsonb),
           Self.numericKind(target) != nil || target == .sql(.boolean) { return }
        if let from = Self.temporalType(source), let to = Self.temporalType(target) {
            guard Self.temporalCastAllowed(from: from, to: to, context: context) else { throw DatabaseExpressionEvaluationError.invalidCast("temporal conversion") }
            return
        }
        if context == .explicit, Self.isText(source), Self.temporalType(target) != nil { return }
        if context != .implicit, Self.temporalType(source) != nil, Self.isText(target) { return }
        if case .sql(.time(let sourceZone, _)) = source,
           case .sql(.time(let targetZone, _)) = target, sourceZone == targetZone { return }
        if Self.numericKind(source) != nil && Self.numericKind(target) != nil {
            guard context != .implicit || Self.permitsImplicit(from: source, to: target) else {
                throw mismatch("implicit coercion")
            }
            return
        }
        if context == .implicit {
            guard Self.permitsImplicit(from: source, to: target) else { throw mismatch("implicit coercion") }
        }
        if context != .explicit, Self.sqlScalar(source) != Self.sqlScalar(target) {
            throw DatabaseExpressionEvaluationError.invalidCast("unsupported source-to-target conversion")
        }
        if (Self.sqlScalar(source) == .integer && Self.sqlScalar(target) == .boolean)
            || (Self.sqlScalar(source) == .boolean && Self.sqlScalar(target) == .integer) { return }
        if context == .explicit, Self.isText(source) {
            switch target {
            case .sql(.boolean), .sql(.smallint), .sql(.integer), .sql(.bigint),
                 .sql(.real), .sql(.doublePrecision), .sql(.decimal), .sql(.uuid), .sql(.blob): return
            default: break
            }
        }
        if context == .explicit, Self.isText(target), Self.permitsScalarText(source) { return }
        if Self.isText(source), case .sql(.decimal) = target { return }
        if Self.isText(source), target == .sql(.uuid) { return }
        if Self.isText(source) {
            switch target {
            case .sql(.date), .sql(.time), .sql(.timestamp): return
            default: break
            }
        }
        if Self.isText(target), Self.numericKind(source) == .signed || Self.numericKind(source) == .unsigned { return }
        if Self.isPrimitiveArray(source) == Self.isPrimitiveArray(target),
           Self.primitiveKind(source) == Self.primitiveKind(target), Self.primitiveKind(source) != nil { return }
        throw DatabaseExpressionEvaluationError.invalidCast("unsupported source-to-target conversion")
    }

    /// An untyped string or NULL literal; two of them leave an operator ambiguous.
    private static func isUntypedLiteral(_ expression: DatabaseKit.Expression) -> Bool {
        switch expression {
        case .literal(.null), .literal(.sqlUnknownString), .literal(.queryValue(.primitive(.null), type: nil)): return true
        default: return false
        }
    }

    /// Renders a non-text operand as text through an explicit cast.
    private func textOutput(_ child: CanonicalBoundExpression) throws -> CanonicalBoundExpression {
        try validateCast(from: child.type, to: .sql(.text), context: .explicit)
        return CanonicalBoundExpression(expression: .cast(child.expression, targetType: .text), type: .sql(.text),
            isNullable: child.isNullable, children: [child], occurrence: child.occurrence,
            subqueryBinding: child.subqueryBinding, castContext: .explicit,
            comparisonMode: child.comparisonMode, collation: child.collation, bindingMetadata: bindingMetadata)
    }

    private func coerce(_ child: CanonicalBoundExpression, to type: QueryColumnType) throws -> CanonicalBoundExpression {
        guard child.type != type else { return child }
        guard Self.permitsImplicit(from: child.type, to: type),
              (Self.arrayElement(child.type) != nil && Self.arrayElement(type) != nil)
                || (Self.compositeType(child.type) != nil && Self.compositeType(type) != nil)
                || (Self.numericKind(child.type) != nil && Self.numericKind(type) != nil)
                || (Self.temporalType(child.type) != nil && Self.temporalType(type) != nil)
                || (Self.primitiveKind(child.type) == .bool && Self.primitiveKind(type) == .bool
                    && !Self.isPrimitiveArray(child.type) && !Self.isPrimitiveArray(type))
                || (Self.isText(child.type) && Self.isText(type))
                || (Self.isBytes(child.type) && Self.isBytes(type)) else { throw mismatch("implicit coercion") }
        let target: DataType
        switch type {
        case .sql(let sql): target = sql
        case .primitive(.bool, false): target = .boolean
        case .primitive(.int16, false): target = .smallint
        case .primitive(.int32, false): target = .integer
        case .primitive(.int64, false): target = .bigint
        case .primitive(.float32, false): target = .real
        case .primitive(.float64, false): target = .doublePrecision
        case .primitive(.decimal, false): target = .decimal(precision: nil, scale: nil)
        case .primitive(.string, false): target = .text
        case .primitive(.bytes, false): target = .blob
        default: throw DatabaseExpressionEvaluationError.unsupportedExpression("primitive common coercion")
        }
        try validateCast(from: child.type, to: type, context: .implicit)
        return CanonicalBoundExpression(expression: .cast(child.expression, targetType: target), type: type,
            isNullable: child.isNullable, children: [child], occurrence: child.occurrence,
            subqueryBinding: child.subqueryBinding, castContext: .implicit,
            comparisonMode: child.comparisonMode, collation: child.collation, bindingMetadata: bindingMetadata)
    }

    private static func isSPARQLProducer(_ expression: DatabaseKit.Expression) -> Bool {
        if case .function(let function) = expression {
            return function.schema == nil && function.name == "sparql"
        }
        return false
    }

    private func bindSPARQLProducer(_ expression: DatabaseKit.Expression) throws {
        guard case .function(let function) = expression, (2...3).contains(function.arguments.count), !function.distinct, !function.finalArrayIsVariadic else {
            throw mismatch("SPARQL arguments")
        }
        for (index, argument) in function.arguments.enumerated() {
            if index == 0, case .column(let name) = argument {
                guard name.table == nil else { throw mismatch("SPARQL entity identity") }
                continue
            }
            let scoped = childBinder(.argument, index)
            let bound = try scoped.bind(argument, contextualType: .sql(.text))
            guard Self.isText(bound.type),
                  case .literal(let literal) = bound.expression,
                  case .queryValue(.primitive(.string), _) = literal else {
                if case .literal(.string) = bound.expression { continue }
                let cause = mismatch("SPARQL string argument")
                guard occurrences != nil else { throw cause }
                throw CanonicalSQLBindingFailure(cause: .expression(cause), occurrence: bound.occurrence, root: root)
            }
        }
    }

    private static func isStaticallyNull(_ expression: DatabaseKit.Expression) -> Bool {
        switch expression {
        case .literal(let literal): return literal.isNull
        case .cast(let nested, _): return isStaticallyNull(nested)
        default: return false
        }
    }

    private func unknownString(_ text: String, to type: QueryColumnType) throws -> QueryValue {
        let scratch: DatabaseIntermediateReservation?
        if let workMeter, Self.numericKind(type) != nil || type == .sql(.boolean) || type == .primitive(.bool) || type == .sql(.uuid) || Self.isBytes(type) {
            scratch = try workMeter.reserveIntermediate(
                bytes: Self.inputScratchFootprint(utf8Count: text.utf8.count, type: type), at: .bindingCandidate)
        } else {
            scratch = nil
        }
        defer { scratch?.release() }
        if let workMeter {
            try DatabaseByteProcessingMeter.consume(byteCount: UInt64(text.utf8.count), workMeter: workMeter, stage: .bindingCandidate)
        }
        let value: QueryValue
        switch type {
        case .sql(.text), .sql(.char), .sql(.varchar), .primitive(.string, false):
            value = .primitive(.string(text))
        case .sql(.boolean), .primitive(.bool, false):
            switch Self.trimmed(text).lowercased() {
            case "true", "t", "yes", "y", "on", "1": value = .primitive(.bool(true))
            case "false", "f", "no", "n", "off", "0": value = .primitive(.bool(false))
            default: throw DatabaseExpressionEvaluationError.invalidCast("boolean input")
            }
        case .sql(.smallint), .sql(.integer), .sql(.bigint),
             .primitive(.int8, false), .primitive(.int16, false), .primitive(.int32, false), .primitive(.int64, false),
             .primitive(.uint8, false), .primitive(.uint16, false), .primitive(.uint32, false), .primitive(.uint64, false):
            let input = try numericInput(text)
            var digits = input[...]
            let negative = digits.first == "-"
            if digits.first == "-" || digits.first == "+" { digits = digits.dropFirst() }
            let radix: Int
            if digits.hasPrefix("0x") || digits.hasPrefix("0X") { radix = 16; digits = digits.dropFirst(2) }
            else if digits.hasPrefix("0o") || digits.hasPrefix("0O") { radix = 8; digits = digits.dropFirst(2) }
            else if digits.hasPrefix("0b") || digits.hasPrefix("0B") { radix = 2; digits = digits.dropFirst(2) }
            else { radix = 10 }
            guard let magnitude = Int128(digits, radix: radix), magnitude >= 0 else {
                throw DatabaseExpressionEvaluationError.invalidCast("integer input")
            }
            let integer = negative ? -magnitude : magnitude
            let carrier: FieldValue
            if Self.numericKind(type) == .unsigned {
                guard let unsigned = UInt64(exactly: integer) else {
                    throw DatabaseExpressionEvaluationError.invalidCast("unsigned integer input")
                }
                carrier = .uint64(unsigned)
            } else {
                guard let signed = Int64(exactly: integer) else {
                    throw DatabaseExpressionEvaluationError.invalidCast("integer input")
                }
                carrier = .int64(signed)
            }
            value = try Self.normalize(.primitive(carrier), to: type)
        case .sql(.real), .sql(.doublePrecision), .primitive(.float32, false), .primitive(.float64, false):
            guard let number = Double(Self.trimmed(text)), number.isFinite else {
                throw DatabaseExpressionEvaluationError.invalidCast("floating-point input")
            }
            value = try Self.normalize(.primitive(.float64(number)), to: type, permitsFloatingRounding: true)
        case .sql(.decimal):
            do { value = .numeric(try QueryNumericValue(sqlLexicalForm: text[...], maximumUTF8Count: UInt64(text.utf8.count))) }
            catch QueryValueError.numericOutOfRange { throw DatabaseExpressionEvaluationError.numericOverflow }
            catch { throw DatabaseExpressionEvaluationError.invalidCast("numeric input") }
        case .primitive(.decimal, false):
            guard case .decimal(let number) = Literal.parseDecimal(try numericInput(text)) else {
                throw DatabaseExpressionEvaluationError.invalidCast("numeric input")
            }
            value = .primitive(.decimal(number))
        case .sql(.uuid):
            do { value = try QueryValue.uuid(sqlLexicalForm: text[...], maximumUTF8Count: UInt64(text.utf8.count)) }
            catch { throw DatabaseExpressionEvaluationError.invalidCast("uuid") }
        case .sql(.blob), .sql(.binary), .sql(.varbinary):
            do { value = try QueryValue.binary(sqlLexicalForm: text[...], maximumUTF8Count: UInt64(text.utf8.count)) }
            catch { throw DatabaseExpressionEvaluationError.invalidCast("bytea") }
        default: throw DatabaseExpressionEvaluationError.unsupportedExpression("SQL unknown string input for unsupported target type")
        }
        guard QueryValueValidator.accepts(value, type: type) else { throw mismatch("SQL unknown string input") }
        return value
    }

    private static func inputScratchFootprint(utf8Count: Int, type: QueryColumnType) throws -> UInt64 {
        let count = UInt64(utf8Count).addingReportingOverflow(1)
        guard !count.overflow, let elementCount = Int(exactly: count.partialValue) else {
            throw DatabaseExpressionEvaluationError.numericOverflow
        }
        // The existing input kernels can retain trim/filter/digit/sign copies together.
        var footprint = try DatabaseIntermediateFootprint(bytes: count.partialValue)
            .adding(DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<String>.stride)))
            .multiplied(by: 8)
        if Self.numericKind(type) == .decimal {
            // Literal.parseDecimal splits exponent and fraction before rejecting malformed input.
            let layout = try DatabaseRetainedArrayLayout.validated(
                containerByteCount: UInt64(MemoryLayout<[Substring]>.stride),
                elementCapacitySlotByteCount: UInt64(MemoryLayout<Substring>.stride),
                sharedOwnerByteCount: 32, appendAdmissionByteCount: 16)
            let growth = try layout.growth(from: 0, toFit: elementCount)
            footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: layout.containerByteCount)
                .adding(DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)).multiplied(by: 2))
        }
        return footprint.bytes
    }

    private static func trimmed(_ text: String) -> String {
        String(text.drop(while: { $0.isWhitespace }).reversed().drop(while: { $0.isWhitespace }).reversed())
    }

    private func numericInput(_ text: String) throws -> String {
        let input = Self.trimmed(text)
        var previous: Character?
        for index in input.indices {
            let character = input[index]
            if character == "_" {
                let next = input.index(after: index)
                guard previous?.hexDigitValue != nil, next != input.endIndex,
                      input[next].hexDigitValue != nil else { throw mismatch("numeric input separator") }
            }
            previous = character
        }
        return String(input.filter { $0 != "_" })
    }

    private func requirePrimitiveOperation(_ children: [CanonicalBoundExpression], operation: String) throws {
        for child in children {
            if Self.temporalType(child.type) != nil { continue }
            if case .literal(.queryValue(let value, _)) = child.expression, !value.isNull {
                switch value {
                case .primitive, .numeric: break
                default: throw DatabaseExpressionEvaluationError.unsupportedExpression("\(operation) of rich values")
                }
            }
            switch child.type {
            case .sql(.json), .sql(.jsonb), .sql(.interval), .sql(.array):
                throw DatabaseExpressionEvaluationError.unsupportedExpression("\(operation) of rich values")
            default: break
            }
        }
    }

    private func scalarColumn(_ query: SelectQuery, binding: CanonicalBoundSelect? = nil) throws -> QueryColumn {
        let columns = try binding?.columns ?? subqueryColumns(query)
        guard columns.count == 1, let column = columns.first else { throw mismatch("scalar subquery column count") }
        return column
    }

    private func subqueryColumns(_ query: SelectQuery) throws -> [QueryColumn] {
        if let bindLocatedSubquery { return try bindLocatedSubquery(query, childBinder(.subquery).currentOccurrence).columns }
        guard let bindSubquery else {
            throw DatabaseExpressionEvaluationError.unsupportedExpression("subquery binding context is required")
        }
        return try bindSubquery(query)
    }

    private func outputColumns(slots: [Int]) throws -> [QueryColumn] {
        try slots.enumerated().map { index, slot in
            let input = try column(at: slot)
            return try QueryColumn(number: ordinal(index), name: input.name, type: input.type, isNullable: input.isNullable)
        }
    }

    private func column(at slot: Int) throws -> QueryColumn {
        guard columns.indices.contains(slot) else { throw mismatch("column slot outside relation schema") }
        let column = columns[slot]
        try validate(column.type)
        return column
    }

    private func ordinal(_ index: Int) throws -> UInt32 {
        guard let value = UInt32(exactly: index), value < UInt32.max else { throw DatabaseExpressionEvaluationError.numericOverflow }
        return value + 1
    }

    private func validate(_ type: QueryColumnType) throws {
        guard QueryValueValidator.accepts(.primitive(.null), type: type) else { throw mismatch("unresolved type") }
    }

    private func node(
        _ expression: DatabaseKit.Expression, _ type: QueryColumnType,
        _ nullable: Bool, _ children: [CanonicalBoundExpression] = [],
        subqueryBinding: CanonicalBoundSelect? = nil,
        wholeRow: CanonicalBoundWholeRow? = nil, fieldOrdinal: Int? = nil,
        comparisonMode: CanonicalSQLCompositeComparisonMode = .record,
        collation: SQLCollation? = nil, subqueryComparisonTypes: [QueryColumnType] = [],
        metadataLease: DatabaseQueryPayloadLease? = nil,
        aggregate: CanonicalBoundAggregate? = nil, window: CanonicalBoundWindow? = nil
    ) -> CanonicalBoundExpression {
        CanonicalBoundExpression(expression: expression, type: type, isNullable: nullable, children: children, aggregate: aggregate, window: window,
                                 occurrence: currentOccurrence, subqueryBinding: subqueryBinding, wholeRow: wholeRow,
                                 fieldOrdinal: fieldOrdinal, comparisonMode: comparisonMode, collation: collation,
                                 subqueryComparisonTypes: subqueryComparisonTypes, metadataLease: metadataLease,
                                 bindingMetadata: bindingMetadata)
    }

    private func childBinder(_ field: QueryOccurrenceField, _ index: Int? = nil) -> Self {
        var child = self
        child.isNested = true
        child.currentOccurrence = currentOccurrence.flatMap { parent in
            occurrences?.child(of: parent, field: field, index: index.flatMap(UInt32.init(exactly:)))
        }
        return child
    }

    private func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }

    private func located(_ cause: QueryValueError, at occurrence: QueryOccurrenceID?) -> any Error {
        located(DatabaseExpressionEvaluationError.queryValue(cause), at: occurrence)
    }

    private func located(_ cause: DatabaseExpressionEvaluationError, at occurrence: QueryOccurrenceID?) -> any Error {
        guard occurrences != nil else { return cause }
        return CanonicalSQLBindingFailure(cause: .expression(cause), occurrence: occurrence, root: root)
    }

    private enum NumericKind { case signed, unsigned, floating, decimal }

    private static func numericKind(_ type: QueryColumnType) -> NumericKind? {
        guard !isPrimitiveArray(type) else { return nil }
        switch primitiveKind(type) {
        case .int8?, .int16?, .int32?, .int64?: return .signed
        case .uint8?, .uint16?, .uint32?, .uint64?: return .unsigned
        case .float32?, .float64?: return .floating
        case .decimal?: return .decimal
        default: return nil
        }
    }

    private static func primitiveKind(_ type: QueryColumnType) -> FieldSchemaType? {
        switch type {
        case .primitive(let kind, _): return kind
        case .sql(let sql):
            switch sql {
            case .boolean: return .bool
            case .smallint: return .int16
            case .integer: return .int32
            case .bigint: return .int64
            case .real: return .float32
            case .doublePrecision: return .float64
            case .decimal: return .decimal
            case .char, .varchar, .text: return .string
            case .binary, .varbinary, .blob: return .bytes
            case .date: return .date
            case .time(false, _): return .time
            case .timestamp(false, _): return .dateTime
            case .timestamp(true, _): return .timestamp
            case .uuid: return .uuid
            default: return nil
            }
        }
    }

    private static func isText(_ type: QueryColumnType) -> Bool { !isPrimitiveArray(type) && primitiveKind(type) == .string }
    private static func isBytes(_ type: QueryColumnType) -> Bool { !isPrimitiveArray(type) && primitiveKind(type) == .bytes }
    private static func isPrimitiveArray(_ type: QueryColumnType) -> Bool {
        if case .primitive(_, let isArray) = type { return isArray }
        return false
    }

    /// Names an unaliased output column with PostgreSQL's FigureColname rules.
    ///
    /// PostgreSQL names a scalar subquery after its first output column once
    /// wildcards are expanded. `subqueryName` supplies that name from the
    /// caller's resolved subquery output; without it only an explicit first
    /// projection expression names the subquery.
    static func outputName(_ expression: DatabaseKit.Expression, index: Int,
        subqueryName: (SelectQuery) -> String? = { _ in nil }) -> String {
        figuredName(expression, subqueryName: subqueryName)?.name ?? "?column?"
    }

    /// The first output name of the scalar subquery `query` bound inside
    /// `roots`, read from its retained binding.
    static func boundSubqueryName(_ query: SelectQuery, in roots: [CanonicalBoundExpression]) -> String? {
        var pending = roots
        while let candidate = pending.popLast() {
            if case .subquery(let bound) = candidate.expression, bound == query,
               let column = candidate.subqueryBinding?.columns.first {
                return column.name
            }
            pending.append(contentsOf: candidate.children)
        }
        return nil
    }

    /// A name and its strength: 2 for a referenced name, 1 for a fallback keyword or type.
    private static func figuredName(_ expression: DatabaseKit.Expression,
        subqueryName: (SelectQuery) -> String?) -> (name: String, strength: Int)? {
        func figured(_ expression: DatabaseKit.Expression) -> (name: String, strength: Int)? {
            figuredName(expression, subqueryName: subqueryName)
        }
        switch expression {
        case .column(let column): return (column.column, 2)
        case .field(_, let name): return (name, 2)
        case .arrayAccess(let base, _), .collate(let base, _): return figured(base)
        case .function(let call): return (call.name, 2)
        case .grouping: return ("grouping", 2)
        case .window(let call):
            switch call.function {
            case .aggregate(let aggregate): return (aggregate.function.rawValue, 2)
            case .analytic(let function, _): return (function.rawValue, 2)
            }
        case .aggregate(let aggregate):
            switch aggregate {
            case .sql(let call): return (call.function.rawValue, 2)
            case .count: return ("count", 2)
            case .sum: return ("sum", 2)
            case .avg: return ("avg", 2)
            case .min: return ("min", 2)
            case .max: return ("max", 2)
            case .groupConcat: return ("group_concat", 2)
            case .sample: return ("sample", 2)
            case .arrayAgg: return ("array_agg", 2)
            }
        case .currentValue(let kind, _):
            switch kind {
            case .date: return ("current_date", 2)
            case .time: return ("current_time", 2)
            case .timestamp: return ("current_timestamp", 2)
            case .localTime: return ("localtime", 2)
            case .localTimestamp: return ("localtimestamp", 2)
            }
        case .cast(let operand, let type):
            if let inner = figured(operand), inner.strength > 1 { return inner }
            return (SQLCastOutputName.name(for: type), 1)
        case .caseWhen(_, _, let elseResult):
            if let elseResult, let inner = figured(elseResult), inner.strength > 1 { return inner }
            return ("case", 1)
        case .array, .arraySubquery: return ("array", 2)
        case .row: return ("row", 2)
        case .coalesce: return ("coalesce", 2)
        case .nullIf: return ("nullif", 2)
        case .greatest: return ("greatest", 2)
        case .least: return ("least", 2)
        case .exists: return ("exists", 2)
        case .subquery(let query):
            if let name = subqueryName(query) { return (name, 2) }
            guard case .expression(let inner, let alias)? = query.projection.first else { return nil }
            return (alias ?? outputName(inner, index: 0, subqueryName: subqueryName), 2)
        default: return nil
        }
    }

    /// Primitive relation cells keep a primitive carrier for the existing
    /// evaluator. Rich canonical values never enter this conversion branch.
    static func normalizeSourceCarrier(_ value: QueryValue, to type: QueryColumnType) throws -> QueryValue {
        if case .primitive(let primitive) = value, !value.isNull,
           case .sql(.decimal) = type {
            let canonical: FieldValue
            do { canonical = try RelationalValueIdentity.canonicalize(primitive).value }
            catch { throw DatabaseExpressionEvaluationError.numericOverflow }
            if let decimal = exactDecimal(canonical) {
                let result = QueryValue.primitive(.decimal(decimal))
                guard QueryValueValidator.accepts(result, type: type) else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "primitive numeric source normalization")
                }
                return result
            }
        }
        return try normalize(value, to: type)
    }

    static func normalize(
        _ value: QueryValue, to type: QueryColumnType,
        permitsFloatingRounding: Bool = false
    ) throws -> QueryValue {
        if QueryValueValidator.accepts(value, type: type) { return value }
        if case .primitive(_, false) = type, case .numeric(let numeric) = value {
            return try normalize(.primitive(.decimal(numeric.value)), to: type,
                permitsFloatingRounding: permitsFloatingRounding)
        }
        guard case .primitive(let primitive) = value, !isPrimitiveArray(type), let kind = primitiveKind(type) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound result normalization")
        }
        let normalized: QueryValue
        if kind == .decimal, let decimal = exactDecimal(primitive) {
            normalized = .numeric(try QueryNumericValue(value: decimal, displayScale: max(0, decimal.scale)))
        } else if let integer = try normalizationInteger(primitive, permitsDecimal:
            numericKind(type) == .signed || numericKind(type) == .unsigned) {
            let scalar: FieldValue
            switch kind {
            case .int8: guard let value = Int8(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int8(value)
            case .int16: guard let value = Int16(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int16(value)
            case .int32: guard let value = Int32(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int32(value)
            case .int64: guard let value = Int64(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int64(value)
            case .uint8: guard let value = UInt8(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint8(value)
            case .uint16: guard let value = UInt16(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint16(value)
            case .uint32: guard let value = UInt32(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint32(value)
            case .uint64: guard let value = UInt64(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint64(value)
            case .float32:
                let value = permitsFloatingRounding ? Float(integer) : Float(exactly: integer)
                guard let value, value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
                scalar = .float32(value)
            case .float64:
                let value = permitsFloatingRounding ? Double(integer) : Double(exactly: integer)
                guard let value, value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
                scalar = .float64(value)
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound integer normalization")
            }
            normalized = .primitive(scalar)
        } else if kind == .float64, case .float32(let scalar) = primitive, scalar.isFinite {
            normalized = .primitive(.float64(Double(scalar)))
        } else if kind == .float32, case .float64(let scalar) = primitive {
            let result = permitsFloatingRounding ? Float(scalar) : Float(exactly: scalar)
            guard let result, result.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
            normalized = .primitive(.float32(result))
        } else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound result normalization")
        }
        guard QueryValueValidator.accepts(normalized, type: type) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound result does not satisfy declared type")
        }
        return normalized
    }

    private static func normalizationInteger(_ value: FieldValue, permitsDecimal: Bool) throws -> Int128? {
        if let integer = integer(value) { return integer }
        guard permitsDecimal, case .decimal(let decimal) = value else { return nil }
        guard decimal.scale <= 0 else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound integer normalization")
        }
        // ExactDecimal canonicalizes trailing zeroes, so positive scale is fractional.
        // At most 38 decimal shifts can fit an Int128; reject larger exponents first.
        let shifts = -Int64(decimal.scale)
        guard shifts <= 38 else { throw DatabaseExpressionEvaluationError.numericOverflow }
        var integer = decimal.coefficient
        for _ in 0..<shifts {
            let product = integer.multipliedReportingOverflow(by: 10)
            guard !product.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            integer = product.partialValue
        }
        return integer
    }

    private static func integer(_ value: FieldValue) -> Int128? {
        switch value {
        case .int8(let value): return Int128(value)
        case .int16(let value): return Int128(value)
        case .int32(let value): return Int128(value)
        case .int64(let value): return Int128(value)
        case .uint8(let value): return Int128(value)
        case .uint16(let value): return Int128(value)
        case .uint32(let value): return Int128(value)
        case .uint64(let value): return Int128(value)
        default: return nil
        }
    }

    private static func exactDecimal(_ value: FieldValue) -> ExactDecimal? {
        if case .decimal(let decimal) = value { return decimal }
        return integer(value).map { ExactDecimal(coefficient: $0, scale: 0) }
    }
}

extension CanonicalQueryTypeBinder {
    private func bindSQLAggregate(_ call: SQLAggregateCall, asWindow: Bool = false,
        indexedArgumentOccurrences: Bool = true) throws -> CanonicalBoundExpression {
        // The optional SPARQL producer does not change this explicitly selected SQL call.
        guard call.schema == nil || call.schema == "pg_catalog" else {
            throw mismatch("SQL aggregate namespace or language")
        }
        let ordered = [.percentileCont, .percentileDisc, .mode].contains(call.function)
        guard ordered == (call.withinGroup != nil), !ordered || (!call.distinct && call.orderBy.isEmpty),
              !asWindow || (!ordered && !call.distinct && call.orderBy.isEmpty),
              !call.distinct || !call.arguments.isEmpty else { throw mismatch("aggregate modifiers") }
        let arity: ClosedRange<Int>
        switch call.function {
        case .count: arity = 0...1
        case .mode: arity = 0...0
        case .stringAgg, .jsonObjectAgg, .jsonbObjectAgg, .corr, .covarPop, .covarSamp,
             .regrCount, .regrAvgx, .regrAvgy, .regrSxx, .regrSxy, .regrSyy,
             .regrSlope, .regrIntercept, .regrR2: arity = 2...2
        default: arity = 1...1
        }
        guard arity.contains(call.arguments.count), !ordered || call.withinGroup?.count == 1 else {
            throw DatabaseExpressionEvaluationError.invalidFunctionArity(function: call.function.rawValue)
        }
        var scoped = self
        scoped.permitsWindows = false
        scoped.permitsAggregates = asWindow
        let claim = try workMeter?.reserveIntermediate(bytes: CanonicalBoundAggregate.storageByteCount
            + UInt64(MemoryLayout<[CanonicalBoundExpression]>.stride), at: .bindingCandidate)
        var successful = false
        defer { if !successful { claim?.release() } }
        @AdmittedArray var arguments: [CanonicalBoundExpression] = []
        func append(_ child: CanonicalBoundExpression) throws {
            try CanonicalBindingStorage.append(child, to: &$arguments, reservation: claim)
        }
        for (index, expression) in call.arguments.enumerated() {
            let binder = scoped.childBinder(.aggregateArgument, indexedArgumentOccurrences ? index : nil)
            let child: CanonicalBoundExpression
            switch call.function {
            case .sum, .avg, .varPop, .varSamp, .variance, .stddevPop, .stddevSamp, .stddev:
                child = try binder.bind(expression, contextualType: .sql(.bigint))
            case .boolAnd, .boolOr, .every:
                child = try binder.bindSourceValue(expression, to: .sql(.boolean))
            case .corr, .covarPop, .covarSamp, .regrCount, .regrAvgx, .regrAvgy, .regrSxx,
                 .regrSxy, .regrSyy, .regrSlope, .regrIntercept, .regrR2:
                let input = try binder.bind(expression, contextualType: .sql(.doublePrecision))
                guard Self.numericKind(input.type) != nil else { throw mismatch("regression numeric input") }
                child = try coerce(input, to: .sql(.doublePrecision))
            case .percentileCont, .percentileDisc:
                let input = try binder.bind(expression, contextualType: .sql(.doublePrecision))
                let target: QueryColumnType = Self.arrayElement(input.type) == nil
                    ? .sql(.doublePrecision) : .sql(.array(.doublePrecision))
                child = try binder.bindAssignment(input, to: target)
            default: child = try binder.bind(expression)
            }
            try append(child)
        }
        var type: QueryColumnType
        let nullable = call.function != .count && call.function != .regrCount
        switch call.function {
        case .count, .regrCount: type = .sql(.bigint)
        case .sum, .avg:
            if case .sql(.interval) = arguments[0].type {
                type = .sql(.interval())
            } else {
                guard let input = Self.numericAggregateInputType(arguments[0].type) else { throw mismatch("numeric expression") }
                arguments[0] = try coerce(arguments[0], to: input)
                if call.function == .avg {
                    type = Self.numericKind(input) == .floating ? .sql(.doublePrecision) : .sql(.decimal(precision: nil, scale: nil))
                } else {
                    switch input {
                    case .sql(.smallint), .sql(.integer): type = .sql(.bigint)
                    case .sql(.real): type = .sql(.real)
                    case .sql(.doublePrecision): type = .sql(.doublePrecision)
                    default: type = .sql(.decimal(precision: nil, scale: nil))
                    }
                }
            }
        case .min, .max:
            try requireSQLComparison(arguments[0].type, operation: "MIN/MAX")
            type = arguments[0].type
        case .arrayAgg:
            if let element = Self.arrayElement(arguments[0].type) {
                type = .sql(.array(try sqlElement(element)))
                arguments[0] = try bindSourceValue(arguments[0], to: type)
            } else {
                let element = try sqlElement(arguments[0].type)
                arguments[0] = try bindSourceValue(arguments[0], to: .sql(element))
                type = .sql(.array(element))
            }
        case .stringAgg:
            let binary = arguments.contains { Self.isBytes($0.type) }
            let target: QueryColumnType = binary ? .sql(.blob) : .sql(.text)
            for index in arguments.indices where try isContextualUnknown(call.arguments[index]) {
                arguments[index] = try scoped.childBinder(.aggregateArgument, indexedArgumentOccurrences ? index : nil)
                    .bind(call.arguments[index], contextualType: target)
            }
            guard arguments.allSatisfy({ binary ? Self.isBytes($0.type) : Self.isText($0.type) }) else {
                throw mismatch("STRING_AGG text or bytes signature")
            }
            for index in arguments.indices { arguments[index] = try coerce(arguments[index], to: target) }
            type = target
        case .boolAnd, .boolOr, .every: type = .sql(.boolean)
        case .bitAnd, .bitOr, .bitXor:
            guard let selected = Self.sqlScalar(arguments[0].type), [.smallint, .integer, .bigint].contains(selected) else {
                throw mismatch("bit aggregate integer input")
            }
            type = .sql(selected)
            arguments[0] = try coerce(arguments[0], to: type)
        case .jsonAgg, .jsonObjectAgg: type = .sql(.json)
        case .jsonbAgg, .jsonbObjectAgg: type = .sql(.jsonb)
        case .varPop, .varSamp, .variance, .stddevPop, .stddevSamp, .stddev:
            guard let input = Self.numericAggregateInputType(arguments[0].type) else { throw mismatch("statistic numeric input") }
            arguments[0] = try coerce(arguments[0], to: input)
            type = Self.numericKind(input) == .floating ? .sql(.doublePrecision) : .sql(.decimal(precision: nil, scale: nil))
        case .corr, .covarPop, .covarSamp, .regrAvgx, .regrAvgy, .regrSxx, .regrSxy,
             .regrSyy, .regrSlope, .regrIntercept, .regrR2: type = .sql(.doublePrecision)
        case .percentileCont, .percentileDisc, .mode: type = .sql(.text)
        }
        if call.distinct {
            for argument in arguments { try requireSQLComparison(argument.type, operation: "aggregate DISTINCT") }
        }
        @AdmittedArray var ordering: [CanonicalBoundKey] = []
        for (index, key) in call.orderBy.enumerated() {
            let input = try scoped.childBinder(.aggregateOrderBy, index).bind(key.expression)
            try requireSQLComparison(input.type, operation: "aggregate ORDER BY")
            var selected: Int?
            for position in arguments.indices {
                let left = CanonicalBoundKey(expression: arguments[position])
                let right = CanonicalBoundKey(expression: input)
                if try key.expression == call.arguments[position]
                    || left.isSemanticallyEquivalent(to: right, workMeter: workMeter, isVolatileSubquery: { _ in true }) {
                    selected = position
                    break
                }
            }
            guard !call.distinct || selected != nil else { throw mismatch("DISTINCT aggregate ORDER BY argument membership") }
            try CanonicalBindingStorage.append(CanonicalBoundKey(expression: selected.map { arguments[$0] } ?? input,
                projectionIndex: selected, sortKey: key, occurrence: input.occurrence), to: &$ordering, reservation: claim)
        }
        let filter = try call.filter.map { try scoped.childBinder(.aggregateFilter).boolean($0) }
        @AdmittedArray var within: [CanonicalBoundKey] = []
        for (index, key) in (call.withinGroup ?? []).enumerated() {
            let binder = scoped.childBinder(.aggregateWithinGroup, index)
            var input = try binder.bind(key.expression, contextualType: call.function == .percentileCont ? .sql(.doublePrecision) : nil)
            if call.function == .percentileCont {
                if case .sql(.interval) = input.type { type = .sql(.interval()) }
                else {
                    guard Self.numericKind(input.type) != nil else { throw mismatch("PERCENTILE_CONT numeric or interval ordering") }
                    input = try coerce(input, to: .sql(.doublePrecision))
                    type = .sql(.doublePrecision)
                }
            } else { type = input.type }
            try requireSQLComparison(input.type, operation: "WITHIN GROUP")
            try CanonicalBindingStorage.append(CanonicalBoundKey(expression: input, sortKey: key), to: &$within, reservation: claim)
        }
        if ordered, let fraction = arguments.first, Self.arrayElement(fraction.type) != nil {
            type = .sql(.array(try sqlElement(type)))
        }
        @AdmittedArray var children: [CanonicalBoundExpression] = []
        for argument in arguments { try CanonicalBindingStorage.append(argument, to: &$children, reservation: claim) }
        for key in ordering { try CanonicalBindingStorage.append(key.expression, to: &$children, reservation: claim) }
        if let filter { try CanonicalBindingStorage.append(filter, to: &$children, reservation: claim) }
        for key in within { try CanonicalBindingStorage.append(key.expression, to: &$children, reservation: claim) }
        let aggregate = CanonicalBoundAggregate(call: call, arguments: arguments,
            directArguments: ordered ? arguments : [], orderBy: ordering, filter: filter, withinGroup: within)
        try bindingMetadata.retain(claim)
        successful = true
        return node(.aggregate(.sql(call)), type, nullable, children, aggregate: aggregate)
    }
}

extension CanonicalQueryTypeBinder {
    func preparingWindows(_ definitions: [NamedWindow]) throws -> Self {
        var scoped = self
        scoped.windowDefinitions = []
        let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[CanonicalBoundWindowDefinition]>.stride), at: .bindingCandidate)
        var successful = false
        defer { if !successful { claim?.release() } }
        for (index, definition) in definitions.enumerated() {
            guard !scoped.windowDefinitions.contains(where: { $0.name == definition.name }) else {
                throw DatabaseExpressionEvaluationError.windowingViolation
            }
            let selected = try scoped.childBinder(.namedWindow, index).childBinder(.windowReference)
                .bindWindowSpecification(definition.specification, identity: index, name: definition.name)
            try CanonicalBindingStorage.appendGrowing(selected, to: &scoped.windowDefinitions, reservation: claim)
        }
        try bindingMetadata.retain(claim)
        successful = true
        return scoped
    }

    private func bindWindowSpecification(_ specification: WindowSpecification, identity: Int,
        name: String = "") throws -> CanonicalBoundWindowDefinition {
        var scoped = self
        scoped.permitsWindows = false
        let inherited: CanonicalBoundWindowDefinition?
        if let inheritedName = specification.inheritedName {
            guard let definition = windowDefinitions.first(where: { $0.name == inheritedName }) else {
                throw DatabaseExpressionEvaluationError.windowingViolation
            }
            guard specification.partitionBy.isEmpty, definition.frame == nil,
                  specification.orderBy.isEmpty || definition.orderBy.isEmpty else { throw DatabaseExpressionEvaluationError.windowingViolation }
            inherited = definition
        } else { inherited = nil }
        let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<CanonicalBoundWindowDefinition>.stride), at: .bindingCandidate)
        var successful = false
        defer { if !successful { claim?.release() } }
        @AdmittedArray var partitions = inherited?.partitionBy ?? []
        @AdmittedArray var ordering = inherited?.orderBy ?? []
        for (index, expression) in specification.partitionBy.enumerated() {
            let selected = try scoped.childBinder(.windowPartitionBy, index).bind(expression)
            try requireSQLComparison(selected.type, operation: "window PARTITION BY")
            try CanonicalBindingStorage.append(selected, to: &$partitions, reservation: claim)
        }
        for (index, key) in specification.orderBy.enumerated() {
            let selected = try scoped.childBinder(.windowOrderBy, index).bind(key.expression)
            try requireSQLComparison(selected.type, operation: "window ORDER BY")
            try CanonicalBindingStorage.append(CanonicalBoundKey(expression: selected, sortKey: key), to: &$ordering, reservation: claim)
        }
        let frame = try specification.frame.map { try scoped.bindWindowFrame($0, orderBy: ordering) }
        try bindingMetadata.retain(claim)
        successful = true
        return CanonicalBoundWindowDefinition(name: name, partitionBy: partitions,
            orderBy: ordering, frame: frame, identity: identity)
    }

    private func bindWindowFrame(_ frame: WindowFrame, orderBy: [CanonicalBoundKey]) throws -> CanonicalBoundWindowFrame {
        func position(_ bound: WindowFrameBound) -> Int {
            switch bound {
            case .unboundedPreceding: return 0
            case .preceding: return 1
            case .currentRow: return 2
            case .following: return 3
            case .unboundedFollowing: return 4
            }
        }
        guard position(frame.start) != 4, position(frame.end) != 0,
              position(frame.start) <= position(frame.end),
              frame.mode != .groups || !orderBy.isEmpty else { throw DatabaseExpressionEvaluationError.windowingViolation }
        func offset(_ bound: WindowFrameBound, field: QueryOccurrenceField) throws -> CanonicalBoundExpression? {
            let expression: Expression
            switch bound {
            case .preceding(let selected), .following(let selected): expression = selected
            default: return nil
            }
            let target: QueryColumnType
            switch frame.mode {
            case .rows, .groups: target = .sql(.bigint)
            case .range:
                guard orderBy.count == 1 else { throw mismatch("RANGE offset requires one ORDER BY key") }
                let type = orderBy[0].expression.type
                if let scalar = Self.sqlScalar(type), Self.numericKind(type) != nil { target = .sql(scalar) }
                else {
                    switch type {
                    case .sql(.date), .sql(.timestamp), .sql(.time(withTimeZone: false, precision: _)), .sql(.interval):
                        target = .sql(.interval())
                    default: throw mismatch("RANGE offset ordering type")
                    }
                }
            }
            var scoped = childBinder(field)
            scoped.permitsWindows = false
            scoped.permitsAggregates = false
            let selected = try scoped.bindAssignment(expression, to: target)
            let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[CanonicalBoundExpression]>.stride), at: .bindingCandidate)
            defer { claim?.release() }
            @AdmittedArray var pending: [CanonicalBoundExpression] = []
            try CanonicalBindingStorage.append(selected, to: &$pending, reservation: claim)
            while let node = pending.popLast() {
                try workMeter?.consume(at: .bindingCandidate)
                guard node.columnSlot == nil, node.wholeRow == nil, node.subqueryBinding == nil else {
                    throw DatabaseExpressionEvaluationError.windowingViolation
                }
                switch node.expression {
                case .aggregate, .window, .grouping, .subquery, .arraySubquery, .exists, .inSubquery:
                    throw DatabaseExpressionEvaluationError.windowingViolation
                default: break
                }
                for child in node.children { try CanonicalBindingStorage.append(child, to: &$pending, reservation: claim) }
            }
            return selected
        }
        return try CanonicalBoundWindowFrame(mode: frame.mode, start: frame.start, end: frame.end,
            startOffset: offset(frame.start, field: .windowFrameStart),
            endOffset: offset(frame.end, field: .windowFrameEnd), exclusion: frame.exclusion)
    }

    private func bindWindow(_ call: WindowCall, expression: Expression) throws -> CanonicalBoundExpression {
        let reference = childBinder(.windowReference)
        let definition: CanonicalBoundWindowDefinition
        switch call.reference {
        case .named(let name):
            guard let selected = windowDefinitions.first(where: { $0.name == name }) else { throw DatabaseExpressionEvaluationError.windowingViolation }
            definition = selected
        case .specification(let specification):
            definition = try reference.bindWindowSpecification(specification,
                identity: currentOccurrence.map { Int($0.rawValue) } ?? -1)
        }
        var scoped = childBinder(.windowFunction)
        scoped.permitsWindows = false
        let claim = try workMeter?.reserveIntermediate(bytes: CanonicalBoundWindow.storageByteCount
            + UInt64(MemoryLayout<[CanonicalBoundExpression]>.stride), at: .bindingCandidate)
        var successful = false
        defer { if !successful { claim?.release() } }
        let aggregate: CanonicalBoundExpression?
        @AdmittedArray var arguments: [CanonicalBoundExpression] = []
        let type: QueryColumnType
        let nullable: Bool
        switch call.function {
        case .aggregate(let call):
            let selected = try scoped.bindSQLAggregate(call, asWindow: true)
            aggregate = selected
            type = selected.type
            nullable = selected.isNullable
        case .analytic(let function, let inputs):
            aggregate = nil
            let arity: ClosedRange<Int>
            switch function {
            case .rowNumber, .rank, .denseRank, .percentRank, .cumeDist: arity = 0...0
            case .ntile, .firstValue, .lastValue: arity = 1...1
            case .nthValue: arity = 2...2
            case .lag, .lead: arity = 1...3
            }
            guard arity.contains(inputs.count) else { throw DatabaseExpressionEvaluationError.invalidFunctionArity(function: function.rawValue) }
            let navigationPair: [CanonicalBoundExpression]?
            if [.lag, .lead].contains(function), inputs.count == 3 {
                navigationPair = try scoped.bindCommon([inputs[0], inputs[2]], defaultType: .sql(.text),
                    addresses: [scoped.childBinder(.argument, 0).currentOccurrence,
                        scoped.childBinder(.argument, 2).currentOccurrence])
            } else { navigationPair = nil }
            for (index, input) in inputs.enumerated() {
                let binder = scoped.childBinder(.argument, index)
                let selected: CanonicalBoundExpression
                if let pair = navigationPair, index == 0 || index == 2 {
                    selected = pair[index == 0 ? 0 : 1]
                } else if (function == .ntile && index == 0)
                    || ([.lag, .lead, .nthValue].contains(function) && index == 1) {
                    selected = try binder.bindAssignment(input, to: .sql(.integer))
                } else { selected = try binder.bind(input) }
                try CanonicalBindingStorage.append(selected, to: &$arguments, reservation: claim)
            }
            switch function {
            case .rowNumber, .rank, .denseRank: type = .sql(.bigint); nullable = false
            case .percentRank, .cumeDist: type = .sql(.doublePrecision); nullable = false
            case .ntile: type = .sql(.integer); nullable = true
            case .lag, .lead:
                if arguments.count == 3 {
                    let selected = try commonType([arguments[0].type, arguments[2].type])
                    arguments[0] = try coerce(arguments[0], to: selected)
                    arguments[2] = try coerce(arguments[2], to: selected)
                }
                type = arguments[0].type
                nullable = true
            case .firstValue, .lastValue, .nthValue: type = arguments[0].type; nullable = true
            }
        }
        @AdmittedArray var children: [CanonicalBoundExpression] = []
        if let aggregate { try CanonicalBindingStorage.append(aggregate, to: &$children, reservation: claim) }
        for argument in arguments { try CanonicalBindingStorage.append(argument, to: &$children, reservation: claim) }
        for key in definition.partitionBy { try CanonicalBindingStorage.append(key, to: &$children, reservation: claim) }
        for key in definition.orderBy { try CanonicalBindingStorage.append(key.expression, to: &$children, reservation: claim) }
        if let offset = definition.frame?.startOffset { try CanonicalBindingStorage.append(offset, to: &$children, reservation: claim) }
        if let offset = definition.frame?.endOffset { try CanonicalBindingStorage.append(offset, to: &$children, reservation: claim) }
        let selected = CanonicalBoundWindow(call: call, arguments: arguments, aggregate: aggregate,
            partitionBy: definition.partitionBy, orderBy: definition.orderBy, frame: definition.frame,
            identity: definition.identity)
        try bindingMetadata.retain(claim)
        successful = true
        return node(expression, type, nullable, children, window: selected)
    }
}

extension CanonicalQueryTypeBinder {
    /// Checks the current scalar scope without imposing SQL value adaptation.
    func requireScalarPhase(_ expression: Expression) throws {
        let claim = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[Expression]>.stride), at: .bindingCandidate)
        defer { claim?.release() }
        @AdmittedArray var pending: [Expression] = []
        func append(_ expression: Expression) throws {
            try CanonicalBindingStorage.append(expression, to: &$pending, reservation: claim)
        }
        try append(expression)
        while let expression = pending.popLast() {
            try workMeter?.consume(at: .bindingCandidate)
            switch expression {
            case .aggregate, .window, .grouping:
                throw mismatch("aggregate, GROUPING or window in scalar assignment")
            case .add(let left, let right), .subtract(let left, let right), .multiply(let left, let right),
                 .divide(let left, let right), .modulo(let left, let right), .concatenate(let left, let right),
                 .equal(let left, let right), .notEqual(let left, let right), .lessThan(let left, let right),
                 .lessThanOrEqual(let left, let right), .greaterThan(let left, let right),
                 .greaterThanOrEqual(let left, let right), .and(let left, let right), .or(let left, let right),
                 .nullIf(let left, let right), .isDistinctFrom(let left, let right, _),
                 .collectionOperation(_, let left, let right):
                try append(right)
                try append(left)
            case .negate(let operand), .not(let operand), .isNull(let operand), .isNotNull(let operand),
                 .cast(let operand, _), .collate(let operand, _), .field(let operand, _), .fieldExpansion(let operand),
                 .truthTest(let operand, _, _), .regex(let operand, _, _), .isTriple(let operand),
                 .subject(let operand), .predicate(let operand), .object(let operand), .inSubquery(let operand, _):
                try append(operand)
            case .array(let children), .row(let children), .coalesce(let children), .greatest(let children), .least(let children):
                for child in children.reversed() { try append(child) }
            case .function(let function):
                for child in function.arguments.reversed() { try append(child) }
            case .inList(let operand, let children), .notInList(let operand, let children):
                for child in children.reversed() { try append(child) }
                try append(operand)
            case .between(let operand, let lower, let upper):
                try append(upper)
                try append(lower)
                try append(operand)
            case .patternMatch(let subject, let pattern, let escape, _):
                if let escape { try append(escape) }
                try append(pattern)
                try append(subject)
            case .quantifiedComparison(let operand, _, _, let source):
                if case .array(let array) = source { try append(array) }
                try append(operand)
            case .arrayAccess(let base, let subscripts):
                for subscriptValue in subscripts.reversed() {
                    switch subscriptValue {
                    case .index(let value): try append(value)
                    case .slice(let lower, let upper):
                        if let upper { try append(upper) }
                        if let lower { try append(lower) }
                    }
                }
                try append(base)
            case .caseWhen(let operand, let pairs, let fallback):
                if let fallback { try append(fallback) }
                for pair in pairs.reversed() { try append(pair.result); try append(pair.condition) }
                if let operand { try append(operand) }
            case .triple(let subject, let predicate, let object):
                try append(object)
                try append(predicate)
                try append(subject)
            case .subquery, .arraySubquery, .exists:
                // A child SELECT owns its own aggregate and analytic phase.
                break
            case .defaultValue, .literal, .currentValue, .wholeRow, .column, .variable, .parameter, .bound: break
            }
        }
    }
}

extension CanonicalQueryTypeBinder {
    func bindTableFunction(_ call: FunctionCall, withOrdinality: Bool,
        occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundTableFunction {
        var scoped = self
        if let occurrence { scoped.currentOccurrence = occurrence }
        scoped.permitsAggregates = false
        scoped.permitsWindows = false
        do { return try scoped.selectTableFunction(call, withOrdinality: withOrdinality) }
        catch let error as QueryValueError { throw scoped.located(error, at: scoped.currentOccurrence) }
        catch let error as DatabaseExpressionEvaluationError { throw scoped.located(error, at: scoped.currentOccurrence) }
    }

    private func selectTableFunction(_ call: FunctionCall, withOrdinality: Bool) throws -> CanonicalBoundTableFunction {
        guard call.schema == nil || call.schema == "pg_catalog", !call.distinct else { throw mismatch("table function modifiers") }
        let name = call.name, expression = Expression.function(call)
        let kind: CanonicalBoundTableFunction.Kind
        let bound: CanonicalBoundExpression
        let reservation = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<CanonicalBoundTableFunction>.stride), at: .bindingCandidate)
        @AdmittedArray var output: [(String, QueryColumnType, Bool)] = []
        func column(_ name: String, _ type: QueryColumnType, _ nullable: Bool) throws {
            try CanonicalBindingStorage.append((name, type, nullable), to: &$output, reservation: reservation)
        }
        switch name {
        case "unnest", "generate_subscripts":
            kind = .array
            guard !call.finalArrayIsVariadic else { throw mismatch("table function VARIADIC signature") }
            guard name == "unnest" ? !call.arguments.isEmpty : (2...3).contains(call.arguments.count) else { throw mismatch(name) }
            var children: [CanonicalBoundExpression] = []
            for (index, argument) in call.arguments.enumerated() {
                let child: CanonicalBoundExpression
                if name == "unnest" || index == 0 {
                    child = try childBinder(.argument, index).bind(argument)
                    guard let element = Self.arrayElement(child.type) else { throw mismatch("\(name) requires a typed array") }
                    if name == "unnest" { try appendTableColumns(name: name, type: element, nullable: true, to: &$output, reservation: reservation) }
                } else {
                    let target = QueryColumnType.sql(index == 1 ? .integer : .boolean)
                    child = try coerce(childBinder(.argument, index).bind(argument, contextualType: target), to: target)
                }
                children.append(child)
            }
            if name == "generate_subscripts" { try column(name, .sql(.integer), false) }
            bound = node(expression, output.first?.1 ?? .sql(.integer), true, children)
        case "json_each", "jsonb_each", "json_each_text", "jsonb_each_text",
             "json_array_elements", "jsonb_array_elements", "json_array_elements_text", "jsonb_array_elements_text",
             "json_object_keys", "jsonb_object_keys":
            kind = .json
            guard !call.finalArrayIsVariadic else { throw mismatch("table function VARIADIC signature") }
            guard call.arguments.count == 1 else { throw mismatch(name) }
            let family = QueryColumnType.sql(name.hasPrefix("jsonb") ? .jsonb : .json)
            let children = try collectionArguments(call.arguments, targets: [family])
            let text = name.hasSuffix("_text"), keys = name.hasSuffix("object_keys")
            if name == "json_each" || name == "jsonb_each" || name == "json_each_text" || name == "jsonb_each_text" { try column("key", .sql(.text), false) }
            try column(keys ? name : "value", text || keys ? .sql(.text) : family, text)
            bound = node(expression, output.last!.1, text, children)
        case "regexp_matches", "regexp_split_to_table":
            kind = name == "regexp_matches" ? .patternMatches : .patternSplit
            guard !call.finalArrayIsVariadic else { throw mismatch("table function VARIADIC signature") }
            guard (2...3).contains(call.arguments.count) else { throw mismatch(name) }
            let children = try collectionArguments(call.arguments, targets: Array(repeating: .sql(.text), count: call.arguments.count))
            let type = QueryColumnType.sql(name == "regexp_matches" ? .array(.text) : .text)
            try column(name, type, false)
            bound = node(expression, type, false, children)
        case "generate_series":
            kind = .series
            guard !call.finalArrayIsVariadic else { throw mismatch("table function VARIADIC signature") }
            let numeric = DataType.decimal(precision: nil, scale: nil)
            let timestamp = DataType.timestamp(withTimeZone: false)
            let aware = DataType.timestamp(withTimeZone: true)
            let signatures: [[DataType]]
            switch call.arguments.count {
            case 2: signatures = [[.integer, .integer], [.bigint, .bigint], [numeric, numeric]]
            case 3: signatures = [[.integer, .integer, .integer], [.bigint, .bigint, .bigint], [numeric, numeric, numeric], [timestamp, timestamp, .interval()], [aware, aware, .interval()]]
            case 4: signatures = [[aware, aware, .interval(), .text]]
            default: throw mismatch(name)
            }
            let inputs: [CanonicalBoundExpression?] = try call.arguments.enumerated().map { index, argument in
                if try isContextualUnknown(argument) { return nil }
                return try childBinder(.argument, index).bind(argument)
            }
            if signatures.count > 1 && inputs.allSatisfy({ $0 == nil }) { throw mismatch("ambiguous function \(name)") }
            let signature = try selectFunction(signatures, inputs: inputs, name: name)
            let children = try call.arguments.indices.map { index in
                let target = QueryColumnType.sql(signature[index])
                return try coerce(inputs[index] ?? childBinder(.argument, index).bind(call.arguments[index], contextualType: target), to: target)
            }
            let type = QueryColumnType.sql(signature[0])
            try column(name, type, false)
            bound = node(expression, type, false, children)
        default:
            kind = .scalar
            bound = try bindFunction(call, expression: expression)
            try appendTableColumns(name: name, type: bound.type, nullable: bound.isNullable, to: &$output, reservation: reservation)
        }
        let fixedNames = SQLTableFunctionOutput.names(for: call, withOrdinality: false)
        guard fixedNames == nil || fixedNames?.count == output.count else { throw mismatch("table function output layout") }
        @AdmittedArray var columns: [QueryColumn] = []
        for (name, type, nullable) in output {
            let selectedName = fixedNames?[columns.count] ?? name
            try reservation?.reserveAdditional(bytes: UInt64(selectedName.utf8.count), at: .bindingCandidate)
            try CanonicalBindingStorage.append(QueryColumn(number: ordinal(columns.count), name: selectedName, type: type, isNullable: nullable), to: &$columns, reservation: reservation)
        }
        if withOrdinality {
            let name = SQLTableFunctionOutput.ordinalityName
            try reservation?.reserveAdditional(bytes: UInt64(name.utf8.count), at: .bindingCandidate)
            try CanonicalBindingStorage.append(QueryColumn(number: ordinal(columns.count), name: name, type: .sql(.bigint), isNullable: false), to: &$columns, reservation: reservation)
        }
        try bindingMetadata.retain(reservation)
        return CanonicalBoundTableFunction(bound: try finalizing(bound), columns: columns, kind: kind, withOrdinality: withOrdinality)
    }

    private func appendTableColumns(name: String, type: QueryColumnType, nullable: Bool,
        to output: inout AdmittedArray<(String, QueryColumnType, Bool)>, reservation: DatabaseIntermediateReservation?) throws {
        if case .sql(.composite(let descriptor)) = type {
            for field in descriptor.columns {
                try CanonicalBindingStorage.append((field.name, field.type, nullable || field.isNullable), to: &output, reservation: reservation)
            }
        } else { try CanonicalBindingStorage.append((name, type, nullable), to: &output, reservation: reservation) }
    }
}
