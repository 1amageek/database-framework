import DatabaseKit

/// Resolves the persisted fields one canonical query can observe while
/// filtering, ordering, joining, projecting, or reading an index.
package struct DatabaseFieldReadAuthorizationPlan: Sendable {
    package let fieldsByEntity: [String: Set<String>]

    package func merging(
        _ other: DatabaseFieldReadAuthorizationPlan
    ) -> DatabaseFieldReadAuthorizationPlan {
        var merged = fieldsByEntity
        for (entity, fields) in other.fieldsByEntity {
            merged[entity, default: []].formUnion(fields)
        }
        return DatabaseFieldReadAuthorizationPlan(fieldsByEntity: merged)
    }

    package func isCovered(
        by authorizedFieldsByEntity: [String: Set<String>]?
    ) -> Bool {
        guard let authorizedFieldsByEntity else { return false }
        return fieldsByEntity.allSatisfy { entity, requiredFields in
            guard let authorizedFields = authorizedFieldsByEntity[entity]
            else { return false }
            return authorizedFields.isSuperset(of: requiredFields)
        }
    }

    package static func make(
        query: SelectQuery,
        schema: Schema
    ) -> DatabaseFieldReadAuthorizationPlan {
        var collector = Collector(schema: schema)
        collector.collect(query)
        return DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: collector.fieldsByEntity
        )
    }

    /// Resolves selected ORDER inputs without treating source JOIN predicates
    /// or unrelated projected fields as ordering dependencies.
    package static func orderingFields(query: SelectQuery, inputs: [Expression], schema: Schema)
        -> [String: Set<String>] {
        var collector = Collector(schema: schema, collectingOrdering: true)
        let bindings = collector.collectBindings(query.source,
            commonTableNames: Set(query.subqueries?.map { $0.name } ?? []), outerScopes: [])
        let sourceDependencies = collector.fieldsByEntity
        collector.fieldsByEntity = [:]
        for expression in inputs {
            collector.collect(expression, localBindings: bindings, outerScopes: [])
        }
        // Derived sources conservatively retain their underlying read dependencies.
        if bindings.isEmpty {
            collector.collect(query)
            for (entity, fields) in sourceDependencies {
                collector.fieldsByEntity[entity, default: []].formUnion(fields)
            }
        }
        return collector.fieldsByEntity
    }

    package static func rdfDataset(
        schema: Schema
    ) -> DatabaseFieldReadAuthorizationPlan {
        var collector = Collector(schema: schema)
        collector.collectRDFDatasetFields()
        return DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: collector.fieldsByEntity
        )
    }

    package static func index(
        entity: Schema.Entity,
        descriptor: IndexDescriptor
    ) -> DatabaseFieldReadAuthorizationPlan {
        var fields = Set(descriptor.fieldNames)
        fields.formUnion(descriptor.includedFieldNames)
        if descriptor.type == .graph(.ontologyProjection) {
            fields.formUnion(entity.allFields)
        }
        return DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [entity.name: fields]
        )
    }

    private struct SourceBinding {
        struct Field: Sendable {
            let visibleName: String
            let entityFieldName: String
            let outputOrdinal: Int
        }

        let entity: Schema.Entity
        let qualifiers: Set<String>
        let fields: [Field]

        init(
            entity: Schema.Entity,
            qualifiers: Set<String>,
            fields: [Field]? = nil
        ) {
            self.entity = entity
            self.qualifiers = qualifiers
            self.fields = fields ?? entity.allFields.enumerated().map {
                Field(visibleName: $0.element, entityFieldName: $0.element, outputOrdinal: $0.offset)
            }
        }

        func entityFieldName(for visibleName: String) -> String? {
            fields.first { $0.visibleName == visibleName }?.entityFieldName
        }
    }

    private struct Collector {
        let schema: Schema
        var collectingOrdering = false
        var fieldsByEntity: [String: Set<String>] = [:]
        private struct CommonTableColumns {
            let names: [String]?
        }
        private var commonTableColumns: [String: CommonTableColumns] = [:]

        mutating func collect(
            _ query: SelectQuery,
            outerScopes: [[SourceBinding]] = []
        ) {
            let inherited = commonTableColumns
            defer { commonTableColumns = inherited }
            for subquery in query.subqueries ?? [] {
                // A CTE sees inherited declarations and its earlier siblings only.
                collect(subquery.query, outerScopes: outerScopes)
                let names = queryColumnNames(subquery.query, declarations: commonTableColumns)
                commonTableColumns[subquery.name] = CommonTableColumns(
                    names: aliasedNames(names, columns: subquery.columns))
            }
            let commonTableNames = Set(commonTableColumns.keys)
            let bindings = collectBindings(
                query.source,
                commonTableNames: commonTableNames,
                outerScopes: outerScopes
            )
            for item in query.projection {
                switch item {
                case .wildcard(nil):
                    for binding in bindings { includeAll(binding.entity) }
                case .wildcard(.some(let qualifier)):
                    for binding in matchingBindings(qualifier: qualifier, localBindings: bindings, outerScopes: outerScopes) {
                        includeAll(binding.entity)
                    }
                case .expression(let expression, _):
                    collect(expression, localBindings: bindings, outerScopes: outerScopes)
                }
            }
            if case .distinctOn(let expressions) = query.quantifier {
                for expression in expressions { collect(expression, localBindings: bindings, outerScopes: outerScopes) }
            }
            if case .expression(let expression) = query.pagination.offset {
                collect(expression, localBindings: bindings, outerScopes: outerScopes)
            }
            if case .count(.expression(let expression), _) = query.pagination.limit {
                collect(expression, localBindings: bindings, outerScopes: outerScopes)
            }
            if let filter = query.filter {
                collect(
                    filter,
                    localBindings: bindings,
                    outerScopes: outerScopes
                )
            }
            for expression in query.groupBy ?? [] {
                collect(
                    expression,
                    localBindings: bindings,
                    outerScopes: outerScopes
                )
            }
            if let having = query.having {
                collect(
                    having,
                    localBindings: bindings,
                    outerScopes: outerScopes
                )
            }
            for sortKey in query.orderBy ?? [] {
                collect(
                    sortKey.expression,
                    localBindings: bindings,
                    outerScopes: outerScopes
                )
            }
            collectIndexFields(query.accessPath, bindings: bindings)
        }

        // Keep case payloads and post-child work out of recursive source dispatch.
        mutating func collectBindings(
            _ source: DataSource,
            commonTableNames: Set<String>,
            outerScopes: [[SourceBinding]]
        ) -> [SourceBinding] {
            switch source {
            case .table:
                return collectTableBindings(source, commonTableNames: commonTableNames)
            case .logical:
                return collectLogicalBindings(source)
            case .subquery:
                return collectDerivedBindings(source, outerScopes: outerScopes)
            case .join:
                return collectJoinBindings(source, commonTableNames: commonTableNames, outerScopes: outerScopes)
            case .setOperation:
                return collectSetBindings(source, outerScopes: outerScopes)
            case .aliased:
                return collectAliasedBindings(source, commonTableNames: commonTableNames, outerScopes: outerScopes)
            #if DATABASE_MULTI_BASE
            case .base:
                return collectBaseBindings(source, commonTableNames: commonTableNames, outerScopes: outerScopes)
            #endif
            case .graphTable:
                for entity in schema.entities { includeAll(entity) }
                return []
            case .graphPattern, .namedGraph, .service:
                collectRDFDatasetFields()
                return []
            case .values:
                return collectValuesBindings(source, outerScopes: outerScopes)
            }
        }

        private mutating func collectTableBindings(_ source: DataSource, commonTableNames: Set<String>) -> [SourceBinding] {
            guard case .table(let table) = source else { preconditionFailure("Expected table source") }
            guard !commonTableNames.contains(table.table),
                  let entity = schema.entity(named: table.table) else {
                return []
            }
            let qualifiers: Set<String> = [table.effectiveName]
            if fieldsByEntity[entity.name] == nil {
                fieldsByEntity[entity.name] = []
            }
            return [
                SourceBinding(entity: entity, qualifiers: qualifiers)
            ]
        }

        private mutating func collectLogicalBindings(_ source: DataSource) -> [SourceBinding] {
            guard case .logical(let source) = source else { preconditionFailure("Expected logical source") }
            guard source.kindIdentifier == LogicalSourceKind.polymorphic
            else { return [] }
            var bindings: [SourceBinding] = []
            for entity in schema.entities
                where entity.polymorphicMembership?.identifier
                    == source.identifier {
                if fieldsByEntity[entity.name] == nil {
                    fieldsByEntity[entity.name] = []
                }
                bindings.append(
                    SourceBinding(
                        entity: entity,
                        qualifiers: [source.effectiveName]
                    )
                )
            }
            return bindings
        }

        private mutating func collectDerivedBindings(_ source: DataSource, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            guard case .subquery(let query) = source else { preconditionFailure("Expected derived source") }
            if collectingOrdering {
                var derived = self
                derived.fieldsByEntity = [:]
                derived.collect(query, outerScopes: outerScopes)
                for (entity, fields) in derived.fieldsByEntity {
                    fieldsByEntity[entity, default: []].formUnion(fields)
                }
                let names = queryColumnNames(query, declarations: commonTableColumns) ?? []
                return derived.fieldsByEntity.keys.sorted().compactMap { entityName in
                    guard let entity = schema.entity(named: entityName),
                          let dependencies = derived.fieldsByEntity[entityName] else { return nil }
                    // Output ordinals preserve this conservative lineage through
                    // enclosing aliases and JOIN layouts, independent of siblings.
                    let fields = names.enumerated().flatMap { ordinal, name in
                        dependencies.sorted().map { field in
                            SourceBinding.Field(visibleName: name, entityFieldName: field,
                                outputOrdinal: ordinal)
                        }
                    }
                    return SourceBinding(entity: entity, qualifiers: [], fields: fields)
                }
            }
            collect(query, outerScopes: outerScopes)
            return []
        }

        private mutating func collectJoinBindings(_ source: DataSource, commonTableNames: Set<String>, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            guard case .join(let join) = source else { preconditionFailure("Expected JOIN source") }
            let leftBindings = collectBindings(
                join.left,
                commonTableNames: commonTableNames,
                outerScopes: outerScopes
            )
            let rightOuterScopes: [[SourceBinding]]
            switch join.type {
            case .lateral, .leftLateral:
                rightOuterScopes = [leftBindings] + outerScopes
            default:
                rightOuterScopes = outerScopes
            }
            let rightBindings = collectBindings(
                join.right,
                commonTableNames: commonTableNames,
                outerScopes: rightOuterScopes
            )
            return finishJoinBindings(join, leftBindings: leftBindings, rightBindings: rightBindings, outerScopes: outerScopes)
        }

        private mutating func finishJoinBindings(_ join: JoinClause, leftBindings: [SourceBinding], rightBindings: [SourceBinding], outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            // Child scopes have completed before condition and layout temporaries are allocated.
            let bindings = leftBindings + rightBindings
            if let condition = join.condition {
                switch condition {
                case .on(let expression):
                    collect(
                        expression,
                        localBindings: bindings,
                        outerScopes: outerScopes
                    )
                case .using(let columns):
                    for column in columns {
                        include(
                            ColumnRef(column: column),
                            localBindings: bindings,
                            outerScopes: outerScopes
                        )
                    }
                }
            } else if isNatural(join.type) {
                let commonColumns = fieldNames(in: leftBindings)
                    .intersection(fieldNames(in: rightBindings))
                for column in commonColumns {
                    include(
                        ColumnRef(column: column),
                        localBindings: bindings,
                        outerScopes: outerScopes
                    )
                }
            }
            guard let leftNames = sourceColumnNames(join.left, bindings: leftBindings),
                  let rightNames = sourceColumnNames(join.right, bindings: rightBindings),
                  let layout = joinedColumns(join, left: leftNames, right: rightNames) else {
                return conservativeSetOperationBindings([leftBindings, rightBindings])
            }
            return remap(leftBindings, ordinals: layout.left, names: layout.names)
                + remap(rightBindings, ordinals: layout.right, names: layout.names)
        }

        private mutating func collectSetBindings(_ source: DataSource, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            guard case .setOperation(_, _, let left, let right) = source else { preconditionFailure("Expected set source") }
            collect(left, outerScopes: outerScopes)
            collect(right, outerScopes: outerScopes)
            return []
        }

        private mutating func collectAliasedBindings(_ source: DataSource, commonTableNames: Set<String>, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            guard case .aliased(let inner, let name, let columns) = source else { preconditionFailure("Expected aliased source") }
            let bindings = collectBindings(inner, commonTableNames: commonTableNames, outerScopes: outerScopes)
            return aliasedBindings(bindings, name: name, columns: columns)
        }

        private func aliasedBindings(_ bindings: [SourceBinding], name: String, columns: [String]?) -> [SourceBinding] {
            // Alias mapping begins after the inner source has returned.
            return bindings.map { binding in
                SourceBinding(entity: binding.entity, qualifiers: [name], fields: binding.fields.map { field in
                    let visibleName: String
                    if let columns, field.outputOrdinal < columns.count { visibleName = columns[field.outputOrdinal] }
                    else { visibleName = field.visibleName }
                    return SourceBinding.Field(visibleName: visibleName,
                        entityFieldName: field.entityFieldName, outputOrdinal: field.outputOrdinal)
                })
            }
        }

        #if DATABASE_MULTI_BASE
        private mutating func collectBaseBindings(_ source: DataSource, commonTableNames: Set<String>, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            guard case .base(_, let source) = source else { preconditionFailure("Expected Base source") }
            return collectBindings(source, commonTableNames: commonTableNames, outerScopes: outerScopes)
        }
        #endif

        private mutating func collectValuesBindings(_ source: DataSource, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            guard case .values(let rows) = source else { preconditionFailure("Expected VALUES source") }
            for row in rows { for expression in row { collect(expression, localBindings: [], outerScopes: outerScopes) } }
            return []
        }

        mutating func include(
            _ column: ColumnRef,
            localBindings: [SourceBinding],
            outerScopes: [[SourceBinding]]
        ) {
            let bindings = matchingBindings(
                column: column,
                localBindings: localBindings,
                outerScopes: outerScopes
            )
            if bindings.isEmpty, column.table == nil {
                includeWholeRow(qualifier: column.column,
                    localBindings: localBindings, outerScopes: outerScopes)
            }
            for binding in bindings {
                fieldsByEntity[binding.entity.name, default: []]
                    .insert(binding.entityFieldName)
            }
        }

        mutating func includeWholeRow(qualifier: String,
            localBindings: [SourceBinding], outerScopes: [[SourceBinding]]) {
            for binding in matchingBindings(qualifier: qualifier,
                localBindings: localBindings, outerScopes: outerScopes) {
                fieldsByEntity[binding.entity.name, default: []].formUnion(
                    binding.fields.lazy.map { $0.entityFieldName })
            }
        }

        private struct ResolvedFieldBinding {
            let entity: Schema.Entity
            let entityFieldName: String
        }

        private func matchingBindings(
            column: ColumnRef,
            localBindings: [SourceBinding],
            outerScopes: [[SourceBinding]]
        ) -> [ResolvedFieldBinding] {
            for scope in [localBindings] + outerScopes {
                let matches = scope.flatMap { binding -> [ResolvedFieldBinding] in
                    guard column.table.map(binding.qualifiers.contains) ?? true
                    else { return [] }
                    let fields: [String]
                    if collectingOrdering {
                        fields = binding.fields.filter { $0.visibleName == column.column }.map { $0.entityFieldName }
                    } else { fields = binding.entityFieldName(for: column.column).map { [$0] } ?? [] }
                    return fields.map { ResolvedFieldBinding(entity: binding.entity, entityFieldName: $0) }
                }
                if !matches.isEmpty { return matches }
            }
            return []
        }

        func matchingBindings(
            qualifier: String,
            localBindings: [SourceBinding],
            outerScopes: [[SourceBinding]]
        ) -> [SourceBinding] {
            for scope in [localBindings] + outerScopes {
                let matches = scope.filter {
                    $0.qualifiers.contains(qualifier)
                }
                if !matches.isEmpty { return matches }
            }
            return []
        }

        mutating func includeAll(_ entity: Schema.Entity) {
            fieldsByEntity[entity.name, default: []]
                .formUnion(entity.allFields)
        }

        mutating func collect(
            _ expression: Expression,
            localBindings: [SourceBinding],
            outerScopes: [[SourceBinding]]
        ) {
            switch expression {
            case .wholeRow(let qualifier):
                includeWholeRow(qualifier: qualifier,
                    localBindings: localBindings, outerScopes: outerScopes)
            case .column(let column):
                include(
                    column,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
            case .field(let operand, let name) where collectingOrdering:
                let qualifier: String?
                if case .wholeRow(let source) = operand { qualifier = source }
                else if case .column(let column) = operand, column.table == nil,
                        matchingBindings(column: column, localBindings: localBindings, outerScopes: outerScopes).isEmpty,
                        !matchingBindings(qualifier: column.column, localBindings: localBindings, outerScopes: outerScopes).isEmpty {
                    qualifier = column.column
                } else { qualifier = nil }
                if let qualifier {
                    include(ColumnRef(table: qualifier, column: name),
                        localBindings: localBindings, outerScopes: outerScopes)
                } else {
                    collect(operand, localBindings: localBindings, outerScopes: outerScopes)
                }
            case .collectionOperation(_, let lhs, let rhs),
                 .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                    .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                    .modulo(let lhs, let rhs), .concatenate(let lhs, let rhs), .equal(let lhs, let rhs),
                    .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                    .lessThanOrEqual(let lhs, let rhs),
                    .greaterThan(let lhs, let rhs),
                    .greaterThanOrEqual(let lhs, let rhs),
                    .and(let lhs, let rhs), .or(let lhs, let rhs),
                    .nullIf(let lhs, let rhs), .isDistinctFrom(let lhs, let rhs, _):
                collect(
                    lhs,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
                collect(
                    rhs,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
            case .collate(let operand, _), .field(let operand, _), .fieldExpansion(let operand), .negate(let operand), .not(let operand),
                    .isNull(let operand), .isNotNull(let operand), .truthTest(let operand, _, _),
                    .regex(let operand, _, _),
                    .cast(let operand, _), .isTriple(let operand),
                    .subject(let operand), .predicate(let operand),
                    .object(let operand):
                collect(
                    operand,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
            case .arrayAccess(let base, let subscripts):
                collect(base, localBindings: localBindings, outerScopes: outerScopes)
                for subscriptValue in subscripts {
                    switch subscriptValue {
                    case .index(let value):
                        collect(value, localBindings: localBindings, outerScopes: outerScopes)
                    case .slice(let lower, let upper):
                        if let lower { collect(lower, localBindings: localBindings, outerScopes: outerScopes) }
                        if let upper { collect(upper, localBindings: localBindings, outerScopes: outerScopes) }
                    }
                }
            case .patternMatch(let subject, let pattern, let escape, _):
                collect(subject, localBindings: localBindings, outerScopes: outerScopes)
                collect(pattern, localBindings: localBindings, outerScopes: outerScopes)
                if let escape {
                    collect(escape, localBindings: localBindings, outerScopes: outerScopes)
                }
            case .between(let operand, let lower, let upper):
                for child in [operand, lower, upper] {
                    collect(
                        child,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            case .inList(let operand, let values),
                    .notInList(let operand, let values):
                collect(
                    operand,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
                for value in values {
                    collect(
                        value,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            case .quantifiedComparison(let lhs, _, _, let source):
                collect(lhs, localBindings: localBindings, outerScopes: outerScopes)
                switch source {
                case .array(let value): collect(value, localBindings: localBindings, outerScopes: outerScopes)
                case .subquery(let query): collect(query, outerScopes: [localBindings] + outerScopes)
                }
            case .inSubquery(let operand, let query):
                collect(
                    operand,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
                collect(
                    query,
                    outerScopes: [localBindings] + outerScopes
                )
            case .aggregate(let aggregate):
                collect(
                    aggregate,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
            case .function(let function):
                for argument in function.arguments {
                    collect(
                        argument,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            case .caseWhen(let operand, let cases, let elseResult):
                if let operand { collect(operand, localBindings: localBindings, outerScopes: outerScopes) }
                for pair in cases {
                    collect(
                        pair.condition,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                    collect(
                        pair.result,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
                if let elseResult {
                    collect(
                        elseResult,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            case .coalesce(let expressions), .array(let expressions), .row(let expressions), .greatest(let expressions), .least(let expressions):
                for expression in expressions {
                    collect(
                        expression,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            case .triple(let subject, let predicate, let object):
                for child in [subject, predicate, object] {
                    collect(
                        child,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            case .subquery(let query), .exists(let query), .arraySubquery(let query):
                collect(
                    query,
                    outerScopes: [localBindings] + outerScopes
                )
            case .literal, .currentValue, .variable, .parameter, .bound:
                break
            }
        }

        mutating func collect(
            _ aggregate: AggregateFunction,
            localBindings: [SourceBinding],
            outerScopes: [[SourceBinding]]
        ) {
            switch aggregate {
            case .count(let expression, _):
                if let expression {
                    collect(
                        expression,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            case .sum(let expression, _), .avg(let expression, _),
                    .min(let expression), .max(let expression),
                    .groupConcat(let expression, _, _),
                    .sample(let expression):
                collect(
                    expression,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
            case .arrayAgg(let expression, let orderBy, _):
                collect(
                    expression,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
                for sortKey in orderBy ?? [] {
                    collect(
                        sortKey.expression,
                        localBindings: localBindings,
                        outerScopes: outerScopes
                    )
                }
            }
        }

        func fieldNames(in bindings: [SourceBinding]) -> Set<String> {
            bindings.reduce(into: Set<String>()) { result, binding in
                result.formUnion(binding.fields.map { $0.visibleName })
            }
        }

        mutating func conservativeSetOperationBindings(
            _ branches: [[SourceBinding]]
        ) -> [SourceBinding] {
            let bindings = branches.flatMap { $0 }
            for binding in bindings {
                includeAll(binding.entity)
            }
            return bindings
        }

        func sourceColumnNames(
            _ source: DataSource,
            bindings: [SourceBinding]
        ) -> [String]? {
            switch source {
            case .table(let table):
                if let declaration = commonTableColumns[table.table] { return declaration.names }
                return schema.entity(named: table.table)?.allFields
            case .logical:
                guard bindings.count == 1, let first = bindings.first else { return nil }
                return first.entity.allFields
            case .values(let rows):
                return rows.first?.indices.map { "column\($0 + 1)" } ?? []
            case .aliased(let inner, _, let columns):
                guard let names = sourceColumnNames(inner, bindings: bindings) else { return nil }
                return names.enumerated().map { index, name in
                    guard let columns, index < columns.count else { return name }
                    return columns[index]
                }
            case .subquery(let query):
                return queryColumnNames(query, declarations: commonTableColumns)
            case .setOperation(_, _, let left, _):
                return queryColumnNames(left, declarations: commonTableColumns)
            case .graphTable(let source):
                return source.columns?.map { $0.alias }
            #if DATABASE_MULTI_BASE
            case .base(_, let source):
                return sourceColumnNames(source, bindings: bindings)
            #endif
            case .join(let join):
                guard let left = sourceColumnNames(join.left, bindings: bindings),
                      let right = sourceColumnNames(join.right, bindings: bindings) else { return nil }
                return joinedColumns(join, left: left, right: right)?.names
            case .graphPattern, .namedGraph, .service:
                return nil
            }
        }

        func joinedColumns(_ join: JoinClause, left: [String], right: [String])
            -> (names: [String], left: [Int], right: [Int])? {
            let merged: [String]
            if case .using(let columns) = join.condition { merged = columns }
            else if isNatural(join.type) { merged = left.filter { right.contains($0) } }
            else { merged = [] }
            var names: [String] = []
            var leftOrdinals = Array(repeating: -1, count: left.count)
            var rightOrdinals = Array(repeating: -1, count: right.count)
            for name in merged {
                guard let leftIndex = left.firstIndex(of: name), let rightIndex = right.firstIndex(of: name) else { return nil }
                leftOrdinals[leftIndex] = names.count
                rightOrdinals[rightIndex] = names.count
                names.append(name)
            }
            for index in left.indices where leftOrdinals[index] < 0 {
                leftOrdinals[index] = names.count
                names.append(left[index])
            }
            for index in right.indices where rightOrdinals[index] < 0 {
                rightOrdinals[index] = names.count
                names.append(right[index])
            }
            return (names, leftOrdinals, rightOrdinals)
        }

        func remap(_ bindings: [SourceBinding], ordinals: [Int], names: [String]) -> [SourceBinding] {
            bindings.map { binding in
                SourceBinding(entity: binding.entity, qualifiers: binding.qualifiers,
                    fields: binding.fields.map { field in
                        let ordinal = ordinals[field.outputOrdinal]
                        return SourceBinding.Field(visibleName: names[ordinal],
                            entityFieldName: field.entityFieldName, outputOrdinal: ordinal)
                    })
            }
        }

        func projectionColumnNames(
            _ projection: [ProjectionItem], source: DataSource, sourceColumns: [String]?
        ) -> [String]? {
            var names: [String] = []
            for item in projection {
                switch item {
                case .wildcard(let qualifier):
                    let columns = qualifier.map { qualifiedColumnNames(source, qualifier: $0) } ?? sourceColumns
                    guard let columns else { return nil }
                    names.append(contentsOf: columns)
                case .expression(let expression, let alias):
                    names.append(alias ?? CanonicalQueryTypeBinder.outputName(expression, index: names.count))
                }
            }
            return names
        }

        private func qualifiedColumnNames(_ source: DataSource, qualifier: String) -> [String]? {
            switch source {
            case .aliased(_, let name, _):
                return name == qualifier ? sourceColumnNames(source, bindings: []) : nil
            case .table(let table):
                return table.effectiveName == qualifier ? sourceColumnNames(source, bindings: []) : nil
            case .logical(let logical):
                return logical.effectiveName == qualifier ? sourceColumnNames(source, bindings: []) : nil
            case .join(let join):
                return qualifiedColumnNames(join.left, qualifier: qualifier)
                    ?? qualifiedColumnNames(join.right, qualifier: qualifier)
            #if DATABASE_MULTI_BASE
            case .base(_, let nested): return qualifiedColumnNames(nested, qualifier: qualifier)
            #endif
            default: return nil
            }
        }

        private func aliasedNames(_ names: [String]?, columns: [String]?) -> [String]? {
            guard let names else { return nil }
            guard let columns else { return names }
            guard columns.count <= names.count else { return nil }
            return columns + names.dropFirst(columns.count)
        }

        private func queryColumnNames(_ query: SelectQuery,
            declarations: [String: CommonTableColumns]) -> [String]? {
            var nested = self
            nested.commonTableColumns = declarations
            for cte in query.subqueries ?? [] {
                let names = nested.queryColumnNames(cte.query, declarations: nested.commonTableColumns)
                nested.commonTableColumns[cte.name] = CommonTableColumns(
                    names: aliasedNames(names, columns: cte.columns))
            }
            return nested.projectionColumnNames(query.projection, source: query.source,
                sourceColumns: nested.sourceColumnNames(query.source, bindings: []))
        }

        func isNatural(_ type: JoinType) -> Bool {
            switch type {
            case .natural, .naturalLeft, .naturalRight, .naturalFull:
                return true
            default:
                return false
            }
        }

        mutating func collectIndexFields(
            _ accessPath: AccessPath?,
            bindings: [SourceBinding]
        ) {
            guard case .index(let scan) = accessPath else { return }
            for binding in bindings {
                guard let descriptor = binding.entity.indexDescriptors.first(
                    where: { $0.name == scan.indexName }
                ) else { continue }
                fieldsByEntity[binding.entity.name, default: []].formUnion(
                    descriptor.fieldNames
                )
                fieldsByEntity[binding.entity.name, default: []].formUnion(
                    descriptor.includedFieldNames
                )
            }
        }

        mutating func collectRDFDatasetFields() {
            for entity in schema.entities {
                let descriptors = entity.indexDescriptors.filter {
                    $0.type == .graph(.rdf)
                        || $0.type == .graph(.ontologyProjection)
                }
                guard !descriptors.isEmpty else { continue }
                for descriptor in descriptors {
                    fieldsByEntity[entity.name, default: []].formUnion(
                        descriptor.fieldNames
                    )
                    fieldsByEntity[entity.name, default: []].formUnion(
                        descriptor.includedFieldNames
                    )
                }
                if descriptors.contains(where: {
                    $0.type == .graph(.ontologyProjection)
                }) {
                    includeAll(entity)
                }
            }
        }
    }
}
