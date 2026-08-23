import DatabaseKit

/// Resolves the persisted fields one canonical query can observe while
/// filtering, ordering, joining, projecting, or reading an index.
package struct DatabaseFieldReadAuthorizationPlan: Sendable {
    package let fieldsByEntity: [String: Set<String>]

    package init(fieldsByEntity: [String: Set<String>]) {
        self.fieldsByEntity = fieldsByEntity
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

    package static func fields(
        entity: Schema.Entity,
        names: Set<String>
    ) -> DatabaseFieldReadAuthorizationPlan {
        DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [entity.name: names]
        )
    }

    package static func fullEntity(
        _ entity: Schema.Entity,
        including additionalFields: Set<String> = []
    ) -> DatabaseFieldReadAuthorizationPlan {
        DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [
                entity.name: Set(entity.allFields).union(additionalFields)
            ]
        )
    }

    private struct SourceBinding {
        struct Field: Sendable {
            let visibleName: String
            let entityFieldName: String
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
            self.fields = fields ?? entity.allFields.map {
                Field(visibleName: $0, entityFieldName: $0)
            }
        }

        func entityFieldName(for visibleName: String) -> String? {
            fields.first { $0.visibleName == visibleName }?.entityFieldName
        }
    }

    private struct Collector {
        let schema: Schema
        var fieldsByEntity: [String: Set<String>] = [:]

        mutating func collect(
            _ query: SelectQuery,
            outerScopes: [[SourceBinding]] = []
        ) {
            let commonTableNames = Set(
                query.subqueries?.map { $0.name } ?? []
            )
            if let subqueries = query.subqueries {
                for subquery in subqueries {
                    collect(
                        subquery.query,
                        outerScopes: outerScopes
                    )
                }
            }
            let bindings = collectBindings(
                query.source,
                commonTableNames: commonTableNames,
                outerScopes: outerScopes
            )
            switch query.projection {
            case .all:
                for binding in bindings { includeAll(binding.entity) }
            case .allFrom(let qualifier):
                for binding in matchingBindings(
                    qualifier: qualifier,
                    localBindings: bindings,
                    outerScopes: outerScopes
                ) {
                    includeAll(binding.entity)
                }
            case .items(let items), .distinctItems(let items):
                for item in items {
                    collect(
                        item.expression,
                        localBindings: bindings,
                        outerScopes: outerScopes
                    )
                }
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

        mutating func collectBindings(
            _ source: DataSource,
            commonTableNames: Set<String>,
            outerScopes: [[SourceBinding]]
        ) -> [SourceBinding] {
            switch source {
            case .table(let table):
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
            case .logical(let source):
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
            case .subquery(let query, _):
                collect(query, outerScopes: outerScopes)
                return []
            case .join(let join):
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
                return bindings
            case .union(let sources), .unionAll(let sources),
                 .intersect(let sources):
                var branchBindings: [[SourceBinding]] = []
                branchBindings.reserveCapacity(sources.count)
                for source in sources {
                    branchBindings.append(collectBindings(
                        source,
                        commonTableNames: commonTableNames,
                        outerScopes: outerScopes
                    ))
                }
                return setOperationBindings(
                    sources: sources,
                    branches: branchBindings
                )
            case .except(let left, let right):
                let leftBindings = collectBindings(
                    left,
                    commonTableNames: commonTableNames,
                    outerScopes: outerScopes
                )
                let rightBindings = collectBindings(
                    right,
                    commonTableNames: commonTableNames,
                    outerScopes: outerScopes
                )
                return setOperationBindings(
                    sources: [left, right],
                    branches: [leftBindings, rightBindings]
                )
            #if DATABASE_MULTI_BASE
            case .base(_, let source):
                return collectBindings(
                    source,
                    commonTableNames: commonTableNames,
                    outerScopes: outerScopes
                )
            #endif
            case .graphTable:
                for entity in schema.entities { includeAll(entity) }
                return []
            case .graphPattern, .namedGraph, .service:
                collectRDFDatasetFields()
                return []
            case .values:
                return []
            }
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
            for binding in bindings {
                fieldsByEntity[binding.entity.name, default: []]
                    .insert(binding.entityFieldName)
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
                let matches = scope.compactMap { binding -> ResolvedFieldBinding? in
                    guard column.table.map(binding.qualifiers.contains) ?? true
                    else { return nil }
                    guard let entityFieldName = binding.entityFieldName(
                        for: column.column
                    ) else {
                        return nil
                    }
                    return ResolvedFieldBinding(
                        entity: binding.entity,
                        entityFieldName: entityFieldName
                    )
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
            case .column(let column):
                include(
                    column,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
            case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                    .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                    .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                    .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                    .lessThanOrEqual(let lhs, let rhs),
                    .greaterThan(let lhs, let rhs),
                    .greaterThanOrEqual(let lhs, let rhs),
                    .and(let lhs, let rhs), .or(let lhs, let rhs),
                    .nullIf(let lhs, let rhs):
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
            case .negate(let operand), .not(let operand),
                    .isNull(let operand), .isNotNull(let operand),
                    .like(let operand, _), .regex(let operand, _, _),
                    .cast(let operand, _), .isTriple(let operand),
                    .subject(let operand), .predicate(let operand),
                    .object(let operand):
                collect(
                    operand,
                    localBindings: localBindings,
                    outerScopes: outerScopes
                )
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
            case .caseWhen(let cases, let elseResult):
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
            case .coalesce(let expressions):
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
            case .subquery(let query), .exists(let query):
                collect(
                    query,
                    outerScopes: [localBindings] + outerScopes
                )
            case .literal, .variable, .parameter, .bound:
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

        mutating func setOperationBindings(
            sources: [DataSource],
            branches: [[SourceBinding]]
        ) -> [SourceBinding] {
            guard sources.count == branches.count,
                  let firstSource = sources.first,
                  let firstBindings = branches.first,
                  let outputNames = sourceColumnNames(
                      firstSource,
                      bindings: firstBindings
                  ) else {
                return conservativeSetOperationBindings(branches)
            }
            var result: [SourceBinding] = []
            for (source, branch) in zip(sources, branches) {
                guard branch.count <= 1,
                      let branchNames = sourceColumnNames(
                          source,
                          bindings: branch
                      ),
                      branchNames.count == outputNames.count else {
                    return conservativeSetOperationBindings(branches)
                }
                guard let binding = branch.first else { continue }
                guard binding.fields.count == branchNames.count else {
                    return conservativeSetOperationBindings(branches)
                }
                result.append(
                    SourceBinding(
                        entity: binding.entity,
                        qualifiers: [],
                        fields: zip(outputNames, binding.fields).map {
                            outputName, branchField in
                            SourceBinding.Field(
                                visibleName: outputName,
                                entityFieldName: branchField.entityFieldName
                            )
                        }
                    )
                )
            }
            return result
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
            case .table, .logical:
                guard bindings.count == 1 else { return nil }
                return bindings[0].fields.map { $0.visibleName }
            case .values(let rows, let columnNames):
                return columnNames
                    ?? rows.first?.indices.map { "column\($0)" }
                    ?? []
            case .subquery(let query, _):
                return projectionColumnNames(
                    query.projection,
                    sourceColumns: sourceColumnNames(
                        query.source,
                        bindings: bindings
                    )
                )
            case .union(let sources), .unionAll(let sources),
                    .intersect(let sources):
                guard let first = sources.first else { return [] }
                return sourceColumnNames(first, bindings: bindings)
            case .except(let left, _):
                return sourceColumnNames(left, bindings: bindings)
            case .graphTable(let source):
                return source.columns?.map { $0.alias }
            #if DATABASE_MULTI_BASE
            case .base(_, let source):
                return sourceColumnNames(source, bindings: bindings)
            #endif
            case .join, .graphPattern, .namedGraph, .service:
                return nil
            }
        }

        func projectionColumnNames(
            _ projection: Projection,
            sourceColumns: [String]?
        ) -> [String]? {
            switch projection {
            case .all:
                return sourceColumns
            case .allFrom:
                return sourceColumns
            case .items(let items), .distinctItems(let items):
                return items.enumerated().map { index, item in
                    item.alias ?? projectionName(
                        item.expression,
                        index: index
                    )
                }
            }
        }

        func projectionName(
            _ expression: Expression,
            index: Int
        ) -> String {
            switch expression {
            case .column(let column):
                return column.column
            case .aggregate(let aggregate):
                switch aggregate {
                case .count: return "count"
                case .sum: return "sum"
                case .avg: return "avg"
                case .min: return "min"
                case .max: return "max"
                case .groupConcat: return "group_concat"
                case .sample: return "sample"
                case .arrayAgg: return "array_agg"
                }
            default:
                return "column\(index)"
            }
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
