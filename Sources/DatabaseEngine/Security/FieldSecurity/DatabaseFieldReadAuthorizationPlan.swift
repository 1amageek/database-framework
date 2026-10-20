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

    package static func make(
        query: SelectQuery,
        schema: Schema,
        sqlResolution: SQLReadResolution?
    ) throws -> DatabaseFieldReadAuthorizationPlan {
        var collector = Collector(schema: schema, sqlResolution: sqlResolution)
        collector.collect(query)
        if let failure = collector.resolutionFailure { throw failure }
        return DatabaseFieldReadAuthorizationPlan(fieldsByEntity: collector.fieldsByEntity)
    }

    /// Resolves selected ORDER inputs without treating source JOIN predicates
    /// or unrelated projected fields as ordering dependencies.
    package static func orderingFields(query: SelectQuery, inputs: [Expression], schema: Schema)
        -> [String: Set<String>] {
        var collector = Collector(schema: schema, collectingOrdering: true)
        return collector.orderingFields(query: query, inputs: inputs)
    }

    package static func orderingFields(query: SelectQuery, inputs: [Expression], schema: Schema,
        sqlResolution: SQLReadResolution?) throws -> [String: Set<String>] {
        var collector = Collector(schema: schema, sqlResolution: sqlResolution, collectingOrdering: true)
        let fields = collector.orderingFields(query: query, inputs: inputs)
        if let failure = collector.resolutionFailure { throw failure }
        return fields
    }

    package static func orderingProjection(query: SelectQuery, schema: Schema,
        sqlResolution: SQLReadResolution) throws -> [(expression: Expression, alias: String?)]? {
        var collector = Collector(schema: schema, sqlResolution: sqlResolution, collectingOrdering: true)
        collector.collectCommonTables(query, outerScopes: [])
        let bindings = collector.collectBindings(query.source,
            commonTableNames: CanonicalSourceNames(collector.commonTableColumns.keys.map { $0.value }), outerScopes: [])
        if let failure = collector.resolutionFailure { throw failure }
        var items: [(expression: Expression, alias: String?)] = []
        for item in query.projection {
            switch item {
            case .expression(let expression, let alias):
                // The output name is resolved here, where subquery output columns are known.
                items.append((expression, alias ?? collector.outputName(expression, index: items.count)))
            case .wildcard(let qualifier):
                let names = qualifier.map { collector.qualifiedColumnNames(query.source, qualifier: $0) }
                    ?? collector.sourceColumnNames(query.source, bindings: bindings)
                guard let names else { return nil }
                items.append(contentsOf: names.map { (.column(ColumnRef(table: qualifier, column: $0)), nil) })
            }
        }
        return items
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
            let entityFieldNames: [String]
            let outputOrdinal: Int

            init(visibleName: String, entityFieldName: String, outputOrdinal: Int) {
                self.init(visibleName: visibleName, entityFieldNames: [entityFieldName], outputOrdinal: outputOrdinal)
            }

            init(visibleName: String, entityFieldNames: [String], outputOrdinal: Int) {
                self.visibleName = visibleName
                self.entityFieldNames = entityFieldNames
                self.outputOrdinal = outputOrdinal
            }
        }

        let entity: Schema.Entity
        let qualifiers: CanonicalSourceNames
        let fields: [Field]

        init(
            entity: Schema.Entity,
            qualifiers: CanonicalSourceNames,
            fields: [Field]? = nil
        ) {
            self.entity = entity
            self.qualifiers = qualifiers
            self.fields = fields ?? entity.allFields.enumerated().map {
                Field(visibleName: $0.element, entityFieldName: $0.element, outputOrdinal: $0.offset)
            }
        }

        func entityFieldNames(for visibleName: String) -> [String] {
            fields.filter { CanonicalSourceName.equal($0.visibleName, visibleName) }.flatMap { $0.entityFieldNames }
        }
    }

    private struct Collector {
        let schema: Schema
        var sqlResolution: SQLReadResolution? = nil
        var collectingOrdering = false
        var resolutionFailure: (any Error)? = nil
        var fieldsByEntity: [String: Set<String>] = [:]
        struct CommonTableColumns {
            let names: [String]?
            var dependencies: [String: Set<String>] = [:]
        }
        var commonTableColumns: [CanonicalSourceName: CommonTableColumns] = {
            var declarations: [CanonicalSourceName: CommonTableColumns] = [:]
            for relation in ActiveSQLStatementReads.scope?.relationColumns ?? [] {
                declarations[CanonicalSourceName(relation.name)] = CommonTableColumns(names: relation.columns?.map { $0.name })
            }
            return declarations
        }()

        mutating func collect(
            _ query: SelectQuery,
            outerScopes: [[SourceBinding]] = []
        ) {
            let inherited = commonTableColumns
            defer { commonTableColumns = inherited }
            collectCommonTables(query, outerScopes: outerScopes)
            let commonTableNames = CanonicalSourceNames(commonTableColumns.keys.map { $0.value })
            let bindings = collectBindings(
                query.source,
                commonTableNames: commonTableNames,
                outerScopes: outerScopes
            )
            for item in query.projection {
                switch item {
                case .wildcard(nil):
                    for binding in bindings { includeVisibleFields(binding) }
                case .wildcard(.some(let qualifier)):
                    for binding in matchingBindings(qualifier: qualifier, localBindings: bindings, outerScopes: outerScopes) {
                        includeVisibleFields(binding)
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
            for window in query.windows {
                collect(window.specification, localBindings: bindings, outerScopes: outerScopes)
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

        mutating func collectCommonTables(_ query: SelectQuery, outerScopes: [[SourceBinding]]) {
            if query.withRecursive {
                for subquery in query.subqueries ?? [] {
                    commonTableColumns[CanonicalSourceName(subquery.name)] = CommonTableColumns(names: subquery.columns)
                }
            }
            for subquery in query.subqueries ?? [] {
                // Recursive WITH predeclares every local logical name before physical collection.
                if collectingOrdering {
                    var definition = self
                    definition.fieldsByEntity = [:]
                    definition.collect(subquery.query, outerScopes: outerScopes)
                    if resolutionFailure == nil { resolutionFailure = definition.resolutionFailure }
                    for (entity, fields) in definition.fieldsByEntity {
                        fieldsByEntity[entity, default: []].formUnion(fields)
                    }
                    commonTableColumns[CanonicalSourceName(subquery.name)] = CommonTableColumns(
                        names: declarationColumnNames(subquery, recursive: query.withRecursive),
                        dependencies: definition.fieldsByEntity)
                } else {
                    collect(subquery.query, outerScopes: outerScopes)
                    commonTableColumns[CanonicalSourceName(subquery.name)] = CommonTableColumns(
                        names: declarationColumnNames(subquery, recursive: query.withRecursive))
                }
            }
        }

        mutating func orderingFields(query: SelectQuery, inputs: [Expression]) -> [String: Set<String>] {
            collectCommonTables(query, outerScopes: [])
            let bindings = collectBindings(query.source,
                commonTableNames: CanonicalSourceNames(commonTableColumns.keys.map { $0.value }), outerScopes: [])
            let sourceDependencies = fieldsByEntity
            fieldsByEntity = [:]
            for expression in inputs { collect(expression, localBindings: bindings, outerScopes: []) }
            if bindings.isEmpty {
                collect(query)
                for (entity, fields) in sourceDependencies { fieldsByEntity[entity, default: []].formUnion(fields) }
            }
            return fieldsByEntity
        }

        // Keep case payloads and post-child work out of recursive source dispatch.
        mutating func collectBindings(
            _ source: DataSource,
            commonTableNames: CanonicalSourceNames,
            outerScopes: [[SourceBinding]]
        ) -> [SourceBinding] {
            switch source {
            case .tableFunction(let source):
                for argument in source.function.arguments {
                    collect(argument, localBindings: [], outerScopes: outerScopes)
                }
                return []
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

        private mutating func collectTableBindings(_ source: DataSource, commonTableNames: CanonicalSourceNames) -> [SourceBinding] {
            guard case .table(let table) = source else { preconditionFailure("Expected table source") }
            if table.schema == nil, commonTableNames.contains(table.table) {
                guard collectingOrdering,
                      let declaration = commonTableColumns[CanonicalSourceName(table.table)] else { return [] }
                return orderingBindings(names: declaration.names ?? [],
                    dependencies: declaration.dependencies, qualifiers: [table.effectiveName])
            }
            let entity: Schema.Entity
            let fields: [SourceBinding.Field]?
            if let sqlResolution {
                do {
                    if let descriptor = try sqlResolution.informationSchemaRelation(table) {
                        return collectingOrdering ? orderingBindings(names: try sqlResolution.informationSchemaColumns(table, descriptor: descriptor).columns.map { $0.name },
                            dependencies: [:], qualifiers: [table.effectiveName]) : []
                    }
                    let definition: SQLViewDefinition?
                    let viewID: String?
                    if let view = try sqlResolution.ordinaryView(table) { definition = view.definition; viewID = view.id }
                    else { definition = nil; viewID = nil }
                    if let definition, let viewID {
                        let nestedResolution = try sqlResolution.forView(definition, id: viewID,
                            workMeter: sqlResolution.workMeter)
                        var nested = Collector(schema: schema, sqlResolution: nestedResolution,
                            collectingOrdering: collectingOrdering)
                        nested.collect(definition.query)
                        if let failure = nested.resolutionFailure { throw failure }
                        for (name, fields) in nested.fieldsByEntity { fieldsByEntity[name, default: []].formUnion(fields) }
                        if try sqlResolution.ordinaryView(table) != nil {
                            let columns = try sqlResolution.viewColumns(table, definition: definition)
                            return collectingOrdering ? orderingBindings(names: columns.map { $0.name },
                                dependencies: nested.fieldsByEntity, qualifiers: [table.effectiveName]) : []
                        }
                    }
                    let relation = try sqlResolution.resolve(table)
                    entity = relation.entity
                    fields = try relation.columns.enumerated().map { ordinal, column in
                        let dependencies: [String]
                        if let field = column.field { dependencies = [field.name] }
                        else if case .generated(let expression) = column.declaration.policy {
                            let current = sqlResolution.snapshot.resolvedRelation(id: relation.declaration.id) ?? relation
                            guard let currentColumn = current.columns.first(where: {
                                SQLCatalogName($0.declaration.id) == SQLCatalogName(column.declaration.id)
                            }) else { throw SQLViewExecutionError.missingColumn(relation: relation.declaration.id, column: column.declaration.id) }
                            let bound = try SQLConstraintCompiler.bindGenerated(expression, column: currentColumn.declaration,
                                relation: current.declaration, physicalSchema: schema)
                            var names = Set<String>()
                            func collectDependencies(_ value: CanonicalBoundExpression) throws {
                                if let slot = value.columnSlot {
                                    guard current.columns.indices.contains(slot), let field = current.columns[slot].field else {
                                        throw SQLReadResolutionError.missingPhysicalColumn(entity: entity.name, field: column.declaration.name)
                                    }
                                    names.insert(field.name)
                                }
                                for child in value.children { try collectDependencies(child) }
                            }
                            try collectDependencies(bound)
                            dependencies = names.sorted()
                        } else {
                            throw SQLReadResolutionError.missingPhysicalColumn(entity: entity.name, field: column.declaration.name)
                        }
                        return SourceBinding.Field(visibleName: column.declaration.name,
                            entityFieldNames: dependencies, outputOrdinal: ordinal)
                    }
                } catch {
                    if resolutionFailure == nil { resolutionFailure = error }
                    return []
                }
            } else {
                guard let physical = schema.entity(named: table.table) else { return [] }
                entity = physical
                fields = nil
            }
            let qualifiers: CanonicalSourceNames = [table.effectiveName]
            if fieldsByEntity[entity.name] == nil { fieldsByEntity[entity.name] = [] }
            return [SourceBinding(entity: entity, qualifiers: qualifiers, fields: fields)]
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
                if resolutionFailure == nil { resolutionFailure = derived.resolutionFailure }
                for (entity, fields) in derived.fieldsByEntity {
                    fieldsByEntity[entity, default: []].formUnion(fields)
                }
                let names = queryColumnNames(query, declarations: commonTableColumns) ?? []
                return orderingBindings(names: names, dependencies: derived.fieldsByEntity, qualifiers: [])
            }
            collect(query, outerScopes: outerScopes)
            return []
        }

        private func orderingBindings(names: [String], dependencies: [String: Set<String>],
            qualifiers: CanonicalSourceNames) -> [SourceBinding] {
            dependencies.keys.sorted().compactMap { entityName in
                guard let entity = schema.entity(named: entityName),
                      let fields = dependencies[entityName] else { return nil }
                // Output ordinals retain conservative physical lineage through aliases and joins.
                return SourceBinding(entity: entity, qualifiers: qualifiers,
                    fields: names.enumerated().flatMap { ordinal, name in
                        fields.sorted().map { field in
                            SourceBinding.Field(visibleName: name, entityFieldName: field, outputOrdinal: ordinal)
                        }
                    })
            }
        }

        private mutating func collectJoinBindings(_ source: DataSource, commonTableNames: CanonicalSourceNames, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
            guard case .join(let join) = source else { preconditionFailure("Expected JOIN source") }
            let leftBindings = collectBindings(
                join.left,
                commonTableNames: commonTableNames,
                outerScopes: outerScopes
            )
            let rightOuterScopes: [[SourceBinding]]
            switch join.type {
            case _ where isFunctionSource(join.right):
                rightOuterScopes = [leftBindings] + outerScopes
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
            // A right function adds no physical lineage and keeps every left ordinal unchanged.
            if isFunctionSource(join.right), !isNatural(join.type) {
                switch join.condition {
                case .using: break
                default: return bindings
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

        private mutating func collectAliasedBindings(_ source: DataSource, commonTableNames: CanonicalSourceNames, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
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
                        entityFieldNames: field.entityFieldNames, outputOrdinal: field.outputOrdinal)
                })
            }
        }

        #if DATABASE_MULTI_BASE
        private mutating func collectBaseBindings(_ source: DataSource, commonTableNames: CanonicalSourceNames, outerScopes: [[SourceBinding]]) -> [SourceBinding] {
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
                    binding.fields.lazy.flatMap { $0.entityFieldNames })
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
                        fields = binding.fields.filter { CanonicalSourceName.equal($0.visibleName, column.column) }.flatMap { $0.entityFieldNames }
                    } else { fields = binding.entityFieldNames(for: column.column) }
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

        mutating func includeVisibleFields(_ binding: SourceBinding) {
            fieldsByEntity[binding.entity.name, default: []].formUnion(binding.fields.flatMap { $0.entityFieldNames })
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
            case .window(let call):
                switch call.function {
                case .aggregate(let aggregate): collect(aggregate, localBindings: localBindings, outerScopes: outerScopes)
                case .analytic(_, let arguments):
                    for argument in arguments { collect(argument, localBindings: localBindings, outerScopes: outerScopes) }
                }
                if case .specification(let specification) = call.reference {
                    collect(specification, localBindings: localBindings, outerScopes: outerScopes)
                }
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
            case .coalesce(let expressions), .array(let expressions), .row(let expressions), .greatest(let expressions), .least(let expressions), .grouping(let expressions):
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
            case .defaultValue, .literal, .currentValue, .variable, .parameter, .bound:
                break
            }
        }

        mutating func collect(
            _ aggregate: AggregateFunction,
            localBindings: [SourceBinding],
            outerScopes: [[SourceBinding]]
        ) {
            switch aggregate {
            case .sql(let call): collect(call, localBindings: localBindings, outerScopes: outerScopes)
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

        mutating func collect(_ grouping: GroupingElement, localBindings: [SourceBinding], outerScopes: [[SourceBinding]]) {
            var pending = [grouping]
            while let element = pending.popLast() {
                switch element {
                case .expression(let expression): collect(expression, localBindings: localBindings, outerScopes: outerScopes)
                case .rollup(let components), .cube(let components):
                    for expressions in components {
                        for expression in expressions { collect(expression, localBindings: localBindings, outerScopes: outerScopes) }
                    }
                case .groupingSets(let sets):
                    for elements in sets { pending.append(contentsOf: elements) }
                }
            }
        }

        mutating func collect(_ call: SQLAggregateCall, localBindings: [SourceBinding], outerScopes: [[SourceBinding]]) {
            for argument in call.arguments { collect(argument, localBindings: localBindings, outerScopes: outerScopes) }
            for key in call.orderBy { collect(key.expression, localBindings: localBindings, outerScopes: outerScopes) }
            if let filter = call.filter { collect(filter, localBindings: localBindings, outerScopes: outerScopes) }
            for key in call.withinGroup ?? [] { collect(key.expression, localBindings: localBindings, outerScopes: outerScopes) }
        }

        mutating func collect(_ specification: WindowSpecification, localBindings: [SourceBinding], outerScopes: [[SourceBinding]]) {
            for expression in specification.partitionBy { collect(expression, localBindings: localBindings, outerScopes: outerScopes) }
            for key in specification.orderBy { collect(key.expression, localBindings: localBindings, outerScopes: outerScopes) }
            if let frame = specification.frame {
                for bound in [frame.start, frame.end] {
                    switch bound {
                    case .preceding(let expression), .following(let expression):
                        collect(expression, localBindings: localBindings, outerScopes: outerScopes)
                    default: break
                    }
                }
            }
        }

        func fieldNames(in bindings: [SourceBinding]) -> CanonicalSourceNames {
            CanonicalSourceNames(bindings.flatMap { $0.fields.map { $0.visibleName } })
        }

        mutating func conservativeSetOperationBindings(
            _ branches: [[SourceBinding]]
        ) -> [SourceBinding] {
            let bindings = branches.flatMap { $0 }
            for binding in bindings {
                includeVisibleFields(binding)
            }
            return bindings
        }

        func sourceColumnNames(
            _ source: DataSource,
            bindings: [SourceBinding]
        ) -> [String]? {
            switch source {
            case .tableFunction: return nil
            case .table(let table):
                if table.schema == nil, let declaration = commonTableColumns[CanonicalSourceName(table.table)] { return declaration.names }
                if let sqlResolution {
                    do {
                        if let descriptor = try sqlResolution.informationSchemaRelation(table) {
                            return try sqlResolution.informationSchemaColumns(table, descriptor: descriptor).columns.map { $0.name }
                        }
                        return try sqlResolution.resolve(table).columns.map { $0.declaration.name }
                    }
                    catch { return nil } // Collection reports the same source failure before admission.
                }
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
            else if isNatural(join.type) { merged = left.filter { name in right.contains { CanonicalSourceName.equal($0, name) } } }
            else { merged = [] }
            var names: [String] = []
            var leftOrdinals = Array(repeating: -1, count: left.count)
            var rightOrdinals = Array(repeating: -1, count: right.count)
            for name in merged {
                guard let leftIndex = left.firstIndex(where: { CanonicalSourceName.equal($0, name) }), let rightIndex = right.firstIndex(where: { CanonicalSourceName.equal($0, name) }) else { return nil }
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
                            entityFieldNames: field.entityFieldNames, outputOrdinal: ordinal)
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
                    names.append(alias ?? outputName(expression, index: names.count))
                }
            }
            return names
        }

        /// Names an unaliased projection expression as the binder does, with
        /// a scalar subquery named by its own first output column.
        func outputName(_ expression: Expression, index: Int) -> String {
            CanonicalQueryTypeBinder.outputName(expression, index: index, subqueryName: { query in
                queryColumnNames(query, declarations: commonTableColumns)?.first
            })
        }

        func qualifiedColumnNames(_ source: DataSource, qualifier: String) -> [String]? {
            switch source {
            case .aliased(_, let name, _):
                return CanonicalSourceName.equal(name, qualifier) ? sourceColumnNames(source, bindings: []) : nil
            case .table(let table):
                return CanonicalSourceName.equal(table.effectiveName, qualifier) ? sourceColumnNames(source, bindings: []) : nil
            case .logical(let logical):
                return CanonicalSourceName.equal(logical.effectiveName, qualifier) ? sourceColumnNames(source, bindings: []) : nil
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
            declarations: [CanonicalSourceName: CommonTableColumns]) -> [String]? {
            var nested = self
            nested.commonTableColumns = declarations
            if query.withRecursive {
                for cte in query.subqueries ?? [] {
                    nested.commonTableColumns[CanonicalSourceName(cte.name)] = CommonTableColumns(names: cte.columns)
                }
            }
            for cte in query.subqueries ?? [] {
                nested.commonTableColumns[CanonicalSourceName(cte.name)] = CommonTableColumns(
                    names: nested.declarationColumnNames(cte, recursive: query.withRecursive))
            }
            return nested.projectionColumnNames(query.projection, source: query.source,
                sourceColumns: nested.sourceColumnNames(query.source, bindings: []))
        }

        private func declarationColumnNames(_ cte: NamedSubquery, recursive: Bool) -> [String]? {
            let seed: SelectQuery
            if recursive, case .setOperation(.union, _, let left, _) = cte.query.source { seed = left }
            else { seed = cte.query }
            guard var names = aliasedNames(queryColumnNames(seed, declarations: commonTableColumns), columns: cte.columns) else { return nil }
            if let search = cte.search { names.append(search.sequenceColumn) }
            if let cycle = cte.cycle { names.append(cycle.markColumn); names.append(cycle.pathColumn) }
            return names
        }

        private func isFunctionSource(_ source: DataSource) -> Bool {
            switch source {
            case .tableFunction: return true
            case .aliased(let source, _, _): return isFunctionSource(source)
            default: return false
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
