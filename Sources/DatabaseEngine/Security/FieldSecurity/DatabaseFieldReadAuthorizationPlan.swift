import DatabaseKit

/// Resolves the persisted fields one canonical query can observe while
/// filtering, ordering, joining, projecting, or reading an index.
package struct DatabaseFieldReadAuthorizationPlan: Sendable {
    package let fieldsByEntity: [String: Set<String>]

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
        fields.formUnion(descriptor.storedFieldNames)
        if descriptor.kindIdentifier == "owl_class_rdf" {
            fields.formUnion(entity.allFields)
        }
        return DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [entity.name: fields]
        )
    }

    private struct SourceBinding {
        let entity: Schema.Entity
        let qualifiers: Set<String>
    }

    private struct Collector {
        let schema: Schema
        var fieldsByEntity: [String: Set<String>] = [:]

        mutating func collect(_ query: SelectQuery) {
            let commonTableNames = Set(
                query.subqueries?.map { $0.name } ?? []
            )
            if let subqueries = query.subqueries {
                for subquery in subqueries { collect(subquery.query) }
            }
            if isSPARQL(query.source) {
                collectRDFDatasetFields()
                return
            }
            var bindings: [SourceBinding] = []
            collectBindings(
                query.source,
                commonTableNames: commonTableNames,
                into: &bindings
            )
            switch query.projection {
            case .all:
                for binding in bindings { includeAll(binding.entity) }
            case .allFrom(let qualifier):
                for binding in bindings
                    where binding.qualifiers.contains(qualifier) {
                    includeAll(binding.entity)
                }
            case .items, .distinctItems:
                break
            }
            for column in query.referencedColumns {
                include(column, bindings: bindings)
            }
            collectUsingColumns(query.source, bindings: bindings)
            collectIndexFields(query.accessPath, bindings: bindings)
        }

        mutating func collectBindings(
            _ source: DataSource,
            commonTableNames: Set<String>,
            into bindings: inout [SourceBinding]
        ) {
            switch source {
            case .table(let table):
                guard !commonTableNames.contains(table.table),
                      let entity = schema.entity(named: table.table) else {
                    return
                }
                var qualifiers: Set<String> = [
                    table.table,
                    table.effectiveName,
                ]
                if let alias = table.alias { qualifiers.insert(alias) }
                if fieldsByEntity[entity.name] == nil {
                    fieldsByEntity[entity.name] = []
                }
                bindings.append(
                    SourceBinding(entity: entity, qualifiers: qualifiers)
                )
            case .logical(let source):
                guard source.kindIdentifier == LogicalSourceKind.polymorphic
                else { return }
                for entity in schema.entities
                    where entity.polymorphicMembership?.identifier
                        == source.identifier {
                    if fieldsByEntity[entity.name] == nil {
                        fieldsByEntity[entity.name] = []
                    }
                    var qualifiers: Set<String> = [source.effectiveName]
                    if let alias = source.alias { qualifiers.insert(alias) }
                    bindings.append(
                        SourceBinding(entity: entity, qualifiers: qualifiers)
                    )
                }
            case .subquery(let query, _):
                collect(query)
            case .join(let join):
                collectBindings(
                    join.left,
                    commonTableNames: commonTableNames,
                    into: &bindings
                )
                collectBindings(
                    join.right,
                    commonTableNames: commonTableNames,
                    into: &bindings
                )
            case .union(let sources), .unionAll(let sources),
                 .intersect(let sources):
                for source in sources {
                    collectBindings(
                        source,
                        commonTableNames: commonTableNames,
                        into: &bindings
                    )
                }
            case .except(let left, let right):
                collectBindings(
                    left,
                    commonTableNames: commonTableNames,
                    into: &bindings
                )
                collectBindings(
                    right,
                    commonTableNames: commonTableNames,
                    into: &bindings
                )
            case .graphTable:
                for entity in schema.entities { includeAll(entity) }
            case .graphPattern, .namedGraph, .service:
                collectRDFDatasetFields()
            case .values:
                break
            }
        }

        mutating func include(
            _ column: ColumnRef,
            bindings: [SourceBinding]
        ) {
            for binding in bindings {
                guard column.table.map(binding.qualifiers.contains) ?? true,
                      binding.entity.fieldMapByName[column.column] != nil else {
                    continue
                }
                fieldsByEntity[binding.entity.name, default: []]
                    .insert(column.column)
            }
        }

        mutating func includeAll(_ entity: Schema.Entity) {
            fieldsByEntity[entity.name, default: []]
                .formUnion(entity.allFields)
        }

        mutating func collectUsingColumns(
            _ source: DataSource,
            bindings: [SourceBinding]
        ) {
            switch source {
            case .join(let join):
                if case .using(let columns) = join.condition {
                    for column in columns {
                        include(ColumnRef(column: column), bindings: bindings)
                    }
                }
                collectUsingColumns(join.left, bindings: bindings)
                collectUsingColumns(join.right, bindings: bindings)
            case .union(let sources), .unionAll(let sources),
                 .intersect(let sources):
                for source in sources {
                    collectUsingColumns(source, bindings: bindings)
                }
            case .except(let left, let right):
                collectUsingColumns(left, bindings: bindings)
                collectUsingColumns(right, bindings: bindings)
            default:
                break
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
                    descriptor.storedFieldNames
                )
            }
        }

        mutating func collectRDFDatasetFields() {
            for entity in schema.entities {
                let descriptors = entity.indexDescriptors.filter {
                    $0.kindIdentifier == "rdf_quad"
                        || $0.kindIdentifier == "owl_class_rdf"
                }
                guard !descriptors.isEmpty else { continue }
                for descriptor in descriptors {
                    fieldsByEntity[entity.name, default: []].formUnion(
                        descriptor.fieldNames
                    )
                    fieldsByEntity[entity.name, default: []].formUnion(
                        descriptor.storedFieldNames
                    )
                }
                if descriptors.contains(where: {
                    $0.kindIdentifier == "owl_class_rdf"
                }) {
                    includeAll(entity)
                }
            }
        }

        func isSPARQL(_ source: DataSource) -> Bool {
            switch source {
            case .graphPattern, .namedGraph, .service:
                return true
            default:
                return false
            }
        }
    }
}
