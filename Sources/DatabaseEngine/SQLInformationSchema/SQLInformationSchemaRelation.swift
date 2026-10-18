import DatabaseKit

/// A virtual source whose schema does not depend on rows or storage entities.
enum SQLInformationSchemaRelation: String, Sendable, CaseIterable {
    case tables, columns, tableConstraints = "table_constraints"
    case keyColumnUsage = "key_column_usage", referentialConstraints = "referential_constraints"
    case views, sequences

    var columnNames: [String] {
        switch self {
        case .tables: return ["table_catalog", "table_schema", "table_name", "table_type", "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action"]
        case .columns: return ["table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length", "character_octet_length", "numeric_precision", "numeric_precision_radix", "numeric_scale", "datetime_precision", "interval_type", "interval_precision", "character_set_catalog", "character_set_schema", "character_set_name", "collation_catalog", "collation_schema", "collation_name", "domain_catalog", "domain_schema", "domain_name", "udt_catalog", "udt_schema", "udt_name", "scope_catalog", "scope_schema", "scope_name", "maximum_cardinality", "dtd_identifier", "is_self_referencing", "is_identity", "identity_generation", "identity_start", "identity_increment", "identity_maximum", "identity_minimum", "identity_cycle", "is_generated", "generation_expression", "is_updatable"]
        case .tableConstraints: return ["constraint_catalog", "constraint_schema", "constraint_name", "table_catalog", "table_schema", "table_name", "constraint_type", "is_deferrable", "initially_deferred", "enforced", "nulls_distinct"]
        case .keyColumnUsage: return ["constraint_catalog", "constraint_schema", "constraint_name", "table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "position_in_unique_constraint"]
        case .referentialConstraints: return ["constraint_catalog", "constraint_schema", "constraint_name", "unique_constraint_catalog", "unique_constraint_schema", "unique_constraint_name", "match_option", "update_rule", "delete_rule"]
        case .views: return ["table_catalog", "table_schema", "table_name", "view_definition", "check_option", "is_updatable", "is_insertable_into", "is_trigger_updatable", "is_trigger_deletable", "is_trigger_insertable_into"]
        case .sequences: return ["sequence_catalog", "sequence_schema", "sequence_name", "data_type", "numeric_precision", "numeric_precision_radix", "numeric_scale", "start_value", "minimum_value", "maximum_value", "increment", "cycle_option"]
        }
    }

    var columns: [QueryColumn] {
        get throws {
            let numbers: Set<String> = ["ordinal_position", "position_in_unique_constraint", "character_maximum_length", "character_octet_length", "numeric_precision", "numeric_precision_radix", "numeric_scale", "datetime_precision", "interval_precision", "maximum_cardinality"]
            return try columnNames.enumerated().map { index, name in
                try QueryColumn(number: UInt32(index + 1), name: name,
                    type: numbers.contains(name) ? .sql(.integer) : .sql(.text), isNullable: true)
            }
        }
    }
}

extension SQLReadResolution {
    /// Recognition precedes ordinary namespace and physical lookup.
    func informationSchemaRelation(_ table: TableRef) throws -> SQLInformationSchemaRelation? {
        // Durable definitions resolve their typed captured identity before caller search paths.
        if viewBindings != nil {
            guard let binding = sourceBinding(table), binding.kind == .informationSchema else { return nil }
            guard table.partitions.isEmpty else {
                throw CanonicalReadError.invalidPartition(entity: table.table, reason: "Information schema relations have no storage partition")
            }
            return try SQLInformationSchemaRelation.validating(binding)
        }
        let descriptor = SQLInformationSchemaRelation.allCases.first(where: { SQLCatalogName($0.rawValue) == SQLCatalogName(table.table) })
        let qualified: Bool
        if let schema = table.schema { qualified = SQLCatalogName(schema) == SQLCatalogName("information_schema") }
        else {
            // Follow ordinary search-path priority; a prior user relation wins.
            var selected = false
            for entry in searchPath.entries {
                let name: String?
                switch entry { case .schema(let value): name = value; case .currentUser: name = currentUser }
                guard let name else { continue }
                if SQLCatalogName(name) == SQLCatalogName("information_schema") {
                    if descriptor != nil { selected = true; break }
                    continue
                }
                if let namespace = snapshot.catalog.namespaces.first(where: { SQLCatalogName($0.name) == SQLCatalogName(name) }),
                    snapshot.catalog.relations.contains(where: { SQLCatalogName($0.namespaceID) == SQLCatalogName(namespace.id) && SQLCatalogName($0.name) == SQLCatalogName(table.table) })
                    || snapshot.catalog.views.contains(where: { SQLCatalogName($0.namespaceID) == SQLCatalogName(namespace.id) && SQLCatalogName($0.name) == SQLCatalogName(table.table) })
                    || snapshot.catalog.materializedViews.contains(where: { SQLCatalogName($0.namespaceID) == SQLCatalogName(namespace.id) && SQLCatalogName($0.name) == SQLCatalogName(table.table) }) { break }
            }
            qualified = selected
        }
        guard qualified else { return nil }
        guard let selected = descriptor else {
            throw SQLCatalogBindingError.missingRelation(namespace: "information_schema", name: table.table)
        }
        guard table.partitions.isEmpty else {
            throw CanonicalReadError.invalidPartition(entity: table.table, reason: "Information schema relations have no storage partition")
        }
        return selected
    }
}

extension SQLInformationSchemaRelation {
    static func validating(_ binding: SQLViewSourceBinding) throws -> Self {
        guard binding.kind == .informationSchema,
              let descriptor = allCases.first(where: { SQLCatalogName($0.rawValue) == SQLCatalogName(binding.relationID) }) else {
            throw SQLViewExecutionError.missingDependency(binding.relationID)
        }
        let columns = try descriptor.columns
        for binding in binding.columns {
            guard let column = columns.first(where: { SQLCatalogName($0.name) == SQLCatalogName(binding.id) }) else {
                throw SQLViewExecutionError.missingColumn(relation: descriptor.rawValue, column: binding.id)
            }
            guard column.type == binding.type else {
                throw SQLViewExecutionError.incompatibleColumn(relation: descriptor.rawValue, column: binding.id)
            }
        }
        return descriptor
    }
}

extension SQLReadResolution {
    func informationSchemaColumns(_ table: TableRef, descriptor: SQLInformationSchemaRelation)
        throws -> (columns: [QueryColumn], ordinals: [Int]) {
        let canonical = try descriptor.columns
        guard let binding = sourceBinding(table), binding.kind == .informationSchema else {
            return (canonical, Array(canonical.indices))
        }
        _ = try SQLInformationSchemaRelation.validating(binding)
        var columns: [QueryColumn] = []
        var ordinals: [Int] = []
        for (index, captured) in binding.columns.enumerated() {
            guard let ordinal = canonical.firstIndex(where: { SQLCatalogName($0.name) == SQLCatalogName(captured.id) }) else {
                throw SQLViewExecutionError.missingColumn(relation: descriptor.rawValue, column: captured.id)
            }
            let column = canonical[ordinal]
            columns.append(try QueryColumn(number: UInt32(index + 1), name: captured.name,
                type: column.type, isNullable: column.isNullable))
            ordinals.append(ordinal)
        }
        return (columns, ordinals)
    }
}
