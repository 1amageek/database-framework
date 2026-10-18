import DatabaseKit
import DatabaseTypes
import StorageKit

extension DatabaseContext {
    func sqlIndexSourceRows(tableRef: TableRef, options: ReadExecutionContext,
        transaction: DatabaseReadTransaction, authorizationQuery: SelectQuery,
        authorizationRequirement: DatabaseListReadAuthorizationRequirement) async throws -> EntityTableRows? {
        guard authorizationQuery.accessPath == nil, let resolution = options.sqlResolution else { return nil }
        let resolved = try resolution.resolve(tableRef)
        let relation = resolved.declaration
        let catalog = resolution.snapshot.catalog
        let meter = options.workMeter
        var selected: SQLIndex?
        for index in catalog.indexes where index.relationID == relation.id {
            try meter.consume(at: .indexScan)
            guard index.bindings.allSatisfy({ binding in
                relation.columns.contains { SQLConstraintCompiler.same($0.id, binding.id) }
            }) else { continue }
            if index.predicate == nil {
                selected = index
                break
            }
            guard let filter = authorizationQuery.filter else { continue }
            let references = SelectQuery(projection: [.expression(filter)], source: .values([])).referencedColumns
            guard references.allSatisfy({ reference in
                guard let table = reference.table else { return true }
                return SQLConstraintCompiler.same(table, tableRef.effectiveName)
            }) else { continue }
            if try SQLIndexCompiler.predicateImplies(query: filter, index: index, relation: relation) {
                selected = index
                break
            }
        }
        guard let selected else { return nil }
        // Exact retirement may already have committed before an old admitted
        // catalog acquires its storage snapshot. The shared floor rejects it.
        try await container.requireSQLWriterGeneration(resolution.policy.schemaGeneration,
            transaction: transaction.storageAccess)
        guard let authorization = transaction.authorization,
              let fields = authorization.admittedFieldNames(forEntityName: resolved.entity.name),
              authorization.covers(listRequirement: authorizationRequirement) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try resolution.policy.validate(authorization)
        let partition = try CanonicalPartitionBinding.makeAnyBinding(for: resolved.entity, partitions: tableRef.partitions)
        let root = try await container.openDirectory(for: resolved.entity, path: partition,
            transaction: transaction.storageAccess)
        let models: DatabaseRetainedPersistedModels
        if let root {
            let keys = try await SQLIndexRuntime.primaryKeys(index: selected, storeSubspace: root,
                transaction: transaction.storageAccess, workMeter: meter)
            let reader = DatabaseTransaction(storageAccess: transaction.storageAccess, container: container,
                readPolicy: resolution.policy, readAuthorization: authorization)
            var rows = try DatabaseRetainedArrayBuilder<DatabaseRetainedPersistedModels.Entry?>(workMeter: meter,
                stage: .storageRow, layout: DatabaseRetainedArrayLayout.forElement(DatabaseRetainedPersistedModels.Entry?.self),
                expectedCount: keys.count)
            for position in 0..<keys.count {
                let admission = try rows.prepareAppend(footprint: DatabaseIntermediateFootprint(), at: .storageRow)
                var row: DatabaseRetainedPersistedModels.Entry?
                try await keys.withRetainedPrimaryKey(at: position) { identifier in
                    try meter.consume(at: .storageRow)
                    row = try await reader.loadRetainedPersistedModel(entity: resolved.entity.name, id: identifier,
                        partition: partition, snapshot: true, workMeter: meter, fields: fields)
                }
                guard row != nil else {
                    throw CanonicalReadError.corruptedIndexEntry(indexName: selected.name, reason: "SQL index points to a missing entity")
                }
                rows.append(row, using: admission)
            }
            models = try DatabaseRetainedPersistedModels(buffer: rows.finish())
        } else {
            let empty = try DatabaseRetainedArrayBuilder<DatabaseRetainedPersistedModels.Entry?>(workMeter: meter,
                stage: .indexScan, layout: DatabaseRetainedArrayLayout.forElement(DatabaseRetainedPersistedModels.Entry?.self))
            models = try DatabaseRetainedPersistedModels(buffer: empty.finish())
        }
        let converted = try EntityRuntimeDefinition.canonicalTableRows(from: models, entity: resolved.entity,
            admittedFieldNames: fields, sourceName: tableRef.alias ?? tableRef.effectiveName,
            workMeter: meter, retainingCandidateIdentity: true)
        return EntityTableRows(rows: converted.rows, sourceLayout: converted.layout,
            residualFilter: nil, residualOrderBy: nil, limitPushed: false, offsetPushed: false,
            pageWindowPushed: false, continuationPosition: nil, stableSnapshotQueryFingerprint: nil)
    }
}
