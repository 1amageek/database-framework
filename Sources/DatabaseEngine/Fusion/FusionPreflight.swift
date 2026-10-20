import DatabaseKit

/// Resolves, authorizes, and validates Fusion in distinct phases before I/O.
enum FusionPreflight {
    private struct LogicalNode: Sendable {
        let tableRef: TableRef
        let source: FusionSource
        let listAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement
    }

    static func resolveGraph(
        _ root: SelectQuery,
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter,
        sqlResolution: SQLReadResolution? = nil
    ) throws -> FusionResolvedQueryGraph {
        let policy = try context.readPolicy()
        var listAuthorizationRequirements: [
            DatabaseListReadAuthorizationRequirement
        ] = []
        var featureAuthorizationPlan = DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [:]
        )
        var orderingDependencies: [String: Set<String>] = [:]
        var logicalNodes = try DatabaseRetainedArrayBuilder<LogicalNode>(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(
                LogicalNode.self
            )
        )

        var viewGraphs = try DatabaseRetainedArrayBuilder<FusionResolvedQueryGraph>(
            workMeter: workMeter, stage: .validation,
            layout: DatabaseRetainedArrayLayout.forElement(FusionResolvedQueryGraph.self))
        // Collect every logical node and every list requirement before
        // resolving even one entity, selector, or feature executor.
        try FusionSelectQueryGraphWalker.forEachQuery(
            in: root,
            workMeter: workMeter
        ) { query in
            featureAuthorizationPlan = featureAuthorizationPlan.merging(
                try additionalIndexAuthorizationPlan(
                    for: query,
                    policy: policy,
                    sqlResolution: sqlResolution
                )
            )
            guard case .fusion(let source) = query.accessPath else { return }
            guard case .table(let tableRef) = query.source else {
                throw FusionExecutionError.unsupportedSource
            }
            if try sqlResolution?.ordinaryView(tableRef) != nil { throw FusionExecutionError.unsupportedSource }
            for stage in source.stages {
                for input in stage.inputs {
                    guard case .connected(let connected) = input.operation
                    else { continue }
                    listAuthorizationRequirements.append(
                        try DatabaseReadPolicy.listRequirement(
                            entityName: connected.edgeEntity,
                            selectQuery: SelectQuery(
                                projection: [.wildcard()],
                                source: .table(
                                    TableRef(
                                        connected.edgeEntity,
                                        partitions: connected.edgePartitions
                                    )
                                )
                            ), schema: policy.schema
                        )
                    )
                }
            }
            try logicalNodes.append(
                footprint: DatabaseIntermediateFootprint(rows: 1)
            ) {
                LogicalNode(
                    tableRef: tableRef,
                    source: source,
                    listAuthorizationRequirement: try DatabaseReadPolicy
                        .listRequirement(
                            entityName: try sqlResolution?.resolve(tableRef).entity.name ?? tableRef.table,
                            selectQuery: query, schema: policy.schema, sqlResolution: sqlResolution
                        )
                )
            }
        } visitTable: { tableRef, query in
            if try sqlResolution?.informationSchemaRelation(tableRef) != nil { return }
            if let sqlResolution {
                let definition: SQLViewDefinition?
                let id: String?
                if let view = try sqlResolution.ordinaryView(tableRef) { definition = view.definition; id = view.id }
                else { definition = nil; id = nil }
                if let definition, let id {
                    let nested = try resolveGraph(definition.query, context: context, workMeter: workMeter,
                        sqlResolution: sqlResolution.forView(definition, id: id, workMeter: workMeter))
                    listAuthorizationRequirements.append(contentsOf: nested.listAuthorizationRequirements)
                    try viewGraphs.append(footprint: DatabaseIntermediateFootprint(rows: 1)) { nested }
                    if try sqlResolution.ordinaryView(tableRef) != nil { return }
                }
            }
            listAuthorizationRequirements.append(
                try DatabaseReadPolicy.listRequirement(
                    entityName: try sqlResolution?.resolve(tableRef).entity.name ?? tableRef.table,
                    selectQuery: query, schema: policy.schema, sqlResolution: sqlResolution
                )
            )
        } visitPolymorphic: { logicalSource, query in
            guard let group = policy.schema.polymorphicGroup(
                identifier: logicalSource.identifier
            ) else { return }
            for entityName in group.memberTypeNames {
                listAuthorizationRequirements.append(
                    try DatabaseReadPolicy.listRequirement(
                        entityName: entityName,
                        selectQuery: query, schema: policy.schema, sqlResolution: sqlResolution
                    )
                )
            }
        } visitOrdering: { query, commonTables in
            for (entity, fields) in try query.orderingDependencies(schema: policy.schema,
                sqlResolution: sqlResolution, commonTables: commonTables) {
                orderingDependencies[entity, default: []].formUnion(fields)
            }
        }
        let retainedViewGraphs = try viewGraphs.finish().moveToSharedOwnership(at: .validation)
        let retainedLogicalNodes = try logicalNodes.finish()
            .moveToSharedOwnership(at: .validation)
        listAuthorizationRequirements.append(contentsOf: orderingCompanions(
            of: listAuthorizationRequirements
                + retainedLogicalNodes.map { $0.listAuthorizationRequirement },
            orderingDependencies: orderingDependencies))
        var authorizationPlan = try DatabaseFieldReadAuthorizationPlan.make(
            query: root,
            schema: policy.schema,
            sqlResolution: sqlResolution
        ).merging(featureAuthorizationPlan)
        var resolvedNodes = try DatabaseRetainedArrayBuilder<
            FusionResolvedQueryGraph.Node
        >(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionResolvedQueryGraph.Node.self
            ),
            expectedCount: retainedLogicalNodes.count
        )
        for graph in retainedViewGraphs {
            authorizationPlan = authorizationPlan.merging(graph.authorizationPlan)
            if let nodes = graph.nodes {
                for node in nodes { try resolvedNodes.append(footprint: DatabaseIntermediateFootprint(rows: 1)) { node } }
            }
        }
        for logicalNode in retainedLogicalNodes {
            let resolvedEntity = try sqlResolution?.resolve(logicalNode.tableRef).entity
            guard let entity = resolvedEntity ?? policy.schema.entity(
                named: logicalNode.tableRef.table
            ) else {
                try resolvedNodes.append(
                    footprint: DatabaseIntermediateFootprint(rows: 1)
                ) {
                    FusionResolvedQueryGraph.Node(
                        source: logicalNode.source,
                        resolution: .missingEntity(
                            logicalNode.tableRef.table
                        )
                    )
                }
                continue
            }
            let plan = try resolve(
                schema: policy.schema,
                tableRef: logicalNode.tableRef,
                entity: entity,
                source: logicalNode.source,
                listAuthorizationRequirement:
                    logicalNode.listAuthorizationRequirement,
                workMeter: workMeter,
                sqlResolution: sqlResolution
            )
            authorizationPlan = authorizationPlan.merging(
                plan.authorizationPlan
            )
            try resolvedNodes.append(
                footprint: DatabaseIntermediateFootprint(rows: 1)
            ) {
                FusionResolvedQueryGraph.Node(
                    source: logicalNode.source,
                    resolution: .resolved(plan)
                )
            }
        }
        let resolvedNodeCount = resolvedNodes.count
        let retainedResolvedNodes = try resolvedNodes.finish()
            .moveToSharedOwnership(at: .validation)
        return FusionResolvedQueryGraph(
            nodes: resolvedNodeCount == 0 ? nil : retainedResolvedNodes,
            listAuthorizationRequirements: listAuthorizationRequirements,
            authorizationPlan: authorizationPlan,
            schemaGeneration: policy.schemaGeneration
        )
    }

    /// Ordering may happen in a query enclosing the one that reads an entity.
    /// Each requirement whose ordering omits a field the statement orders by
    /// gains a companion with the same entity and window that names it; the
    /// original stays unchanged because Fusion entries match it exactly.
    private static func orderingCompanions(
        of requirements: [DatabaseListReadAuthorizationRequirement],
        orderingDependencies: [String: Set<String>]
    ) -> [DatabaseListReadAuthorizationRequirement] {
        var companions: [DatabaseListReadAuthorizationRequirement] = []
        for requirement in requirements {
            guard let fields = orderingDependencies[requirement.entityName] else { continue }
            let ordered = requirement.orderBy ?? []
            let missing = fields.subtracting(ordered)
            guard !missing.isEmpty else { continue }
            companions.append(DatabaseListReadAuthorizationRequirement(
                entityName: requirement.entityName, window: requirement.window,
                orderBy: ordered + missing.sorted()))
        }
        return companions
    }

    private static func additionalIndexAuthorizationPlan(
        for query: SelectQuery,
        policy: DatabaseReadPolicy,
        sqlResolution: SQLReadResolution?
    ) throws -> DatabaseFieldReadAuthorizationPlan {
        guard case .index(let indexScan) = query.accessPath else {
            return DatabaseFieldReadAuthorizationPlan(fieldsByEntity: [:])
        }
        switch query.source {
        case .table(let tableRef):
            if try sqlResolution?.ordinaryView(tableRef) != nil { throw FusionExecutionError.unsupportedSource }
            guard let entity = try sqlResolution?.resolve(tableRef).entity ?? policy.schema.entity(named: tableRef.table)
            else {
                return DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: [:]
                )
            }
            let conservative = DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: [entity.name: Set(entity.allFields)]
            )
            guard entity.indexDescriptors.contains(where: {
                $0.name == indexScan.indexName
                    && $0.type == indexScan.indexType
            }),
                let runtime = policy.entityRuntime(named: entity.name)
            else {
                return conservative
            }
            do {
                guard let required = try runtime
                    .additionalRequiredFieldNames(for: indexScan),
                    required.isSubset(of: Set(entity.allFields))
                else {
                    return conservative
                }
                return DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: [entity.name: required]
                )
            } catch {
                return conservative
            }

        case .logical(let source)
            where source.kindIdentifier == LogicalSourceKind.polymorphic:
            guard let group = policy.schema.polymorphicGroup(
                identifier: source.identifier
            ) else {
                return DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: [:]
                )
            }
            let members = group.memberTypeNames.compactMap {
                policy.schema.entity(named: $0)
            }
            func conservativePlan() -> DatabaseFieldReadAuthorizationPlan {
                DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: Dictionary(
                        uniqueKeysWithValues: members.map {
                            ($0.name, Set($0.allFields))
                        }
                    )
                )
            }
            guard let index = group.indexes.first(where: {
                $0.name == indexScan.indexName
                    && $0.type == indexScan.indexType
            }) else {
                return conservativePlan()
            }
            do {
                guard let required = try policy
                    .additionalPolymorphicIndexRequiredFieldNames(
                        for: indexScan
                    )
                else {
                    return conservativePlan()
                }
                let indexFieldNames = Set(
                    index.fieldNames + index.includedFields
                )
                let requiredFieldNames = indexFieldNames.union(required)
                var fieldsByEntity: [String: Set<String>] = [:]
                for entity in members {
                    guard requiredFieldNames.isSubset(
                        of: Set(entity.allFields)
                    ) else {
                        return conservativePlan()
                    }
                    fieldsByEntity[entity.name] = requiredFieldNames
                }
                return DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: fieldsByEntity
                )
            } catch {
                return conservativePlan()
            }

        default:
            return DatabaseFieldReadAuthorizationPlan(fieldsByEntity: [:])
        }
    }

    static func prepareGraph(
        _ resolved: FusionResolvedQueryGraph,
        authorization: DatabaseReadAuthorization,
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionPreparedQueryGraph {
        let policy = try context.readPolicy()
        try policy.validate(authorization)
        guard policy.schemaGeneration == resolved.schemaGeneration else {
            throw DatabaseReadSessionError.schemaGenerationMismatch
        }
        guard authorization.covers(
                listRequirements: resolved.listAuthorizationRequirements,
                fields: resolved.authorizationPlan
              ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        guard let nodes = resolved.nodes else {
            return FusionPreparedQueryGraph(
                entries: nil,
                entryIndices: [:],
                lookupReservation: nil,
                authorization: authorization
            )
        }
        var entries = try DatabaseRetainedArrayBuilder<
            FusionPreparedQueryGraph.Entry
        >(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionPreparedQueryGraph.Entry.self
            ),
            expectedCount: nodes.count
        )
        let lookupLayout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(
                MemoryLayout<[
                    FusionPreparedQueryGraph.EntryKey: Int
                ]>.stride
            ),
            elementCapacitySlotByteCount: UInt64(
                max(
                    1,
                    MemoryLayout<(
                        FusionPreparedQueryGraph.EntryKey,
                        Int
                    )>.stride
                )
            )
        )
        let lookupGrowth = try lookupLayout.growth(
            from: 0,
            toFit: nodes.count
        )
        let lookupReservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: lookupLayout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: lookupGrowth.additionalByteCount
                )
            ).bytes,
            at: .validation
        )
        var entryIndices: [FusionPreparedQueryGraph.EntryKey: Int] = [:]
        entryIndices.reserveCapacity(lookupGrowth.capacity)
        for node in nodes {
            let resolvedPlan: FusionResolvedPlan
            switch node.resolution {
            case .resolved(let plan):
                resolvedPlan = plan
            case .missingEntity(let entityName):
                throw CanonicalReadError.unsupportedSource(
                    "Entity '\(entityName)' not found in schema"
                )
            }
            let plan = try prepare(
                resolvedPlan,
                policy: policy,
                workMeter: workMeter
            )
            let index = entries.count
            try entries.append(
                footprint: DatabaseIntermediateFootprint(rows: 1)
            ) {
                FusionPreparedQueryGraph.Entry(
                    source: node.source,
                    plan: plan
                )
            }
            try workMeter.consume(at: .validation)
            let key = FusionPreparedQueryGraph.EntryKey(
                tableRef: plan.tableRef,
                source: node.source,
                listAuthorizationRequirement:
                    plan.listAuthorizationRequirement
            )
            if entryIndices[key] == nil {
                entryIndices[key] = index
            }
        }
        return FusionPreparedQueryGraph(
            entries: try entries.finish().moveToSharedOwnership(
                at: .validation
            ),
            entryIndices: entryIndices,
            lookupReservation: lookupReservation,
            authorization: authorization
        )
    }

    static func resolve(
        schema: Schema,
        tableRef: TableRef,
        entity: Schema.Entity,
        source: FusionSource,
        listAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement,
        workMeter: DatabaseWorkMeter,
        sqlResolution: SQLReadResolution? = nil
    ) throws -> FusionResolvedPlan {
        do {
            try source.validate()
        } catch let error {
            throw FusionExecutionError.invalidPlan(error)
        }

        var stages = try DatabaseRetainedArrayBuilder<FusionResolvedPlan.Stage>(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionResolvedPlan.Stage.self
            ),
            expectedCount: source.stages.count
        )
        var authorizationPlan = DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [:]
        )
        for (stageIndex, stage) in source.stages.enumerated() {
            var inputs = try DatabaseRetainedArrayBuilder<
                FusionResolvedPlan.Input
            >(
                workMeter: workMeter,
                stage: .validation,
                layout: try DatabaseRetainedArrayLayout.forElement(
                    FusionResolvedPlan.Input.self
                ),
                expectedCount: stage.inputs.count
            )
            for (inputIndex, input) in stage.inputs.enumerated() {
                let resolved = try resolve(
                    input,
                    stageIndex: stageIndex,
                    inputIndex: inputIndex,
                    schema: schema,
                    tableRef: tableRef,
                    entity: entity,
                    workMeter: workMeter,
                    sqlResolution: sqlResolution
                )
                authorizationPlan = authorizationPlan.merging(
                    resolved.authorizationPlan
                )
                try inputs.append(
                    footprint: DatabaseIntermediateFootprint(rows: 1)
                ) {
                    resolved.input
                }
            }
            let retainedInputs = try inputs.finish().moveToSharedOwnership(
                at: .validation
            )
            try stages.append(
                footprint: DatabaseIntermediateFootprint(rows: 1)
            ) {
                FusionResolvedPlan.Stage(inputs: retainedInputs)
            }
        }
        return FusionResolvedPlan(
            stages: try stages.finish().moveToSharedOwnership(
                at: .validation
            ),
            authorizationPlan: authorizationPlan,
            entity: entity,
            tableRef: tableRef,
            listAuthorizationRequirement: listAuthorizationRequirement
        )
    }

    /// Runs feature-owned validation only after field authorization succeeds.
    static func prepare(
        _ resolved: FusionResolvedPlan,
        policy: DatabaseReadPolicy,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionPreparedPlan {
        let entity = resolved.entity
        var stages = try DatabaseRetainedArrayBuilder<FusionPreparedPlan.Stage>(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionPreparedPlan.Stage.self
            ),
            expectedCount: resolved.stages.count
        )
        for stage in resolved.stages {
            var inputs = try DatabaseRetainedArrayBuilder<
                FusionPreparedPlan.Input
            >(
                workMeter: workMeter,
                stage: .validation,
                layout: try DatabaseRetainedArrayLayout.forElement(
                    FusionPreparedPlan.Input.self
                ),
                expectedCount: stage.inputs.count
            )
            for input in stage.inputs {
                let operation: FusionPreparedPlan.Input.Operation
                switch input.operation {
                case .index(let source, let resolution):
                    let descriptor: IndexDescriptor
                    switch resolution {
                    case .resolved(let resolvedDescriptor):
                        descriptor = resolvedDescriptor
                    case .failed(let error):
                        throw error
                    }
                    for (fieldIndex, field) in source.referencedFields
                        .enumerated() {
                        for priorField in source.referencedFields[..<fieldIndex] {
                            try workMeter.consume(at: .validation)
                            guard priorField != field else {
                                throw FusionExecutionError.invalidIndexInput(
                                    indexType: descriptor.type,
                                    parameter: "referencedFields"
                                )
                            }
                        }
                        guard entity.fieldMapByName[field.name]?.fieldNumber
                            == field.number else {
                            throw FusionExecutionError.invalidIndexInput(
                                indexType: descriptor.type,
                                parameter: "referencedFields"
                            )
                        }
                    }
                    // QueryIR is independent of the selected package traits.
                    // A runtime without the feature-owned executor rejects
                    // that capability explicitly after authorization.
                    guard let executor = policy.fusionIndexExecutor(
                        for: descriptor.type
                    ) else {
                        throw FusionExecutionError.indexExecutorNotRegistered(
                            descriptor.type
                        )
                    }
                    try executor.validate(
                        FusionIndexValidationRequest(
                            source: source,
                            scoring: input.scoring,
                            descriptor: descriptor
                        )
                    )
                    operation = .index(
                        source: source,
                        descriptor: descriptor,
                        executor: executor
                    )
                case .filter(let expression):
                    operation = .filter(expression)
                case .order(let keys):
                    operation = .order(keys)
                case .connected(let source, let resolution):
                    guard let resultField = entity.fieldMapByName[
                        source.resultField.name
                    ],
                    resultField.fieldNumber == source.resultField.number,
                    resultField.type == .string,
                    !resultField.isArray else {
                        throw FusionExecutionError.invalidIndexInput(
                            indexType: .graph(.property),
                            parameter: "resultField"
                        )
                    }
                    let edgeEntity: Schema.Entity
                    let descriptor: IndexDescriptor
                    switch resolution {
                    case .resolved(
                        let resolvedEdgeEntity,
                        let resolvedDescriptor
                    ):
                        edgeEntity = resolvedEdgeEntity
                        descriptor = resolvedDescriptor
                    case .missingEdgeEntity(let entityName):
                        throw CanonicalReadError.unsupportedSource(
                            "Entity '\(entityName)' not found in schema"
                        )
                    case .failed(let error):
                        throw error
                    }
                    guard let executor = policy.fusionConnectedExecutor(
                        for: descriptor.type
                    ) else {
                        throw FusionExecutionError
                            .indexExecutorNotRegistered(descriptor.type)
                    }
                    try executor.validate(
                        FusionConnectedValidationRequest(
                            source: source,
                            scoring: input.scoring,
                            descriptor: descriptor
                        )
                    )
                    operation = .connected(
                        source: source,
                        edgeEntity: edgeEntity,
                        descriptor: descriptor,
                        executor: executor
                    )
                }
                try inputs.append(
                    footprint: DatabaseIntermediateFootprint(rows: 1)
                ) {
                    FusionPreparedPlan.Input(
                        operation: operation,
                        scoring: input.scoring,
                        requirement: input.requirement,
                        limit: input.limit,
                        stageIndex: input.stageIndex,
                        inputIndex: input.inputIndex
                    )
                }
            }
            let retainedInputs = try inputs.finish().moveToSharedOwnership(
                at: .validation
            )
            try stages.append(
                footprint: DatabaseIntermediateFootprint(rows: 1)
            ) {
                FusionPreparedPlan.Stage(inputs: retainedInputs)
            }
        }
        return FusionPreparedPlan(
            stages: try stages.finish().moveToSharedOwnership(
                at: .validation
            ),
            entity: resolved.entity,
            tableRef: resolved.tableRef,
            listAuthorizationRequirement:
                resolved.listAuthorizationRequirement
        )
    }

    private static func resolve(
        _ input: FusionInput,
        stageIndex: Int,
        inputIndex: Int,
        schema: Schema,
        tableRef: TableRef,
        entity: Schema.Entity,
        workMeter: DatabaseWorkMeter,
        sqlResolution: SQLReadResolution?
    ) throws -> (
        input: FusionResolvedPlan.Input,
        authorizationPlan: DatabaseFieldReadAuthorizationPlan
    ) {
        if case .annotation(let name, _) = input.scoring,
           name == FusionExecutor.scoreAnnotation {
            throw FusionExecutionError.reservedAnnotationCollision(name)
        }
        let limit = try input.limit.map { value in
            guard let result = Int(exactly: value) else {
                throw FusionExecutionError.inputLimitOutOfRange(value)
            }
            return result
        }

        let operation: FusionResolvedPlan.Input.Operation
        let authorizationPlan: DatabaseFieldReadAuthorizationPlan
        switch input.operation {
        case .index(let source):
            let resolution = resolveIndexSelection(
                source.selection,
                in: entity
            )
            operation = .index(source: source, resolution: resolution)
            authorizationPlan = selectionAuthorizationPlan(
                resolution,
                entity: entity
            ).merging(
                DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: [
                        entity.name: Set(
                            source.referencedFields.map { $0.name }
                        )
                    ]
                )
            )
        case .filter(let expression):
            guard try !FusionSelectQueryGraphWalker.containsNestedQuery(
                in: expression,
                workMeter: workMeter
            ) else {
                throw FusionExecutionError.relationalSubqueryNotSupported
            }
            let query = SelectQuery(
                projection: [.wildcard()],
                source: .table(tableRef),
                filter: expression,
                pagination: QueryPagination(limit: input.limit.map { .count(.fixed($0), withTies: false) })
            )
            operation = .filter(expression)
            authorizationPlan = try .make(
                query: query.replacing(projection: []),
                schema: schema,
                sqlResolution: sqlResolution
            )
        case .order(let keys):
            for key in keys {
                guard try !FusionSelectQueryGraphWalker.containsNestedQuery(
                    in: key.expression,
                    workMeter: workMeter
                ) else {
                    throw FusionExecutionError.relationalSubqueryNotSupported
                }
            }
            let query = SelectQuery(
                projection: [.wildcard()],
                source: .table(tableRef),
                orderBy: keys,
                pagination: QueryPagination(limit: input.limit.map { .count(.fixed($0), withTies: false) })
            )
            operation = .order(keys)
            authorizationPlan = try .make(
                query: query.replacing(projection: []),
                schema: schema,
                sqlResolution: sqlResolution
            )
        case .connected(let source):
            let resultFields = DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: [entity.name: [source.resultField.name]]
            )
            guard let edgeEntity = schema.entity(
                named: source.edgeEntity
            ) else {
                operation = .connected(
                    source: source,
                    resolution: .missingEdgeEntity(source.edgeEntity)
                )
                authorizationPlan = resultFields
                break
            }
            let indexResolution = resolveIndexSelection(
                source.selection,
                in: edgeEntity
            )
            let connectedResolution: FusionResolvedPlan.ConnectedResolution
            switch indexResolution {
            case .resolved(let descriptor):
                connectedResolution = .resolved(
                    edgeEntity: edgeEntity,
                    descriptor: descriptor
                )
            case .failed(let error):
                connectedResolution = .failed(error)
            }
            operation = .connected(
                source: source,
                resolution: connectedResolution
            )
            authorizationPlan = resultFields.merging(
                selectionAuthorizationPlan(
                    indexResolution,
                    entity: edgeEntity
                )
            )
        }

        return (
            FusionResolvedPlan.Input(
                operation: operation,
                scoring: input.scoring,
                requirement: input.requirement,
                limit: limit,
                stageIndex: stageIndex,
                inputIndex: inputIndex
            ),
            authorizationPlan
        )
    }

    /// Derives the least field authority that can be proven without exposing
    /// schema-resolution failures. Invalid or ambiguous selectors require
    /// whole-entity authority before their precise error is reported later.
    private static func resolveIndexSelection(
        _ selection: FusionIndexSelection,
        in entity: Schema.Entity
    ) -> FusionResolvedPlan.IndexResolution {
        do {
            return .resolved(
                try FusionIndexSelectionResolver.resolve(
                    selection,
                    in: entity
                )
            )
        } catch let error as FusionExecutionError {
            return .failed(error)
        } catch {
            return .failed(.executionContractViolation)
        }
    }

    private static func selectionAuthorizationPlan(
        _ resolution: FusionResolvedPlan.IndexResolution,
        entity: Schema.Entity
    ) -> DatabaseFieldReadAuthorizationPlan {
        switch resolution {
        case .resolved(let descriptor):
            return .index(entity: entity, descriptor: descriptor)
        case .failed:
            return DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: [entity.name: Set(entity.allFields)]
            )
        }
    }
}
