import DatabaseKit

/// Resolves, authorizes, and validates Fusion in distinct phases before I/O.
enum FusionPreflight {
    static func resolveGraph(
        _ root: SelectQuery,
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionResolvedQueryGraph {
        var fusionQueryCount: UInt64 = 0

        // Complete logical list authorization for every Fusion node before
        // resolving even one entity or physical selector.
        try FusionSelectQueryGraphWalker.forEachQuery(
            in: root,
            workMeter: workMeter
        ) { query in
            guard case .fusion(let source) = query.accessPath else { return }
            let (nextCount, overflow) = fusionQueryCount
                .addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseIntermediateFootprintError
                    .rowAdditionOverflow(
                        left: fusionQueryCount,
                        right: 1
                    )
            }
            fusionQueryCount = nextCount
            guard case .table(let tableRef) = query.source else { return }
            try context.authorizeCanonicalListAccess(
                entityName: tableRef.table,
                selectQuery: query
            )
            for stage in source.stages {
                for input in stage.inputs {
                    guard case .connected(let connected) = input.operation
                    else { continue }
                    try context.authorizeCanonicalListAccess(
                        entityName: connected.edgeEntity,
                        selectQuery: SelectQuery(
                            projection: .all,
                            source: .table(
                                TableRef(
                                    connected.edgeEntity,
                                    partitions: connected.edgePartitions
                                )
                            )
                        )
                    )
                }
            }
        }
        var authorizationPlan = DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [:]
        )
        try FusionSelectQueryGraphWalker.forEachQuery(
            in: root,
            workMeter: workMeter
        ) { query in
            guard case .fusion = query.accessPath else { return }
            guard case .fusion(let source) = query.accessPath,
                  case .table(let tableRef) = query.source else {
                throw FusionExecutionError.unsupportedSource
            }
            let entity = try context.resolveEntity(named: tableRef.table)
            let plan = try resolve(
                context: context,
                tableRef: tableRef,
                entity: entity,
                source: source,
                workMeter: workMeter
            )
            authorizationPlan = authorizationPlan.merging(
                plan.authorizationPlan
            )
        }
        return FusionResolvedQueryGraph(
            authorizationPlan: authorizationPlan,
            fusionQueryCount: fusionQueryCount
        )
    }

    static func prepareGraph(
        _ root: SelectQuery,
        _ resolved: FusionResolvedQueryGraph,
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionPreparedQueryGraph {
        guard resolved.fusionQueryCount > 0 else { return .empty }
        var preparedCount: UInt64 = 0
        try FusionSelectQueryGraphWalker.forEachQuery(
            in: root,
            workMeter: workMeter
        ) { query in
            guard case .fusion(let source) = query.accessPath,
                  case .table(let tableRef) = query.source else {
                return
            }
            let plan = try resolve(
                context: context,
                tableRef: tableRef,
                entity: try context.resolveEntity(named: tableRef.table),
                source: source,
                workMeter: workMeter
            )
            _ = try prepare(
                plan,
                context: context,
                workMeter: workMeter
            )
            let (next, overflow) = preparedCount.addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseIntermediateFootprintError.rowAdditionOverflow(
                    left: preparedCount,
                    right: 1
                )
            }
            preparedCount = next
        }
        guard preparedCount == resolved.fusionQueryCount else {
            throw FusionExecutionError.executionContractViolation
        }
        return FusionPreparedQueryGraph(isValidated: true)
    }

    static func prepareForExecution(
        tableRef: TableRef,
        entity: Schema.Entity,
        source: FusionSource,
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionPreparedPlan {
        try prepare(
            resolve(
                context: context,
                tableRef: tableRef,
                entity: entity,
                source: source,
                workMeter: workMeter
            ),
            context: context,
            workMeter: workMeter
        )
    }

    static func resolve(
        context: DatabaseContext,
        tableRef: TableRef,
        entity: Schema.Entity,
        source: FusionSource,
        workMeter: DatabaseWorkMeter
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
                    context: context,
                    tableRef: tableRef,
                    entity: entity
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
            tableRef: tableRef
        )
    }

    /// Runs feature-owned validation only after field authorization succeeds.
    static func prepare(
        _ resolved: FusionResolvedPlan,
        context: DatabaseContext,
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
                case .index(let source):
                    let descriptor = try FusionIndexSelectionResolver.resolve(
                        source.selection,
                        in: entity
                    )
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
                    // FIXME(INCOMPLETE_IMPLEMENTATION): QueryIR can describe
                    // index inputs whose feature-owned physical Fusion
                    // executor is not registered. Production reaches this
                    // post-authorization phase and fails explicitly. Do not
                    // treat another index input as supported until its
                    // executor and behavioral/resource tests are complete.
                    guard let executor = context.container.runtimeConfiguration
                        .fusionReadExecutors.indexExecutor(
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
                    try context.validateFusionRelationalInput(
                        SelectQuery(
                            projection: .all,
                            source: .table(resolved.tableRef),
                            filter: expression,
                            limit: input.limit.map(UInt64.init)
                        )
                    )
                    operation = .filter(expression)
                case .order(let keys):
                    try context.validateFusionRelationalInput(
                        SelectQuery(
                            projection: .all,
                            source: .table(resolved.tableRef),
                            orderBy: keys,
                            limit: input.limit.map(UInt64.init)
                        )
                    )
                    operation = .order(keys)
                case .connected(let source):
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
                    let edgeEntity = try context.resolveEntity(
                        named: source.edgeEntity
                    )
                    let descriptor = try FusionIndexSelectionResolver
                        .resolve(source.selection, in: edgeEntity)
                    guard let executor = context.container
                        .runtimeConfiguration.fusionReadExecutors
                        .connectedExecutor(for: descriptor.type) else {
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
            )
        )
    }

    private static func resolve(
        _ input: FusionInput,
        stageIndex: Int,
        inputIndex: Int,
        context: DatabaseContext,
        tableRef: TableRef,
        entity: Schema.Entity
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
            operation = .index(source)
            authorizationPlan = selectionAuthorizationPlan(
                for: source.selection,
                entity: entity
            ).merging(
                DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: [
                        entity.name: Set(
                            source.referencedFields.map(\.name)
                        )
                    ]
                )
            )
        case .filter(let expression):
            let query = SelectQuery(
                projection: .all,
                source: .table(tableRef),
                filter: expression,
                limit: input.limit
            )
            operation = .filter(expression)
            authorizationPlan = .make(
                query: query.replacing(projection: .items([])),
                schema: context.container.schema
            )
        case .order(let keys):
            let query = SelectQuery(
                projection: .all,
                source: .table(tableRef),
                orderBy: keys,
                limit: input.limit
            )
            operation = .order(keys)
            authorizationPlan = .make(
                query: query.replacing(projection: .items([])),
                schema: context.container.schema
            )
        case .connected(let source):
            operation = .connected(source)
            let resultFields = DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: [entity.name: [source.resultField.name]]
            )
            guard let edgeEntity = context.container.schema.entity(
                named: source.edgeEntity
            ) else {
                authorizationPlan = resultFields
                break
            }
            authorizationPlan = resultFields.merging(
                selectionAuthorizationPlan(
                    for: source.selection,
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
    private static func selectionAuthorizationPlan(
        for selection: FusionIndexSelection,
        entity: Schema.Entity
    ) -> DatabaseFieldReadAuthorizationPlan {
        do {
            let descriptor = try FusionIndexSelectionResolver.resolve(
                selection,
                in: entity
            )
            return .index(entity: entity, descriptor: descriptor)
        } catch {
            return DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: [entity.name: Set(entity.allFields)]
            )
        }
    }
}
