import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import QueryIR
import StorageKit

struct SPARQLUpdateExecutor: Sendable {
    private let graphStore: any RDFGraphMutationStore
    private let runtimeLimits: DatabaseRuntimeLimits
    private let structuralLimits: QueryStructuralLimits
    private let functionRegistry: SPARQLFunctionRegistry
    private let resolver = SPARQLUpdateQuadResolver()

    init(
        graphStore: any RDFGraphMutationStore,
        runtimeLimits: DatabaseRuntimeLimits,
        structuralLimits: QueryStructuralLimits,
        functionRegistry: SPARQLFunctionRegistry = .empty
    ) {
        self.graphStore = graphStore
        self.runtimeLimits = runtimeLimits
        self.structuralLimits = structuralLimits
        self.functionRegistry = functionRegistry
    }

    func execute(
        _ request: PreparedSPARQLUpdateRequest,
        preconditions: [MutationExecuteOperation.Precondition],
        context: DatabaseOperationContext,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler,
        records: DatabaseRecordMutationExecutor,
        workMeter: DatabaseWorkMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        try await records.validate(
            preconditions,
            transaction: transaction,
            persistence: persistence,
            workMeter: workMeter
        )
        let mutationMeter = SPARQLMutationMeter(
            maximum: runtimeLimits.maximumMutations,
            workMeter: workMeter
        )
        var result = MutationExecuteOperation.RDFEffect()
        for index in 0..<request.count {
            let operationResult = try await executeOperation(
                request.operation(at: index),
                operationOrdinal: UInt64(index),
                context: context,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
            result = try adding(result, operationResult)
        }
        return result
    }

    private func executeOperation(
        _ operation: PreparedSPARQLUpdateOperation,
        operationOrdinal: UInt64,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        switch operation {
        case .insertData(let query):
            return try await insertData(
                query,
                operationOrdinal: operationOrdinal,
                context: context,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .deleteData(let query):
            return try await deleteData(
                query,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .modify(let query):
            return try await deleteInsert(
                query,
                operationOrdinal: operationOrdinal,
                context: context,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .deleteWhere(let query):
            return try await deleteWhere(
                query,
                context: context,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .load(let prepared):
            return try await executeLoad(
                prepared,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .silentLoadNoOp:
            return MutationExecuteOperation.RDFEffect()
        case .clear(let query):
            return try await clear(
                query,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .createGraph(let query):
            return try await createGraph(
                iri: query.graph,
                silent: query.silent,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .drop(let query):
            return try await drop(
                query,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        case .graphTransfer(let query):
            return try await transfer(
                query,
                transaction: transaction,
                workMeter: workMeter,
                mutationMeter: mutationMeter
            )
        }
    }

    private func executeLoad(
        _ prepared: PreparedSPARQLLoad,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let graph: DatabaseRDFTerm?
        var createdGraphs: UInt64 = 0
        if let destination = prepared.destination {
            let graphName = try RDFGraphName(iri: destination)
            graph = .iri(destination)
            if try await !graphStore.containsGraph(
                graphName,
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            ) {
                try mutationMeter.consume()
                try await graphStore.createGraph(
                    graphName,
                    transaction: transaction,
                    workMeter: workMeter
                )
                createdGraphs = 1
            }
        } else {
            graph = nil
        }

        var inserted: UInt64 = 0
        for triple in prepared.triples {
            try workMeter.consume(at: .mutationPlanning)
            try mutationMeter.consume()
            let insertResult = try await graphStore.insert(
                Graph.RDFQuad(
                    subject: triple.subject,
                    predicate: triple.predicate,
                    object: triple.object,
                    graph: graph
                ),
                transaction: transaction,
                workMeter: workMeter
            )
            try accumulate(
                insertResult,
                mutationMeter: mutationMeter,
                insertedQuads: &inserted,
                createdGraphs: &createdGraphs
            )
        }
        return MutationExecuteOperation.RDFEffect(
            insertedQuads: inserted,
            createdGraphs: createdGraphs
        )
    }

    private func insertData(
        _ query: InsertDataQuery,
        operationOrdinal: UInt64,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let scope = try blankNodeScope(
            context: context,
            operationOrdinal: operationOrdinal,
            solutionOrdinal: 0
        )
        var inserted: UInt64 = 0
        var createdGraphs: UInt64 = 0
        for quad in query.quads {
            try workMeter.consume(at: .mutationPlanning)
            let resolved = try resolver.resolve(
                quad,
                row: nil,
                blankNodeScope: scope,
                variablesAllowed: false,
                blankNodesAllowed: true
            )
            for item in resolved {
                try mutationMeter.consume()
                let insertResult = try await graphStore.insert(
                    item,
                    transaction: transaction,
                    workMeter: workMeter
                )
                try accumulate(
                    insertResult,
                    mutationMeter: mutationMeter,
                    insertedQuads: &inserted,
                    createdGraphs: &createdGraphs
                )
            }
        }
        return MutationExecuteOperation.RDFEffect(
            insertedQuads: inserted,
            createdGraphs: createdGraphs
        )
    }

    private func deleteData(
        _ query: DeleteDataQuery,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        var deleted: UInt64 = 0
        for quad in query.quads {
            try workMeter.consume(at: .mutationPlanning)
            let resolved = try resolver.resolve(
                quad,
                row: nil,
                blankNodeScope: nil,
                variablesAllowed: false,
                blankNodesAllowed: false
            )
            for item in resolved {
                try mutationMeter.consume()
                if try await graphStore.delete(
                    item,
                    transaction: transaction,
                    workMeter: workMeter
                ) {
                    deleted = try increment(deleted)
                }
            }
        }
        return MutationExecuteOperation.RDFEffect(deletedQuads: deleted)
    }

    private func deleteInsert(
        _ query: SPARQLModifyOperation,
        operationOrdinal: UInt64,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let rows = try await solutionRows(
            for: query,
            context: context,
            transaction: transaction,
            workMeter: workMeter
        )
        let deletePattern: [QueryIR.Quad]
        let insertPattern: [QueryIR.Quad]
        switch query.action {
        case .delete(let pattern):
            deletePattern = pattern
            insertPattern = []
        case .insert(let pattern):
            deletePattern = []
            insertPattern = pattern
        case .deleteAndInsert(let delete, let insert):
            deletePattern = delete
            insertPattern = insert
        }
        var deleted: UInt64 = 0
        for row in rows {
            for template in deletePattern {
                try workMeter.consume(at: .mutationPlanning)
                let quads = try resolver.resolve(
                    templateQuad(
                        template,
                        defaultGraph: query.withGraph
                    ),
                    row: row,
                    blankNodeScope: nil,
                    variablesAllowed: true,
                    blankNodesAllowed: false
                )
                for quad in quads {
                    try mutationMeter.consume()
                    if try await graphStore.delete(
                        quad,
                        transaction: transaction,
                        workMeter: workMeter
                    ) {
                        deleted = try increment(deleted)
                    }
                }
            }
        }

        var inserted: UInt64 = 0
        var createdGraphs: UInt64 = 0
        for (ordinal, row) in rows.enumerated() {
            let scope = try blankNodeScope(
                context: context,
                operationOrdinal: operationOrdinal,
                solutionOrdinal: UInt64(ordinal)
            )
            for template in insertPattern {
                try workMeter.consume(at: .mutationPlanning)
                let quads = try resolver.resolve(
                    templateQuad(
                        template,
                        defaultGraph: query.withGraph
                    ),
                    row: row,
                    blankNodeScope: scope,
                    variablesAllowed: true,
                    blankNodesAllowed: true
                )
                for quad in quads {
                    try mutationMeter.consume()
                    let insertResult = try await graphStore.insert(
                        quad,
                        transaction: transaction,
                        workMeter: workMeter
                    )
                    try accumulate(
                        insertResult,
                        mutationMeter: mutationMeter,
                        insertedQuads: &inserted,
                        createdGraphs: &createdGraphs
                    )
                }
            }
        }
        return MutationExecuteOperation.RDFEffect(
            insertedQuads: inserted,
            deletedQuads: deleted,
            createdGraphs: createdGraphs
        )
    }

    private func solutionRows(
        for query: SPARQLModifyOperation,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> [VariableBinding] {
        let dataset: SPARQLDataset
        if !query.using.isEmpty {
            dataset = explicitDataset(query.using)
        } else if let withGraph = query.withGraph {
            dataset = .explicit(
                defaultGraphs: [withGraph],
                namedGraphs: []
            )
        } else {
            dataset = .implicit
        }
        return try await solutionRows(
            pattern: query.wherePattern,
            dataset: dataset,
            context: context,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    private func solutionRows(
        pattern: GraphPattern,
        dataset: SPARQLDataset,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> [VariableBinding] {
        let detectionLimit = try mutationDetectionLimit()
        let executor = SPARQLQueryExecutor(
            database: context.container.engine,
            datasetScanner: graphStore,
            readMode: .serializable,
            datasetScope: try SPARQLDatasetExecutionScope(dataset),
            functionRegistry: functionRegistry
        )
        let (bindings, _) = try await executor.executeInTransaction(
            pattern: try GraphPatternConverter.convert(
                pattern,
                structuralLimits: structuralLimits
            ),
            transaction: transaction,
            limit: detectionLimit,
            offset: 0,
            workMeter: workMeter
        )
        guard bindings.count <= runtimeLimits.maximumMutations else {
            throw DatabaseMutationError.mutationLimitExceeded(
                actual: bindings.count,
                maximum: runtimeLimits.maximumMutations
            )
        }
        return bindings
    }

    private func deleteWhere(
        _ query: DeleteWhereQuery,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let rows = try await solutionRows(
            pattern: graphPattern(from: query.pattern),
            dataset: .implicit,
            context: context,
            transaction: transaction,
            workMeter: workMeter
        )
        var deleted: UInt64 = 0
        for row in rows {
            for template in query.pattern {
                try workMeter.consume(at: .mutationPlanning)
                let quads = try resolver.resolve(
                    template,
                    row: row,
                    blankNodeScope: nil,
                    variablesAllowed: true,
                    blankNodesAllowed: false
                )
                for quad in quads {
                    try mutationMeter.consume()
                    if try await graphStore.delete(
                        quad,
                        transaction: transaction,
                        workMeter: workMeter
                    ) {
                        deleted = try increment(deleted)
                    }
                }
            }
        }
        return MutationExecuteOperation.RDFEffect(deletedQuads: deleted)
    }

    private func clear(
        _ query: ClearQuery,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let scope = try mutationScope(query.target)
        if case .graph(let iri) = query.target {
            let graph = try RDFGraphName(iri: iri)
            guard try await graphStore.containsGraph(
                graph,
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            ) else {
                if query.silent {
                    return MutationExecuteOperation.RDFEffect()
                }
                throw RDFGraphStoreError.graphNotFound(graph)
            }
        }
        let deleted = try await graphStore.clear(
            scope,
            transaction: transaction,
            workMeter: workMeter
        )
        try mutationMeter.consume(amount: deleted)
        return MutationExecuteOperation.RDFEffect(
            deletedQuads: deleted
        )
    }

    private func createGraph(
        iri: String,
        silent: Bool,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let graph = try RDFGraphName(iri: iri)
        try mutationMeter.consume()
        do {
            try await graphStore.createGraph(
                graph,
                transaction: transaction,
                workMeter: workMeter
            )
            return MutationExecuteOperation.RDFEffect(createdGraphs: 1)
        } catch let error as RDFGraphStoreError {
            if silent, isExistingGraph(error) {
                return MutationExecuteOperation.RDFEffect()
            }
            throw error
        }
    }

    private func drop(
        _ query: DropQuery,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let scope = try mutationScope(query.target)
        if case .graph(let iri) = query.target {
            let graph = try RDFGraphName(iri: iri)
            guard try await graphStore.containsGraph(
                graph,
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            ) else {
                if query.silent {
                    return MutationExecuteOperation.RDFEffect()
                }
                throw RDFGraphStoreError.graphNotFound(graph)
            }
        }
        let droppedGraphs = try await droppedGraphCount(
            query.target,
            transaction: transaction,
            workMeter: workMeter
        )
        let deleted = try await graphStore.drop(
            scope,
            transaction: transaction,
            workMeter: workMeter
        )
        try mutationMeter.consume(amount: deleted)
        try mutationMeter.consume(amount: droppedGraphs)
        return MutationExecuteOperation.RDFEffect(
            deletedQuads: deleted,
            droppedGraphs: droppedGraphs
        )
    }

    private func transfer(
        _ query: GraphTransferQuery,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        guard query.source != query.destination else {
            return MutationExecuteOperation.RDFEffect()
        }
        if case .graph(let iri) = query.source {
            let graph = try RDFGraphName(iri: iri)
            guard try await graphStore.containsGraph(
                graph,
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            ) else {
                if query.silent {
                    return MutationExecuteOperation.RDFEffect()
                }
                throw RDFGraphStoreError.graphNotFound(graph)
            }
        }
        let sourceQuads = try await transferSourceQuads(
            query.source,
            transaction: transaction,
            workMeter: workMeter
        )
        var inserted: UInt64 = 0
        var deleted: UInt64 = 0
        var createdGraphs: UInt64 = 0
        var droppedGraphs: UInt64 = 0

        if query.operation != .add {
            let destinationDeleted = try await clearTransferDestination(
                query.destination,
                transaction: transaction,
                workMeter: workMeter
            )
            try mutationMeter.consume(amount: destinationDeleted)
            deleted = try add(deleted, destinationDeleted)
        }

        if try await ensureTransferDestination(
            query.destination,
            transaction: transaction,
            workMeter: workMeter
        ) {
            try mutationMeter.consume()
            createdGraphs = 1
        }

        for sourceQuad in sourceQuads {
            try mutationMeter.consume()
            let destinationQuad = retarget(
                sourceQuad.quad,
                to: query.destination
            )
            let insertResult = try await graphStore.insert(
                destinationQuad,
                transaction: transaction,
                workMeter: workMeter
            )
            try accumulate(
                insertResult,
                mutationMeter: mutationMeter,
                insertedQuads: &inserted,
                createdGraphs: &createdGraphs
            )
        }

        if query.operation == .move {
            let sourceEffect = try await removeTransferSource(
                query.source,
                transaction: transaction,
                workMeter: workMeter
            )
            try mutationMeter.consume(amount: sourceEffect.deletedQuads)
            try mutationMeter.consume(amount: sourceEffect.droppedGraphs)
            deleted = try add(deleted, sourceEffect.deletedQuads)
            droppedGraphs = sourceEffect.droppedGraphs
        }

        return MutationExecuteOperation.RDFEffect(
            insertedQuads: inserted,
            deletedQuads: deleted,
            createdGraphs: createdGraphs,
            droppedGraphs: droppedGraphs
        )
    }

    private func mutationScope(
        _ target: SPARQLGraphTarget
    ) throws -> RDFGraphMutationScope {
        switch target {
        case .graph(let iri):
            return .named(try RDFGraphName(iri: iri))
        case .default:
            return .defaultGraph
        case .named:
            return .allNamedGraphs
        case .all:
            return .allGraphs
        }
    }

    private func droppedGraphCount(
        _ target: SPARQLGraphTarget,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        switch target {
        case .graph:
            return 1
        case .default:
            return 0
        case .named, .all:
            let graphs = try await graphStore.namedGraphs(
                limit: try mutationDetectionLimit(),
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            )
            guard graphs.count <= runtimeLimits.maximumMutations else {
                throw DatabaseMutationError.mutationLimitExceeded(
                    actual: graphs.count,
                    maximum: runtimeLimits.maximumMutations
                )
            }
            return UInt64(graphs.count)
        }
    }

    private func transferSourceQuads(
        _ endpoint: SPARQLGraphTransferEndpoint,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        let graphScope: RDFGraphScanScope
        switch endpoint {
        case .default:
            graphScope = .defaultGraph
        case .graph(let iri):
            graphScope = .named(try RDFGraphName(iri: iri))
        }
        let result = try await graphStore.scan(
            subject: nil,
            predicate: nil,
            object: nil,
            graphScope: graphScope,
            limit: try mutationDetectionLimit(),
            readMode: .serializable,
            transaction: transaction,
            workMeter: workMeter
        )
        guard result.count <= runtimeLimits.maximumMutations else {
            throw DatabaseMutationError.mutationLimitExceeded(
                actual: result.count,
                maximum: runtimeLimits.maximumMutations
            )
        }
        return result
    }

    private func clearTransferDestination(
        _ endpoint: SPARQLGraphTransferEndpoint,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        switch endpoint {
        case .default:
            return try await graphStore.clear(
                .defaultGraph,
                transaction: transaction,
                workMeter: workMeter
            )
        case .graph(let iri):
            let graph = try RDFGraphName(iri: iri)
            guard try await graphStore.containsGraph(
                graph,
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            ) else {
                return 0
            }
            return try await graphStore.clear(
                .named(graph),
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    private func ensureTransferDestination(
        _ endpoint: SPARQLGraphTransferEndpoint,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        guard case .graph(let iri) = endpoint else { return false }
        let graph = try RDFGraphName(iri: iri)
        if try await graphStore.containsGraph(
            graph,
            readMode: .serializable,
            transaction: transaction,
            workMeter: workMeter
        ) {
            return false
        }
        try await graphStore.createGraph(
            graph,
            transaction: transaction,
            workMeter: workMeter
        )
        return true
    }

    private func removeTransferSource(
        _ endpoint: SPARQLGraphTransferEndpoint,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> MutationExecuteOperation.RDFEffect {
        switch endpoint {
        case .default:
            let deleted = try await graphStore.clear(
                .defaultGraph,
                transaction: transaction,
                workMeter: workMeter
            )
            return MutationExecuteOperation.RDFEffect(
                deletedQuads: deleted
            )
        case .graph(let iri):
            let deleted = try await graphStore.drop(
                .named(try RDFGraphName(iri: iri)),
                transaction: transaction,
                workMeter: workMeter
            )
            return MutationExecuteOperation.RDFEffect(
                deletedQuads: deleted,
                droppedGraphs: 1
            )
        }
    }

    private func retarget(
        _ quad: Graph.RDFQuad,
        to endpoint: SPARQLGraphTransferEndpoint
    ) -> Graph.RDFQuad {
        let graph: DatabaseRDFTerm?
        switch endpoint {
        case .default:
            graph = nil
        case .graph(let iri):
            graph = .iri(iri)
        }
        return Graph.RDFQuad(
            subject: quad.subject,
            predicate: quad.predicate,
            object: quad.object,
            graph: graph
        )
    }

    private func explicitDataset(_ references: [GraphRef]) -> SPARQLDataset {
        var defaultGraphs: [String] = []
        var namedGraphs: [String] = []
        defaultGraphs.reserveCapacity(references.count)
        namedGraphs.reserveCapacity(references.count)
        for reference in references {
            if reference.isNamed {
                namedGraphs.append(reference.iri)
            } else {
                defaultGraphs.append(reference.iri)
            }
        }
        return .explicit(
            defaultGraphs: defaultGraphs,
            namedGraphs: namedGraphs
        )
    }

    private func graphPattern(from quads: [QueryIR.Quad]) -> GraphPattern {
        var result: GraphPattern?
        for quad in quads {
            let component: GraphPattern
            if let graph = quad.graph {
                component = .graph(
                    name: graph,
                    pattern: .basic([quad.triple])
                )
            } else {
                component = .basic([quad.triple])
            }
            if let existing = result {
                result = .join(existing, component)
            } else {
                result = component
            }
        }
        return result ?? .basic([])
    }

    private func templateQuad(
        _ quad: QueryIR.Quad,
        defaultGraph: String?
    ) -> QueryIR.Quad {
        guard quad.graph == nil, let defaultGraph else { return quad }
        return QueryIR.Quad(
            graph: .iri(defaultGraph),
            triple: quad.triple
        )
    }

    private func blankNodeScope(
        context: DatabaseOperationContext,
        operationOrdinal: UInt64,
        solutionOrdinal: UInt64
    ) throws -> SPARQLBlankNodeScope {
        guard let key = context.metadata.idempotencyKey, !key.isEmpty else {
            throw DatabaseMutationError.idempotencyKeyRequired
        }
        return SPARQLBlankNodeScope(
            idempotencyKey: key,
            operationOrdinal: operationOrdinal,
            solutionOrdinal: solutionOrdinal
        )
    }

    private func mutationDetectionLimit() throws -> Int {
        let (limit, overflow) = runtimeLimits.maximumMutations
            .addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseMutationError.mutationLimitExceeded(
                actual: Int.max,
                maximum: runtimeLimits.maximumMutations
            )
        }
        return limit
    }

    private func accumulate(
        _ result: RDFGraphInsertResult,
        mutationMeter: SPARQLMutationMeter,
        insertedQuads: inout UInt64,
        createdGraphs: inout UInt64
    ) throws {
        if result.quadInserted {
            insertedQuads = try increment(insertedQuads)
        }
        if result.graphCreated {
            try mutationMeter.consume()
            createdGraphs = try increment(createdGraphs)
        }
    }

    private func adding(
        _ lhs: MutationExecuteOperation.RDFEffect,
        _ rhs: MutationExecuteOperation.RDFEffect
    ) throws -> MutationExecuteOperation.RDFEffect {
        MutationExecuteOperation.RDFEffect(
            insertedQuads: try add(lhs.insertedQuads, rhs.insertedQuads),
            deletedQuads: try add(lhs.deletedQuads, rhs.deletedQuads),
            createdGraphs: try add(lhs.createdGraphs, rhs.createdGraphs),
            droppedGraphs: try add(lhs.droppedGraphs, rhs.droppedGraphs)
        )
    }

    private func increment(_ count: UInt64) throws -> UInt64 {
        let (next, overflow) = count.addingReportingOverflow(1)
        guard !overflow else { throw SPARQLUpdateError.effectCountOverflow }
        return next
    }

    private func add(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else { throw SPARQLUpdateError.effectCountOverflow }
        return result
    }

    private func isExistingGraph(_ error: RDFGraphStoreError) -> Bool {
        if case .graphAlreadyExists = error { return true }
        return false
    }

}
