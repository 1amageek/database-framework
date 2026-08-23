@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

@_spi(DatabaseExecution)
public struct SPARQLUpdateExecutor: Sendable {
    private let graphStore: any RDFGraphMutationStore
    private let runtimeLimits: SPARQLUpdateLimits
    private let structuralLimits: QueryStructuralLimits
    private let functionRegistry: SPARQLFunctionRegistry
    private let resolver = SPARQLUpdateQuadResolver()

    public init(
        graphStore: any RDFGraphMutationStore,
        runtimeLimits: SPARQLUpdateLimits,
        structuralLimits: QueryStructuralLimits,
        functionRegistry: SPARQLFunctionRegistry = .empty
    ) {
        self.graphStore = graphStore
        self.runtimeLimits = runtimeLimits
        self.structuralLimits = structuralLimits
        self.functionRegistry = functionRegistry
    }

    public func execute(
        _ request: PreparedSPARQLUpdateRequest,
        preconditions: [EntityMutationPrecondition],
        context: any SPARQLUpdateExecutionContext,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFMutationEffect {
        try await entities.validate(
            preconditions,
            transaction: transaction,
            workMeter: workMeter
        )
        let storageAccess = transaction.executionStorageAccess
        let mutationMeter = SPARQLMutationMeter(
            maximum: runtimeLimits.maximumMutations,
            workMeter: workMeter
        )
        var result = RDFMutationEffect()
        for index in 0..<request.count {
            let operationResult = try await executeOperation(
                request.operation(at: index),
                operationOrdinal: UInt64(index),
                context: context,
                transaction: storageAccess,
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
        context: any SPARQLUpdateExecutionContext,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
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
            return RDFMutationEffect()
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
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        let graph: RDFGraphName?
        var createdGraphs: UInt64 = 0
        if let destination = prepared.destination {
            let graphName = try RDFGraphName(iri: destination)
            graph = graphName
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
                RDFQuad(
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
        return RDFMutationEffect(
            insertedQuads: inserted,
            createdGraphs: createdGraphs
        )
    }

    private func insertData(
        _ query: InsertDataQuery,
        operationOrdinal: UInt64,
        context: any SPARQLUpdateExecutionContext,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        let blankNodeResolver = try makeBlankNodeResolver(
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
                blankNodeResolver: blankNodeResolver,
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
        return RDFMutationEffect(
            insertedQuads: inserted,
            createdGraphs: createdGraphs
        )
    }

    private func deleteData(
        _ query: DeleteDataQuery,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        var deleted: UInt64 = 0
        for quad in query.quads {
            try workMeter.consume(at: .mutationPlanning)
            let resolved = try resolver.resolve(
                quad,
                row: nil,
                blankNodeResolver: nil,
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
        return RDFMutationEffect(deletedQuads: deleted)
    }

    private func deleteInsert(
        _ query: SPARQLModifyOperation,
        operationOrdinal: UInt64,
        context: any SPARQLUpdateExecutionContext,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        let rows = try await solutionRows(
            for: query,
            context: context,
            transaction: transaction,
            workMeter: workMeter
        )
        let deletePattern: [Quad]
        let insertPattern: [Quad]
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
        for index in 0..<rows.count {
            try await rows.withBinding(at: index, workMeter: workMeter) { row in
                for template in deletePattern {
                    try workMeter.consume(at: .mutationPlanning)
                    let quads = try resolver.resolve(
                        templateQuad(
                            template,
                            defaultGraph: query.withGraph
                        ),
                        row: row,
                        blankNodeResolver: nil,
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
        }

        var inserted: UInt64 = 0
        var createdGraphs: UInt64 = 0
        for ordinal in 0..<rows.count {
            let blankNodeResolver = try makeBlankNodeResolver(
                context: context,
                operationOrdinal: operationOrdinal,
                solutionOrdinal: UInt64(ordinal)
            )
            try await rows.withBinding(
                at: ordinal,
                workMeter: workMeter
            ) { row in
                for template in insertPattern {
                    try workMeter.consume(at: .mutationPlanning)
                    let quads = try resolver.resolve(
                        templateQuad(
                            template,
                            defaultGraph: query.withGraph
                        ),
                        row: row,
                        blankNodeResolver: blankNodeResolver,
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
        }
        return RDFMutationEffect(
            insertedQuads: inserted,
            deletedQuads: deleted,
            createdGraphs: createdGraphs
        )
    }

    private func solutionRows(
        for query: SPARQLModifyOperation,
        context: any SPARQLUpdateExecutionContext,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> SPARQLRetainedResult {
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
        context: any SPARQLUpdateExecutionContext,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> SPARQLRetainedResult {
        let detectionLimit = try mutationDetectionLimit()
        let executor = try context.makeSPARQLQueryExecutor(
            datasetScanner: graphStore,
            readMode: .serializable,
            dataset: try SPARQLExecutionDataset(dataset),
            functionRegistry: functionRegistry
        )
        let bindings = try await executor.executeRetainedInTransaction(
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
            throw SPARQLUpdateError.mutationLimitExceeded(
                actual: bindings.count,
                maximum: runtimeLimits.maximumMutations
            )
        }
        return bindings
    }

    private func deleteWhere(
        _ query: DeleteWhereQuery,
        context: any SPARQLUpdateExecutionContext,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        let rows = try await solutionRows(
            pattern: graphPattern(from: query.pattern),
            dataset: .implicit,
            context: context,
            transaction: transaction,
            workMeter: workMeter
        )
        var deleted: UInt64 = 0
        for index in 0..<rows.count {
            try await rows.withBinding(at: index, workMeter: workMeter) { row in
                for template in query.pattern {
                    try workMeter.consume(at: .mutationPlanning)
                    let quads = try resolver.resolve(
                        template,
                        row: row,
                        blankNodeResolver: nil,
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
        }
        return RDFMutationEffect(deletedQuads: deleted)
    }

    private func clear(
        _ query: ClearQuery,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        let graphTarget = try mutationTarget(query.target)
        if case .graph(let iri) = query.target {
            let graph = try RDFGraphName(iri: iri)
            guard try await graphStore.containsGraph(
                graph,
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            ) else {
                if query.silent {
                    return RDFMutationEffect()
                }
                throw RDFGraphStoreError.graphNotFound(graph)
            }
        }
        let deleted = try await graphStore.clear(
            graphTarget,
            transaction: transaction,
            workMeter: workMeter
        )
        try mutationMeter.consume(amount: deleted)
        return RDFMutationEffect(
            deletedQuads: deleted
        )
    }

    private func createGraph(
        iri: String,
        silent: Bool,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        let graph = try RDFGraphName(iri: iri)
        try mutationMeter.consume()
        do {
            try await graphStore.createGraph(
                graph,
                transaction: transaction,
                workMeter: workMeter
            )
            return RDFMutationEffect(createdGraphs: 1)
        } catch let error as RDFGraphStoreError {
            if silent, isExistingGraph(error) {
                return RDFMutationEffect()
            }
            throw error
        }
    }

    private func drop(
        _ query: DropQuery,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        let graphTarget = try mutationTarget(query.target)
        if case .graph(let iri) = query.target {
            let graph = try RDFGraphName(iri: iri)
            guard try await graphStore.containsGraph(
                graph,
                readMode: .serializable,
                transaction: transaction,
                workMeter: workMeter
            ) else {
                if query.silent {
                    return RDFMutationEffect()
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
            graphTarget,
            transaction: transaction,
            workMeter: workMeter
        )
        try mutationMeter.consume(amount: deleted)
        try mutationMeter.consume(amount: droppedGraphs)
        return RDFMutationEffect(
            deletedQuads: deleted,
            droppedGraphs: droppedGraphs
        )
    }

    private func transfer(
        _ query: GraphTransferQuery,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        mutationMeter: SPARQLMutationMeter
    ) async throws -> RDFMutationEffect {
        guard query.source != query.destination else {
            return RDFMutationEffect()
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
                    return RDFMutationEffect()
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
            let destinationQuad = try retarget(
                sourceQuad.ownedQuad(),
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

        return RDFMutationEffect(
            insertedQuads: inserted,
            deletedQuads: deleted,
            createdGraphs: createdGraphs,
            droppedGraphs: droppedGraphs
        )
    }

    private func mutationTarget(
        _ target: SPARQLGraphTarget
    ) throws -> RDFGraphMutationTarget {
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
        transaction: any TransactionAccess,
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
                throw SPARQLUpdateError.mutationLimitExceeded(
                    actual: graphs.count,
                    maximum: runtimeLimits.maximumMutations
                )
            }
            return UInt64(graphs.count)
        }
    }

    private func transferSourceQuads(
        _ endpoint: SPARQLGraphTransferEndpoint,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        let graphTarget: RDFGraphScanTarget
        switch endpoint {
        case .default:
            graphTarget = .defaultGraph
        case .graph(let iri):
            graphTarget = .named(try RDFGraphName(iri: iri))
        }
        let result = try await graphStore.scan(
            subject: nil,
            predicate: nil,
            object: nil,
            graphTarget: graphTarget,
            limit: try mutationDetectionLimit(),
            readMode: .serializable,
            transaction: transaction,
            workMeter: workMeter
        )
        guard result.count <= runtimeLimits.maximumMutations else {
            throw SPARQLUpdateError.mutationLimitExceeded(
                actual: result.count,
                maximum: runtimeLimits.maximumMutations
            )
        }
        return result
    }

    private func clearTransferDestination(
        _ endpoint: SPARQLGraphTransferEndpoint,
        transaction: any TransactionAccess,
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
        transaction: any TransactionAccess,
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
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFMutationEffect {
        switch endpoint {
        case .default:
            let deleted = try await graphStore.clear(
                .defaultGraph,
                transaction: transaction,
                workMeter: workMeter
            )
            return RDFMutationEffect(
                deletedQuads: deleted
            )
        case .graph(let iri):
            let deleted = try await graphStore.drop(
                .named(try RDFGraphName(iri: iri)),
                transaction: transaction,
                workMeter: workMeter
            )
            return RDFMutationEffect(
                deletedQuads: deleted,
                droppedGraphs: 1
            )
        }
    }

    private func retarget(
        _ quad: RDFQuad,
        to endpoint: SPARQLGraphTransferEndpoint
    ) throws -> RDFQuad {
        let graph: RDFGraphName?
        switch endpoint {
        case .default:
            graph = nil
        case .graph(let iri):
            graph = try RDFGraphName(iri: iri)
        }
        return RDFQuad(
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

    private func graphPattern(from quads: [Quad]) -> GraphPattern {
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
        _ quad: Quad,
        defaultGraph: String?
    ) -> Quad {
        guard quad.graph == nil, let defaultGraph else { return quad }
        return Quad(
            graph: .iri(defaultGraph),
            triple: quad.triple
        )
    }

    private func makeBlankNodeResolver(
        context: any SPARQLUpdateExecutionContext,
        operationOrdinal: UInt64,
        solutionOrdinal: UInt64
    ) throws -> SPARQLUpdateBlankNodeResolver {
        guard let key = context.idempotencyKey, !key.isEmpty else {
            throw SPARQLUpdateError.idempotencyKeyRequired
        }
        return SPARQLUpdateBlankNodeResolver(
            idempotencyKey: key,
            operationOrdinal: operationOrdinal,
            solutionOrdinal: solutionOrdinal
        )
    }

    private func mutationDetectionLimit() throws -> Int {
        let (limit, overflow) = runtimeLimits.maximumMutations
            .addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLUpdateError.mutationLimitExceeded(
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
        _ lhs: RDFMutationEffect,
        _ rhs: RDFMutationEffect
    ) throws -> RDFMutationEffect {
        RDFMutationEffect(
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
