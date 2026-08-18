#if DATABASE_MULTIPLE_BASES
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes

private actor CompositionRDFResultCollector {
    private var metadata: CompositionRDFMetadata?
    private var values: [CompositionResult<RDFQuad>] = []

    func receive(
        _ event: CompositionRDFQueryEvent,
        workMeter: DatabaseWorkMeter
    ) throws {
        switch event {
        case .began(let value):
            metadata = value
        case .quad(let value):
            guard let metadata else {
                throw CompositionQueryError.workspaceCorrupted
            }
            try workMeter.recordOutputRows()
            values.append(
                CompositionResult(
                    composition: metadata.composition,
                    origin: value.origin,
                    value: value.quad
                )
            )
        }
    }

    func result() -> [CompositionResult<RDFQuad>] {
        values
    }
}

private actor CompositionSPARQLResultCollector {
    private var metadata: CompositionQueryMetadata?
    private var values: [CompositionResult<QueryRow>] = []

    func receive(
        _ event: CompositionQueryEvent,
        workMeter: DatabaseWorkMeter
    ) throws {
        switch event {
        case .began(let value):
            metadata = value
        case .row(let value):
            guard let metadata else {
                throw CompositionQueryError.workspaceCorrupted
            }
            try workMeter.recordOutputRows()
            values.append(
                CompositionResult(
                    composition: metadata.composition,
                    origin: value.origin,
                    value: value.row
                )
            )
        }
    }

    func result() -> [CompositionResult<QueryRow>] {
        values
    }
}

public extension CompositionDataSource {
    /// Executes a Composition SPARQL SELECT in-process through GraphIndex.
    /// Blank-node identity is qualified before global DISTINCT and ordering.
    func select(
        _ query: SelectQuery,
        graphPartitions: FieldObject = FieldObject(),
        options: ReadExecutionOptions = .default
    ) async throws -> [CompositionResult<QueryRow>] {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock
        )
        let collector = CompositionSPARQLResultCollector()
        try await CompositionSPARQLQueryPlanner(
            structuralLimits: execution.queryStructuralLimits
        ).execute(
            query,
            source: self,
            graphPartitions: graphPartitions,
            pageSize: try Self.plannerPageSize(options: options),
            readContext: execution
        ) { event in
            try await collector.receive(
                event,
                workMeter: execution.workMeter
            )
            return true
        }
        return await collector.result()
    }

    /// Executes a Composition CONSTRUCT in-process. Blank-node identity and
    /// duplicate provenance are resolved by GraphIndex before values return.
    func construct(
        _ query: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        graphPartitions: FieldObject = FieldObject(),
        options: ReadExecutionOptions = .default
    ) async throws -> [CompositionResult<RDFQuad>] {
        try await executeRDF(
            .construct(query),
            nodeNamespace: nodeNamespace,
            graphPartitions: graphPartitions,
            options: options
        )
    }

    /// Executes a Composition DESCRIBE in-process through GraphIndex.
    func describe(
        _ query: DescribeQuery,
        graphPartitions: FieldObject = FieldObject(),
        options: ReadExecutionOptions = .default
    ) async throws -> [CompositionResult<RDFQuad>] {
        try await executeRDF(
            .describe(query),
            nodeNamespace: nil,
            graphPartitions: graphPartitions,
            options: options
        )
    }

    /// Executes a Composition ASK in-process and retains the Bases that prove
    /// the boolean result.
    func ask(
        _ query: AskQuery,
        graphPartitions: FieldObject = FieldObject(),
        options: ReadExecutionOptions = .default
    ) async throws -> CompositionResult<Bool> {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock
        )
        let result = try await CompositionRDFQueryPlanner().executeAsk(
            query,
            source: self,
            graphPartitions: graphPartitions,
            readContext: execution
        )
        try execution.workMeter.recordOutputRows()
        return CompositionResult(
            composition: result.metadata.composition,
            origin: result.origin,
            value: result.value
        )
    }

    private func executeRDF(
        _ statement: CompositionRDFStatement,
        nodeNamespace: GraphResultNodeNamespace?,
        graphPartitions: FieldObject,
        options: ReadExecutionOptions
    ) async throws -> [CompositionResult<RDFQuad>] {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock
        )
        let collector = CompositionRDFResultCollector()
        try await CompositionRDFQueryPlanner().execute(
            statement,
            source: self,
            graphPartitions: graphPartitions,
            nodeNamespace: nodeNamespace,
            readContext: execution
        ) { event in
            try await collector.receive(
                event,
                workMeter: execution.workMeter
            )
            return true
        }
        return await collector.result()
    }
}
#endif
