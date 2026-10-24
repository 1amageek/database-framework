import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseEngine
@_spi(DatabaseExecution) import DatabaseWire
import QueryAST
import StorageKit

extension DatabaseContext {
    /// Runs canonical query input and returns only an authoritatively closed owner.
    @_spi(DatabaseExecution)
    public func executeQueryEncoded(_ input: QueryInput, parameters: [QueryParameter] = [],
        options: QueryExecutionOptions = QueryExecutionOptions(), requestID: UInt64,
        limits: DatabaseWireLimits = .default, structuralLimits: QueryStructuralLimits = .default,
        checkpointCeiling: TransactionCheckpointLimits? = nil,
        configuration: TransactionConfiguration = .default) async throws -> EncodedQueryResponse {
        try options.validate()
        guard ActiveDatabaseTransactionContext.binding == nil else {
            throw SQLExecutionFailure(underlyingError: SQLBlockExecutionError.enclosingTransaction,
                diagnostic: SQLDiagnostics.diagnostic(for: SQLBlockExecutionError.enclosingTransaction))
        }
        switch input {
        case .text(language: .sparql, statement: let text):
            return try await executeGraphEncoded(text: text, statement: nil, parameters: parameters,
                options: options, requestID: requestID, limits: limits, structuralLimits: structuralLimits)
        case .statement(let statement) where isGraphRead(statement):
            return try await executeGraphEncoded(text: nil, statement: statement, parameters: parameters,
                options: options, requestID: requestID, limits: limits, structuralLimits: structuralLimits)
        default: break
        }
        guard options.graphPartitions.isEmpty else {
            throw SQLExecutionFailure(underlyingError: QueryConnectionError.graphPartitionsRequireGraphQuery,
                diagnostic: try SQLDiagnostic(sqlState: "22023", category: .data,
                    message: "Graph partitions require a graph query"))
        }
        let checkpoints: TransactionCheckpointLimits
        do {
            checkpoints = try SQLCheckpointPolicy.limits(for: options.budget,
                structuralLimits: structuralLimits, ceiling: checkpointCeiling)
        } catch {
            throw SQLExecutionFailure(underlyingError: error, diagnostic: SQLDiagnostics.diagnostic(for: error))
        }
        let blockOptions = SQLBlockExecutionOptions(checkpointLimits: checkpoints,
            budget: options.budget, structuralLimits: structuralLimits, configuration: configuration,
            requiresReadOnlyQuery: options.pageSize != nil)
        let encoder = QueryResponseEncoder(requestID: requestID, limits: limits)
        switch input {
        case .text(language: .sql, statement: let text):
            return try await executeEncodedSQLBlock(text, parameters: parameters, options: blockOptions, encoder: encoder)
        case .statement(let statement):
            return try await executeEncodedStatement(statement, parameters: parameters, options: blockOptions, encoder: encoder)
        case .block(let block):
            return try await executeEncodedSQLBlock(block, parameters: parameters, options: blockOptions, encoder: encoder)
        case .text(language: .sparql, statement: _):
            throw SQLBlockExecutionError.unsupportedStatement
        }
    }

    private func executeGraphEncoded(text: String?, statement: QueryStatement?, parameters: [QueryParameter],
        options: QueryExecutionOptions, requestID: UInt64, limits: DatabaseWireLimits,
        structuralLimits: QueryStructuralLimits) async throws -> EncodedQueryResponse {
        let clock = container.monotonicClock
        let meter = DatabaseWorkMeter(budget: options.budget, monotonicClock: clock)
        let deadline = TransactionExecutionDeadline(instant: clock.now.advanced(
            by: .milliseconds(Int64(options.budget.timeoutMilliseconds))),
            timeoutMilliseconds: options.budget.timeoutMilliseconds)
        let parsed: QueryStatement
        if let statement { parsed = statement }
        else if let text {
            #if DATABASE_GRAPH_INDEXES
            parsed = try SPARQLParser(structuralLimits: structuralLimits).parse(text)
            #else
            _ = text
            throw CanonicalReadError.unsupportedSource("SPARQL text requires the GraphIndexes trait")
            #endif
        } else { throw SQLBlockExecutionError.unsupportedStatement }
        let bound = try QueryParameterBinder(parameters: parameters, structuralLimits: structuralLimits).bind(parsed)
        try SPARQLSemanticValidator.validate(bound, limits: structuralLimits)
        try meter.checkpoint(at: .bindingCandidate)
        let projection = QueryReadProjection(requestID: requestID, limits: limits)
        return try await withEncodedReadSnapshot(workMeter: meter, executionDeadline: deadline,
            produce: { snapshot in
                let execution = ReadExecutionContext(options: ReadExecutionOptions(budget: options.budget),
                    monotonicClock: clock, workMeter: meter, queryStructuralLimits: structuralLimits)
                switch bound {
                case .select(let query):
                    let prepared = try await self.prepareSQLSelectForCanonicalExecution(query,
                        workMeter: meter, session: snapshot.session, structuralLimits: structuralLimits)
                    let rows = try await prepared.stageCompleteRows(in: snapshot.session,
                        execution: execution, graphPartitions: options.graphPartitions)
                    try recordQueryRows(rows.count, meter: meter)
                    return QueryReadOutput.rows(rows)
                case .ask(let query):
                    let value = try await snapshot.session.executeSPARQLAsk(query, options: execution,
                        partitions: options.graphPartitions)
                    try meter.recordOutputRows(1)
                    return .boolean(value, snapshot.position, try self.executionStorage().domainIdentifier)
                case .construct(let query):
                    let namespace = try graphNamespace(bound, limits: limits, meter: meter)
                    let graph = try await snapshot.session.executeSPARQLConstruct(query,
                        nodeNamespace: namespace, options: execution, partitions: options.graphPartitions)
                    let shared = try canonicalGraph(consume graph, meter: meter)
                    try recordQueryRows(shared.count, meter: meter)
                    return .graph(shared, snapshot.position, try self.executionStorage().domainIdentifier)
                case .describe(let query):
                    let graph = try await snapshot.session.executeSPARQLDescribe(query,
                        options: execution, partitions: options.graphPartitions)
                    let shared = try canonicalGraph(consume graph, meter: meter)
                    try recordQueryRows(shared.count, meter: meter)
                    return .graph(shared, snapshot.position, try self.executionStorage().domainIdentifier)
                default: throw SQLBlockExecutionError.unsupportedStatement
                }
            }, measure: { try projection.measure($0) },
            encode: { output, destination in try projection.encode(output, into: &destination) })
    }
}

private func isGraphRead(_ statement: QueryStatement) -> Bool {
    switch statement {
    case .select(let query): query.source.usesSPARQLSemantics
    case .ask, .construct, .describe: true
    default: false
    }
}

private func recordQueryRows(_ count: Int, meter: DatabaseWorkMeter) throws {
    guard let count = UInt32(exactly: count) else {
        throw DatabaseWorkLimitError.maximumRows(stage: .resultMaterialization,
            consumed: meter.consumedRows, requested: .max, maximum: meter.budget.maximumRows)
    }
    try meter.recordOutputRows(count)
}

private func graphNamespace(_ statement: QueryStatement, limits: DatabaseWireLimits,
                            meter: DatabaseWorkMeter) throws -> GraphResultNodeNamespace {
    var hash = SHA256Accumulator()
    hash.update(utf8: "query-result-graph-v1")
    try QueryIRWireFormat.emitCanonicalEncoding(statement, limits: limits,
        prepare: { count in
            try meter.consume(UInt64(count / 256 + (count % 256 == 0 ? 0 : 1)), at: .resultMaterialization)
        }, consume: { hash.update($0) })
    return try GraphResultNodeNamespace(hash.finalize())
}

private func canonicalGraph(_ graph: consuming DatabaseRetainedRDFGraph,
                            meter: DatabaseWorkMeter) throws -> DatabaseSharedRetainedRDFGraph {
    try meter.consume(UInt64(graph.count), at: .sortInput)
    let sorted = try graph.sorting { left, right in
        try meter.consume(2, at: .sortComparison)
        if left.subject != right.subject { return left.subject < right.subject }
        if left.predicate != right.predicate { return left.predicate < right.predicate }
        if left.object != right.object { return left.object < right.object }
        switch (left.graph, right.graph) {
        case (.none, .some): return true
        case (.some, .none), (.none, .none): return false
        case (.some(let left), .some(let right)): return left < right
        }
    }
    let unique = try sorted.removingAdjacentDuplicates { left, right in
        try meter.consume(at: .deduplication)
        return left == right
    }
    return try unique.moveToSharedOwnership(at: .resultMaterialization)
}
