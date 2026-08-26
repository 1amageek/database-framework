import DatabaseKit

extension DatabaseContext {
    /// Executes a context-free Fusion plan through the canonical read path.
    public func execute<Item: Persistable>(
        _ query: FusionQuery<Item>,
        options: ReadExecutionOptions = .default
    ) async throws -> FusionResponse<Item> {
        do {
            return try await executeUnmapped(query, options: options)
        } catch {
            throw sanitizedFusionExecutionError(error)
        }
    }

    private func executeUnmapped<Item: Persistable>(
        _ query: FusionQuery<Item>,
        options: ReadExecutionOptions
    ) async throws -> FusionResponse<Item> {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock
        )
        let response = try await queryRetained(
            query.selectQuery,
            execution: execution
        )
        guard let outputRows = UInt32(exactly: response.visibleRange.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: execution.workMeter.consumedRows,
                requested: UInt32.max,
                maximum: execution.workMeter.budget.maximumRows
            )
        }
        try execution.workMeter.recordOutputRows(outputRows)

        // Typed model decoding is the public application-output boundary. It
        // may execute application initializers and transient properties, so it
        // is deliberately excluded from the framework-intermediate memory
        // ledger. Output cardinality remains admitted above, while the
        // canonical row owners stay alive until every model is decoded.
        var results: [ScoredResult<Item>] = []
        results.reserveCapacity(response.visibleRange.count)
        let visibleRows = response.visibleRows
        for position in 0..<visibleRows.count {
            try visibleRows.withElement(at: position) { row in
                guard let score = row.annotations[
                    FusionExecutor.scoreAnnotation
                ]?.float64Value, score.isFinite else {
                    throw FusionExecutionContractError.invalidScoreSignal
                }
                results.append(
                    ScoredResult(
                        item: try QueryRowCodec.decode(row, as: Item.self),
                        score: score
                    )
                )
            }
        }
        return FusionResponse(
            results: results,
            continuation: response.continuation,
            metadata: response.metadata
        )
    }
}
