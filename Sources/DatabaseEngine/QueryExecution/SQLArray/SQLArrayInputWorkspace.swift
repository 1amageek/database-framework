import DatabaseKit

/// Synchronous admission of the intrinsic parser without knowing its private cell layout.
enum SQLArrayInputWorkspace {
    static func withInput<Result>(sqlLexicalForm: String, limits: QueryValueLimits,
        workMeter: DatabaseWorkMeter, _ body: (borrowing QueryArrayInput) throws -> Result) throws -> Result {
        let rank = 6
        func storage<Element>(_ type: Element.Type, count: Int) throws -> UInt64 {
            let layout = try DatabaseRetainedArrayLayout.forElement(type)
            return try DatabaseIntermediateFootprint(bytes: layout.containerByteCount)
                .adding(.init(bytes: layout.growth(from: 0, toFit: count).additionalByteCount)).bytes
        }
        // Per intrinsic recursion level: child shape, singleton prefix and joined
        // shape. Explicit bounds and their length projection are separate owners.
        let shapes = try DatabaseIntermediateFootprint(bytes: storage(Int.self, count: rank)).multiplied(by: UInt64(3 * rank))
        let scratch = try shapes.adding(.init(bytes: storage((Int32, Int32).self, count: rank)))
            .adding(.init(bytes: storage(Int.self, count: rank)))
            .adding(.init(bytes: storage((Int, Int, Bool).self, count: rank)))
        let temporary = try workMeter.reserveIntermediate(bytes: scratch.bytes, at: .expressionEvaluation)
        defer { temporary.release() }
        let retained = workMeter.retainedIntermediateBytes
        guard retained <= workMeter.budget.maximumIntermediateBytes else { throw SQLArrayStorage.mismatch("array input admission state") }
        let allowance = min(limits.maximumRetainedBytes, workMeter.budget.maximumIntermediateBytes - retained)
        let claim = try workMeter.reserveIntermediate(bytes: allowance, at: .expressionEvaluation)
        defer { claim.release() }
        let bounded = QueryValueLimits(maximumRetainedBytes: allowance, maximumTotalNodes: limits.maximumTotalNodes,
            maximumNestingDepth: limits.maximumNestingDepth, maximumCollectionElements: limits.maximumCollectionElements)
        try workMeter.consume(UInt64(sqlLexicalForm.utf8.count), at: .expressionEvaluation)
        let parsed: QueryArrayInput
        do { parsed = try QueryArrayInput(sqlLexicalForm: sqlLexicalForm, limits: bounded) }
        catch let error {
            if case .malformedArrayInput(let offset, let reason) = error {
                throw SQLArrayStorage.failure(.invalidArrayInput(offset: offset, reason: reason))
            }
            throw error
        }
        try workMeter.checkpoint(at: .expressionEvaluation)
        guard parsed.retainedByteCount <= allowance else { throw QueryValueError.arithmeticOverflow }
        try claim.releasePartial(bytes: allowance - parsed.retainedByteCount)
        temporary.release()
        return try body(parsed)
    }
}
