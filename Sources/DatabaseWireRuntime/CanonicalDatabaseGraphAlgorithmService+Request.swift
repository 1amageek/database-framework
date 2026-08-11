#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

extension CanonicalDatabaseGraphAlgorithmService {
    func paginationKind(
        for invocation: GraphAlgorithmOperation.Invocation
    ) -> DatabaseGraphAlgorithmPageCursor.Kind {
        switch invocation {
        case .shortestPath, .weightedShortestPath:
            return .path
        case .pageRank:
            return .ranking
        case .community:
            return .communities
        case .cycleDetection:
            return .cycles
        case .stronglyConnectedComponents:
            return .components
        case .topologicalSort:
            return .topologicalOrder
        }
    }

    func requestFingerprint(
        for request: GraphAlgorithmOperation.Request
    ) throws -> ByteString {
        let normalized = GraphAlgorithmOperation.Request(
            source: request.source,
            invocation: request.invocation,
            page: GraphAlgorithmOperation.Page(
                limit: request.page.limit,
                continuation: nil
            ),
            budget: request.budget
        )
        let payload = try DatabaseWireEncoder(
            limits: wireLimits
        ).encodeRequestPayload(
            DatabaseOperations.graphAlgorithm,
            request: normalized
        )
        return DatabaseRequestDigest.compute(
            operation: .graphAlgorithm,
            prefix: [0x47, 0x52, 0x01],
            payload: payload
        )
    }

    func deterministicSeed(from fingerprint: ByteString) -> UInt64 {
        fingerprint.prefix(8).reduce(UInt64(0)) { partial, byte in
            (partial << 8) | UInt64(byte)
        }
    }

    func positiveInt(_ value: UInt64, field: String) throws -> Int {
        guard value > 0 else {
            throw DatabaseGraphAlgorithmError.invalidInvocation(
                "\(field) must be greater than zero"
            )
        }
        guard let result = Int(exactly: value) else {
            throw DatabaseGraphAlgorithmError.numericLimitOutOfRange(
                field: field,
                value: value
            )
        }
        return result
    }

    func positiveInt(_ value: UInt32, field: String) throws -> Int {
        try positiveInt(UInt64(value), field: field)
    }

    func unsigned(_ value: Int, field: String) throws -> UInt64 {
        guard let result = UInt64(exactly: value) else {
            throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                "\(field) is negative or out of range"
            )
        }
        return result
    }

    func unsigned32(_ value: Int, field: String) throws -> UInt32 {
        guard let result = UInt32(exactly: value) else {
            throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                "\(field) is negative or out of range"
            )
        }
        return result
    }
}
#endif
