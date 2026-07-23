import DatabaseDigest
import DatabaseValue
import DatabaseWire

enum DatabaseGraphAlgorithmResultFingerprint {
    static func compute(
        _ response: GraphAlgorithmOperation.Response,
        limits: DatabaseWireLimits
    ) throws -> DatabaseBytes {
        var hasher = SHA256Accumulator()
        update([0x47, 0x52, 0x02], hasher: &hasher)

        switch response {
        case .path(let result):
            updateByte(1, hasher: &hasher)
            updateBool(result.found, hasher: &hasher)
            try updateTerms(result.nodes, limits: limits, hasher: &hasher)
            try updateTerms(result.edgeLabels, limits: limits, hasher: &hasher)
            updateUInt64(UInt64(result.weights.count), hasher: &hasher)
            for weight in result.weights {
                updateDouble(weight, hasher: &hasher)
            }
            updateBool(result.totalWeight != nil, hasher: &hasher)
            if let totalWeight = result.totalWeight {
                updateDouble(totalWeight, hasher: &hasher)
            }
            updateUInt64(result.nodesExplored, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)

        case .ranking(let result):
            updateByte(2, hasher: &hasher)
            updateUInt64(UInt64(result.scores.count), hasher: &hasher)
            for score in result.scores {
                try updateTerm(score.vertex, limits: limits, hasher: &hasher)
                updateDouble(score.score, hasher: &hasher)
            }
            updateUInt32(result.iterations, hasher: &hasher)
            updateDouble(result.convergenceDelta, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)

        case .communities(let result):
            updateByte(3, hasher: &hasher)
            updateUInt64(UInt64(result.assignments.count), hasher: &hasher)
            for assignment in result.assignments {
                try updateTerm(assignment.vertex, limits: limits, hasher: &hasher)
                try updateTerm(assignment.community, limits: limits, hasher: &hasher)
            }
            updateUInt32(result.iterations, hasher: &hasher)
            updateBool(result.modularity != nil, hasher: &hasher)
            if let modularity = result.modularity {
                updateDouble(modularity, hasher: &hasher)
            }
            updateProgress(result.progress, hasher: &hasher)

        case .cycles(let result):
            updateByte(4, hasher: &hasher)
            updateUInt64(UInt64(result.cycles.count), hasher: &hasher)
            for cycle in result.cycles {
                try updateTerms(cycle, limits: limits, hasher: &hasher)
            }
            updateUInt64(UInt64(result.backEdges.count), hasher: &hasher)
            for edge in result.backEdges {
                try updateTerm(edge.source, limits: limits, hasher: &hasher)
                try updateTerm(edge.target, limits: limits, hasher: &hasher)
            }
            updateUInt64(result.nodesExplored, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)

        case .components(let result):
            updateByte(5, hasher: &hasher)
            updateUInt64(UInt64(result.components.count), hasher: &hasher)
            for component in result.components {
                try updateTerms(component, limits: limits, hasher: &hasher)
            }
            updateUInt64(result.nodesExplored, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)

        case .topologicalOrder(let result):
            updateByte(6, hasher: &hasher)
            updateBool(result.order != nil, hasher: &hasher)
            if let order = result.order {
                try updateTerms(order, limits: limits, hasher: &hasher)
            }
            try updateTerms(result.cyclicNodes, limits: limits, hasher: &hasher)
            updateUInt64(result.totalNodes, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)
        }

        return hasher.finalize()
    }

    private static func updateProgress(
        _ progress: GraphAlgorithmOperation.Progress,
        hasher: inout SHA256Accumulator
    ) {
        updateBool(progress.algorithmComplete, hasher: &hasher)
        updateByte(progress.limitReason?.rawValue ?? 0, hasher: &hasher)
    }

    private static func updateTerms(
        _ values: [DatabaseGraphTerm],
        limits: DatabaseWireLimits,
        hasher: inout SHA256Accumulator
    ) throws {
        updateUInt64(UInt64(values.count), hasher: &hasher)
        for value in values {
            try updateTerm(value, limits: limits, hasher: &hasher)
        }
    }

    private static func updateTerm(
        _ value: DatabaseGraphTerm,
        limits: DatabaseWireLimits,
        hasher: inout SHA256Accumulator
    ) throws {
        let bytes = try DatabaseEnvelopeCodec.encode(value, limits: limits)
        updateUInt64(UInt64(bytes.count), hasher: &hasher)
        update(bytes, hasher: &hasher)
    }

    private static func updateBool(
        _ value: Bool,
        hasher: inout SHA256Accumulator
    ) {
        updateByte(value ? 1 : 0, hasher: &hasher)
    }

    private static func updateByte(
        _ value: UInt8,
        hasher: inout SHA256Accumulator
    ) {
        var value = value
        withUnsafeBytes(of: &value) { hasher.update($0) }
    }

    private static func updateUInt32(
        _ value: UInt32,
        hasher: inout SHA256Accumulator
    ) {
        var value = value.bigEndian
        withUnsafeBytes(of: &value) { hasher.update($0) }
    }

    private static func updateUInt64(
        _ value: UInt64,
        hasher: inout SHA256Accumulator
    ) {
        var value = value.bigEndian
        withUnsafeBytes(of: &value) { hasher.update($0) }
    }

    private static func updateDouble(
        _ value: Double,
        hasher: inout SHA256Accumulator
    ) {
        updateUInt64(value.bitPattern, hasher: &hasher)
    }

    private static func update(
        _ bytes: DatabaseBytes,
        hasher: inout SHA256Accumulator
    ) {
        hasher.update(bytes)
    }
}
