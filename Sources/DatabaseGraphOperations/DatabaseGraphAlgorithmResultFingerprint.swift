import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire
import DatabaseTypes

package enum DatabaseGraphAlgorithmResultFingerprint {
    package static func compute(
        _ response: GraphAlgorithmOperation.Response,
        limits: DatabaseWireLimits
    ) throws -> ByteString {
        var hasher = SHA256Accumulator()
        update([0x47, 0x52, 0x02], hasher: &hasher)

        switch response {
        case .path(let result):
            updateByte(1, hasher: &hasher)
            updateBool(result.found, hasher: &hasher)
            var nodes = result.makeNodeIterator()
            var edgeLabels = result.makeEdgeLabelIterator()
            var weights = result.makeWeightIterator()
            try updateTerms(
                count: result.nodeCount,
                iterator: &nodes,
                limits: limits,
                hasher: &hasher
            )
            try updateTerms(
                count: result.edgeLabelCount,
                iterator: &edgeLabels,
                limits: limits,
                hasher: &hasher
            )
            updateUInt64(UInt64(result.weightCount), hasher: &hasher)
            while let weight = try weights.next() {
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
            var scores = result.makeScoreIterator()
            updateUInt64(UInt64(result.scoreCount), hasher: &hasher)
            while let score = try scores.next() {
                try updateTerm(score.vertex, limits: limits, hasher: &hasher)
                updateDouble(score.score, hasher: &hasher)
            }
            updateUInt32(result.iterations, hasher: &hasher)
            updateDouble(result.convergenceDelta, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)

        case .communities(let result):
            updateByte(3, hasher: &hasher)
            var assignments = result.makeAssignmentIterator()
            updateUInt64(UInt64(result.assignmentCount), hasher: &hasher)
            while let assignment = try assignments.next() {
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
            var cycles = result.makeCycleIterator()
            var backEdges = result.makeBackEdgeIterator()
            updateUInt64(UInt64(result.cycleCount), hasher: &hasher)
            while let cycle = try cycles.next() {
                var terms = cycle.makeTermIterator()
                try updateTerms(
                    count: cycle.termCount,
                    iterator: &terms,
                    limits: limits,
                    hasher: &hasher
                )
            }
            updateUInt64(UInt64(result.backEdgeCount), hasher: &hasher)
            while let edge = try backEdges.next() {
                try updateTerm(edge.source, limits: limits, hasher: &hasher)
                try updateTerm(edge.target, limits: limits, hasher: &hasher)
            }
            updateUInt64(result.nodesExplored, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)

        case .components(let result):
            updateByte(5, hasher: &hasher)
            var components = result.makeComponentIterator()
            updateUInt64(UInt64(result.componentCount), hasher: &hasher)
            while let component = try components.next() {
                var terms = component.makeTermIterator()
                try updateTerms(
                    count: component.termCount,
                    iterator: &terms,
                    limits: limits,
                    hasher: &hasher
                )
            }
            updateUInt64(result.nodesExplored, hasher: &hasher)
            updateProgress(result.progress, hasher: &hasher)

        case .topologicalOrder(let result):
            updateByte(6, hasher: &hasher)
            let order = result.makeOrderIterator()
            var cyclicNodes = result.makeCyclicNodeIterator()
            updateBool(order != nil, hasher: &hasher)
            if var order {
                try updateTerms(
                    count: result.orderCount ?? 0,
                    iterator: &order,
                    limits: limits,
                    hasher: &hasher
                )
            }
            try updateTerms(
                count: result.cyclicNodeCount,
                iterator: &cyclicNodes,
                limits: limits,
                hasher: &hasher
            )
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
        count: Int,
        iterator: inout ResultIterator<GraphAlgorithmOperation.Term>,
        limits: DatabaseWireLimits,
        hasher: inout SHA256Accumulator
    ) throws {
        updateUInt64(UInt64(count), hasher: &hasher)
        while let value = try iterator.next() {
            try updateTerm(value, limits: limits, hasher: &hasher)
        }
    }

    private static func updateTerm(
        _ value: GraphAlgorithmOperation.Term,
        limits: DatabaseWireLimits,
        hasher: inout SHA256Accumulator
    ) throws {
        try DatabaseRuntimePayloadEncoder.emit(
            value,
            limits: limits,
            prepare: { byteCount in
                updateUInt64(UInt64(byteCount), hasher: &hasher)
            },
            consume: { bytes in
                hasher.update(bytes)
            }
        )
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
        _ bytes: ByteString,
        hasher: inout SHA256Accumulator
    ) {
        hasher.update(bytes)
    }
}

#endif
