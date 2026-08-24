import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Property-graph physical reader for cross-entity Connected Fusion inputs.
package struct GraphFusionConnectedReadExecutor: FusionConnectedReadExecutor {
    package let indexType: IndexType = .graph(.property)

    package init() {}

    package func validate(
        _ request: FusionConnectedValidationRequest
    ) throws {
        guard request.descriptor.type == indexType,
              PropertyGraphIndexConfiguration(
                descriptor: request.descriptor
              ) != nil else {
            throw invalid("selection")
        }
        guard request.scoring == .annotation(
            name: "hops",
            order: .lowerIsBetter
        ) else {
            throw invalid("scoring")
        }
        guard Double(exactly: request.source.maximumHops) != nil else {
            throw invalid("maximumHops")
        }
    }

    package func execute(
        _ request: FusionConnectedReadRequest,
        output: FusionConnectedMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            guard !output.hasReachedLimit else { return .satisfiedLimit }
            let index = request.access.index
            guard index.physicalLayout.name == "standard",
                  index.physicalLayout.revision == 1,
                  index.physicalLayout.parameters.isEmpty,
                  let configuration = PropertyGraphIndexConfiguration(
                    descriptor: index.descriptor
                  ) else {
                throw FusionExecutionError.executionContractViolation
            }

            let scanner = GraphEdgeScanner(
                indexSubspace: index.subspace,
                strategy: configuration.strategy
            )
            let label = request.source.edgeLabel.map(
                GraphIdentity.identifier
            )
            var traversal = try TraversalState(
                origin: .identifier(request.source.origin),
                workMeter: request.workMeter
            )
            var hops: UInt64 = 0
            while hops < request.source.maximumHops,
                  !traversal.currentLevel.isEmpty {
                let (nextHops, overflow) = hops.addingReportingOverflow(1)
                guard !overflow else {
                    throw FusionExecutionError.executionContractViolation
                }
                hops = nextHops
                for index in traversal.currentLevel.indices {
                    let vertex = traversal.currentLevel[index]
                    switch request.source.direction {
                    case .outgoing:
                        try await scan(
                            scanner: scanner,
                            source: vertex,
                            target: nil,
                            edgeLabel: label,
                            hops: hops,
                            request: request,
                            traversal: &traversal,
                            output: output
                        )
                    case .incoming:
                        try await scan(
                            scanner: scanner,
                            source: nil,
                            target: vertex,
                            edgeLabel: label,
                            hops: hops,
                            request: request,
                            traversal: &traversal,
                            output: output
                        )
                    case .both:
                        try await scan(
                            scanner: scanner,
                            source: vertex,
                            target: nil,
                            edgeLabel: label,
                            hops: hops,
                            request: request,
                            traversal: &traversal,
                            output: output
                        )
                        try await scan(
                            scanner: scanner,
                            source: nil,
                            target: vertex,
                            edgeLabel: label,
                            hops: hops,
                            request: request,
                            traversal: &traversal,
                            output: output
                        )
                    }
                }
                // All equal-hop candidates are considered before stopping, so
                // primary-key tie ordering remains exact.
                if output.hasReachedLimit { return .satisfiedLimit }
                traversal.advanceLevel()
            }
            return .exhausted
        } catch is GraphIndexError {
            throw FusionExecutionError.corruptedIndex(indexType)
        }
    }

    private func scan(
        scanner: GraphEdgeScanner,
        source: GraphIdentity?,
        target: GraphIdentity?,
        edgeLabel: GraphIdentity?,
        hops: UInt64,
        request: FusionConnectedReadRequest,
        traversal: inout TraversalState,
        output: FusionConnectedMatchSink
    ) async throws {
        let prepared = try scanner.prepareScan(
            source: source,
            target: target,
            edgeLabel: edgeLabel
        )
        let cursor = try request.access.rangeCursor(
            from: prepared.begin,
            to: prepared.end,
            reverse: false
        )
        while let row = try await cursor.next() {
            let edge = try scanner.decodeEdge(
                row.key,
                ordering: prepared.ordering,
                subspace: prepared.subspace
            )
            guard source == nil || edge.source == source,
                  target == nil || edge.target == target,
                  edgeLabel == nil || edge.edgeLabel == edgeLabel,
                  scanner.containsGraph(edge.graph) else {
                continue
            }
            let adjacent = source == nil ? edge.source : edge.target
            guard try traversal.discover(adjacent) else { continue }
            try output.submit(
                vertexIdentifier: try adjacent
                    .requirePropertyGraphIdentifier(),
                hops: hops
            )
        }
    }

    private func invalid(_ parameter: String) -> FusionExecutionError {
        .invalidIndexInput(indexType: indexType, parameter: parameter)
    }
}

private extension GraphFusionConnectedReadExecutor {
    struct TraversalState {
        var currentLevel: [GraphIdentity] = []
        var nextLevel: [GraphIdentity] = []
        var visited: Set<GraphIdentity> = []

        private let workMeter: DatabaseWorkMeter
        private let arrayLayout: DatabaseRetainedArrayLayout
        private let hashLayout: DatabaseRetainedHashTableLayout
        private let reservation: DatabaseIntermediateReservation
        private var currentCapacity = 0
        private var nextCapacity = 0
        private var visitedCapacity = 0

        init(
            origin: GraphIdentity,
            workMeter: DatabaseWorkMeter
        ) throws {
            let arrayLayout = try DatabaseRetainedArrayLayout.forElement(
                GraphIdentity.self
            )
            let hashLayout = try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: UInt64(
                    MemoryLayout<Set<GraphIdentity>>.stride
                ),
                elementCapacitySlotByteCount: UInt64(
                    max(1, MemoryLayout<GraphIdentity>.stride)
                )
            )
            self.workMeter = workMeter
            self.arrayLayout = arrayLayout
            self.hashLayout = hashLayout
            self.reservation = try workMeter.reserveIntermediate(
                bytes: try DatabaseIntermediateFootprint(
                    bytes: arrayLayout.containerByteCount
                ).multiplied(by: 2).adding(
                    DatabaseIntermediateFootprint(
                        bytes: hashLayout.containerByteCount
                    )
                ).bytes,
                at: .indexScan
            )
            try admitOrigin(origin)
        }

        mutating func discover(_ identity: GraphIdentity) throws -> Bool {
            let identifier = try identity.requirePropertyGraphIdentifier()
            try DatabaseByteProcessingMeter.consume(
                byteCount: identifier.utf8.count,
                workMeter: workMeter,
                stage: .indexScan
            )
            guard !visited.contains(identity) else { return false }

            let arrayGrowth = try arrayLayout.growth(
                from: nextCapacity,
                toFit: nextLevel.count + 1
            )
            let hashGrowth = try hashLayout.growth(
                from: visitedCapacity,
                toFit: visited.count + 1
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: try DatabaseIntermediateFootprint(
                    bytes: UInt64(identifier.utf8.count) + 64
                ).adding(
                    DatabaseIntermediateFootprint(
                        bytes: arrayGrowth.additionalByteCount
                    )
                ).adding(
                    DatabaseIntermediateFootprint(
                        bytes: hashGrowth.additionalByteCount
                    )
                ).bytes,
                at: .indexScan
            )
            if arrayGrowth.capacity != nextCapacity {
                nextLevel.reserveCapacity(arrayGrowth.capacity)
                nextCapacity = arrayGrowth.capacity
            }
            if hashGrowth.capacity != visitedCapacity {
                visited.reserveCapacity(hashGrowth.capacity)
                visitedCapacity = hashGrowth.capacity
            }
            visited.insert(identity)
            nextLevel.append(identity)
            return true
        }

        mutating func advanceLevel() {
            currentLevel.removeAll(keepingCapacity: true)
            swap(&currentLevel, &nextLevel)
            swap(&currentCapacity, &nextCapacity)
        }

        private mutating func admitOrigin(
            _ origin: GraphIdentity
        ) throws {
            let identifier = try origin.requirePropertyGraphIdentifier()
            let arrayGrowth = try arrayLayout.growth(
                from: currentCapacity,
                toFit: 1
            )
            let hashGrowth = try hashLayout.growth(
                from: visitedCapacity,
                toFit: 1
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: try DatabaseIntermediateFootprint(
                    bytes: UInt64(identifier.utf8.count) + 64
                ).adding(
                    DatabaseIntermediateFootprint(
                        bytes: arrayGrowth.additionalByteCount
                    )
                ).adding(
                    DatabaseIntermediateFootprint(
                        bytes: hashGrowth.additionalByteCount
                    )
                ).bytes,
                at: .indexScan
            )
            currentLevel.reserveCapacity(arrayGrowth.capacity)
            currentCapacity = arrayGrowth.capacity
            visited.reserveCapacity(hashGrowth.capacity)
            visitedCapacity = hashGrowth.capacity
            currentLevel.append(origin)
            visited.insert(origin)
        }
    }
}
