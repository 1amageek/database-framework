import Core
import DatabaseEngine
import DatabaseValue
import Graph
import StorageKit

/// One canonical graph edge decoded from an index key.
public struct EdgeInfo: Sendable, Equatable {
    public let source: GraphIdentity
    public let target: GraphIdentity
    public let edgeLabel: GraphIdentity
    public let graph: GraphIdentity?

    public init(
        source: GraphIdentity,
        target: GraphIdentity,
        edgeLabel: GraphIdentity,
        graph: GraphIdentity? = nil
    ) {
        self.source = source
        self.target = target
        self.edgeLabel = edgeLabel
        self.graph = graph
    }
}

/// Reads property-graph and RDF indexes through one typed identity model.
///
/// Property-graph indexes use String tuple components. Canonical RDF quad
/// indexes use binary RDF term components. The scanner validates this boundary
/// and never converts either representation through `String(describing:)`.
public struct GraphEdgeScanner: Sendable {
    public enum Direction: Sendable {
        case outgoing
        case incoming
    }

    private struct ScanPlan {
        let ordering: GraphIndexOrdering
        let prefixElements: [any TupleElement]
    }

    private struct RDFScanPlan {
        let ordering: GraphIndexOrdering
        let prefix: RDFQuadIndexPrefixWritePlan
    }

    private let indexSubspace: Subspace
    private let strategy: GraphIndexStrategy
    private let scope: GraphScanScope
    private let identityPool: GraphIdentityPool
    private let rdfPhysicalCodec: RDFQuadIndexPhysicalCodec
    fileprivate let readBudget: GraphAlgorithmWorkBudget?

    public init(
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        scope: GraphScanScope = .all
    ) {
        self.indexSubspace = indexSubspace
        self.strategy = strategy
        self.scope = scope
        self.identityPool = GraphIdentityPool()
        self.rdfPhysicalCodec = RDFQuadIndexPhysicalCodec(
            baseSubspace: indexSubspace
        )
        self.readBudget = nil
    }

    package init(
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        scope: GraphScanScope,
        snapshot: GraphReadSnapshot
    ) {
        self.indexSubspace = indexSubspace
        self.strategy = strategy
        self.scope = scope
        self.identityPool = snapshot.identityPool
        self.rdfPhysicalCodec = RDFQuadIndexPhysicalCodec(
            baseSubspace: indexSubspace
        )
        self.readBudget = snapshot.workBudget
    }

    public func scanOutgoing(
        from source: GraphIdentity,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeSequence {
        scan(
            source: source,
            target: nil,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    public func scanIncoming(
        to target: GraphIdentity,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeSequence {
        scan(
            source: nil,
            target: target,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    public func scanAllEdges(
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeSequence {
        scan(
            source: nil,
            target: nil,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    public func batchScanOutgoing(
        from sources: [GraphIdentity],
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeBatchSequence {
        batchScan(
            identities: sources,
            direction: .outgoing,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    public func batchScanIncoming(
        to targets: [GraphIdentity],
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeBatchSequence {
        batchScan(
            identities: targets,
            direction: .incoming,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    public func getAllNodes(
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) async throws -> Set<GraphIdentity> {
        var nodes = Set<GraphIdentity>()
        for try await edge in scanAllEdges(
            edgeLabel: edgeLabel,
            transaction: transaction
        ) {
            nodes.insert(edge.source)
            nodes.insert(edge.target)
        }
        return nodes
    }

    public func getAllNodes(
        edgeLabel: GraphIdentity?,
        maxNodes: Int,
        transaction: any TransactionAccess
    ) async throws -> Set<GraphIdentity> {
        guard maxNodes > 0 else { return [] }
        var nodes = Set<GraphIdentity>()
        for try await edge in scanAllEdges(
            edgeLabel: edgeLabel,
            transaction: transaction
        ) {
            nodes.insert(edge.source)
            if nodes.count >= maxNodes { break }
            nodes.insert(edge.target)
            if nodes.count >= maxNodes { break }
        }
        return nodes
    }

    public func scanAllOutgoing(
        from source: GraphIdentity,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) async throws -> [EdgeInfo] {
        var edges: [EdgeInfo] = []
        for try await edge in scanOutgoing(
            from: source,
            edgeLabel: edgeLabel,
            transaction: transaction
        ) {
            edges.append(edge)
        }
        return edges
    }

    public func scanAllIncoming(
        to target: GraphIdentity,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) async throws -> [EdgeInfo] {
        var edges: [EdgeInfo] = []
        for try await edge in scanIncoming(
            to: target,
            edgeLabel: edgeLabel,
            transaction: transaction
        ) {
            edges.append(edge)
        }
        return edges
    }

    public func scanAllEdges(
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess,
        while shouldContinue: (EdgeInfo) throws -> Bool
    ) async throws {
        for try await edge in scanAllEdges(
            edgeLabel: edgeLabel,
            transaction: transaction
        ) {
            if try !shouldContinue(edge) { break }
        }
    }

    private func scan(
        source: GraphIdentity?,
        target: GraphIdentity?,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeSequence {
        GraphEdgeSequence(
            entries: scanEntries(
                source: source,
                target: target,
                edgeLabel: edgeLabel,
                transaction: transaction
            )
        )
    }

    package func scanEntries(
        source: GraphIdentity?,
        target: GraphIdentity?,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeEntrySequence {
        GraphEdgeEntrySequence(
            scanner: self,
            source: source,
            target: target,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    private func batchScan(
        identities: [GraphIdentity],
        direction: Direction,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) -> GraphEdgeBatchSequence {
        let identityPlan: GraphEdgeBatchIdentityPlan
        if requiresFullScanForBatch(edgeLabel: edgeLabel) {
            identityPlan = .fullScan(Set(identities))
        } else {
            var orderedIdentities = identities
            orderedIdentities.sort()
            if orderedIdentities.count > 1 {
                var writeIndex = 1
                for readIndex in 1..<orderedIdentities.count {
                    guard orderedIdentities[readIndex] != orderedIdentities[writeIndex - 1] else {
                        continue
                    }
                    if writeIndex != readIndex {
                        orderedIdentities[writeIndex] = orderedIdentities[readIndex]
                    }
                    writeIndex += 1
                }
                if writeIndex < orderedIdentities.count {
                    orderedIdentities.removeSubrange(writeIndex...)
                }
            }
            identityPlan = .pointScans(orderedIdentities)
        }
        return GraphEdgeBatchSequence(
            scanner: self,
            identityPlan: identityPlan,
            direction: direction,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    private func requiresFullScanForBatch(
        edgeLabel: GraphIdentity?
    ) -> Bool {
        switch strategy {
        case .adjacency:
            return false
        case .namedGraphStore:
            if case .all = scope { return true }
            return false
        case .tripleStore, .hexastore, .quadStore:
            return false
        }
    }

    private func makeScanPlan(
        source: GraphIdentity?,
        target: GraphIdentity?,
        edgeLabel: GraphIdentity?
    ) throws -> ScanPlan {
        guard strategy != .quadStore else {
            throw GraphIndexError.invalidScanState
        }
        try validateRepresentation(source)
        try validateRepresentation(target)
        try validateRepresentation(edgeLabel)

        let graphBinding = try physicalGraphBinding()
        let ordering = GraphIndexScanPlanner.ordering(
            strategy: strategy,
            subjectBound: source != nil,
            predicateBound: edgeLabel != nil,
            objectBound: target != nil,
            graphBound: graphBinding.isBound
        )
        let values = [source, edgeLabel, target]
        var prefixElements: [any TupleElement] = []

        if ordering.isGraphFirst {
            guard let graphElement = graphBinding.element else {
                return ScanPlan(ordering: ordering, prefixElements: [])
            }
            prefixElements.append(graphElement)
        }

        var allTripleComponentsBound = true
        for componentIndex in ordering.elementOrder {
            guard let identity = values[componentIndex] else {
                allTripleComponentsBound = false
                break
            }
            prefixElements.append(identity.tupleElement)
        }

        if !ordering.isGraphFirst,
           allTripleComponentsBound,
           let graphElement = graphBinding.element {
            prefixElements.append(graphElement)
        }

        return ScanPlan(
            ordering: ordering,
            prefixElements: prefixElements
        )
    }

    private func makeRDFScanPlan(
        source: GraphIdentity?,
        target: GraphIdentity?,
        edgeLabel: GraphIdentity?
    ) throws -> RDFScanPlan {
        try validateRepresentation(source)
        try validateRepresentation(target)
        try validateRepresentation(edgeLabel)

        let graphBinding = try rdfGraphBinding()
        let ordering = GraphIndexScanPlanner.ordering(
            strategy: .quadStore,
            subjectBound: source != nil,
            predicateBound: edgeLabel != nil,
            objectBound: target != nil,
            graphBound: graphBinding.isBound
        )
        var prefix = RDFQuadIndexPrefixWritePlan()
        if ordering.isGraphFirst,
           let graphComponent = graphBinding.component {
            try prefix.append(graphComponent)
        }

        switch ordering {
        case .spo, .gspo:
            try appendContiguousRDFIdentities(
                first: source,
                firstRole: .subject,
                second: edgeLabel,
                secondRole: .predicate,
                third: target,
                thirdRole: .object,
                to: &prefix
            )
        case .pos, .gpos:
            try appendContiguousRDFIdentities(
                first: edgeLabel,
                firstRole: .predicate,
                second: target,
                secondRole: .object,
                third: source,
                thirdRole: .subject,
                to: &prefix
            )
        case .osp, .gosp:
            try appendContiguousRDFIdentities(
                first: target,
                firstRole: .object,
                second: source,
                secondRole: .subject,
                third: edgeLabel,
                thirdRole: .predicate,
                to: &prefix
            )
        case .out, .in, .sop, .pso, .ops:
            throw GraphIndexError.rdfPhysicalIndex(
                .unsupportedOrdering(ordering)
            )
        }
        return RDFScanPlan(ordering: ordering, prefix: prefix)
    }

    private func appendContiguousRDFIdentities(
        first: GraphIdentity?,
        firstRole: GraphRDFComponentRole,
        second: GraphIdentity?,
        secondRole: GraphRDFComponentRole,
        third: GraphIdentity?,
        thirdRole: GraphRDFComponentRole,
        to prefix: inout RDFQuadIndexPrefixWritePlan
    ) throws {
        guard let first else { return }
        try prefix.append(try rdfComponent(first, role: firstRole))
        guard let second else { return }
        try prefix.append(try rdfComponent(second, role: secondRole))
        guard let third else { return }
        try prefix.append(try rdfComponent(third, role: thirdRole))
    }

    private func rdfComponent(
        _ identity: GraphIdentity,
        role: GraphRDFComponentRole
    ) throws -> RDFQuadIndexComponentWritePlan {
        guard let bytes = identity.canonicalRDFBytes else {
            throw GraphIndexError.identityRepresentationMismatch(
                expected: .rdf,
                actual: identity.representation
            )
        }
        do {
            return try RDFQuadIndexComponentWritePlan(
                canonicalBytes: bytes,
                role: role.databaseRole
            )
        } catch let error {
            if case .invalidRole = error {
                switch role {
                case .subject: throw GraphIndexError.invalidRDFSubject
                case .predicate: throw GraphIndexError.invalidRDFPredicate
                case .object: throw GraphIndexError.invalidRDFObject
                case .graph: throw GraphIndexError.invalidRDFGraphName
                }
            }
            throw GraphIndexError.invalidRDFEncoding(error)
        }
    }

    private func rdfGraphBinding(
    ) throws -> (isBound: Bool, component: RDFQuadIndexComponentWritePlan?) {
        switch scope {
        case .all:
            return (false, nil)
        case .defaultGraph:
            return (true, .defaultGraph)
        case .named(let graph):
            try validateRepresentation(graph)
            return (true, try rdfComponent(graph, role: .graph))
        }
    }

    private func physicalGraphBinding(
    ) throws -> (isBound: Bool, element: (any TupleElement)?) {
        switch scope {
        case .all:
            return (false, nil)
        case .defaultGraph:
            switch strategy {
            case .namedGraphStore:
                return (true, Bytes())
            case .adjacency, .tripleStore, .hexastore:
                return (true, nil)
            case .quadStore:
                throw GraphIndexError.invalidScanState
            }
        case .named(let graph):
            guard strategy != .quadStore else {
                throw GraphIndexError.invalidScanState
            }
            try validateRepresentation(graph)
            return (true, graph.tupleElement)
        }
    }

    private func validateRepresentation(
        _ identity: GraphIdentity?
    ) throws {
        guard let identity else { return }
        let expected = expectedRepresentation
        guard identity.representation == expected else {
            throw GraphIndexError.identityRepresentationMismatch(
                expected: expected,
                actual: identity.representation
            )
        }
    }

    private var expectedRepresentation: GraphIdentity.Representation {
        strategy == .quadStore ? .rdf : .propertyGraph
    }

    private func orderingSubspace(_ ordering: GraphIndexOrdering) -> Subspace {
        let key: Int64
        switch ordering {
        case .out: key = 0
        case .in: key = 1
        case .spo: key = 2
        case .pos: key = 3
        case .osp: key = 4
        case .sop: key = 5
        case .pso: key = 6
        case .ops: key = 7
        case .gspo: key = 8
        case .gpos: key = 9
        case .gosp: key = 10
        }
        return indexSubspace.subspace(key)
    }

    package func prepareScan(
        source: GraphIdentity?,
        target: GraphIdentity?,
        edgeLabel: GraphIdentity?
    ) throws -> GraphEdgePreparedScan {
        if strategy == .quadStore {
            let plan = try makeRDFScanPlan(
                source: source,
                target: target,
                edgeLabel: edgeLabel
            )
            do {
                let subspace = try rdfPhysicalCodec.subspace(
                    for: plan.ordering
                )
                let range = try rdfPhysicalCodec.range(
                    prefix: plan.prefix,
                    ordering: plan.ordering
                )
                return GraphEdgePreparedScan(
                    ordering: plan.ordering,
                    subspace: subspace,
                    begin: range.begin,
                    end: range.end
                )
            } catch let error {
                throw GraphIndexError.rdfPhysicalIndex(error)
            }
        }

        let plan = try makeScanPlan(
            source: source,
            target: target,
            edgeLabel: edgeLabel
        )
        let subspace = orderingSubspace(plan.ordering)
        let range = try scanRange(
            subspace: subspace,
            prefixElements: plan.prefixElements
        )
        return GraphEdgePreparedScan(
            ordering: plan.ordering,
            subspace: subspace,
            begin: range.begin,
            end: range.end
        )
    }

    package func containsGraph(_ graph: GraphIdentity?) -> Bool {
        scope.contains(graph)
    }

    private func scanRange(
        subspace: Subspace,
        prefixElements: [any TupleElement]
    ) throws -> (begin: Bytes, end: Bytes) {
        guard !prefixElements.isEmpty else { return subspace.range() }
        let prefix = Subspace(prefix: subspace.pack(Tuple(prefixElements)))
        return try prefix.prefixRange()
    }

    package func decodeEdge(
        _ key: Bytes,
        ordering: GraphIndexOrdering,
        subspace: Subspace
    ) throws -> EdgeInfo {
        if strategy == .quadStore {
            return try decodeRDFEdge(key, ordering: ordering)
        }

        var cursor: TupleCursor
        do {
            cursor = try subspace.tupleCursor(for: key)
        } catch {
            throw GraphIndexError.malformedIndexKey(
                ordering: ordering,
                elementCount: 0
            )
        }
        var elementCount = 0

        let leadingGraph: GraphIdentity?
        if ordering.isGraphFirst {
            leadingGraph = try decodeGraph(
                requiredElement(
                    from: &cursor,
                    count: &elementCount,
                    ordering: ordering
                )
            )
        } else {
            leadingGraph = nil
        }

        let first = try requiredElement(
            from: &cursor,
            count: &elementCount,
            ordering: ordering
        )
        let second = try requiredElement(
            from: &cursor,
            count: &elementCount,
            ordering: ordering
        )
        let third = try requiredElement(
            from: &cursor,
            count: &elementCount,
            ordering: ordering
        )

        let source: GraphIdentity
        let edgeLabel: GraphIdentity
        let target: GraphIdentity
        switch ordering {
        case .out, .spo, .gspo:
            source = try decodeIdentity(first)
            edgeLabel = try decodeIdentity(second)
            target = try decodeIdentity(third)
        case .in:
            edgeLabel = try decodeIdentity(second)
            target = try decodeIdentity(first)
            source = try decodeIdentity(third)
        case .pos, .gpos:
            edgeLabel = try decodeIdentity(first)
            target = try decodeIdentity(second)
            source = try decodeIdentity(third)
        case .osp, .gosp:
            target = try decodeIdentity(first)
            source = try decodeIdentity(second)
            edgeLabel = try decodeIdentity(third)
        case .sop:
            source = try decodeIdentity(first)
            target = try decodeIdentity(second)
            edgeLabel = try decodeIdentity(third)
        case .pso:
            edgeLabel = try decodeIdentity(first)
            source = try decodeIdentity(second)
            target = try decodeIdentity(third)
        case .ops:
            target = try decodeIdentity(first)
            edgeLabel = try decodeIdentity(second)
            source = try decodeIdentity(third)
        }

        let graph: GraphIdentity?
        if ordering.isGraphFirst {
            graph = leadingGraph
        } else if strategy == .quadStore {
            graph = try decodeGraph(
                requiredElement(
                    from: &cursor,
                    count: &elementCount,
                    ordering: ordering
                )
            )
        } else if let graphElement = try optionalElement(
            from: &cursor,
            count: &elementCount,
            ordering: ordering
        ) {
            graph = try decodeGraph(graphElement)
        } else {
            graph = nil
        }

        if try optionalElement(
            from: &cursor,
            count: &elementCount,
            ordering: ordering
        ) != nil {
            throw GraphIndexError.malformedIndexKey(
                ordering: ordering,
                elementCount: elementCount
            )
        }

        return EdgeInfo(
            source: source,
            target: target,
            edgeLabel: edgeLabel,
            graph: graph
        )
    }

    private func decodeRDFEdge(
        _ key: Bytes,
        ordering: GraphIndexOrdering
    ) throws -> EdgeInfo {
        let encoded: RDFQuadIndexEncodedQuad
        do {
            encoded = try rdfPhysicalCodec.decodeEncodedQuad(
                key: key,
                ordering: ordering
            )
        } catch let error {
            throw GraphIndexError.rdfPhysicalIndex(error)
        }

        let source = try identityPool.internRDF(
            encoded.subject,
            role: .subject
        )
        let edgeLabel = try identityPool.internRDF(
            encoded.predicate,
            role: .predicate
        )
        let target = try identityPool.internRDF(
            encoded.object,
            role: .object
        )
        let graph: GraphIdentity?
        if let encodedGraph = encoded.graph {
            graph = try identityPool.internRDF(
                encodedGraph,
                role: .graph
            )
        } else {
            graph = nil
        }
        return EdgeInfo(
            source: source,
            target: target,
            edgeLabel: edgeLabel,
            graph: graph
        )
    }

    private func requiredElement(
        from cursor: inout TupleCursor,
        count: inout Int,
        ordering: GraphIndexOrdering
    ) throws -> any TupleElement {
        guard let element = try optionalElement(
            from: &cursor,
            count: &count,
            ordering: ordering
        ) else {
            throw GraphIndexError.malformedIndexKey(
                ordering: ordering,
                elementCount: count
            )
        }
        return element
    }

    private func optionalElement(
        from cursor: inout TupleCursor,
        count: inout Int,
        ordering: GraphIndexOrdering
    ) throws -> (any TupleElement)? {
        do {
            guard let element = try cursor.next() else { return nil }
            count += 1
            return element
        } catch {
            throw GraphIndexError.malformedIndexKey(
                ordering: ordering,
                elementCount: count
            )
        }
    }

    private func decodeIdentity(
        _ element: any TupleElement
    ) throws -> GraphIdentity {
        guard let value = element as? String else {
            throw GraphIndexError.unexpectedElementType(
                expected: "String",
                actual: String(describing: type(of: element))
            )
        }
        return .identifier(value)
    }

    private func decodeGraph(
        _ element: any TupleElement
    ) throws -> GraphIdentity? {
        switch strategy {
        case .quadStore:
            throw GraphIndexError.invalidScanState
        case .namedGraphStore:
            if let bytes = element as? Bytes, bytes.isEmpty {
                return nil
            }
            guard let value = element as? String else {
                throw GraphIndexError.unexpectedElementType(
                    expected: "String or empty Bytes default-graph discriminator",
                    actual: String(describing: type(of: element))
                )
            }
            return .identifier(value)
        case .adjacency, .tripleStore, .hexastore:
            guard let value = element as? String else {
                throw GraphIndexError.unexpectedElementType(
                    expected: "String",
                    actual: String(describing: type(of: element))
                )
            }
            return .identifier(value)
        }
    }
}

public struct GraphEdgeSequence: AsyncSequence, Sendable {
    public typealias Element = EdgeInfo

    private let entries: GraphEdgeEntrySequence

    package init(entries: GraphEdgeEntrySequence) {
        self.entries = entries
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(iterator: entries.makeAsyncIterator())
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private var iterator: GraphEdgeEntrySequence.AsyncIterator

        package init(iterator: GraphEdgeEntrySequence.AsyncIterator) {
            self.iterator = iterator
        }

        public mutating func next() async throws -> EdgeInfo? {
            try await iterator.next()?.edge
        }
    }
}

/// Lazily scans multiple graph identities without materializing an edge batch.
///
/// At most one backend range cursor is active at a time. Edge key/value buffers
/// remain owned by the backend sequence and flow directly into GraphEdgeScanner.
fileprivate enum GraphEdgeBatchIdentityPlan: Sendable {
    case pointScans([GraphIdentity])
    case fullScan(Set<GraphIdentity>)

    var isEmpty: Bool {
        switch self {
        case .pointScans(let identities):
            identities.isEmpty
        case .fullScan(let identities):
            identities.isEmpty
        }
    }
}

public struct GraphEdgeBatchSequence: AsyncSequence, Sendable {
    public typealias Element = EdgeInfo

    private let scanner: GraphEdgeScanner
    private let identityPlan: GraphEdgeBatchIdentityPlan
    private let direction: GraphEdgeScanner.Direction
    private let edgeLabel: GraphIdentity?
    private let transaction: any TransactionAccess

    fileprivate init(
        scanner: GraphEdgeScanner,
        identityPlan: GraphEdgeBatchIdentityPlan,
        direction: GraphEdgeScanner.Direction,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) {
        self.scanner = scanner
        self.identityPlan = identityPlan
        self.direction = direction
        self.edgeLabel = edgeLabel
        self.transaction = transaction
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            scanner: scanner,
            identityPlan: identityPlan,
            direction: direction,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private let scanner: GraphEdgeScanner
        private let identityPlan: GraphEdgeBatchIdentityPlan
        private let direction: GraphEdgeScanner.Direction
        private let edgeLabel: GraphIdentity?
        private let transaction: any TransactionAccess
        private var identityIndex = 0
        private var activeIterator: GraphEdgeSequence.AsyncIterator?
        private var isFinished = false

        fileprivate init(
            scanner: GraphEdgeScanner,
            identityPlan: GraphEdgeBatchIdentityPlan,
            direction: GraphEdgeScanner.Direction,
            edgeLabel: GraphIdentity?,
            transaction: any TransactionAccess
        ) {
            self.scanner = scanner
            self.identityPlan = identityPlan
            self.direction = direction
            self.edgeLabel = edgeLabel
            self.transaction = transaction
        }

        public mutating func next() async throws -> EdgeInfo? {
            guard !isFinished, !identityPlan.isEmpty else {
                isFinished = true
                return nil
            }
            try Task.checkCancellation()

            switch identityPlan {
            case .fullScan(let identitySet):
                if activeIterator == nil {
                    activeIterator = scanner.scanAllEdges(
                        edgeLabel: edgeLabel,
                        transaction: transaction
                    ).makeAsyncIterator()
                }
                while var iterator = activeIterator {
                    activeIterator = nil
                    guard let edge = try await iterator.next() else {
                        isFinished = true
                        return nil
                    }
                    activeIterator = iterator
                    let identity = direction == .outgoing
                        ? edge.source
                        : edge.target
                    if identitySet.contains(identity) {
                        return edge
                    }
                }
                isFinished = true
                return nil

            case .pointScans(let identities):
                while identityIndex < identities.count {
                    if activeIterator == nil {
                        let identity = identities[identityIndex]
                        let sequence = direction == .outgoing
                            ? scanner.scanOutgoing(
                                from: identity,
                                edgeLabel: edgeLabel,
                                transaction: transaction
                            )
                            : scanner.scanIncoming(
                                to: identity,
                                edgeLabel: edgeLabel,
                                transaction: transaction
                            )
                        activeIterator = sequence.makeAsyncIterator()
                    }
                    guard var iterator = activeIterator else {
                        isFinished = true
                        return nil
                    }
                    activeIterator = nil
                    if let edge = try await iterator.next() {
                        activeIterator = iterator
                        return edge
                    }
                    identityIndex += 1
                }
                isFinished = true
                return nil
            }
        }
    }
}

package struct GraphEdgeEntry: Sendable {
    package let edge: EdgeInfo
    package let value: Bytes
}

package struct GraphEdgePreparedScan: Sendable {
    package let ordering: GraphIndexOrdering
    package let subspace: Subspace
    package let begin: Bytes
    package let end: Bytes
}

package struct GraphEdgeEntrySequence: AsyncSequence, Sendable {
    package typealias Element = GraphEdgeEntry

    private let scanner: GraphEdgeScanner
    private let source: GraphIdentity?
    private let target: GraphIdentity?
    private let edgeLabel: GraphIdentity?
    private let transaction: any TransactionAccess

    package init(
        scanner: GraphEdgeScanner,
        source: GraphIdentity?,
        target: GraphIdentity?,
        edgeLabel: GraphIdentity?,
        transaction: any TransactionAccess
    ) {
        self.scanner = scanner
        self.source = source
        self.target = target
        self.edgeLabel = edgeLabel
        self.transaction = transaction
    }

    package func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            scanner: scanner,
            source: source,
            target: target,
            edgeLabel: edgeLabel,
            transaction: transaction
        )
    }

    package struct AsyncIterator: AsyncIteratorProtocol {
        private let scanner: GraphEdgeScanner
        private let source: GraphIdentity?
        private let target: GraphIdentity?
        private let edgeLabel: GraphIdentity?
        private let transaction: any TransactionAccess
        private var preparedScan: GraphEdgePreparedScan?
        private var cursor: KeyValueCursor?
        private var reservation: GraphPhysicalReadReservation?
        private var isFinished = false

        fileprivate init(
            scanner: GraphEdgeScanner,
            source: GraphIdentity?,
            target: GraphIdentity?,
            edgeLabel: GraphIdentity?,
            transaction: any TransactionAccess
        ) {
            self.scanner = scanner
            self.source = source
            self.target = target
            self.edgeLabel = edgeLabel
            self.transaction = transaction
        }

        package mutating func next() async throws -> GraphEdgeEntry? {
            guard !isFinished else { return nil }
            try Task.checkCancellation()
            if cursor == nil {
                let prepared = try scanner.prepareScan(
                    source: source,
                    target: target,
                    edgeLabel: edgeLabel
                )
                preparedScan = prepared
                let reservation = try scanner.readBudget?.reservePhysicalReads()
                if scanner.readBudget != nil, reservation == nil {
                    isFinished = true
                    return nil
                }
                self.reservation = reservation
                cursor = transaction.rangeCursor(
                    from: .firstGreaterOrEqual(prepared.begin),
                    to: .firstGreaterOrEqual(prepared.end),
                    limit: reservation?.limit ?? 0,
                    snapshot: true,
                    streamingMode: .iterator
                )
            }

            guard let preparedScan else {
                throw GraphIndexError.invalidScanState
            }
            while var activeCursor = cursor {
                cursor = nil
                guard let (key, value) = try await activeCursor.next() else {
                    reservation?.finishAtRangeEnd()
                    reservation = nil
                    isFinished = true
                    return nil
                }
                cursor = activeCursor
                try reservation?.recordPhysicalRead()
                try Task.checkCancellation()
                let edge = try scanner.decodeEdge(
                    key,
                    ordering: preparedScan.ordering,
                    subspace: preparedScan.subspace
                )
                guard source == nil || edge.source == source,
                      target == nil || edge.target == target,
                      edgeLabel == nil || edge.edgeLabel == edgeLabel,
                      scanner.containsGraph(edge.graph) else {
                    continue
                }
                return GraphEdgeEntry(edge: edge, value: value)
            }
            isFinished = true
            return nil
        }
    }
}
