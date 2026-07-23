import DatabaseDigest
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import DatabaseEngine
import StorageKit

public struct DatabaseSHACLValidationProcessor: DatabaseSHACLProcessor {
    private let documentStore: DatabaseRDFDocumentStore
    private let dataSourceResolver: any DatabaseSHACLDataSourceResolver
    private let wireLimits: DatabaseWireLimits

    public init(
        documentStore: DatabaseRDFDocumentStore,
        dataSourceResolver: any DatabaseSHACLDataSourceResolver,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.documentStore = documentStore
        self.dataSourceResolver = dataSourceResolver
        self.wireLimits = wireLimits
    }

    public func replace(
        graph: String,
        quads: [DatabaseRDFQuad],
        workBudget: SHACLValidationWorkBudget,
        transaction: any Transaction
    ) async throws {
        try Task.checkCancellation()
        try workBudget.consume(UInt64(quads.count), at: .storageRow)
        _ = try decodeShapes(graph: graph, quads: quads)
    }

    public func delete(
        graph: String,
        workBudget: SHACLValidationWorkBudget,
        transaction: any Transaction
    ) async throws {
        try workBudget.consume(at: .storageRow)
        _ = graph
        _ = transaction
    }

    public func validate(
        shapesGraph: String,
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        page: QueryExecuteOperation.Page,
        workBudget: SHACLValidationWorkBudget,
        transaction: any Transaction
    ) async throws -> DatabaseValidationReport {
        let budget = workBudget.workMeter.budget
        let stored = try await loadShapes(
            graph: shapesGraph,
            budget: budget,
            transaction: transaction
        )
        try workBudget.consume(UInt64(stored.quads.count))
        let shapesGraphModel = try decodeShapes(
            graph: shapesGraph,
            quads: stored.quads
        )
        let resolved = try await dataSourceResolver.resolve(
            data: data,
            focus: focus,
            entailment: entailment,
            workBudget: workBudget,
            transaction: transaction
        )
        try validate(
            resolved: resolved,
            expectedData: data,
            expectedFocus: focus,
            expectedEntailment: entailment
        )
        let effectiveFocus = resolved.selectedFocusNodes.map {
            Array(Set(focusTerms($0))).sorted()
        }
        try workBudget.consume(UInt64(effectiveFocus?.count ?? 0))
        let fingerprint = try validationFingerprint(
            shapesGraph: shapesGraph,
            shapesRevision: stored.revision,
            data: data,
            focus: focus,
            entailment: entailment,
            selectedFocusNodes: resolved.selectedFocusNodes,
            snapshotFingerprint: resolved.snapshotFingerprint
        )
        let offset = try pageOffset(
            page,
            shapesGraph: shapesGraph,
            fingerprint: fingerprint
        )
        let targetResolver = SHACLTargetResolver(
            executor: resolved.executor,
            transaction: transaction,
            graphScope: resolved.graphScope,
            budget: workBudget
        )
        let evaluator = SHACLConstraintEvaluator(
            executor: resolved.executor,
            transaction: transaction,
            graphScope: resolved.graphScope,
            reasoner: resolved.reasoner,
            budget: workBudget
        )
        let validator = SHACLValidator(
            shapesGraph: shapesGraphModel,
            targetResolver: targetResolver,
            constraintEvaluator: evaluator,
            budget: workBudget
        )
        let validationReport: SHACLValidationReport
        if let effectiveFocus {
            validationReport = try await validator.validate(
                focusNodes: effectiveFocus
            )
        } else {
            validationReport = try await validator.validate()
        }
        let allIssues = try canonicalIssues(
            validationReport.results,
            workBudget: workBudget
        )
        let lower = min(offset, allIssues.count)
        let upper = min(lower + Int(page.limit), allIssues.count)
        let nextOffset = upper < allIssues.count ? upper : nil
        let issues = Array(allIssues[lower..<upper])
        return DatabaseValidationReport(
            conforms: validationReport.conforms,
            issues: issues,
            continuation: try nextOffset.map {
                try continuation(
                    shapesGraph: shapesGraph,
                    fingerprint: fingerprint,
                    offset: $0
                )
            }
        )
    }

    private func loadShapes(
        graph: String,
        budget: DatabaseExecutionBudget,
        transaction: any Transaction
    ) async throws -> DatabaseRDFStoredDocumentPage {
        guard budget.maximumWorkUnits > 0,
              let limit = Int(exactly: min(
                budget.maximumWorkUnits,
                UInt64(Int.max)
              )) else {
            throw DatabaseSHACLValidationError.workLimitExceeded(
                requested: 1,
                maximum: budget.maximumWorkUnits
            )
        }
        guard let stored = try await documentStore.page(
            identifier: graph,
            offset: 0,
            limit: limit,
            transaction: transaction
        ) else {
            throw DatabaseSHACLValidationError.shapesGraphNotFound(graph)
        }
        guard stored.nextOffset == nil else {
            throw DatabaseSHACLValidationError.workLimitExceeded(
                requested: stored.totalQuadCount,
                maximum: budget.maximumWorkUnits
            )
        }
        return stored
    }

    private func decodeShapes(
        graph: String,
        quads: [DatabaseRDFQuad]
    ) throws -> SHACLShapesGraph {
        do {
            let dataset = RDFDataset(databaseQuads: quads)
            return try SHACLRDFDecoder().decode(
                from: dataset,
                graphIRI: graph
            )
        } catch {
            throw DatabaseSHACLValidationError.invalidShapesGraph(
                String(describing: error)
            )
        }
    }

    private func validate(
        resolved: DatabaseSHACLResolvedDataSource,
        expectedData: SHACLExecuteOperation.DataSource,
        expectedFocus: SHACLExecuteOperation.Focus,
        expectedEntailment: SHACLExecuteOperation.Entailment
    ) throws {
        guard resolved.data == expectedData,
              resolved.focus == expectedFocus else {
            throw DatabaseSHACLValidationError.resolvedScopeMismatch
        }
        guard resolved.entailment == expectedEntailment else {
            throw DatabaseSHACLValidationError.resolvedEntailmentMismatch
        }
        let expectedGraphScope: SHACLDataGraphScope
        switch expectedData.graph {
        case .defaultGraph:
            expectedGraphScope = .defaultGraph
        case .named(let graph):
            expectedGraphScope = .named(try RDFGraphName(graph))
        }
        guard resolved.graphScope == expectedGraphScope else {
            throw DatabaseSHACLValidationError
                .resolvedGraphScopeMismatch
        }
        guard !resolved.snapshotFingerprint.isEmpty,
              resolved.snapshotFingerprint.count <=
                wireLimits.maximumByteStringBytes else {
            throw DatabaseSHACLValidationError.invalidSnapshotFingerprint
        }
        if case .owl(let ontology) = expectedEntailment,
           resolved.reasoner == nil {
            throw DatabaseSHACLValidationError.missingOWLReasoner(
                ontology
            )
        }
    }

    private func focusTerms(
        _ nodes: [DatabaseRDFTerm]
    ) -> [RDFTerm] {
        nodes
    }

    private func canonicalIssues(
        _ results: [SHACLValidationResult],
        workBudget: SHACLValidationWorkBudget
    ) throws -> [DatabaseValidationReport.Issue] {
        try workBudget.consume(UInt64(results.count), at: .sortInput)
        var encoded: [(DatabaseBytes, DatabaseValidationReport.Issue)] = []
        encoded.reserveCapacity(results.count)
        for result in results {
            try workBudget.consume(at: .resultMaterialization)
            let issue = DatabaseValidationReport.Issue(
                severity: severity(result.resultSeverity),
                code: result.sourceConstraintComponent,
                messages: result.resultMessage,
                focusNode: result.focusNode,
                path: result.resultPath.map(wirePath),
                value: result.value,
                sourceConstraintComponent: result.sourceConstraintComponent,
                sourceShape: result.sourceShape
            )
            encoded.append((
                try DatabaseEnvelopeCodec.encode(issue, limits: wireLimits),
                issue
            ))
        }
        try encoded.sort { left, right in
            try workBudget.consume(2, at: .sortComparison)
            return left.0.lexicographicallyPrecedes(right.0)
        }
        return encoded.map(\.1)
    }

    private func severity(
        _ severity: SHACLSeverity
    ) -> DatabaseValidationReport.Severity {
        switch severity {
        case .info: return .information
        case .warning: return .warning
        case .violation: return .violation
        }
    }

    private func wirePath(_ path: SHACLPath) -> DatabaseSHACLPath {
        switch path {
        case .predicate(let iri): return .predicate(iri)
        case .inverse(let inner): return .inverse(wirePath(inner))
        case .sequence(let values): return .sequence(values.map(wirePath))
        case .alternative(let values):
            return .alternative(values.map(wirePath))
        case .zeroOrMore(let inner): return .zeroOrMore(wirePath(inner))
        case .oneOrMore(let inner): return .oneOrMore(wirePath(inner))
        case .zeroOrOne(let inner): return .zeroOrOne(wirePath(inner))
        }
    }

    private func pageOffset(
        _ page: QueryExecuteOperation.Page,
        shapesGraph: String,
        fingerprint: DatabaseBytes
    ) throws -> Int {
        guard let bytes = page.continuation else { return 0 }
        let cursor: DatabaseSHACLPageCursor
        do {
            cursor = try DatabaseEnvelopeCodec.decode(
                DatabaseSHACLPageCursor.self,
                from: bytes,
                limits: wireLimits
            )
        } catch {
            throw DatabaseSHACLValidationError.invalidContinuation
        }
        guard cursor.shapesGraph == shapesGraph,
              cursor.validationFingerprint == fingerprint,
              let offset = Int(exactly: cursor.offset) else {
            throw DatabaseSHACLValidationError.invalidContinuation
        }
        return offset
    }

    private func continuation(
        shapesGraph: String,
        fingerprint: DatabaseBytes,
        offset: Int
    ) throws -> DatabaseBytes {
        guard let encodedOffset = UInt64(exactly: offset) else {
            throw DatabaseSHACLValidationError.invalidContinuation
        }
        return try DatabaseEnvelopeCodec.encode(
            DatabaseSHACLPageCursor(
                shapesGraph: shapesGraph,
                validationFingerprint: fingerprint,
                offset: encodedOffset
            ),
            limits: wireLimits
        )
    }

    private func validationFingerprint(
        shapesGraph: String,
        shapesRevision: UInt64,
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        selectedFocusNodes: [DatabaseRDFTerm]?,
        snapshotFingerprint: DatabaseBytes
    ) throws -> DatabaseBytes {
        let encoded = try DatabaseWireWriter.encode(
            limits: wireLimits
        ) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try writer.writeString(shapesGraph)
            writer.writeUInt64(shapesRevision)
            try encode(data, into: &writer)
            try encode(focus, into: &writer)
            try encode(entailment, into: &writer)
            writer.writeBool(selectedFocusNodes != nil)
            if let selectedFocusNodes {
                try encodeTerms(selectedFocusNodes, into: &writer)
            }
            try writer.writeBytes(snapshotFingerprint)
        }
        var hasher = SHA256Accumulator()
        hasher.update(encoded)
        return hasher.finalize()
    }

    private func encode(
        _ data: SHACLExecuteOperation.DataSource,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try writer.writeString(data.entity)
        try writer.writeString(data.index)
        try writer.writeCount(data.partitions.count)
        for partition in data.partitions {
            try partition.encode(into: &writer)
        }
        switch data.graph {
        case .defaultGraph:
            writer.writeUInt8(1)
        case .named(let graph):
            writer.writeUInt8(2)
            try graph.encode(into: &writer)
        }
    }

    private func encode(
        _ focus: SHACLExecuteOperation.Focus,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        switch focus {
        case .targets:
            writer.writeUInt8(1)
        case .nodes(let nodes):
            writer.writeUInt8(2)
            try encodeTerms(nodes, into: &writer)
        case .records(let identities):
            writer.writeUInt8(3)
            try writer.writeCount(identities.count)
            for identity in identities {
                try identity.encode(into: &writer)
            }
        }
    }

    private func encode(
        _ entailment: SHACLExecuteOperation.Entailment,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        switch entailment {
        case .none:
            writer.writeUInt8(1)
        case .rdfs:
            writer.writeUInt8(2)
        case .owl(let ontology):
            writer.writeUInt8(3)
            try writer.writeString(ontology)
        }
    }

    private func encodeTerms(
        _ terms: [DatabaseRDFTerm],
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        var encoded: [DatabaseBytes] = []
        encoded.reserveCapacity(terms.count)
        for term in terms {
            encoded.append(
                try DatabaseWireWriter.encode(limits: wireLimits) {
                    (termWriter: inout DatabaseWireWriter) throws(DatabaseWireError) in
                    try term.encode(into: &termWriter)
                }
            )
        }
        let canonical = Array(Set(encoded)).sorted {
            $0.lexicographicallyPrecedes($1)
        }
        try writer.writeCount(canonical.count)
        for term in canonical { try writer.writeBytes(term) }
    }

}
