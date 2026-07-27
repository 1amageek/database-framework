@_spi(DatabaseServer) import DatabaseWire
import DatabaseTypes
import DatabaseKit
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

    public func validateShapes(
        graph: String,
        quads: [RDFQuad],
        workBudget: SHACLValidationWorkBudget
    ) throws {
        try Task.checkCancellation()
        try workBudget.consume(UInt64(quads.count), at: .storageRow)
        _ = try decodeShapes(graph: graph, quads: quads)
    }

    public func validate(
        shapesGraph: String,
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        page: QueryExecuteOperation.Page,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> ValidationReport {
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
        return ValidationReport(
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
        budget: ExecutionBudget,
        transaction: any TransactionAccess
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
        quads: [RDFQuad]
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
        _ nodes: [RDFTerm]
    ) -> [RDFTerm] {
        nodes
    }

    private func canonicalIssues(
        _ results: [SHACLValidationResult],
        workBudget: SHACLValidationWorkBudget
    ) throws -> [ValidationReport.Issue] {
        try workBudget.consume(UInt64(results.count), at: .sortInput)
        var encoded: [(ByteString, ValidationReport.Issue)] = []
        encoded.reserveCapacity(results.count)
        for result in results {
            try workBudget.consume(at: .resultMaterialization)
            let issue = ValidationReport.Issue(
                severity: severity(result.resultSeverity),
                code: result.sourceConstraintComponent,
                messages: result.resultMessage,
                focusNode: result.focusNode,
                path: result.resultPath,
                value: result.value,
                sourceConstraintComponent: result.sourceConstraintComponent,
                sourceShape: result.sourceShape
            )
            encoded.append((
                try ServerPayloadEncoder.encode(issue, limits: wireLimits),
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
    ) -> ValidationReport.Severity {
        switch severity {
        case .info: return .information
        case .warning: return .warning
        case .violation: return .violation
        }
    }

    private func pageOffset(
        _ page: QueryExecuteOperation.Page,
        shapesGraph: String,
        fingerprint: ByteString
    ) throws -> Int {
        guard let bytes = page.continuation else { return 0 }
        let cursor: DatabaseSHACLPageCursor
        do {
            cursor = try ServerPayloadDecoder.decode(
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
        fingerprint: ByteString,
        offset: Int
    ) throws -> ByteString {
        guard let encodedOffset = UInt64(exactly: offset) else {
            throw DatabaseSHACLValidationError.invalidContinuation
        }
        return try ServerPayloadEncoder.encode(
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
        selectedFocusNodes: [RDFTerm]?,
        snapshotFingerprint: ByteString
    ) throws -> ByteString {
        let encoder = DatabaseWireEncoder(limits: wireLimits)
        let requestPayload = try encoder.encodeRequestPayload(
            DatabaseOperations.shaclExecute,
            request: SHACLExecuteOperation.Request(
                invocation: .validate(
                    shapesGraph: shapesGraph,
                    data: data,
                    focus: focus,
                    entailment: entailment
                )
            )
        )
        let selectedFocusPayload: ByteString?
        if let selectedFocusNodes {
            selectedFocusPayload = try encoder.encodeRequestPayload(
                DatabaseOperations.shaclExecute,
                request: SHACLExecuteOperation.Request(
                    invocation: .validate(
                        shapesGraph: shapesGraph,
                        data: data,
                        focus: .nodes(selectedFocusNodes),
                        entailment: entailment
                    )
                )
            )
        } else {
            selectedFocusPayload = nil
        }
        let encoded = try DatabaseWireWriter.encode(
            limits: wireLimits
        ) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            writer.writeUInt64(shapesRevision)
            try writer.writeBytes(requestPayload)
            writer.writeBool(selectedFocusPayload != nil)
            if let selectedFocusPayload {
                try writer.writeBytes(selectedFocusPayload)
            }
            try writer.writeBytes(snapshotFingerprint)
        }
        var hasher = SHA256Accumulator()
        hasher.update(encoded)
        return hasher.finalize()
    }

}
