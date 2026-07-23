import DatabaseDigest
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import OntologyIndex
import StorageKit

public struct DatabaseOntologyReasoningProcessor: DatabaseOntologyProcessor {
    private let documentStore: DatabaseRDFDocumentStore
    private let ontologyStore: OntologyStore
    private let wireLimits: DatabaseWireLimits

    public init(
        documentStore: DatabaseRDFDocumentStore,
        ontologyStore: OntologyStore,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.documentStore = documentStore
        self.ontologyStore = ontologyStore
        self.wireLimits = wireLimits
    }

    public func replace(
        _ document: OntologyExecuteOperation.Document,
        budget: DatabaseExecutionBudget,
        transaction: any Transaction
    ) async throws {
        var work = WorkBudget(maximum: budget.maximumWorkUnits)
        _ = try sourceOntology(
            identifier: document.ontology,
            imports: document.imports,
            quads: document.axioms
        )
        try await validateImportGraph(
            root: document.ontology,
            transaction: transaction,
            work: &work
        )
        try await rebuild(
            ontology: document.ontology,
            transaction: transaction,
            work: &work
        )

        let identifiers = try await ontologyStore.listOntologies(
            transaction: transaction
        ).sorted()
        for identifier in identifiers where identifier != document.ontology {
            try Task.checkCancellation()
            if try await depends(
                ontology: identifier,
                on: document.ontology,
                transaction: transaction,
                visited: []
            ) {
                try await rebuild(
                    ontology: identifier,
                    transaction: transaction,
                    work: &work
                )
            }
        }
    }

    public func delete(
        ontology: String,
        budget: DatabaseExecutionBudget,
        transaction: any Transaction
    ) async throws {
        var work = WorkBudget(maximum: budget.maximumWorkUnits)
        let identifiers = try await ontologyStore.listOntologies(
            transaction: transaction
        ).sorted()
        for identifier in identifiers where identifier != ontology {
            try Task.checkCancellation()
            try work.consume()
            guard let metadata = try await ontologyStore.getMetadata(
                ontologyIRI: identifier,
                transaction: transaction
            ) else {
                continue
            }
            if metadata.imports.contains(ontology) {
                throw DatabaseOntologyProcessingError.ontologyInUse(
                    ontology: ontology,
                    dependent: identifier
                )
            }
        }
        try ontologyStore.deleteOntology(
            ontology,
            transaction: transaction
        )
    }

    public func reason(
        ontology: String,
        profile: OntologyExecuteOperation.ReasoningProfile,
        page: QueryExecuteOperation.Page,
        budget: DatabaseExecutionBudget,
        transaction: any Transaction
    ) async throws -> OntologyExecuteOperation.InferencePage {
        var work = WorkBudget(maximum: budget.maximumWorkUnits)
        let entailmentClosure = try await loadEntailmentClosure(
            root: ontology,
            transaction: transaction,
            work: &work
        )
        let kind = DatabaseOntologyPageCursor.Kind.reason(profile)
        let offset = try pageOffset(
            page,
            ontology: ontology,
            fingerprint: entailmentClosure.fingerprint,
            kind: kind
        )
        let materializer = OWL2RLMaterializer(ontologyStore: ontologyStore)
        var explicit = Set<ReasoningTriple>()
        for document in entailmentClosure.documents {
            for quad in document.quads {
                try work.consume()
                explicit.insert(try reasoningTriple(from: quad))
            }
        }

        var known = explicit
        var queue = explicit.sorted()
        var cursor = 0
        while cursor < queue.count {
            try Task.checkCancellation()
            try work.consume()
            let triple = queue[cursor]
            cursor += 1
            let result: InferenceResult
            do {
                result = try await materializer.materializeOnWrite(
                    triple: triple,
                    ontologyIRI: ontology,
                    transaction: transaction
                )
            } catch let error as OWL2RLMaterializationError {
                throw DatabaseOntologyProcessingError.materialization(error)
            }
            for inferred in result.inferred where Self.includes(
                inferred.provenance.rule,
                profile: profile
            ) {
                try work.consume()
                if known.insert(inferred.triple).inserted {
                    queue.append(inferred.triple)
                }
            }
        }

        let inferred = known.subtracting(explicit).sorted()
        let lower = min(offset, inferred.count)
        let upper = min(lower + Int(page.limit), inferred.count)
        let values = try inferred[lower..<upper].map { triple in
            try DatabaseRDFQuad(
                subject: triple.subject,
                predicate: triple.predicate.term,
                object: triple.object,
                graph: .iri(ontology)
            )
        }
        let next = upper < inferred.count ? upper : nil
        return OntologyExecuteOperation.InferencePage(
            inferredAxioms: Array(values),
            isComplete: next == nil,
            continuation: try next.map {
                try continuation(
                    ontology: ontology,
                    fingerprint: entailmentClosure.fingerprint,
                    offset: $0,
                    kind: kind
                )
            }
        )
    }

    public func hierarchy(
        ontology: String,
        resource: String,
        resourceKind: OntologyExecuteOperation.HierarchyResourceKind,
        direction: OntologyExecuteOperation.HierarchyDirection,
        maximumDepth: UInt32,
        page: QueryExecuteOperation.Page,
        budget: DatabaseExecutionBudget,
        transaction: any Transaction
    ) async throws -> OntologyExecuteOperation.HierarchyPage {
        var work = WorkBudget(maximum: budget.maximumWorkUnits)
        let entailmentClosure = try await loadEntailmentClosure(
            root: ontology,
            transaction: transaction,
            work: &work
        )
        let kind = DatabaseOntologyPageCursor.Kind.hierarchy(
            resource: resource,
            resourceKind: resourceKind,
            direction: direction,
            maximumDepth: maximumDepth
        )
        let offset = try pageOffset(
            page,
            ontology: ontology,
            fingerprint: entailmentClosure.fingerprint,
            kind: kind
        )
        guard let ontologyModel = try await ontologyStore.reconstruct(
            iri: ontology,
            transaction: transaction
        ) else {
            throw DatabaseOntologyProcessingError.ontologyNotFound(
                ontology
            )
        }
        try validate(
            resource: resource,
            kind: resourceKind,
            ontology: ontologyModel
        )
        let reasoner = OWLReasoner(
            ontology: ontologyModel,
            configuration: reasonerConfiguration(budget)
        )
        var entries: [OntologyExecuteOperation.HierarchyEntry] = []
        var visited: Set<String> = [resource]
        var queue: [(resource: String, depth: UInt32)] = [(resource, 0)]
        var queueIndex = 0
        while queueIndex < queue.count {
            try Task.checkCancellation()
            let current = queue[queueIndex]
            queueIndex += 1
            guard current.depth < maximumDepth else { continue }
            let nextResources = hierarchyNeighbors(
                of: current.resource,
                kind: resourceKind,
                direction: direction,
                reasoner: reasoner
            ).sorted()
            for nextResource in nextResources {
                try work.consume()
                guard visited.insert(nextResource).inserted else { continue }
                let depth = current.depth + 1
                entries.append(
                    OntologyExecuteOperation.HierarchyEntry(
                        resource: nextResource,
                        depth: depth
                    )
                )
                queue.append((nextResource, depth))
            }
        }
        entries.sort {
            $0.depth == $1.depth
                ? $0.resource < $1.resource
                : $0.depth < $1.depth
        }
        let lower = min(offset, entries.count)
        let upper = min(lower + Int(page.limit), entries.count)
        let next = upper < entries.count ? upper : nil
        return OntologyExecuteOperation.HierarchyPage(
            entries: Array(entries[lower..<upper]),
            continuation: try next.map {
                try continuation(
                    ontology: ontology,
                    fingerprint: entailmentClosure.fingerprint,
                    offset: $0,
                    kind: kind
                )
            }
        )
    }

    public func validateSchema(
        ontology: String,
        page: QueryExecuteOperation.Page,
        budget: DatabaseExecutionBudget,
        transaction: any Transaction
    ) async throws -> DatabaseValidationReport {
        var work = WorkBudget(maximum: budget.maximumWorkUnits)
        let entailmentClosure = try await loadEntailmentClosure(
            root: ontology,
            transaction: transaction,
            work: &work
        )
        let kind = DatabaseOntologyPageCursor.Kind.validation
        let offset = try pageOffset(
            page,
            ontology: ontology,
            fingerprint: entailmentClosure.fingerprint,
            kind: kind
        )
        guard let ontologyModel = try await ontologyStore.reconstruct(
            iri: ontology,
            transaction: transaction
        ) else {
            throw DatabaseOntologyProcessingError.ontologyNotFound(
                ontology
            )
        }
        try work.consume(UInt64(ontologyModel.axioms.count))
        let reasoner = OWLReasoner(
            ontology: ontologyModel,
            configuration: reasonerConfiguration(budget)
        )
        var issues = reasoner.validateStructure().map { error in
            DatabaseValidationReport.Issue(
                severity: .violation,
                code: Self.validationCode(error),
                messages: [String(describing: error)]
            )
        }
        let regularity = reasoner.validateOWLDL()
        issues.append(contentsOf: regularity.violations.map { violation in
            DatabaseValidationReport.Issue(
                severity: .violation,
                code: "OWL_DL_REGULARITY",
                messages: [violation.description]
            )
        })
        let consistency = reasoner.isConsistent()
        if !consistency.value {
            issues.append(
                DatabaseValidationReport.Issue(
                    severity: .violation,
                    code: "OWL_INCONSISTENT",
                    messages: consistency.explanation.isEmpty
                        ? ["The ontology is inconsistent"]
                        : consistency.explanation
                )
            )
        }
        issues.sort {
            let leftMessage = $0.messages.first ?? ""
            let rightMessage = $1.messages.first ?? ""
            return $0.code == $1.code
                ? leftMessage < rightMessage
                : $0.code < $1.code
        }
        let lower = min(offset, issues.count)
        let upper = min(lower + Int(page.limit), issues.count)
        let next = upper < issues.count ? upper : nil
        return DatabaseValidationReport(
            conforms: issues.isEmpty,
            issues: Array(issues[lower..<upper]),
            continuation: try next.map {
                try continuation(
                    ontology: ontology,
                    fingerprint: entailmentClosure.fingerprint,
                    offset: $0,
                    kind: kind
                )
            }
        )
    }

    private func rebuild(
        ontology: String,
        transaction: any Transaction,
        work: inout WorkBudget
    ) async throws {
        let entailmentClosure = try await loadEntailmentClosure(
            root: ontology,
            transaction: transaction,
            work: &work
        )
        let merged = try mergedOntology(from: entailmentClosure.documents, root: ontology)
        try await ontologyStore.loadOntology(
            merged,
            transaction: transaction
        )
    }

    private func validateImportGraph(
        root: String,
        transaction: any Transaction,
        work: inout WorkBudget
    ) async throws {
        var visited = Set<String>()
        try await visitImport(
            root,
            path: [],
            visited: &visited,
            transaction: transaction,
            work: &work
        )
    }

    private func visitImport(
        _ ontology: String,
        path: [String],
        visited: inout Set<String>,
        transaction: any Transaction,
        work: inout WorkBudget
    ) async throws {
        if let cycleStart = path.firstIndex(of: ontology) {
            throw DatabaseOntologyProcessingError.importCycle(
                Array(path[cycleStart...]) + [ontology]
            )
        }
        guard visited.insert(ontology).inserted else { return }
        try Task.checkCancellation()
        try work.consume()
        let document = try await loadDocument(
            ontology,
            root: path.isEmpty ? ontology : path[0],
            transaction: transaction,
            maximumQuadCount: work.remaining
        )
        for imported in document.imports.sorted() {
            try await visitImport(
                imported,
                path: path + [ontology],
                visited: &visited,
                transaction: transaction,
                work: &work
            )
        }
    }

    private func depends(
        ontology: String,
        on dependency: String,
        transaction: any Transaction,
        visited: Set<String>
    ) async throws -> Bool {
        guard ontology != dependency else { return true }
        guard !visited.contains(ontology) else { return false }
        guard let metadata = try await ontologyStore.getMetadata(
            ontologyIRI: ontology,
            transaction: transaction
        ) else {
            return false
        }
        let nextVisited = visited.union([ontology])
        for imported in metadata.imports {
            if try await depends(
                ontology: imported,
                on: dependency,
                transaction: transaction,
                visited: nextVisited
            ) {
                return true
            }
        }
        return false
    }

    private func loadEntailmentClosure(
        root: String,
        transaction: any Transaction,
        work: inout WorkBudget
    ) async throws -> EntailmentClosureSnapshot {
        var documents: [DocumentSnapshot] = []
        var visited = Set<String>()
        var queue = [root]
        var index = 0
        while index < queue.count {
            try Task.checkCancellation()
            let identifier = queue[index]
            index += 1
            guard visited.insert(identifier).inserted else { continue }
            try work.consume()
            let document = try await loadDocument(
                identifier,
                root: root,
                transaction: transaction,
                maximumQuadCount: work.remaining
            )
            try work.consume(
                UInt64(document.quads.count + document.imports.count)
            )
            documents.append(document)
            queue.append(contentsOf: document.imports.sorted())
        }
        documents.sort { $0.identifier < $1.identifier }
        return EntailmentClosureSnapshot(
            documents: documents,
            fingerprint: try dependencyFingerprint(documents)
        )
    }

    private func loadDocument(
        _ identifier: String,
        root: String,
        transaction: any Transaction,
        maximumQuadCount: UInt64
    ) async throws -> DocumentSnapshot {
        guard maximumQuadCount > 0,
              let limit = Int(exactly: min(maximumQuadCount, UInt64(Int.max))) else {
            throw DatabaseOntologyProcessingError.workLimitExceeded(
                requested: 1,
                maximum: maximumQuadCount
            )
        }
        guard let stored = try await documentStore.page(
            identifier: identifier,
            offset: 0,
            limit: limit,
            transaction: transaction
        ) else {
            if identifier == root {
                throw DatabaseOntologyProcessingError.ontologyNotFound(
                    identifier
                )
            }
            throw DatabaseOntologyProcessingError.importedOntologyNotFound(
                identifier
            )
        }
        guard stored.nextOffset == nil else {
            throw DatabaseOntologyProcessingError.workLimitExceeded(
                requested: stored.totalQuadCount,
                maximum: maximumQuadCount
            )
        }
        return DocumentSnapshot(
            identifier: identifier,
            revision: stored.revision,
            imports: stored.auxiliaryIdentifiers,
            quads: stored.quads
        )
    }

    private func sourceOntology(
        identifier: String,
        imports: [String],
        quads: [DatabaseRDFQuad]
    ) throws -> OWLOntology {
        let decoded: OWLOntology
        do {
            let dataset = RDFDataset(databaseQuads: quads)
            decoded = try TurtleDecoder().decode(
                from: dataset,
                fallbackIRI: identifier
            )
        } catch {
            throw DatabaseOntologyProcessingError.invalidDocument(
                String(describing: error)
            )
        }
        guard decoded.iri == identifier else {
            throw DatabaseOntologyProcessingError.ontologyIdentifierMismatch(
                expected: identifier,
                actual: decoded.iri
            )
        }
        let expectedImports = Array(Set(imports)).sorted()
        let decodedImports = Array(Set(decoded.imports)).sorted()
        guard decodedImports.isEmpty || decodedImports == expectedImports else {
            throw DatabaseOntologyProcessingError.importsMismatch(
                expected: expectedImports,
                actual: decodedImports
            )
        }
        return OWLOntology(
            iri: decoded.iri,
            versionIRI: decoded.versionIRI,
            imports: expectedImports,
            prefixes: decoded.prefixes,
            classes: decoded.classes,
            objectProperties: decoded.objectProperties,
            dataProperties: decoded.dataProperties,
            annotationProperties: decoded.annotationProperties,
            individuals: decoded.individuals,
            axioms: decoded.axioms
        )
    }

    private func mergedOntology(
        from documents: [DocumentSnapshot],
        root: String
    ) throws -> OWLOntology {
        guard let rootDocument = documents.first(where: {
            $0.identifier == root
        }) else {
            throw DatabaseOntologyProcessingError.ontologyNotFound(root)
        }
        let decoded = try documents.map { document in
            try sourceOntology(
                identifier: document.identifier,
                imports: document.imports,
                quads: document.quads
            )
        }
        var prefixes: [String: String] = [:]
        var classes: [String: OWLClass] = [:]
        var objectProperties: [String: OWLObjectProperty] = [:]
        var dataProperties: [String: OWLDataProperty] = [:]
        var annotationProperties: [String: OWLAnnotationProperty] = [:]
        var individuals: [String: OWLNamedIndividual] = [:]
        var axioms = Set<OWLAxiom>()
        let ordered = decoded.sorted {
            if $0.iri == root { return false }
            if $1.iri == root { return true }
            return $0.iri < $1.iri
        }
        for ontology in ordered {
            for (prefix, iri) in ontology.prefixes { prefixes[prefix] = iri }
            for value in ontology.classes { classes[value.iri] = value }
            for value in ontology.objectProperties {
                objectProperties[value.iri] = value
            }
            for value in ontology.dataProperties {
                dataProperties[value.iri] = value
            }
            for value in ontology.annotationProperties {
                annotationProperties[value.iri] = value
            }
            for value in ontology.individuals { individuals[value.iri] = value }
            axioms.formUnion(ontology.axioms)
        }
        let rootOntology = try sourceOntology(
            identifier: rootDocument.identifier,
            imports: rootDocument.imports,
            quads: rootDocument.quads
        )
        return OWLOntology(
            iri: root,
            versionIRI: rootOntology.versionIRI,
            imports: rootDocument.imports,
            prefixes: prefixes,
            classes: classes.values.sorted { $0.iri < $1.iri },
            objectProperties: objectProperties.values.sorted { $0.iri < $1.iri },
            dataProperties: dataProperties.values.sorted { $0.iri < $1.iri },
            annotationProperties: annotationProperties.values.sorted {
                $0.iri < $1.iri
            },
            individuals: individuals.values.sorted { $0.iri < $1.iri },
            axioms: axioms.sorted {
                String(describing: $0) < String(describing: $1)
            }
        )
    }

    private func reasoningTriple(
        from quad: DatabaseRDFQuad
    ) throws -> ReasoningTriple {
        guard case .iri(let predicateIRI) = quad.predicate else {
            throw DatabaseOntologyProcessingError.invalidDocument(
                "RDF predicates must be IRIs"
            )
        }
        do {
            return try ReasoningTriple(
                subject: quad.subject,
                predicateIRI: predicateIRI,
                object: quad.object
            )
        } catch let error {
            throw DatabaseOntologyProcessingError.invalidReasoningTriple(error)
        }
    }

    private func validate(
        resource: String,
        kind: OntologyExecuteOperation.HierarchyResourceKind,
        ontology: OWLOntology
    ) throws {
        let exists: Bool
        switch kind {
        case .class:
            exists = ontology.classSignature.contains(resource)
        case .objectProperty:
            exists = ontology.objectPropertySignature.contains(resource)
        case .dataProperty:
            exists = ontology.dataPropertySignature.contains(resource)
        }
        guard exists else {
            throw DatabaseOntologyProcessingError.resourceNotFound(resource)
        }
    }

    private func hierarchyNeighbors(
        of resource: String,
        kind: OntologyExecuteOperation.HierarchyResourceKind,
        direction: OntologyExecuteOperation.HierarchyDirection,
        reasoner: OWLReasoner
    ) -> Set<String> {
        switch (kind, direction) {
        case (.class, .ancestors):
            return reasoner.superClasses(of: resource, direct: true)
        case (.class, .descendants):
            return reasoner.subClasses(of: resource, direct: true)
        case (.objectProperty, .ancestors), (.dataProperty, .ancestors):
            return reasoner.superProperties(of: resource, direct: true)
        case (.objectProperty, .descendants), (.dataProperty, .descendants):
            return reasoner.subProperties(of: resource, direct: true)
        }
    }

    private func pageOffset(
        _ page: QueryExecuteOperation.Page,
        ontology: String,
        fingerprint: DatabaseBytes,
        kind: DatabaseOntologyPageCursor.Kind
    ) throws -> Int {
        guard let bytes = page.continuation else { return 0 }
        let cursor: DatabaseOntologyPageCursor
        do {
            cursor = try DatabaseEnvelopeCodec.decode(
                DatabaseOntologyPageCursor.self,
                from: bytes,
                limits: wireLimits
            )
        } catch {
            throw DatabaseOntologyProcessingError.invalidContinuation
        }
        guard cursor.ontology == ontology,
              cursor.dependencyFingerprint == fingerprint,
              cursor.kind == kind,
              let offset = Int(exactly: cursor.offset) else {
            throw DatabaseOntologyProcessingError.invalidContinuation
        }
        return offset
    }

    private func continuation(
        ontology: String,
        fingerprint: DatabaseBytes,
        offset: Int,
        kind: DatabaseOntologyPageCursor.Kind
    ) throws -> DatabaseBytes {
        guard let encodedOffset = UInt64(exactly: offset) else {
            throw DatabaseOntologyProcessingError.invalidContinuation
        }
        return try DatabaseEnvelopeCodec.encode(
            DatabaseOntologyPageCursor(
                ontology: ontology,
                dependencyFingerprint: fingerprint,
                offset: encodedOffset,
                kind: kind
            ),
            limits: wireLimits
        )
    }

    private func dependencyFingerprint(
        _ documents: [DocumentSnapshot]
    ) throws -> DatabaseBytes {
        let encoded = try DatabaseWireWriter.encode(
            limits: wireLimits
        ) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try writer.writeCount(documents.count)
            for document in documents {
                try writer.writeString(document.identifier)
                writer.writeUInt64(document.revision)
            }
        }
        var hasher = SHA256Accumulator()
        hasher.update(encoded)
        return hasher.finalize()
    }

    private func reasonerConfiguration(
        _ budget: DatabaseExecutionBudget
    ) -> OWLReasoner.Configuration {
        OWLReasoner.Configuration(
            maxExpansionSteps: Int(
                min(budget.maximumWorkUnits, UInt64(Int.max))
            ),
            enableIncrementalReasoning: true,
            cacheClassification: true,
            timeout: Double(budget.timeoutMilliseconds) / 1_000
        )
    }

    private static func includes(
        _ rule: OWL2RLRule,
        profile: OntologyExecuteOperation.ReasoningProfile
    ) -> Bool {
        switch profile {
        case .owlRL:
            return true
        case .rdfs:
            switch rule {
            case .caxSco, .scmSco, .prpSpo1, .scmSpo, .prpDom, .prpRng,
                 .scmDom1, .scmDom2, .scmRng1, .scmRng2:
                return true
            default:
                return false
            }
        }
    }

    private static func validationCode(
        _ error: OWLOntology.ValidationError
    ) -> String {
        switch error {
        case .undeclaredClass:
            return "OWL_UNDECLARED_CLASS"
        case .undeclaredObjectProperty:
            return "OWL_UNDECLARED_OBJECT_PROPERTY"
        case .undeclaredDataProperty:
            return "OWL_UNDECLARED_DATA_PROPERTY"
        case .undeclaredIndividual:
            return "OWL_UNDECLARED_INDIVIDUAL"
        case .duplicateClass:
            return "OWL_DUPLICATE_CLASS"
        case .duplicateObjectProperty:
            return "OWL_DUPLICATE_OBJECT_PROPERTY"
        case .duplicateDataProperty:
            return "OWL_DUPLICATE_DATA_PROPERTY"
        case .duplicateIndividual:
            return "OWL_DUPLICATE_INDIVIDUAL"
        }
    }

    private struct DocumentSnapshot: Sendable {
        let identifier: String
        let revision: UInt64
        let imports: [String]
        let quads: [DatabaseRDFQuad]
    }

    private struct EntailmentClosureSnapshot: Sendable {
        let documents: [DocumentSnapshot]
        let fingerprint: DatabaseBytes
    }

    private struct WorkBudget: Sendable {
        let maximum: UInt64
        private(set) var used: UInt64 = 0

        var remaining: UInt64 {
            maximum >= used ? maximum - used : 0
        }

        mutating func consume(_ amount: UInt64 = 1) throws {
            guard amount <= remaining else {
                let requested = used.addingReportingOverflow(amount)
                throw DatabaseOntologyProcessingError.workLimitExceeded(
                    requested: requested.overflow ? UInt64.max : requested.partialValue,
                    maximum: maximum
                )
            }
            used += amount
        }
    }
}
