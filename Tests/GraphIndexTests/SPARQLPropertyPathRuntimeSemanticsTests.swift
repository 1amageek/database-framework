#if FOUNDATION_DB
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import FDBStorage
import Foundation
import StorageKit
import TestHeartbeat
import Testing
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
struct RuntimeSemanticPathEdge {
    #Directory<RuntimeSemanticPathEdge>(
        "property_path_runtime_semantics",
        "edges"
    )

    var id: String = UUID().uuidString
    var from: RDFTerm = .iri(.xsdString)
    var relationship: RDFTerm = .iri(.xsdString)
    var to: RDFTerm = .iri(.xsdString)

    #Index(
        .rdfDataset,
        from: \RuntimeSemanticPathEdge.from,
        edge: \RuntimeSemanticPathEdge.relationship,
        to: \RuntimeSemanticPathEdge.to
    )
}

@Suite(
    "SPARQL property-path runtime semantics",
    .serialized,
    .foundationDBScenario,
    .heartbeat
)
struct SPARQLPropertyPathRuntimeSemanticsTests {
    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    @Test("Absent bound nodes retain all three zero-hop forms")
    func absentBoundNodesRetainZeroHopForms() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let executor = try await Self.makeExecutor(
                container: container,
                context: context
            )
            let absent = Self.uniqueIRI("absent-zero-hop")
            let predicate = try Self.uniquePredicate("absent-zero-hop")
            let path = ExecutionPropertyPath.zeroOrMore(.iri(predicate))

            let boundSubject = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(absent),
                    path: path,
                    object: .variable("?object")
                ),
                using: executor
            ).bindings
            #expect(boundSubject.count == 1)
            let boundSubjectRow = try #require(boundSubject.first)
            #expect(
                Self.iriValue(boundSubjectRow, for: "?object") == absent
            )

            let boundObject = try await Self.execute(
                .propertyPath(
                    subject: .variable("?subject"),
                    path: path,
                    object: try Self.iriTerm(absent)
                ),
                using: executor
            ).bindings
            #expect(boundObject.count == 1)
            let boundObjectRow = try #require(boundObject.first)
            #expect(
                Self.iriValue(boundObjectRow, for: "?subject") == absent
            )

            let bothBound = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(absent),
                    path: path,
                    object: try Self.iriTerm(absent)
                ),
                using: executor
            ).bindings
            #expect(bothBound.count == 1)
            let bothBoundRow = try #require(bothBound.first)
            #expect(bothBoundRow.isEmpty)

            let optionalPath = ExecutionPropertyPath.zeroOrOne(
                .iri(predicate)
            )
            let optionalBoundSubject = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(absent),
                    path: optionalPath,
                    object: .variable("?object")
                ),
                using: executor
            ).bindings
            #expect(optionalBoundSubject.count == 1)
            let optionalBoundSubjectRow = try #require(
                optionalBoundSubject.first
            )
            #expect(
                Self.iriValue(
                    optionalBoundSubjectRow,
                    for: "?object"
                ) == absent
            )

            let optionalBoundObject = try await Self.execute(
                .propertyPath(
                    subject: .variable("?subject"),
                    path: optionalPath,
                    object: try Self.iriTerm(absent)
                ),
                using: executor
            ).bindings
            #expect(optionalBoundObject.count == 1)
            let optionalBoundObjectRow = try #require(
                optionalBoundObject.first
            )
            #expect(
                Self.iriValue(
                    optionalBoundObjectRow,
                    for: "?subject"
                ) == absent
            )
        }
    }

    @Test("Mixed negated directions preserve duplicate solution multiplicity")
    func mixedNegatedDirectionsPreserveBagMultiplicity() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let source = Self.uniqueIRI("mixed-nps-source")
            let target = Self.uniqueIRI("mixed-nps-target")
            let forwardPredicate = try Self.uniquePredicate(
                "mixed-nps-forward"
            )
            let inversePredicate = try Self.uniquePredicate(
                "mixed-nps-inverse"
            )
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: source,
                        relationship: forwardPredicate,
                        to: target
                    ),
                    try Self.makeEdge(
                        from: target,
                        relationship: inversePredicate,
                        to: source
                    ),
                ],
                context: context
            )
            let executor = try await Self.makeExecutor(
                container: container,
                context: context
            )
            let exclusions = try PropertyPathNegatedSet(
                forward: Set<RDFPredicateIRI>(),
                inverse: Set<RDFPredicateIRI>()
            )

            let rows = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(source),
                    path: .negatedPropertySet(exclusions),
                    object: .variable("?target")
                ),
                using: executor
            ).bindings

            #expect(rows.count == 2)
            for row in rows {
                #expect(Self.iriValue(row, for: "?target") == target)
            }
        }
    }

    @Test("Exact one-hop ranges preserve inner alternative multiplicity")
    func exactOneHopRangePreservesAlternativeMultiplicity() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let source = Self.uniqueIRI("exact-one-source")
            let target = Self.uniqueIRI("exact-one-target")
            let predicate = try Self.uniquePredicate("exact-one")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: source,
                        relationship: predicate,
                        to: target
                    )
                ],
                context: context
            )
            let executor = try await Self.makeExecutor(
                container: container,
                context: context
            )
            let bounds = try PropertyPathRange(minimum: 1, maximum: 1)

            let rows = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(source),
                    path: .range(
                        .alternative(.iri(predicate), .iri(predicate)),
                        bounds
                    ),
                    object: .variable("?target")
                ),
                using: executor
            ).bindings

            #expect(rows.count == 2)
            for row in rows {
                #expect(Self.iriValue(row, for: "?target") == target)
            }
        }
    }

    @Test("Exact two-hop ranges preserve hidden intermediate multiplicity")
    func exactTwoHopRangePreservesIntermediateMultiplicity() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let source = Self.uniqueIRI("exact-two-source")
            let firstMiddle = Self.uniqueIRI("exact-two-first-middle")
            let secondMiddle = Self.uniqueIRI("exact-two-second-middle")
            let target = Self.uniqueIRI("exact-two-target")
            let predicate = try Self.uniquePredicate("exact-two")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: source,
                        relationship: predicate,
                        to: firstMiddle
                    ),
                    try Self.makeEdge(
                        from: source,
                        relationship: predicate,
                        to: secondMiddle
                    ),
                    try Self.makeEdge(
                        from: firstMiddle,
                        relationship: predicate,
                        to: target
                    ),
                    try Self.makeEdge(
                        from: secondMiddle,
                        relationship: predicate,
                        to: target
                    ),
                ],
                context: context
            )
            let executor = try await Self.makeExecutor(
                container: container,
                context: context
            )
            let bounds = try PropertyPathRange(minimum: 2, maximum: 2)

            let rows = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(source),
                    path: .range(.iri(predicate), bounds),
                    object: .variable("?target")
                ),
                using: executor
            ).bindings

            #expect(rows.count == 2)
            for row in rows {
                #expect(Self.iriValue(row, for: "?target") == target)
            }
        }
    }

    @Test("Correlated EXISTS applies its seed before the path result limit")
    func correlatedExistsAppliesSeedBeforeResultLimit() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let token = UUID().uuidString.lowercased()
            let noiseStart = Self.orderedIRI("00-noise-start", token: token)
            let noiseTarget = Self.orderedIRI("01-noise-target", token: token)
            let correlatedStart = Self.orderedIRI(
                "ff-correlated-start",
                token: token
            )
            let correlatedTarget = Self.orderedIRI(
                "ff-correlated-target",
                token: token
            )
            let absentStart = Self.orderedIRI("zz-absent-start", token: token)
            let predicate = try Self.uniquePredicate("correlated-limit")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: noiseStart,
                        relationship: predicate,
                        to: noiseTarget
                    ),
                    try Self.makeEdge(
                        from: correlatedStart,
                        relationship: predicate,
                        to: correlatedTarget
                    ),
                ],
                context: context
            )

            let existsQuery = SelectQuery(
                projection: .all,
                source: .graphPattern(
                    GraphPattern.propertyPath(
                        subject: .variable("start"),
                        path: DatabaseKit.PropertyPath.iri(predicate),
                        object: .variable("target")
                    )
                )
            )
            let query = GraphPattern.filter(
                .values(
                    variables: ["start"],
                    bindings: [
                        [.iri(correlatedStart)],
                        [.iri(absentStart)],
                    ]
                ),
                .exists(existsQuery)
            )
            let pattern = try GraphPatternConverter.convert(query)
            let executor = try await Self.makeExecutor(
                container: container,
                context: context,
                configuration: ExecutionPropertyPathConfiguration(
                    maximumExpressionDepth: 16,
                    maximumTraversalDepth: 16,
                    maximumResults: 1
                )
            )

            let execution = try await Self.execute(pattern, using: executor)

            #expect(execution.bindings.count == 1)
            let row = try #require(execution.bindings.first)
            #expect(Self.iriValue(row, for: "?start") == correlatedStart)
            #expect(execution.statistics.indexScans >= 2)
            #expect(execution.workMeter.consumedWorkUnits > 0)
        }
    }

    @Test("Same-variable cycles count only compatible endpoint pairs")
    func sameVariableCyclesApplyLimitAfterCompatibility() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let acyclicStart = Self.uniqueIRI("same-variable-acyclic-start")
            let acyclicMiddle = Self.uniqueIRI("same-variable-acyclic-middle")
            let acyclicEnd = Self.uniqueIRI("same-variable-acyclic-end")
            let cycleFirst = Self.uniqueIRI("same-variable-cycle-first")
            let cycleSecond = Self.uniqueIRI("same-variable-cycle-second")
            let predicate = try Self.uniquePredicate("same-variable-cycle")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: acyclicStart,
                        relationship: predicate,
                        to: acyclicMiddle
                    ),
                    try Self.makeEdge(
                        from: acyclicMiddle,
                        relationship: predicate,
                        to: acyclicEnd
                    ),
                    try Self.makeEdge(
                        from: cycleFirst,
                        relationship: predicate,
                        to: cycleSecond
                    ),
                    try Self.makeEdge(
                        from: cycleSecond,
                        relationship: predicate,
                        to: cycleFirst
                    ),
                ],
                context: context
            )
            let executor = try await Self.makeExecutor(
                container: container,
                context: context,
                configuration: ExecutionPropertyPathConfiguration(
                    maximumExpressionDepth: 16,
                    maximumTraversalDepth: 16,
                    maximumResults: 2
                )
            )

            let rows = try await Self.execute(
                .propertyPath(
                    subject: .variable("?node"),
                    path: .oneOrMore(.iri(predicate)),
                    object: .variable("?node")
                ),
                using: executor
            ).bindings

            let nodes = Set(
                rows.compactMap { Self.iriValue($0, for: "?node") }
            )
            #expect(rows.count == 2)
            #expect(nodes == Set([cycleFirst, cycleSecond]))
        }
    }

    @Test("Compatible result overflow remains a typed failure")
    func compatibleResultOverflowIsTypedFailure() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let predicate = try Self.uniquePredicate("typed-overflow")
            let first = Self.uniqueIRI("typed-overflow-first")
            let second = Self.uniqueIRI("typed-overflow-second")
            let third = Self.uniqueIRI("typed-overflow-third")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: first,
                        relationship: predicate,
                        to: first
                    ),
                    try Self.makeEdge(
                        from: second,
                        relationship: predicate,
                        to: second
                    ),
                    try Self.makeEdge(
                        from: third,
                        relationship: predicate,
                        to: third
                    ),
                ],
                context: context
            )
            let executor = try await Self.makeExecutor(
                container: container,
                context: context,
                configuration: ExecutionPropertyPathConfiguration(
                    maximumExpressionDepth: 16,
                    maximumTraversalDepth: 16,
                    maximumResults: 2
                )
            )
            let pattern = ExecutionPattern.propertyPath(
                subject: .variable("?node"),
                path: .iri(predicate),
                object: .variable("?node")
            )

            let limited = try await Self.execute(
                pattern,
                using: executor,
                limit: 1
            ).bindings
            #expect(limited.count == 1)

            do {
                _ = try await Self.execute(pattern, using: executor)
                Issue.record(
                    "Expected a typed property-path result-limit failure"
                )
            } catch let error as SPARQLQueryError {
                guard case .propertyPathResultLimitExceeded(let maximum) =
                        error else {
                    Issue.record("Unexpected SPARQL error: \(error)")
                    return
                }
                #expect(maximum == 2)
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }
    }

    @Test("Subproperty and inverse axioms compose as one oriented closure")
    func subpropertyAndInverseAxiomsCompose() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let requested = try Self.uniquePredicate("ontology-super")
            let subproperty = try Self.uniquePredicate("ontology-sub")
            let inverse = try Self.uniquePredicate("ontology-sub-inverse")
            let source = Self.uniqueIRI("ontology-composed-source")
            let target = Self.uniqueIRI("ontology-composed-target")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: target,
                        relationship: inverse,
                        to: source
                    )
                ],
                context: context
            )

            var ontology = OWLOntology(
                iri: Self.uniqueIRI("ontology-composed")
            )
            ontology.objectProperties = [
                OWLObjectProperty(iri: requested.rawValue),
                OWLObjectProperty(iri: subproperty.rawValue),
                OWLObjectProperty(iri: inverse.rawValue),
            ]
            ontology.axioms = [
                .subObjectPropertyOf(
                    sub: subproperty.rawValue,
                    sup: requested.rawValue
                ),
                .inverseObjectProperties(
                    first: subproperty.rawValue,
                    second: inverse.rawValue
                ),
            ]
            let executor = try await Self.makeExecutor(
                container: container,
                context: context,
                ontologyContext: OntologyContext(ontology: ontology)
            )

            let rows = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(source),
                    path: .iri(requested),
                    object: .variable("?target")
                ),
                using: executor
            ).bindings

            #expect(rows.count == 1)
            let row = try #require(rows.first)
            #expect(Self.iriValue(row, for: "?target") == target)
        }
    }

    @Test("Symmetric and declared inverse entailments compose")
    func symmetricAndDeclaredInverseEntailmentsCompose() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let symmetric = try Self.uniquePredicate("ontology-symmetric")
            let declaredInverse = try Self.uniquePredicate(
                "ontology-declared-inverse"
            )
            let source = Self.uniqueIRI("ontology-symmetric-source")
            let directTarget = Self.uniqueIRI("ontology-direct-target")
            let symmetricTarget = Self.uniqueIRI("ontology-symmetric-target")
            let inverseTarget = Self.uniqueIRI("ontology-inverse-target")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: source,
                        relationship: symmetric,
                        to: directTarget
                    ),
                    try Self.makeEdge(
                        from: symmetricTarget,
                        relationship: symmetric,
                        to: source
                    ),
                    try Self.makeEdge(
                        from: inverseTarget,
                        relationship: declaredInverse,
                        to: source
                    ),
                ],
                context: context
            )

            var ontology = OWLOntology(
                iri: Self.uniqueIRI("ontology-symmetric-runtime")
            )
            ontology.objectProperties = [
                OWLObjectProperty(
                    iri: symmetric.rawValue,
                    characteristics: [.symmetric],
                    inverseOf: declaredInverse.rawValue
                ),
                OWLObjectProperty(iri: declaredInverse.rawValue),
            ]
            let executor = try await Self.makeExecutor(
                container: container,
                context: context,
                ontologyContext: OntologyContext(ontology: ontology)
            )

            let rows = try await Self.execute(
                .propertyPath(
                    subject: try Self.iriTerm(source),
                    path: .iri(symmetric),
                    object: .variable("?target")
                ),
                using: executor
            ).bindings
            let targets = Set(
                rows.compactMap { Self.iriValue($0, for: "?target") }
            )

            #expect(rows.count == 3)
            #expect(
                targets == Set([
                    directTarget,
                    symmetricTarget,
                    inverseTarget,
                ])
            )
        }
    }

    @Test("Public SPARQL builder executes a persisted sequence end to end")
    func publicBuilderExecutesPersistedSequenceEndToEnd() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await Self.setupContainer()
            let context = container.newContext()
            let source = Self.uniqueIRI("builder-source")
            let middle = Self.uniqueIRI("builder-middle")
            let target = Self.uniqueIRI("builder-target")
            let firstPredicate = try Self.uniquePredicate("builder-first")
            let secondPredicate = try Self.uniquePredicate("builder-second")
            try await Self.insertEdges(
                [
                    try Self.makeEdge(
                        from: source,
                        relationship: firstPredicate,
                        to: middle
                    ),
                    try Self.makeEdge(
                        from: middle,
                        relationship: secondPredicate,
                        to: target
                    ),
                ],
                context: context
            )

            let result = try await context
                .sparql(RuntimeSemanticPathEdge.self)
                .defaultIndex()
                .wherePath(
                    try Self.iriTerm(source),
                    path: .sequence(
                        .iri(firstPredicate),
                        .iri(secondPredicate)
                    ),
                    .variable("?target")
                )
                .execute()

            #expect(result.count == 1)
            #expect(result.isComplete)
            #expect(result.statistics.indexScans >= 2)
            let row = try #require(result.first)
            #expect(Self.iriValue(row, for: "?target") == target)
        }
    }

    private static func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try RuntimeSemanticPathEdge.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(RuntimeSemanticPathEdge.self)]),
            security: .disabled
        )
    }

    private static func makeExecutor(
        container: DBContainer,
        context: DatabaseContext,
        ontologyContext: OntologyContext? = nil,
        configuration: ExecutionPropertyPathConfiguration = .default
    ) async throws -> SPARQLQueryExecutor {
        let selections = try RuntimeSemanticPathEdge.indexDescriptors
            .compactMap(RDFDatasetIndexSelection.init(descriptor:))
        guard selections.count == 1 else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let selection = selections[0]
        let readableIndex = try await context.indexQueryContext.withReadableIndex(
            named: selection.indexName,
            kindIdentifier: selection.kindIdentifier,
            for: RuntimeSemanticPathEdge.self
        ) { index, _ in
            index
        }
        guard let readableIndex else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let source = try RDFDatasetSource(
            entityName: RuntimeSemanticPathEdge.persistableType,
            selection: selection,
            indexSubspace: readableIndex.subspace
        )
        return SPARQLQueryExecutor(
            database: container.engine,
            wallClock: FixedTestWallClock(),
            sources: [source],
            ontologyContext: ontologyContext,
            propertyPathConfiguration: configuration
        )
    }

    private static func execute(
        _ pattern: ExecutionPattern,
        using executor: SPARQLQueryExecutor,
        limit: Int? = nil
    ) async throws -> (
        bindings: [VariableBinding],
        statistics: ExecutionStatistics,
        workMeter: DatabaseWorkMeter
    ) {
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let result = try await executor.execute(
            pattern: pattern,
            limit: limit,
            offset: 0,
            workMeter: workMeter
        )
        return (result.0, result.1, workMeter)
    }

    private static func insertEdges(
        _ edges: [RuntimeSemanticPathEdge],
        context: DatabaseContext
    ) async throws {
        for edge in edges {
            try context.insert(edge)
        }
        try await context.save()
    }

    private static func makeEdge(
        from: String,
        relationship: RDFPredicateIRI,
        to: String
    ) throws -> RuntimeSemanticPathEdge {
        var edge = RuntimeSemanticPathEdge()
        edge.from = try .iri(validating: from)
        edge.relationship = relationship.term
        edge.to = try .iri(validating: to)
        return edge
    }

    private static func iriTerm(_ value: String) throws -> ExecutionTerm {
        .value(.rdfTerm(try .iri(validating: value)))
    }

    private static func iriValue(
        _ binding: VariableBinding,
        for variable: String
    ) -> String? {
        guard case .rdfTerm(.iri(let value)) = binding[variable] else {
            return nil
        }
        return value.rawValue
    }

    private static func uniqueIRI(_ prefix: String) -> String {
        orderedIRI(prefix, token: UUID().uuidString.lowercased())
    }

    private static func orderedIRI(_ prefix: String, token: String) -> String {
        "https://example.invalid/node/\(prefix)-\(token)"
    }

    private static func uniquePredicate(
        _ prefix: String
    ) throws -> RDFPredicateIRI {
        try RDFPredicateIRI(
            "https://example.invalid/property/\(prefix)-"
                + UUID().uuidString.lowercased()
        )
    }
}
#endif
