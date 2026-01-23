// IncrementalReasoningTests.swift
// GraphIndexTests - Tests for Incremental Reasoning (DRed Algorithm)

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Suite

@Suite("Incremental Reasoning Tests", .serialized)
struct IncrementalReasoningTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueIRI(_ prefix: String) -> String {
        "http://example.org/\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupDatabase() throws -> any DatabaseProtocol {
        try FDBClient.openDatabase()
    }

    // MARK: - InferenceChanges Tests

    @Test("InferenceChanges merge")
    func testInferenceChangesMerge() async throws {
        var changes1 = InferenceChanges()
        changes1.addedInferences = [
            InferredTriple(
                triple: TripleKey("a", "type", "B"),
                provenance: InferenceProvenance(rule: .caxSco, antecedents: [])
            )
        ]
        changes1.affectedClasses = ["B"]
        changes1.statistics.inferencesAdded = 1

        var changes2 = InferenceChanges()
        changes2.addedInferences = [
            InferredTriple(
                triple: TripleKey("a", "type", "C"),
                provenance: InferenceProvenance(rule: .caxSco, antecedents: [])
            )
        ]
        changes2.affectedClasses = ["C"]
        changes2.statistics.inferencesAdded = 1

        changes1.merge(changes2)

        #expect(changes1.addedInferences.count == 2)
        #expect(changes1.affectedClasses.contains("B"))
        #expect(changes1.affectedClasses.contains("C"))
        #expect(changes1.statistics.inferencesAdded == 2)
    }

    @Test("InferenceChanges isEmpty")
    func testInferenceChangesIsEmpty() async throws {
        let emptyChanges = InferenceChanges()
        #expect(emptyChanges.isEmpty == true)

        var nonEmptyChanges = InferenceChanges()
        nonEmptyChanges.addedInferences = [
            InferredTriple(
                triple: TripleKey("a", "type", "B"),
                provenance: InferenceProvenance(rule: .caxSco, antecedents: [])
            )
        ]
        #expect(nonEmptyChanges.isEmpty == false)
    }

    // MARK: - IncrementalAxiom Tests

    @Test("IncrementalAxiom types")
    func testIncrementalAxiomTypes() async throws {
        let classA = uniqueIRI("ClassA")
        let classB = uniqueIRI("ClassB")
        let prop1 = uniqueIRI("prop1")
        let prop2 = uniqueIRI("prop2")
        let alice = uniqueIRI("Alice")

        // Test axiom creation
        let subClassAxiom = IncrementalAxiom.subClassOf(subClass: classA, superClass: classB)
        let equivalentAxiom = IncrementalAxiom.equivalentClasses(class1: classA, class2: classB)
        let subPropAxiom = IncrementalAxiom.subPropertyOf(subProperty: prop1, superProperty: prop2)
        let inverseAxiom = IncrementalAxiom.inverseOf(property1: prop1, property2: prop2)
        let domainAxiom = IncrementalAxiom.domain(property: prop1, classIRI: classA)
        let rangeAxiom = IncrementalAxiom.range(property: prop1, classIRI: classB)
        let symmetricAxiom = IncrementalAxiom.symmetricProperty(property: prop1)
        let transitiveAxiom = IncrementalAxiom.transitiveProperty(property: prop1)
        let classAssertionAxiom = IncrementalAxiom.classAssertion(individual: alice, classIRI: classA)
        let propAssertionAxiom = IncrementalAxiom.propertyAssertion(subject: alice, property: prop1, object: classB)

        // Test that axioms are hashable
        var axiomSet = Set<IncrementalAxiom>()
        axiomSet.insert(subClassAxiom)
        axiomSet.insert(equivalentAxiom)
        axiomSet.insert(subPropAxiom)
        axiomSet.insert(inverseAxiom)
        axiomSet.insert(domainAxiom)
        axiomSet.insert(rangeAxiom)
        axiomSet.insert(symmetricAxiom)
        axiomSet.insert(transitiveAxiom)
        axiomSet.insert(classAssertionAxiom)
        axiomSet.insert(propAssertionAxiom)

        #expect(axiomSet.count == 10)
    }

    // MARK: - IncrementalStatistics Tests

    @Test("IncrementalStatistics merge")
    func testIncrementalStatisticsMerge() async throws {
        var stats1 = IncrementalStatistics()
        stats1.triplesProcessed = 5
        stats1.inferencesAdded = 10
        stats1.inferencesRemoved = 2
        stats1.rederivations = 1
        stats1.cascadingChecks = 3
        stats1.processingTime = 0.5

        var stats2 = IncrementalStatistics()
        stats2.triplesProcessed = 3
        stats2.inferencesAdded = 5
        stats2.inferencesRemoved = 1
        stats2.rederivations = 0
        stats2.cascadingChecks = 2
        stats2.processingTime = 0.3

        stats1.merge(stats2)

        #expect(stats1.triplesProcessed == 8)
        #expect(stats1.inferencesAdded == 15)
        #expect(stats1.inferencesRemoved == 3)
        #expect(stats1.rederivations == 1)
        #expect(stats1.cascadingChecks == 5)
        #expect(stats1.processingTime == 0.8)
    }

    // MARK: - DependencyGraph Tests

    @Test("DependencyGraph basic operations")
    func testDependencyGraphBasicOperations() async throws {
        var graph = DependencyGraph()

        let triple1 = TripleKey("A", "subClassOf", "B")
        let triple2 = TripleKey("B", "subClassOf", "C")
        let triple3 = TripleKey("A", "subClassOf", "C")

        // triple3 depends on triple1 and triple2
        graph.addDependency(antecedent: triple1, consequent: triple3)
        graph.addDependency(antecedent: triple2, consequent: triple3)

        // Check dependents
        let dependentsOf1 = graph.getDependents(of: triple1)
        #expect(dependentsOf1.contains(triple3))

        let dependentsOf2 = graph.getDependents(of: triple2)
        #expect(dependentsOf2.contains(triple3))

        // Check dependencies
        let dependenciesOf3 = graph.getDependencies(of: triple3)
        #expect(dependenciesOf3.contains(triple1))
        #expect(dependenciesOf3.contains(triple2))
    }

    @Test("DependencyGraph transitive dependents")
    func testDependencyGraphTransitiveDependents() async throws {
        var graph = DependencyGraph()

        let triple1 = TripleKey("A", "type", "Class1")
        let triple2 = TripleKey("A", "type", "Class2") // depends on triple1
        let triple3 = TripleKey("A", "type", "Class3") // depends on triple2
        let triple4 = TripleKey("A", "type", "Class4") // depends on triple3

        graph.addDependency(antecedent: triple1, consequent: triple2)
        graph.addDependency(antecedent: triple2, consequent: triple3)
        graph.addDependency(antecedent: triple3, consequent: triple4)

        let transitiveDependents = graph.getTransitiveDependents(of: triple1)

        #expect(transitiveDependents.count == 3)
        #expect(transitiveDependents.contains(triple2))
        #expect(transitiveDependents.contains(triple3))
        #expect(transitiveDependents.contains(triple4))
    }

    @Test("DependencyGraph remove")
    func testDependencyGraphRemove() async throws {
        var graph = DependencyGraph()

        let triple1 = TripleKey("A", "p", "B")
        let triple2 = TripleKey("B", "p", "C")
        let triple3 = TripleKey("A", "p", "C")

        graph.addDependency(antecedent: triple1, consequent: triple3)
        graph.addDependency(antecedent: triple2, consequent: triple3)

        // Remove triple3
        graph.remove(triple3)

        // triple1 and triple2 should no longer have triple3 as dependent
        let dependentsOf1 = graph.getDependents(of: triple1)
        #expect(!dependentsOf1.contains(triple3))

        let dependentsOf2 = graph.getDependents(of: triple2)
        #expect(!dependentsOf2.contains(triple3))
    }

    // MARK: - Provenance Tests

    @Test("InferenceProvenance encoding/decoding")
    func testInferenceProvenanceEncodingDecoding() async throws {
        let provenance = InferenceProvenance(
            rule: .caxSco,
            antecedents: [
                TripleKey("ex:Alice", "rdf:type", "ex:Employee"),
                TripleKey("ex:Employee", "rdfs:subClassOf", "ex:Person")
            ],
            inferredAt: Date(),
            isValid: true,
            depth: 2
        )

        let encoded = try provenance.encode()
        let decoded = try InferenceProvenance.decode(from: encoded)

        #expect(decoded.rule == .caxSco)
        #expect(decoded.antecedents.count == 2)
        #expect(decoded.isValid == true)
        #expect(decoded.depth == 2)
    }

    @Test("InferenceProvenance asserted")
    func testInferenceProvenanceAsserted() async throws {
        let asserted = InferenceProvenance.asserted()

        #expect(asserted.isExplicit == true)
        #expect(asserted.antecedents.isEmpty)
        #expect(asserted.depth == 0)
    }

    // MARK: - TripleKey Tests

    @Test("TripleKey hashable")
    func testTripleKeyHashable() async throws {
        let triple1 = TripleKey("A", "B", "C")
        let triple2 = TripleKey("A", "B", "C")
        let triple3 = TripleKey("A", "B", "D")

        #expect(triple1 == triple2)
        #expect(triple1 != triple3)

        var set = Set<TripleKey>()
        set.insert(triple1)
        set.insert(triple2) // duplicate
        set.insert(triple3)

        #expect(set.count == 2)
    }

    @Test("TripleKey description")
    func testTripleKeyDescription() async throws {
        let triple = TripleKey("subject", "predicate", "object")
        #expect(triple.description == "(subject, predicate, object)")
    }

    // MARK: - Configuration Tests

    @Test("IncrementalReasoner configuration")
    func testIncrementalReasonerConfiguration() async throws {
        let defaultConfig = IncrementalReasoner.Configuration.default

        #expect(defaultConfig.maxCascadeDepth == 100)
        #expect(defaultConfig.maxRederivationAttempts == 10)
        #expect(defaultConfig.batchDependencyUpdates == true)
        #expect(defaultConfig.dependencyBatchSize == 100)

        let customConfig = IncrementalReasoner.Configuration(
            maxCascadeDepth: 50,
            maxRederivationAttempts: 5,
            batchDependencyUpdates: false,
            dependencyBatchSize: 50
        )

        #expect(customConfig.maxCascadeDepth == 50)
        #expect(customConfig.maxRederivationAttempts == 5)
        #expect(customConfig.batchDependencyUpdates == false)
        #expect(customConfig.dependencyBatchSize == 50)
    }

    // MARK: - DeletionStatus Tests

    @Test("DeletionStatus values")
    func testDeletionStatusValues() async throws {
        let valid = DeletionStatus.valid
        let tentative = DeletionStatus.tentativelyDeleted
        let deleted = DeletionStatus.deleted
        let rederived = DeletionStatus.rederived

        #expect(valid.rawValue == "valid")
        #expect(tentative.rawValue == "tentativelyDeleted")
        #expect(deleted.rawValue == "deleted")
        #expect(rederived.rawValue == "rederived")
    }

    // MARK: - DRedDeletionResult Tests

    @Test("DRedDeletionResult")
    func testDRedDeletionResult() async throws {
        var result = DRedDeletionResult()

        result.permanentlyDeleted = [TripleKey("A", "p", "B"), TripleKey("B", "p", "C")]
        result.rederived = [TripleKey("C", "p", "D")]
        result.cascadingChecks = 10
        result.maintenanceTime = 1.5

        #expect(result.permanentlyDeleted.count == 2)
        #expect(result.rederived.count == 1)
        #expect(result.cascadingChecks == 10)
        #expect(result.maintenanceTime == 1.5)
    }

    // MARK: - DependencyGraph Cycle Detection Tests

    @Test("DependencyGraph handles circular dependencies")
    func testDependencyGraphCircularDependencies() async throws {
        var graph = DependencyGraph()

        let tripleA = TripleKey("A", "dependsOn", "B")
        let tripleB = TripleKey("B", "dependsOn", "C")
        let tripleC = TripleKey("C", "dependsOn", "A")  // Creates cycle A -> B -> C -> A

        // Create circular dependency
        graph.addDependency(antecedent: tripleA, consequent: tripleB)
        graph.addDependency(antecedent: tripleB, consequent: tripleC)
        graph.addDependency(antecedent: tripleC, consequent: tripleA)

        // Getting transitive dependents should not infinite loop
        let dependentsOfA = graph.getTransitiveDependents(of: tripleA)

        // Should find B and C (and potentially A due to cycle)
        #expect(dependentsOfA.contains(tripleB))
        #expect(dependentsOfA.contains(tripleC))
        // The cycle should be handled gracefully (no infinite loop)
    }

    @Test("DependencyGraph handles self-referential triple")
    func testDependencyGraphSelfReferential() async throws {
        var graph = DependencyGraph()

        let triple = TripleKey("A", "sameAs", "A")

        // Self-dependency
        graph.addDependency(antecedent: triple, consequent: triple)

        // Should handle gracefully
        let dependents = graph.getTransitiveDependents(of: triple)

        // Should include itself (or be empty depending on implementation)
        // The key is that it doesn't crash or infinite loop
        #expect(dependents.count <= 1)
    }

    @Test("DependencyGraph diamond dependency pattern")
    func testDependencyGraphDiamondPattern() async throws {
        var graph = DependencyGraph()

        //     A
        //    / \
        //   B   C
        //    \ /
        //     D
        let tripleA = TripleKey("A", "type", "Root")
        let tripleB = TripleKey("B", "type", "Left")
        let tripleC = TripleKey("C", "type", "Right")
        let tripleD = TripleKey("D", "type", "Bottom")

        graph.addDependency(antecedent: tripleA, consequent: tripleB)
        graph.addDependency(antecedent: tripleA, consequent: tripleC)
        graph.addDependency(antecedent: tripleB, consequent: tripleD)
        graph.addDependency(antecedent: tripleC, consequent: tripleD)

        // Deleting A should affect B, C, and D
        let dependentsOfA = graph.getTransitiveDependents(of: tripleA)

        #expect(dependentsOfA.count == 3)
        #expect(dependentsOfA.contains(tripleB))
        #expect(dependentsOfA.contains(tripleC))
        #expect(dependentsOfA.contains(tripleD))

        // D has multiple paths from A, but should only appear once
        let dCount = dependentsOfA.filter { $0 == tripleD }.count
        #expect(dCount == 1)
    }

    @Test("DependencyGraph empty graph operations")
    func testDependencyGraphEmptyOperations() async throws {
        let graph = DependencyGraph()

        let nonExistent = TripleKey("X", "Y", "Z")

        // Operations on non-existent triples should return empty
        let dependents = graph.getDependents(of: nonExistent)
        let dependencies = graph.getDependencies(of: nonExistent)
        let transitive = graph.getTransitiveDependents(of: nonExistent)

        #expect(dependents.isEmpty)
        #expect(dependencies.isEmpty)
        #expect(transitive.isEmpty)
    }

    @Test("DependencyGraph multiple dependencies same consequent")
    func testDependencyGraphMultipleDependencies() async throws {
        var graph = DependencyGraph()

        // D depends on A, B, and C
        let tripleA = TripleKey("A", "p", "1")
        let tripleB = TripleKey("B", "p", "2")
        let tripleC = TripleKey("C", "p", "3")
        let tripleD = TripleKey("D", "type", "Combined")

        graph.addDependency(antecedent: tripleA, consequent: tripleD)
        graph.addDependency(antecedent: tripleB, consequent: tripleD)
        graph.addDependency(antecedent: tripleC, consequent: tripleD)

        // D should have 3 dependencies
        let dependencies = graph.getDependencies(of: tripleD)
        #expect(dependencies.count == 3)
        #expect(dependencies.contains(tripleA))
        #expect(dependencies.contains(tripleB))
        #expect(dependencies.contains(tripleC))

        // Each antecedent should have D as dependent
        #expect(graph.getDependents(of: tripleA).contains(tripleD))
        #expect(graph.getDependents(of: tripleB).contains(tripleD))
        #expect(graph.getDependents(of: tripleC).contains(tripleD))
    }

    // MARK: - OWL2RLRule Tests

    @Test("OWL2RLRule all values")
    func testOWL2RLRuleValues() async throws {
        // Verify all expected rules exist
        let rules: [OWL2RLRule] = [
            .caxSco, .caxEqc1, .caxEqc2,
            .prpDom, .prpRng,
            .prpInv1, .prpInv2,
            .prpSpo1, .prpSymp, .prpTrp,
            .clsSvf1, .clsSvf2
        ]

        #expect(rules.count == 12)

        // Verify rules are hashable (can be used in Sets/Dictionaries)
        var ruleSet = Set<OWL2RLRule>()
        for rule in rules {
            ruleSet.insert(rule)
        }
        #expect(ruleSet.count == 12)
    }
}
