// OWLReasoningPhase1Tests.swift
// Tests for OWL 2 RL Reasoning Phase 1 components
//
// Tests OntologyStorage, PersistentUnionFind, OWL2RLRules, and InferenceProvenance

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - OntologyMetadata Tests

@Suite("OntologyMetadata")
struct OntologyMetadataTests {

    @Test("Schema version comparison")
    func schemaVersionComparison() {
        let v1 = SchemaVersion(major: 1, minor: 0, patch: 0)
        let v2 = SchemaVersion(major: 1, minor: 1, patch: 0)
        let v3 = SchemaVersion(major: 2, minor: 0, patch: 0)
        let v4 = SchemaVersion(major: 1, minor: 0, patch: 1)

        #expect(v1 < v2)
        #expect(v2 < v3)
        #expect(v1 < v4)
        #expect(v4 < v2)
    }

    @Test("Schema version string representation")
    func schemaVersionString() {
        let version = SchemaVersion(major: 2, minor: 3, patch: 1)
        #expect(version.description == "2.3.1")
    }

    @Test("Metadata encoding and decoding")
    func metadataEncodeDecode() throws {
        let metadata = OntologyMetadata(
            iri: "http://example.org/test-ontology",
            versionIRI: "http://example.org/test-ontology/1.0",
            imports: ["http://www.w3.org/2002/07/owl"],
            prefixes: ["ex": "http://example.org/"]
        )

        let encoded = try JSONEncoder().encode(metadata)
        let decoded = try JSONDecoder().decode(OntologyMetadata.self, from: encoded)

        #expect(decoded.iri == metadata.iri)
        #expect(decoded.versionIRI == metadata.versionIRI)
        #expect(decoded.imports == metadata.imports)
        #expect(decoded.prefixes == metadata.prefixes)
    }

    @Test("Ontology status values")
    func ontologyStatusValues() {
        #expect(OntologyStatus.loading.rawValue == "loading")
        #expect(OntologyStatus.ready.rawValue == "ready")
        #expect(OntologyStatus.failed.rawValue == "failed")
        #expect(OntologyStatus.updating.rawValue == "updating")
        #expect(OntologyStatus.deleted.rawValue == "deleted")
    }

    @Test("Ontology status metadata")
    func ontologyStatusMetadata() {
        let metadata = OntologyMetadata(iri: "http://example.org/test")
        let statusMetadata = OntologyStatusMetadata(
            metadata: metadata,
            status: .ready
        )

        #expect(statusMetadata.status == .ready)
        #expect(statusMetadata.metadata.iri == "http://example.org/test")
    }

    @Test("Ontology statistics")
    func ontologyStatistics() {
        var stats = OntologyStatistics()
        #expect(stats.classCount == 0)
        #expect(stats.propertyCount == 0)

        stats = OntologyStatistics(
            classCount: 10,
            propertyCount: 5,
            axiomCount: 20,
            classHierarchySize: 15,
            propertyHierarchySize: 8
        )

        #expect(stats.classCount == 10)
        #expect(stats.propertyCount == 5)
        #expect(stats.axiomCount == 20)
    }
}

// MARK: - StoredClassDefinition Tests

@Suite("StoredClassDefinition")
struct StoredClassDefinitionTests {

    @Test("Basic class definition")
    func basicClassDefinition() {
        let classDef = StoredClassDefinition(
            iri: "http://example.org/Person",
            label: "Person",
            comment: "A human being",
            directSuperClasses: ["http://www.w3.org/2002/07/owl#Thing"],
            disjointClasses: ["http://example.org/Organization"]
        )

        #expect(classDef.iri == "http://example.org/Person")
        #expect(classDef.label == "Person")
        #expect(classDef.directSuperClasses.contains("http://www.w3.org/2002/07/owl#Thing"))
        #expect(classDef.disjointClasses.contains("http://example.org/Organization"))
        #expect(classDef.isPrimitive == true)
    }

    @Test("Class definition encoding and decoding")
    func classEncodeDecode() throws {
        let classDef = StoredClassDefinition(
            iri: "http://example.org/Employee",
            label: "Employee",
            directSuperClasses: ["http://example.org/Person"],
            equivalentClasses: ["http://example.org/Worker"],
            isPrimitive: false
        )

        let encoded = try classDef.encode()
        let decoded = try StoredClassDefinition.decode(from: Data(encoded))

        #expect(decoded.iri == classDef.iri)
        #expect(decoded.label == classDef.label)
        #expect(decoded.directSuperClasses == classDef.directSuperClasses)
        #expect(decoded.equivalentClasses == classDef.equivalentClasses)
        #expect(decoded.isPrimitive == false)
    }

    @Test("Well-known classes")
    func wellKnownClasses() {
        #expect(StoredClassDefinition.thing.iri == StoredClassDefinition.WellKnown.thing)
        #expect(StoredClassDefinition.thing.isThing)
        #expect(!StoredClassDefinition.thing.isNothing)

        #expect(StoredClassDefinition.nothing.iri == StoredClassDefinition.WellKnown.nothing)
        #expect(StoredClassDefinition.nothing.isNothing)
        #expect(!StoredClassDefinition.nothing.isThing)
    }

    @Test("Factory method from OWLClass")
    func fromOWLClass() {
        let owlClass = OWLClass(
            iri: "http://example.org/Animal",
            label: "Animal",
            comment: "A living creature"
        )

        let stored = StoredClassDefinition.from(owlClass)

        #expect(stored.iri == "http://example.org/Animal")
        #expect(stored.label == "Animal")
        #expect(stored.comment == "A living creature")
    }

    @Test("Modification methods")
    func modificationMethods() {
        var classDef = StoredClassDefinition(iri: "http://example.org/Test")

        classDef.addSuperClass("http://example.org/Parent")
        #expect(classDef.directSuperClasses.contains("http://example.org/Parent"))

        classDef.removeSuperClass("http://example.org/Parent")
        #expect(!classDef.directSuperClasses.contains("http://example.org/Parent"))

        classDef.addEquivalentClass("http://example.org/Equivalent")
        #expect(classDef.equivalentClasses.contains("http://example.org/Equivalent"))

        classDef.addDisjointClass("http://example.org/Disjoint")
        #expect(classDef.disjointClasses.contains("http://example.org/Disjoint"))
    }
}

// MARK: - StoredPropertyDefinition Tests

@Suite("StoredPropertyDefinition")
struct StoredPropertyDefinitionTests {

    @Test("Object property definition")
    func objectPropertyDefinition() {
        let propDef = StoredPropertyDefinition(
            iri: "http://example.org/hasParent",
            type: .objectProperty,
            label: "has parent",
            domains: ["http://example.org/Person"],
            ranges: ["http://example.org/Person"],
            isTransitive: false,
            isSymmetric: false
        )

        #expect(propDef.iri == "http://example.org/hasParent")
        #expect(propDef.type == .objectProperty)
        #expect(propDef.domains.contains("http://example.org/Person"))
        #expect(propDef.ranges.contains("http://example.org/Person"))
    }

    @Test("Property characteristics")
    func propertyCharacteristics() {
        let propDef = StoredPropertyDefinition(
            iri: "http://example.org/ancestorOf",
            type: .objectProperty,
            isFunctional: false,
            isInverseFunctional: false,
            isTransitive: true,
            isSymmetric: false,
            isAsymmetric: true,
            isReflexive: false,
            isIrreflexive: true
        )

        #expect(propDef.isTransitive == true)
        #expect(propDef.isAsymmetric == true)
        #expect(propDef.isIrreflexive == true)
        #expect(propDef.isFunctional == false)
        #expect(propDef.isSymmetric == false)
    }

    @Test("Property with inverse")
    func propertyWithInverse() {
        let propDef = StoredPropertyDefinition(
            iri: "http://example.org/hasChild",
            type: .objectProperty,
            inverseOf: "http://example.org/hasParent"
        )

        #expect(propDef.inverseOf == "http://example.org/hasParent")
    }

    @Test("Property chains")
    func propertyChains() {
        var propDef = StoredPropertyDefinition(
            iri: "http://example.org/hasGrandparent",
            type: .objectProperty
        )

        propDef.addPropertyChain(["http://example.org/hasParent", "http://example.org/hasParent"])

        #expect(propDef.propertyChains.count == 1)
        #expect(propDef.propertyChains[0].count == 2)
    }

    @Test("Property encoding and decoding")
    func propertyEncodeDecode() throws {
        let propDef = StoredPropertyDefinition(
            iri: "http://example.org/knows",
            type: .objectProperty,
            label: "knows",
            isSymmetric: true,
            propertyChains: [["http://example.org/friendOf", "http://example.org/friendOf"]]
        )

        let encoded = try propDef.encode()
        let decoded = try StoredPropertyDefinition.decode(from: Data(encoded))

        #expect(decoded.iri == propDef.iri)
        #expect(decoded.type == .objectProperty)
        #expect(decoded.isSymmetric == true)
        #expect(decoded.propertyChains.count == 1)
    }

    @Test("Data property definition")
    func dataPropertyDefinition() {
        let propDef = StoredPropertyDefinition(
            iri: "http://example.org/hasAge",
            type: .dataProperty,
            label: "has age",
            isFunctional: true
        )

        #expect(propDef.type == .dataProperty)
        #expect(propDef.isFunctional == true)
    }

    @Test("Well-known property IRIs")
    func wellKnownIRIs() {
        #expect(StoredPropertyDefinition.WellKnown.rdfType == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        #expect(StoredPropertyDefinition.WellKnown.rdfsSubClassOf == "http://www.w3.org/2000/01/rdf-schema#subClassOf")
        #expect(StoredPropertyDefinition.WellKnown.owlSameAs == "http://www.w3.org/2002/07/owl#sameAs")
    }

    @Test("Built-in property detection")
    func builtInPropertyDetection() {
        let builtIn = StoredPropertyDefinition(
            iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            type: .objectProperty
        )
        #expect(builtIn.isBuiltIn)

        let custom = StoredPropertyDefinition(
            iri: "http://example.org/hasName",
            type: .dataProperty
        )
        #expect(!custom.isBuiltIn)
    }
}

// MARK: - OWL2RLRules Tests

@Suite("OWL2RLRules")
struct OWL2RLRulesTests {

    @Test("Rule categories")
    func ruleCategories() {
        #expect(OWL2RLRule.eqRef.category == .equality)
        #expect(OWL2RLRule.prpDom.category == .property)
        #expect(OWL2RLRule.clsCom.category == .classExpression)
        #expect(OWL2RLRule.caxSco.category == .classAxiom)
        #expect(OWL2RLRule.scmCls.category == .schemaVocabulary)
    }

    @Test("Rule consistency check capability")
    func ruleConsistencyCapability() {
        // Rules that can produce inconsistency
        #expect(OWL2RLRule.caxDw.canProduceInconsistency)
        #expect(OWL2RLRule.prpIrp.canProduceInconsistency)
        #expect(OWL2RLRule.prpAsyp.canProduceInconsistency)

        // Rules that cannot produce inconsistency directly
        #expect(!OWL2RLRule.caxSco.canProduceInconsistency)
        #expect(!OWL2RLRule.prpDom.canProduceInconsistency)
    }

    @Test("Rules involving sameAs")
    func rulesInvolvingSameAs() {
        #expect(OWL2RLRule.eqRepS.involvesSameAs)
        #expect(OWL2RLRule.eqRepP.involvesSameAs)
        #expect(OWL2RLRule.eqRepO.involvesSameAs)
        #expect(OWL2RLRule.eqDiff1.involvesSameAs)

        #expect(!OWL2RLRule.caxSco.involvesSameAs)
        #expect(!OWL2RLRule.prpDom.involvesSameAs)
    }

    @Test("Recommended strategies")
    func recommendedStrategies() {
        // Materialize strategy
        #expect(OWL2RLRule.caxSco.recommendedStrategy == .materialize)
        #expect(OWL2RLRule.prpInv1.recommendedStrategy == .materialize)

        // Query rewrite strategy
        #expect(OWL2RLRule.prpTrp.recommendedStrategy == .queryRewrite)
        #expect(OWL2RLRule.prpSpo2.recommendedStrategy == .queryRewrite)

        // Union-Find strategy
        #expect(OWL2RLRule.eqRepS.recommendedStrategy == .unionFind)

        // Consistency check strategy
        #expect(OWL2RLRule.caxDw.recommendedStrategy == .consistencyCheck)
    }

    @Test("All rules count")
    func allRulesCount() {
        let allRules = OWL2RLRule.allCases
        // Should have all OWL 2 RL rules
        #expect(allRules.count > 30)
    }

    @Test("Rules by strategy groups")
    func rulesByStrategyGroups() {
        let materializeRules = OWL2RLRule.materializationRules
        let queryRewriteRules = OWL2RLRule.queryRewriteRules
        let unionFindRules = OWL2RLRule.unionFindRules
        let consistencyRules = OWL2RLRule.consistencyRules

        #expect(materializeRules.contains(.caxSco))
        #expect(materializeRules.contains(.prpInv1))
        #expect(queryRewriteRules.contains(.prpTrp))
        #expect(queryRewriteRules.contains(.prpSpo2))
        #expect(unionFindRules.contains(.eqRef))
        #expect(unionFindRules.contains(.eqSym))
        #expect(consistencyRules.contains(.caxDw))
        #expect(consistencyRules.contains(.prpIrp))
    }

    @Test("Rules involving hierarchies")
    func rulesInvolvingHierarchies() {
        #expect(OWL2RLRule.caxSco.involvesClassHierarchy)
        #expect(OWL2RLRule.scmSco.involvesClassHierarchy)
        #expect(!OWL2RLRule.prpDom.involvesClassHierarchy)

        #expect(OWL2RLRule.prpSpo1.involvesPropertyHierarchy)
        #expect(OWL2RLRule.scmSpo.involvesPropertyHierarchy)
        #expect(!OWL2RLRule.caxSco.involvesPropertyHierarchy)
    }
}

// MARK: - InferenceProvenance Tests

@Suite("InferenceProvenance")
struct InferenceProvenanceTests {

    @Test("Triple key creation")
    func tripleKeyCreation() {
        let key = TripleKey(
            subject: "ex:Alice",
            predicate: "rdf:type",
            object: "ex:Person"
        )

        #expect(key.subject == "ex:Alice")
        #expect(key.predicate == "rdf:type")
        #expect(key.object == "ex:Person")
    }

    @Test("Triple key convenience initializer")
    func tripleKeyConvenience() {
        let key = TripleKey("ex:Bob", "ex:knows", "ex:Alice")

        #expect(key.subject == "ex:Bob")
        #expect(key.predicate == "ex:knows")
        #expect(key.object == "ex:Alice")
    }

    @Test("Triple key hashable")
    func tripleKeyHashable() {
        let key1 = TripleKey("ex:A", "ex:p", "ex:B")
        let key2 = TripleKey("ex:A", "ex:p", "ex:B")
        let key3 = TripleKey("ex:A", "ex:p", "ex:C")

        #expect(key1 == key2)
        #expect(key1 != key3)

        let set: Set<TripleKey> = [key1, key2, key3]
        #expect(set.count == 2)
    }

    @Test("Inference provenance creation")
    func inferenceProvenanceCreation() {
        let antecedent1 = TripleKey("ex:Alice", "rdf:type", "ex:Employee")
        let antecedent2 = TripleKey("ex:Employee", "rdfs:subClassOf", "ex:Person")

        let provenance = InferenceProvenance(
            rule: .caxSco,
            antecedents: [antecedent1, antecedent2],
            depth: 1
        )

        #expect(provenance.rule == .caxSco)
        #expect(provenance.antecedents.count == 2)
        #expect(provenance.isValid == true)
        #expect(provenance.depth == 1)
        #expect(!provenance.isExplicit)
    }

    @Test("Explicit assertion provenance")
    func explicitAssertionProvenance() {
        let provenance = InferenceProvenance.asserted()

        #expect(provenance.antecedents.isEmpty)
        #expect(provenance.depth == 0)
        #expect(provenance.isExplicit)
    }

    @Test("Provenance encoding and decoding")
    func provenanceEncodeDecode() throws {
        let provenance = InferenceProvenance(
            rule: .prpInv1,
            antecedents: [TripleKey("ex:A", "ex:p", "ex:B")],
            depth: 2
        )

        let encoded = try provenance.encode()
        let decoded = try InferenceProvenance.decode(from: encoded)

        #expect(decoded.rule == .prpInv1)
        #expect(decoded.antecedents.count == 1)
        #expect(decoded.depth == 2)
    }

    @Test("Inference result")
    func inferenceResult() {
        var result = InferenceResult()

        #expect(result.isEmpty)
        #expect(!result.hasInconsistencies)

        result.inferred.append((
            triple: TripleKey("ex:A", "rdf:type", "ex:B"),
            provenance: InferenceProvenance(rule: .caxSco, antecedents: [])
        ))

        #expect(!result.isEmpty)

        result.inconsistencies.append(InconsistencyReport(
            rule: .caxDw,
            involvedTriples: [],
            description: "Test inconsistency"
        ))

        #expect(result.hasInconsistencies)
    }

    @Test("Deletion status")
    func deletionStatus() {
        #expect(DeletionStatus.valid.rawValue == "valid")
        #expect(DeletionStatus.tentativelyDeleted.rawValue == "tentativelyDeleted")
        #expect(DeletionStatus.deleted.rawValue == "deleted")
        #expect(DeletionStatus.rederived.rawValue == "rederived")
    }

    @Test("DRed deletion result")
    func dredDeletionResult() {
        let result = DRedDeletionResult(
            permanentlyDeleted: [TripleKey("ex:A", "ex:p", "ex:B")],
            rederived: [TripleKey("ex:C", "ex:q", "ex:D")],
            cascadingChecks: 10,
            maintenanceTime: 0.5
        )

        #expect(result.permanentlyDeleted.count == 1)
        #expect(result.rederived.count == 1)
        #expect(result.cascadingChecks == 10)
    }
}

// MARK: - DependencyGraph Tests

@Suite("DependencyGraph")
struct DependencyGraphTests {

    @Test("Add dependency")
    func addDependency() {
        var graph = DependencyGraph()

        let antecedent = TripleKey("ex:A", "ex:p", "ex:B")
        let consequent = TripleKey("ex:C", "ex:q", "ex:D")

        graph.addDependency(antecedent: antecedent, consequent: consequent)

        #expect(graph.getDependents(of: antecedent).contains(consequent))
        #expect(graph.getDependencies(of: consequent).contains(antecedent))
    }

    @Test("Get transitive dependents")
    func getTransitiveDependents() {
        var graph = DependencyGraph()

        let t1 = TripleKey("ex:A", "ex:p", "ex:B")
        let t2 = TripleKey("ex:C", "ex:q", "ex:D")
        let t3 = TripleKey("ex:E", "ex:r", "ex:F")

        graph.addDependency(antecedent: t1, consequent: t2)
        graph.addDependency(antecedent: t2, consequent: t3)

        let transitive = graph.getTransitiveDependents(of: t1)

        #expect(transitive.contains(t2))
        #expect(transitive.contains(t3))
        #expect(!transitive.contains(t1))
    }

    @Test("Remove triple")
    func removeTriple() {
        var graph = DependencyGraph()

        let t1 = TripleKey("ex:A", "ex:p", "ex:B")
        let t2 = TripleKey("ex:C", "ex:q", "ex:D")
        let t3 = TripleKey("ex:E", "ex:r", "ex:F")

        graph.addDependency(antecedent: t1, consequent: t2)
        graph.addDependency(antecedent: t2, consequent: t3)

        graph.remove(t2)

        #expect(graph.getDependents(of: t1).isEmpty)
        #expect(graph.getDependencies(of: t3).isEmpty)
        #expect(graph.getDependents(of: t2).isEmpty)
        #expect(graph.getDependencies(of: t2).isEmpty)
    }
}

// MARK: - InferenceStatistics Tests

@Suite("InferenceStatistics")
struct InferenceStatisticsTests {

    @Test("Default statistics")
    func defaultStatistics() {
        let stats = InferenceStatistics()

        #expect(stats.ruleApplications == 0)
        #expect(stats.triplesInferred == 0)
        #expect(stats.duplicateInferences == 0)
        #expect(stats.inconsistenciesDetected == 0)
        #expect(stats.inferenceTime == 0)
        #expect(stats.triplesExamined == 0)
    }

    @Test("Statistics mutation")
    func statisticsMutation() {
        var stats = InferenceStatistics()

        stats.ruleApplications = 100
        stats.triplesInferred = 50
        stats.duplicateInferences = 10
        stats.inconsistenciesDetected = 2
        stats.inferenceTime = 1.5
        stats.triplesExamined = 1000

        #expect(stats.ruleApplications == 100)
        #expect(stats.triplesInferred == 50)
        #expect(stats.duplicateInferences == 10)
        #expect(stats.inconsistenciesDetected == 2)
        #expect(stats.inferenceTime == 1.5)
        #expect(stats.triplesExamined == 1000)
    }
}
