// HierarchyTests.swift
// Tests for ClassHierarchy and RoleHierarchy

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - ClassHierarchy Tests

@Suite("ClassHierarchy Basic Operations")
struct ClassHierarchyBasicTests {

    @Test("Add subsumption relationship")
    func addSubsumption() {
        var hierarchy = ClassHierarchy()

        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Animal")

        #expect(hierarchy.directSuperClasses(of: "ex:Dog").contains("ex:Animal"))
        #expect(hierarchy.directSubClasses(of: "ex:Animal").contains("ex:Dog"))
    }

    @Test("Transitive closure of subsumption")
    func transitiveSubsumption() {
        var hierarchy = ClassHierarchy()

        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Mammal")
        hierarchy.addSubsumption(subClass: "ex:Mammal", superClass: "ex:Animal")

        let dogSupers = hierarchy.superClasses(of: "ex:Dog")
        #expect(dogSupers.contains("ex:Mammal"))
        #expect(dogSupers.contains("ex:Animal"))
    }

    @Test("Add equivalence relationship")
    func addEquivalence() {
        var hierarchy = ClassHierarchy()

        hierarchy.addEquivalence(class1: "ex:Person", class2: "ex:Human")

        // Both should have each other as equivalent
        #expect(hierarchy.equivalentClasses(of: "ex:Person").contains("ex:Human"))
        #expect(hierarchy.equivalentClasses(of: "ex:Human").contains("ex:Person"))
    }

    @Test("Add disjoint relationship")
    func addDisjoint() {
        var hierarchy = ClassHierarchy()

        hierarchy.addDisjoint(class1: "ex:Dog", class2: "ex:Cat")

        #expect(hierarchy.areDisjoint("ex:Dog", "ex:Cat"))
        #expect(hierarchy.areDisjoint("ex:Cat", "ex:Dog"))
    }

    @Test("Non-disjoint classes")
    func nonDisjoint() {
        var hierarchy = ClassHierarchy()

        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Animal")

        #expect(!hierarchy.areDisjoint("ex:Dog", "ex:Animal"))
    }

    @Test("Direct subclasses only")
    func directSubclasses() {
        var hierarchy = ClassHierarchy()

        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Mammal")
        hierarchy.addSubsumption(subClass: "ex:Mammal", superClass: "ex:Animal")

        let directSubs = hierarchy.directSubClasses(of: "ex:Animal")
        #expect(directSubs.contains("ex:Mammal"))
        #expect(!directSubs.contains("ex:Dog"))  // Dog is indirect
    }

    @Test("Direct superclasses only")
    func directSuperclasses() {
        var hierarchy = ClassHierarchy()

        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Mammal")
        hierarchy.addSubsumption(subClass: "ex:Mammal", superClass: "ex:Animal")

        let directSupers = hierarchy.directSuperClasses(of: "ex:Dog")
        #expect(directSupers.contains("ex:Mammal"))
        #expect(!directSupers.contains("ex:Animal"))  // Animal is indirect
    }
}

@Suite("ClassHierarchy Subsumption Checking")
struct ClassHierarchySubsumptionTests {

    @Test("Direct subsumption holds")
    func directSubsumption() {
        var hierarchy = ClassHierarchy()
        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Animal")

        let result = hierarchy.subsumes("ex:Animal", "ex:Dog")
        #expect(result)
    }

    @Test("Transitive subsumption holds")
    func transitiveSubsumption() {
        var hierarchy = ClassHierarchy()
        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Mammal")
        hierarchy.addSubsumption(subClass: "ex:Mammal", superClass: "ex:Animal")

        let result = hierarchy.subsumes("ex:Animal", "ex:Dog")
        #expect(result)
    }

    @Test("Non-subsumption returns false")
    func nonSubsumption() {
        var hierarchy = ClassHierarchy()
        hierarchy.addSubsumption(subClass: "ex:Dog", superClass: "ex:Animal")

        let result = hierarchy.subsumes("ex:Dog", "ex:Animal")
        #expect(!result)
    }

    @Test("Same class subsumes itself")
    func reflexiveSubsumption() {
        var hierarchy = ClassHierarchy()
        let result = hierarchy.subsumes("ex:Dog", "ex:Dog")
        #expect(result)
    }
}

// MARK: - RoleHierarchy Tests

@Suite("RoleHierarchy Basic Operations")
struct RoleHierarchyBasicTests {

    @Test("Add sub-role relationship")
    func addSubRole() {
        var hierarchy = RoleHierarchy()

        hierarchy.addSubRole(sub: "ex:hasSon", super: "ex:hasChild")

        #expect(hierarchy.directSuperRoles(of: "ex:hasSon").contains("ex:hasChild"))
        #expect(hierarchy.directSubRoles(of: "ex:hasChild").contains("ex:hasSon"))
    }

    @Test("Transitive role hierarchy")
    func transitiveRoleHierarchy() {
        var hierarchy = RoleHierarchy()

        hierarchy.addSubRole(sub: "ex:hasSon", super: "ex:hasChild")
        hierarchy.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")

        let supers = hierarchy.superRoles(of: "ex:hasSon")
        #expect(supers.contains("ex:hasChild"))
        #expect(supers.contains("ex:hasRelative"))
    }

    @Test("Set inverse relationship")
    func setInverse() {
        var hierarchy = RoleHierarchy()

        hierarchy.setInverse("ex:hasChild", "ex:hasParent")

        #expect(hierarchy.inverse(of: "ex:hasChild") == "ex:hasParent")
        #expect(hierarchy.inverse(of: "ex:hasParent") == "ex:hasChild")
    }

    @Test("Inverse is symmetric")
    func inverseSymmetric() {
        var hierarchy = RoleHierarchy()
        hierarchy.setInverse("ex:hasChild", "ex:hasParent")

        // Setting in one direction should set both
        #expect(hierarchy.inverse(of: "ex:hasChild") == "ex:hasParent")
        #expect(hierarchy.inverse(of: "ex:hasParent") == "ex:hasChild")
    }
}

@Suite("RoleHierarchy Characteristics")
struct RoleHierarchyCharacteristicsTests {

    @Test("Set transitive characteristic")
    func setTransitive() {
        var hierarchy = RoleHierarchy()

        hierarchy.setCharacteristic(.transitive, for: "ex:ancestorOf", value: true)

        #expect(hierarchy.isTransitive("ex:ancestorOf"))
    }

    @Test("Set symmetric characteristic")
    func setSymmetric() {
        var hierarchy = RoleHierarchy()

        hierarchy.setCharacteristic(.symmetric, for: "ex:knows", value: true)

        #expect(hierarchy.isSymmetric("ex:knows"))
    }

    @Test("Set functional characteristic")
    func setFunctional() {
        var hierarchy = RoleHierarchy()

        hierarchy.setCharacteristic(.functional, for: "ex:hasMother", value: true)

        #expect(hierarchy.isFunctional("ex:hasMother"))
    }

    @Test("Set inverse functional characteristic")
    func setInverseFunctional() {
        var hierarchy = RoleHierarchy()

        hierarchy.setCharacteristic(.inverseFunctional, for: "ex:socialSecurityNumber", value: true)

        #expect(hierarchy.isInverseFunctional("ex:socialSecurityNumber"))
    }

    @Test("Set reflexive characteristic")
    func setReflexive() {
        var hierarchy = RoleHierarchy()

        hierarchy.setCharacteristic(.reflexive, for: "ex:knows", value: true)

        #expect(hierarchy.isReflexive("ex:knows"))
    }

    @Test("Set irreflexive characteristic")
    func setIrreflexive() {
        var hierarchy = RoleHierarchy()

        hierarchy.setCharacteristic(.irreflexive, for: "ex:parentOf", value: true)

        #expect(hierarchy.isIrreflexive("ex:parentOf"))
    }

    @Test("Set asymmetric characteristic")
    func setAsymmetric() {
        var hierarchy = RoleHierarchy()

        hierarchy.setCharacteristic(.asymmetric, for: "ex:parentOf", value: true)

        #expect(hierarchy.isAsymmetric("ex:parentOf"))
    }

    @Test("Characteristics default to false")
    func defaultCharacteristics() {
        let hierarchy = RoleHierarchy()

        #expect(!hierarchy.isTransitive("ex:unknownRole"))
        #expect(!hierarchy.isFunctional("ex:unknownRole"))
        #expect(!hierarchy.isSymmetric("ex:unknownRole"))
    }
}

@Suite("RoleHierarchy Domain and Range")
struct RoleHierarchyDomainRangeTests {

    @Test("Set domain")
    func setDomain() {
        var hierarchy = RoleHierarchy()

        hierarchy.setDomain(for: "ex:hasChild", domain: "ex:Parent")

        let domains = hierarchy.domains(of: "ex:hasChild")
        #expect(domains.count == 1)
        if case .named(let iri) = domains.first {
            #expect(iri == "ex:Parent")
        }
    }

    @Test("Set range")
    func setRange() {
        var hierarchy = RoleHierarchy()

        hierarchy.setRange(for: "ex:hasChild", range: "ex:Person")

        let ranges = hierarchy.ranges(of: "ex:hasChild")
        #expect(ranges.count == 1)
        if case .named(let iri) = ranges.first {
            #expect(iri == "ex:Person")
        }
    }
}

@Suite("RoleHierarchy Property Chains")
struct RoleHierarchyPropertyChainTests {

    @Test("Add property chain")
    func addPropertyChain() {
        var hierarchy = RoleHierarchy()

        // hasGrandparent ⊑ hasParent ∘ hasParent
        hierarchy.addPropertyChain(
            ["ex:hasParent", "ex:hasParent"],
            implies: "ex:hasGrandparent"
        )

        let chains = hierarchy.propertyChains(implying: "ex:hasGrandparent")
        #expect(chains.count == 1)
        #expect(chains.first == ["ex:hasParent", "ex:hasParent"])
    }

    @Test("Multiple chains for same role")
    func multipleChains() {
        var hierarchy = RoleHierarchy()

        // hasAncestor ⊑ hasParent ∘ hasParent
        hierarchy.addPropertyChain(
            ["ex:hasParent", "ex:hasParent"],
            implies: "ex:hasAncestor"
        )

        // hasAncestor ⊑ hasParent ∘ hasAncestor
        hierarchy.addPropertyChain(
            ["ex:hasParent", "ex:hasAncestor"],
            implies: "ex:hasAncestor"
        )

        let chains = hierarchy.propertyChains(implying: "ex:hasAncestor")
        #expect(chains.count == 2)
    }

    @Test("All property chains")
    func allPropertyChains() {
        var hierarchy = RoleHierarchy()

        hierarchy.addPropertyChain(["ex:a", "ex:b"], implies: "ex:c")
        hierarchy.addPropertyChain(["ex:d", "ex:e"], implies: "ex:f")

        let allChains = hierarchy.allPropertyChains()
        #expect(allChains.count == 2)
    }
}

@Suite("RoleHierarchy Sub-role Checking")
struct RoleHierarchySubRoleTests {

    @Test("Direct sub-role check")
    func directSubRole() {
        var hierarchy = RoleHierarchy()
        hierarchy.addSubRole(sub: "ex:hasSon", super: "ex:hasChild")

        let result = hierarchy.isSubRoleOf(sub: "ex:hasSon", super: "ex:hasChild")
        #expect(result)
    }

    @Test("Transitive sub-role check")
    func transitiveSubRole() {
        var hierarchy = RoleHierarchy()
        hierarchy.addSubRole(sub: "ex:hasSon", super: "ex:hasChild")
        hierarchy.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")

        let result = hierarchy.isSubRoleOf(sub: "ex:hasSon", super: "ex:hasRelative")
        #expect(result)
    }

    @Test("Non sub-role returns false")
    func nonSubRole() {
        var hierarchy = RoleHierarchy()
        hierarchy.addSubRole(sub: "ex:hasSon", super: "ex:hasChild")

        let result = hierarchy.isSubRoleOf(sub: "ex:hasChild", super: "ex:hasSon")
        #expect(!result)
    }

    @Test("Same role is sub-role of itself")
    func reflexiveSubRole() {
        var hierarchy = RoleHierarchy()
        let result = hierarchy.isSubRoleOf(sub: "ex:hasChild", super: "ex:hasChild")
        #expect(result)
    }
}

// MARK: - Combined Hierarchy Tests

@Suite("Combined Hierarchy Operations")
struct CombinedHierarchyTests {

    @Test("Build from ontology")
    func buildFromOntology() {
        var ontology = OWLOntology(iri: "http://test.org/combined")

        // Classes
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Dog"),
            sup: .named("ex:Animal")
        ))

        // Properties
        var hasChild = OWLObjectProperty(iri: "ex:hasChild")
        hasChild.characteristics.insert(.transitive)
        ontology.objectProperties.append(hasChild)

        ontology.axioms.append(.subObjectPropertyOf(
            sub: "ex:hasSon",
            sup: "ex:hasChild"
        ))

        // Build hierarchies from ontology
        let classHierarchy = ClassHierarchy(ontology: ontology)
        let roleHierarchy = RoleHierarchy(ontology: ontology)

        // Verify class hierarchy
        #expect(classHierarchy.directSuperClasses(of: "ex:Dog").contains("ex:Animal"))

        // Verify role hierarchy
        #expect(roleHierarchy.directSuperRoles(of: "ex:hasSon").contains("ex:hasChild"))
        #expect(roleHierarchy.isTransitive("ex:hasChild"))
    }
}
