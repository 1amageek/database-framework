// OntologySubspace.swift
// GraphIndex - Subspace key definitions for ontology storage
//
// Defines the key layout for persistent ontology storage in FoundationDB.
//
// Reference: W3C OWL 2 Profiles https://www.w3.org/TR/owl2-profiles/

import Foundation
import FoundationDB

/// Subspace key definitions for ontology storage
///
/// **Key Layout**:
/// ```
/// [fdb]/O/[ontologyIRI]/
/// ├── 0 (metadata)     → OntologyMetadata
/// ├── 1 (classes)      → [classIRI] → StoredClassDefinition
/// ├── 2 (properties)   → [propertyIRI] → StoredPropertyDefinition
/// ├── 3 (axioms)       → [axiomID] → EncodedAxiom
/// ├── 4 (classHier)    → Class hierarchy index
/// │   ├── 0 (super)    → [subClass]/[superClass]
/// │   └── 1 (sub)      → [superClass]/[subClass]
/// ├── 5 (propHier)     → Property hierarchy index
/// │   ├── 0 (super)    → [subProp]/[superProp]
/// │   └── 1 (sub)      → [superProp]/[subProp]
/// ├── 6 (inverse)      → [prop]/[inverseProp]
/// ├── 7 (transitive)   → [prop] → empty (marker)
/// ├── 8 (chains)       → [targetProp]/[chainID] → [prop1, prop2, ...]
/// └── 9 (sameAs)       → Union-Find structure
/// ```
public enum OntologySubspaceKey: Int, Sendable {
    /// Ontology metadata (IRI, version, imports, prefixes)
    case metadata = 0

    /// Class definitions (TBox)
    case classes = 1

    /// Property definitions (RBox)
    case properties = 2

    /// Encoded axioms
    case axioms = 3

    /// Class hierarchy (pre-computed transitive closure)
    case classHierarchy = 4

    /// Property hierarchy (pre-computed transitive closure)
    case propertyHierarchy = 5

    /// Inverse property mappings
    case inverse = 6

    /// Transitive property markers
    case transitive = 7

    /// Property chain axioms
    case chains = 8

    /// owl:sameAs Union-Find structure
    case sameAs = 9
}

/// Sub-keys for hierarchy indices
public enum HierarchySubspaceKey: Int, Sendable {
    /// Superclass/superproperty lookup: given X, find all Y where X ⊑ Y
    case superOf = 0

    /// Subclass/subproperty lookup: given Y, find all X where X ⊑ Y
    case subOf = 1
}

/// Union-Find subspace keys for owl:sameAs
public enum UnionFindSubspaceKey: Int, Sendable {
    /// Parent pointers: [individual] → parent individual
    case parent = 0

    /// Rank for union by rank optimization
    case rank = 1

    /// Members index: [canonical representative] → Set of members
    case members = 2
}

/// Ontology storage subspace builder
public struct OntologySubspace: Sendable {

    /// Base subspace for all ontologies
    public let base: Subspace

    /// Create ontology subspace from base subspace
    ///
    /// - Parameter base: The base subspace (typically container's subspace + "O")
    public init(base: Subspace) {
        self.base = base
    }

    // MARK: - Ontology-Specific Subspaces

    /// Get subspace for a specific ontology
    ///
    /// - Parameter ontologyIRI: The ontology IRI
    /// - Returns: Subspace for the ontology
    public func ontology(_ ontologyIRI: String) -> Subspace {
        base.subspace(ontologyIRI)
    }

    /// Get metadata subspace for an ontology
    public func metadata(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.metadata.rawValue)
    }

    /// Get classes subspace for an ontology
    public func classes(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.classes.rawValue)
    }

    /// Get properties subspace for an ontology
    public func properties(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.properties.rawValue)
    }

    /// Get axioms subspace for an ontology
    public func axioms(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.axioms.rawValue)
    }

    // MARK: - Hierarchy Subspaces

    /// Get class hierarchy subspace for an ontology
    public func classHierarchy(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.classHierarchy.rawValue)
    }

    /// Get superclass index: given subclass, find superclasses
    public func classSuperOf(_ ontologyIRI: String) -> Subspace {
        classHierarchy(ontologyIRI).subspace(HierarchySubspaceKey.superOf.rawValue)
    }

    /// Get subclass index: given superclass, find subclasses
    public func classSubOf(_ ontologyIRI: String) -> Subspace {
        classHierarchy(ontologyIRI).subspace(HierarchySubspaceKey.subOf.rawValue)
    }

    /// Get property hierarchy subspace for an ontology
    public func propertyHierarchy(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.propertyHierarchy.rawValue)
    }

    /// Get superproperty index: given subproperty, find superproperties
    public func propertySuperOf(_ ontologyIRI: String) -> Subspace {
        propertyHierarchy(ontologyIRI).subspace(HierarchySubspaceKey.superOf.rawValue)
    }

    /// Get subproperty index: given superproperty, find subproperties
    public func propertySubOf(_ ontologyIRI: String) -> Subspace {
        propertyHierarchy(ontologyIRI).subspace(HierarchySubspaceKey.subOf.rawValue)
    }

    // MARK: - Property Characteristics

    /// Get inverse property mappings subspace
    public func inverse(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.inverse.rawValue)
    }

    /// Get transitive property markers subspace
    public func transitive(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.transitive.rawValue)
    }

    /// Get property chain axioms subspace
    public func chains(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.chains.rawValue)
    }

    // MARK: - owl:sameAs Union-Find

    /// Get sameAs Union-Find base subspace
    public func sameAs(_ ontologyIRI: String) -> Subspace {
        ontology(ontologyIRI).subspace(OntologySubspaceKey.sameAs.rawValue)
    }

    /// Get Union-Find parent pointers subspace
    public func sameAsParent(_ ontologyIRI: String) -> Subspace {
        sameAs(ontologyIRI).subspace(UnionFindSubspaceKey.parent.rawValue)
    }

    /// Get Union-Find rank subspace
    public func sameAsRank(_ ontologyIRI: String) -> Subspace {
        sameAs(ontologyIRI).subspace(UnionFindSubspaceKey.rank.rawValue)
    }

    /// Get Union-Find members index subspace
    public func sameAsMembers(_ ontologyIRI: String) -> Subspace {
        sameAs(ontologyIRI).subspace(UnionFindSubspaceKey.members.rawValue)
    }

    // MARK: - Key Builders

    /// Build key for class definition
    public func classKey(_ ontologyIRI: String, classIRI: String) -> FDB.Bytes {
        classes(ontologyIRI).pack(Tuple(classIRI))
    }

    /// Build key for property definition
    public func propertyKey(_ ontologyIRI: String, propertyIRI: String) -> FDB.Bytes {
        properties(ontologyIRI).pack(Tuple(propertyIRI))
    }

    /// Build key for axiom
    public func axiomKey(_ ontologyIRI: String, axiomID: String) -> FDB.Bytes {
        axioms(ontologyIRI).pack(Tuple(axiomID))
    }

    /// Build key for class hierarchy entry (superclass lookup)
    public func classSuperOfKey(_ ontologyIRI: String, subClass: String, superClass: String) -> FDB.Bytes {
        classSuperOf(ontologyIRI).pack(Tuple(subClass, superClass))
    }

    /// Build key for class hierarchy entry (subclass lookup)
    public func classSubOfKey(_ ontologyIRI: String, superClass: String, subClass: String) -> FDB.Bytes {
        classSubOf(ontologyIRI).pack(Tuple(superClass, subClass))
    }

    /// Build key for property hierarchy entry (superproperty lookup)
    public func propertySuperOfKey(_ ontologyIRI: String, subProp: String, superProp: String) -> FDB.Bytes {
        propertySuperOf(ontologyIRI).pack(Tuple(subProp, superProp))
    }

    /// Build key for property hierarchy entry (subproperty lookup)
    public func propertySubOfKey(_ ontologyIRI: String, superProp: String, subProp: String) -> FDB.Bytes {
        propertySubOf(ontologyIRI).pack(Tuple(superProp, subProp))
    }

    /// Build key for inverse property mapping
    public func inverseKey(_ ontologyIRI: String, property: String) -> FDB.Bytes {
        inverse(ontologyIRI).pack(Tuple(property))
    }

    /// Build key for transitive property marker
    public func transitiveKey(_ ontologyIRI: String, property: String) -> FDB.Bytes {
        transitive(ontologyIRI).pack(Tuple(property))
    }

    /// Build key for property chain
    public func chainKey(_ ontologyIRI: String, targetProperty: String, chainID: Int) -> FDB.Bytes {
        chains(ontologyIRI).pack(Tuple(targetProperty, chainID))
    }

    /// Build key for Union-Find parent pointer
    public func sameAsParentKey(_ ontologyIRI: String, individual: String) -> FDB.Bytes {
        sameAsParent(ontologyIRI).pack(Tuple(individual))
    }

    /// Build key for Union-Find rank
    public func sameAsRankKey(_ ontologyIRI: String, individual: String) -> FDB.Bytes {
        sameAsRank(ontologyIRI).pack(Tuple(individual))
    }

    /// Build key for Union-Find member entry
    public func sameAsMemberKey(_ ontologyIRI: String, representative: String, member: String) -> FDB.Bytes {
        sameAsMembers(ontologyIRI).pack(Tuple(representative, member))
    }
}
