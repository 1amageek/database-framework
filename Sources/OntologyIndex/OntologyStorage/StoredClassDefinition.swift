// StoredClassDefinition.swift
// GraphIndex - Persistent class (TBox) storage
//
// Provides stored representations of OWL class definitions.
//
// Reference: W3C OWL 2 Syntax https://www.w3.org/TR/owl2-syntax/#Classes

import Foundation
import Graph

/// Stored class definition for persistent TBox storage
///
/// Stores class information in a format optimized for:
/// - Efficient lookup by IRI
/// - Fast hierarchy traversal
/// - Incremental updates
///
/// **Design Note**: This is a flattened representation separate from
/// `OWLClass` and `OWLClassExpression` in Graph module. Complex class
/// expressions are stored as encoded axioms, not class definitions.
///
/// **Example**:
/// ```swift
/// let classDef = StoredClassDefinition(
///     iri: "http://example.org/Person",
///     label: "Person",
///     directSuperClasses: ["http://www.w3.org/2002/07/owl#Thing"],
///     disjointClasses: ["http://example.org/Organization"]
/// )
/// ```
public struct StoredClassDefinition: Codable, Sendable, Hashable {

    // MARK: - Identity

    /// Class IRI (unique identifier)
    public let iri: String

    /// Human-readable label (rdfs:label)
    public var label: String?

    /// Description (rdfs:comment)
    public var comment: String?

    // MARK: - Hierarchy (Materialized)

    /// Direct superclasses (rdfs:subClassOf)
    ///
    /// Only contains immediate parents, not transitive closure.
    /// Transitive closure is stored separately in class hierarchy index.
    public var directSuperClasses: Set<String>

    /// Equivalent classes (owl:equivalentClass)
    ///
    /// All classes in the equivalence set (symmetric).
    public var equivalentClasses: Set<String>

    /// Disjoint classes (owl:disjointWith)
    ///
    /// Classes that cannot have common instances.
    public var disjointClasses: Set<String>

    // MARK: - Metadata

    /// Whether this is a primitive or defined class
    public var isPrimitive: Bool

    /// Whether this class is deprecated (owl:deprecated)
    public var isDeprecated: Bool

    /// Annotations (key-value pairs)
    public var annotations: [String: String]

    // MARK: - Initialization

    public init(
        iri: String,
        label: String? = nil,
        comment: String? = nil,
        directSuperClasses: Set<String> = [],
        equivalentClasses: Set<String> = [],
        disjointClasses: Set<String> = [],
        isPrimitive: Bool = true,
        isDeprecated: Bool = false,
        annotations: [String: String] = [:]
    ) {
        self.iri = iri
        self.label = label
        self.comment = comment
        self.directSuperClasses = directSuperClasses
        self.equivalentClasses = equivalentClasses
        self.disjointClasses = disjointClasses
        self.isPrimitive = isPrimitive
        self.isDeprecated = isDeprecated
        self.annotations = annotations
    }

    // MARK: - Factory Methods

    /// Create from OWLClass
    public static func from(_ owlClass: OWLClass) -> StoredClassDefinition {
        StoredClassDefinition(
            iri: owlClass.iri,
            label: owlClass.label,
            comment: owlClass.comment,
            annotations: owlClass.annotations
        )
    }

    /// Create owl:Thing (top class)
    public static var thing: StoredClassDefinition {
        StoredClassDefinition(
            iri: "http://www.w3.org/2002/07/owl#Thing",
            label: "Thing",
            comment: "The top class"
        )
    }

    /// Create owl:Nothing (bottom class)
    public static var nothing: StoredClassDefinition {
        StoredClassDefinition(
            iri: "http://www.w3.org/2002/07/owl#Nothing",
            label: "Nothing",
            comment: "The empty class",
            directSuperClasses: ["http://www.w3.org/2002/07/owl#Thing"]
        )
    }

    // MARK: - Modification

    /// Add a superclass
    public mutating func addSuperClass(_ superClassIRI: String) {
        directSuperClasses.insert(superClassIRI)
    }

    /// Remove a superclass
    public mutating func removeSuperClass(_ superClassIRI: String) {
        directSuperClasses.remove(superClassIRI)
    }

    /// Add an equivalent class
    public mutating func addEquivalentClass(_ classIRI: String) {
        equivalentClasses.insert(classIRI)
    }

    /// Add a disjoint class
    public mutating func addDisjointClass(_ classIRI: String) {
        disjointClasses.insert(classIRI)
    }
}

// MARK: - Encoding/Decoding

extension StoredClassDefinition {
    /// Encode to JSON bytes
    public func encode() throws -> Data {
        try JSONEncoder().encode(self)
    }

    /// Decode from JSON bytes
    public static func decode(from data: Data) throws -> StoredClassDefinition {
        try JSONDecoder().decode(StoredClassDefinition.self, from: data)
    }
}

// MARK: - Well-Known IRIs

public extension StoredClassDefinition {
    /// Well-known class IRIs
    enum WellKnown {
        public static let thing = "http://www.w3.org/2002/07/owl#Thing"
        public static let nothing = "http://www.w3.org/2002/07/owl#Nothing"
    }

    /// Check if this is owl:Thing
    var isThing: Bool {
        iri == WellKnown.thing
    }

    /// Check if this is owl:Nothing
    var isNothing: Bool {
        iri == WellKnown.nothing
    }

    /// Check if this is a built-in class
    var isBuiltIn: Bool {
        isThing || isNothing
    }
}
