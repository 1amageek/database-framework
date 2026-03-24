// StoredPropertyDefinition.swift
// GraphIndex - Persistent property (RBox) storage
//
// Provides stored representations of OWL property definitions.
//
// Reference: W3C OWL 2 Syntax https://www.w3.org/TR/owl2-syntax/#Properties

import Foundation
import Graph

/// Property type enumeration
public enum StoredPropertyType: String, Codable, Sendable {
    /// Object property (relates individuals to individuals)
    case objectProperty

    /// Data property (relates individuals to literals)
    case dataProperty

    /// Annotation property (for metadata)
    case annotationProperty
}

/// Stored property definition for persistent RBox storage
///
/// Stores property information in a format optimized for:
/// - Efficient lookup by IRI
/// - Fast hierarchy traversal
/// - Property chain evaluation
/// - Inverse property lookup
///
/// **Design Note**: Property characteristics (transitive, symmetric, etc.)
/// are stored directly here. Property chains and hierarchy are also
/// indexed separately for efficient reasoning.
///
/// **Example**:
/// ```swift
/// let propDef = StoredPropertyDefinition(
///     iri: "http://example.org/hasParent",
///     type: .objectProperty,
///     domains: ["http://example.org/Person"],
///     ranges: ["http://example.org/Person"],
///     inverseOf: "http://example.org/hasChild"
/// )
/// ```
public struct StoredPropertyDefinition: Codable, Sendable, Hashable {

    // MARK: - Identity

    /// Property IRI (unique identifier)
    public let iri: String

    /// Property type (object, data, annotation)
    public let type: StoredPropertyType

    /// Human-readable label (rdfs:label)
    public var label: String?

    /// Description (rdfs:comment)
    public var comment: String?

    // MARK: - Domain and Range

    /// Domain classes (rdfs:domain)
    public var domains: Set<String>

    /// Range classes/datatypes (rdfs:range)
    ///
    /// For object properties: class IRIs
    /// For data properties: datatype IRIs
    public var ranges: Set<String>

    // MARK: - Hierarchy (Materialized)

    /// Direct superproperties (rdfs:subPropertyOf)
    public var directSuperProperties: Set<String>

    /// Equivalent properties (owl:equivalentProperty)
    public var equivalentProperties: Set<String>

    /// Disjoint properties (owl:propertyDisjointWith)
    public var disjointProperties: Set<String>

    // MARK: - Property Characteristics (OWL 2 RL)

    /// Whether property is functional
    ///
    /// Functional: at most one value for each subject
    /// owl:FunctionalProperty
    public var isFunctional: Bool

    /// Whether property is inverse functional
    ///
    /// Inverse functional: at most one subject for each value
    /// owl:InverseFunctionalProperty
    public var isInverseFunctional: Bool

    /// Whether property is transitive
    ///
    /// Transitive: if (a,b) and (b,c) then (a,c)
    /// owl:TransitiveProperty
    public var isTransitive: Bool

    /// Whether property is symmetric
    ///
    /// Symmetric: if (a,b) then (b,a)
    /// owl:SymmetricProperty
    public var isSymmetric: Bool

    /// Whether property is asymmetric
    ///
    /// Asymmetric: if (a,b) then NOT (b,a)
    /// owl:AsymmetricProperty
    public var isAsymmetric: Bool

    /// Whether property is reflexive
    ///
    /// Reflexive: for all a, (a,a) holds
    /// owl:ReflexiveProperty
    public var isReflexive: Bool

    /// Whether property is irreflexive
    ///
    /// Irreflexive: for no a, (a,a) holds
    /// owl:IrreflexiveProperty
    public var isIrreflexive: Bool

    // MARK: - Inverse Property

    /// Inverse property IRI (owl:inverseOf)
    ///
    /// If this property is P and inverseOf is Q, then:
    /// P(a,b) iff Q(b,a)
    public var inverseOf: String?

    // MARK: - Property Chains

    /// Property chains that imply this property
    ///
    /// owl:propertyChainAxiom
    /// Each chain is a sequence of property IRIs.
    /// If P has chain [Q, R], then Q(a,b) ∧ R(b,c) → P(a,c)
    public var propertyChains: [[String]]

    // MARK: - Metadata

    /// Whether this property is deprecated (owl:deprecated)
    public var isDeprecated: Bool

    /// Annotations (key-value pairs)
    public var annotations: [String: String]

    // MARK: - Initialization

    public init(
        iri: String,
        type: StoredPropertyType,
        label: String? = nil,
        comment: String? = nil,
        domains: Set<String> = [],
        ranges: Set<String> = [],
        directSuperProperties: Set<String> = [],
        equivalentProperties: Set<String> = [],
        disjointProperties: Set<String> = [],
        isFunctional: Bool = false,
        isInverseFunctional: Bool = false,
        isTransitive: Bool = false,
        isSymmetric: Bool = false,
        isAsymmetric: Bool = false,
        isReflexive: Bool = false,
        isIrreflexive: Bool = false,
        inverseOf: String? = nil,
        propertyChains: [[String]] = [],
        isDeprecated: Bool = false,
        annotations: [String: String] = [:]
    ) {
        self.iri = iri
        self.type = type
        self.label = label
        self.comment = comment
        self.domains = domains
        self.ranges = ranges
        self.directSuperProperties = directSuperProperties
        self.equivalentProperties = equivalentProperties
        self.disjointProperties = disjointProperties
        self.isFunctional = isFunctional
        self.isInverseFunctional = isInverseFunctional
        self.isTransitive = isTransitive
        self.isSymmetric = isSymmetric
        self.isAsymmetric = isAsymmetric
        self.isReflexive = isReflexive
        self.isIrreflexive = isIrreflexive
        self.inverseOf = inverseOf
        self.propertyChains = propertyChains
        self.isDeprecated = isDeprecated
        self.annotations = annotations
    }

    // MARK: - Factory Methods

    /// Create from OWLObjectProperty
    public static func from(_ owlProp: OWLObjectProperty) -> StoredPropertyDefinition {
        let domains = Set(owlProp.domains.compactMap { expr -> String? in
            if case .named(let iri) = expr { return iri }
            return nil
        })
        let ranges = Set(owlProp.ranges.compactMap { expr -> String? in
            if case .named(let iri) = expr { return iri }
            return nil
        })

        return StoredPropertyDefinition(
            iri: owlProp.iri,
            type: .objectProperty,
            label: owlProp.label,
            comment: owlProp.comment,
            domains: domains,
            ranges: ranges,
            directSuperProperties: Set(owlProp.superProperties),
            equivalentProperties: Set(owlProp.equivalentProperties),
            disjointProperties: Set(owlProp.disjointProperties),
            isFunctional: owlProp.isFunctional,
            isInverseFunctional: owlProp.isInverseFunctional,
            isTransitive: owlProp.isTransitive,
            isSymmetric: owlProp.isSymmetric,
            isAsymmetric: owlProp.isAsymmetric,
            isReflexive: owlProp.isReflexive,
            isIrreflexive: owlProp.isIrreflexive,
            inverseOf: owlProp.inverseOf,
            propertyChains: owlProp.propertyChains,
            annotations: owlProp.annotations
        )
    }

    /// Create from OWLDataProperty
    public static func from(_ owlProp: OWLDataProperty) -> StoredPropertyDefinition {
        let domains = Set(owlProp.domains.compactMap { expr -> String? in
            if case .named(let iri) = expr { return iri }
            return nil
        })

        return StoredPropertyDefinition(
            iri: owlProp.iri,
            type: .dataProperty,
            label: owlProp.label,
            comment: owlProp.comment,
            domains: domains,
            ranges: [], // Data ranges handled separately
            directSuperProperties: Set(owlProp.superProperties),
            equivalentProperties: Set(owlProp.equivalentProperties),
            disjointProperties: Set(owlProp.disjointProperties),
            isFunctional: owlProp.isFunctional,
            annotations: owlProp.annotations
        )
    }

    /// Create from OWLAnnotationProperty
    public static func from(_ owlProp: OWLAnnotationProperty) -> StoredPropertyDefinition {
        StoredPropertyDefinition(
            iri: owlProp.iri,
            type: .annotationProperty,
            label: owlProp.label,
            domains: Set(owlProp.domains),
            ranges: Set(owlProp.ranges),
            directSuperProperties: Set(owlProp.superProperties)
        )
    }

    // MARK: - Modification

    /// Add a superproperty
    public mutating func addSuperProperty(_ superPropertyIRI: String) {
        directSuperProperties.insert(superPropertyIRI)
    }

    /// Add a property chain
    public mutating func addPropertyChain(_ chain: [String]) {
        guard chain.count >= 2 else { return }
        propertyChains.append(chain)
    }

    /// Set inverse property
    public mutating func setInverse(_ inverseIRI: String) {
        inverseOf = inverseIRI
    }
}

// MARK: - Encoding/Decoding

extension StoredPropertyDefinition {
    /// Encode to JSON bytes
    public func encode() throws -> Data {
        try JSONEncoder().encode(self)
    }

    /// Decode from JSON bytes
    public static func decode(from data: Data) throws -> StoredPropertyDefinition {
        try JSONDecoder().decode(StoredPropertyDefinition.self, from: data)
    }
}

// MARK: - Well-Known IRIs

public extension StoredPropertyDefinition {
    /// Well-known property IRIs
    enum WellKnown {
        // RDF
        public static let rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

        // RDFS
        public static let rdfsSubClassOf = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        public static let rdfsSubPropertyOf = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
        public static let rdfsDomain = "http://www.w3.org/2000/01/rdf-schema#domain"
        public static let rdfsRange = "http://www.w3.org/2000/01/rdf-schema#range"
        public static let rdfsLabel = "http://www.w3.org/2000/01/rdf-schema#label"
        public static let rdfsComment = "http://www.w3.org/2000/01/rdf-schema#comment"

        // OWL
        public static let owlSameAs = "http://www.w3.org/2002/07/owl#sameAs"
        public static let owlDifferentFrom = "http://www.w3.org/2002/07/owl#differentFrom"
        public static let owlEquivalentClass = "http://www.w3.org/2002/07/owl#equivalentClass"
        public static let owlEquivalentProperty = "http://www.w3.org/2002/07/owl#equivalentProperty"
        public static let owlInverseOf = "http://www.w3.org/2002/07/owl#inverseOf"
    }

    /// Check if this is a built-in property
    var isBuiltIn: Bool {
        iri.hasPrefix("http://www.w3.org/1999/02/22-rdf-syntax-ns#") ||
        iri.hasPrefix("http://www.w3.org/2000/01/rdf-schema#") ||
        iri.hasPrefix("http://www.w3.org/2002/07/owl#")
    }
}
