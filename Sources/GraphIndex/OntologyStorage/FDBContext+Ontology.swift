// FDBContext+Ontology.swift
// GraphIndex - FDBContext extension for ontology management
//
// Provides high-level API for ontology CRUD operations via FDBContext.
//
// Reference: W3C OWL 2 https://www.w3.org/TR/owl2-syntax/

import Foundation
import FoundationDB
import Graph
import DatabaseEngine

// MARK: - FDBContext Extension

extension FDBContext {
    /// Access ontology management API
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // Load an ontology
    /// try await context.ontology.load(familyOntology)
    ///
    /// // Get an ontology by IRI
    /// if let ontology = try await context.ontology.get(iri: "http://example.org/family") {
    ///     print(ontology.iri)
    /// }
    ///
    /// // List all ontologies
    /// let ontologies = try await context.ontology.list()
    ///
    /// // Delete an ontology
    /// try await context.ontology.delete(iri: "http://example.org/family")
    ///
    /// // Create a reasoner for an ontology
    /// let reasoner = try await context.ontology.reasoner(for: "http://example.org/family")
    /// ```
    ///
    /// - Returns: OntologyContextAPI for ontology operations
    public var ontology: OntologyContextAPI {
        OntologyContextAPI(context: self)
    }
}

// MARK: - OntologyContextAPI

/// High-level API for ontology operations
///
/// Provides CRUD operations for OWL ontologies stored in FoundationDB.
/// All operations are performed within transactions managed by the context.
///
/// **Thread Safety**: This struct is Sendable and all methods are async.
public struct OntologyContextAPI: Sendable {

    // MARK: - Properties

    private let context: FDBContext

    /// Ontology subspace key prefix
    private static let ontologyPrefix: [UInt8] = Array("O".utf8)

    // MARK: - Initialization

    internal init(context: FDBContext) {
        self.context = context
    }

    // MARK: - Store Access

    /// Get the ontology store for performing operations
    private func store() -> OntologyStore {
        let baseSubspace = Subspace(prefix: Self.ontologyPrefix)
        return OntologyStore(subspace: OntologySubspace(base: baseSubspace))
    }

    // MARK: - Load Operations

    /// Load an OWL ontology into the store
    ///
    /// This performs full materialization of class and property hierarchies.
    /// If an ontology with the same IRI already exists, it will be replaced.
    ///
    /// - Parameter ontology: The OWL ontology to load
    /// - Throws: Error if save fails
    ///
    /// **Example**:
    /// ```swift
    /// let ontology = OWLOntology(
    ///     iri: "http://example.org/family",
    ///     axioms: [
    ///         .subClassOf(.named("ex:Parent"), .named("ex:Person")),
    ///         .transitiveProperty("ex:ancestorOf")
    ///     ]
    /// )
    /// try await context.ontology.load(ontology)
    /// ```
    public func load(_ ontology: OWLOntology) async throws {
        let store = store()
        try await context.indexQueryContext.withTransaction { transaction in
            // Delete existing if present
            store.deleteOntology(ontology.iri, transaction: transaction)
            // Load new ontology
            try await store.loadOntology(ontology, transaction: transaction)
        }
    }

    /// Load multiple ontologies in a single transaction
    ///
    /// - Parameter ontologies: Array of ontologies to load
    /// - Throws: Error if save fails
    public func loadAll(_ ontologies: [OWLOntology]) async throws {
        let store = store()
        try await context.indexQueryContext.withTransaction { transaction in
            for ontology in ontologies {
                store.deleteOntology(ontology.iri, transaction: transaction)
                try await store.loadOntology(ontology, transaction: transaction)
            }
        }
    }

    // MARK: - Get Operations

    /// Get an ontology by IRI
    ///
    /// - Parameter iri: The ontology IRI
    /// - Returns: The OWL ontology if found, nil otherwise
    ///
    /// **Note**: This reconstructs the ontology from stored metadata, classes,
    /// properties, and axioms. For simple queries, consider using the
    /// individual store methods directly.
    public func get(iri: String) async throws -> OWLOntology? {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction -> OWLOntology? in
            // Check if metadata exists
            guard let metadata = try await store.getMetadata(
                ontologyIRI: iri,
                transaction: transaction
            ) else {
                return nil
            }

            // Reconstruct ontology from stored data
            let classes = try await store.listClasses(
                ontologyIRI: iri,
                transaction: transaction
            )
            let properties = try await store.listProperties(
                ontologyIRI: iri,
                transaction: transaction
            )

            // Reconstruct OWL classes
            let owlClasses = classes.map { def in
                OWLClass(
                    iri: def.iri,
                    label: def.label,
                    comment: def.comment,
                    annotations: def.annotations
                )
            }

            // Reconstruct OWL object properties
            let owlObjectProperties = properties.compactMap { def -> OWLObjectProperty? in
                guard def.type == .objectProperty else { return nil }

                var characteristics = Set<PropertyCharacteristic>()
                if def.isFunctional { characteristics.insert(.functional) }
                if def.isInverseFunctional { characteristics.insert(.inverseFunctional) }
                if def.isTransitive { characteristics.insert(.transitive) }
                if def.isSymmetric { characteristics.insert(.symmetric) }
                if def.isAsymmetric { characteristics.insert(.asymmetric) }
                if def.isReflexive { characteristics.insert(.reflexive) }
                if def.isIrreflexive { characteristics.insert(.irreflexive) }

                return OWLObjectProperty(
                    iri: def.iri,
                    label: def.label,
                    comment: def.comment,
                    annotations: def.annotations,
                    characteristics: characteristics,
                    inverseOf: def.inverseOf,
                    domains: def.domains.map { .named($0) },
                    ranges: def.ranges.map { .named($0) },
                    superProperties: Array(def.directSuperProperties),
                    equivalentProperties: Array(def.equivalentProperties),
                    disjointProperties: Array(def.disjointProperties),
                    propertyChains: def.propertyChains
                )
            }

            // Reconstruct OWL data properties
            let owlDataProperties = properties.compactMap { def -> OWLDataProperty? in
                guard def.type == .dataProperty else { return nil }
                return OWLDataProperty(
                    iri: def.iri,
                    label: def.label,
                    comment: def.comment,
                    annotations: def.annotations,
                    domains: def.domains.map { .named($0) },
                    ranges: [], // Data ranges not stored in simplified format
                    isFunctional: def.isFunctional,
                    superProperties: Array(def.directSuperProperties),
                    equivalentProperties: Array(def.equivalentProperties),
                    disjointProperties: Array(def.disjointProperties)
                )
            }

            // Create ontology with reconstructed components
            var ontology = OWLOntology(
                iri: metadata.iri,
                versionIRI: metadata.versionIRI,
                imports: metadata.imports,
                prefixes: metadata.prefixes
            )
            ontology.classes = owlClasses
            ontology.objectProperties = owlObjectProperties
            ontology.dataProperties = owlDataProperties

            return ontology
        }
    }

    /// Get ontology metadata only (faster than full ontology)
    ///
    /// - Parameter iri: The ontology IRI
    /// - Returns: Metadata if found
    public func getMetadata(iri: String) async throws -> OntologyMetadata? {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.getMetadata(ontologyIRI: iri, transaction: transaction)
        }
    }

    // MARK: - List Operations

    /// List all ontology IRIs in the store
    ///
    /// - Returns: Array of ontology IRIs
    public func list() async throws -> [String] {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.listOntologies(transaction: transaction)
        }
    }

    /// Check if an ontology exists
    ///
    /// - Parameter iri: The ontology IRI
    /// - Returns: true if the ontology exists
    public func exists(iri: String) async throws -> Bool {
        try await getMetadata(iri: iri) != nil
    }

    // MARK: - Delete Operations

    /// Delete an ontology by IRI
    ///
    /// Removes all stored data for the ontology including metadata,
    /// classes, properties, and hierarchy indices.
    ///
    /// - Parameter iri: The ontology IRI to delete
    public func delete(iri: String) async throws {
        let store = store()
        try await context.indexQueryContext.withTransaction { transaction in
            store.deleteOntology(iri, transaction: transaction)
        }
    }

    /// Delete all ontologies
    ///
    /// **Warning**: This removes all stored ontology data.
    public func deleteAll() async throws {
        let iris = try await list()
        let store = store()
        try await context.indexQueryContext.withTransaction { transaction in
            for iri in iris {
                store.deleteOntology(iri, transaction: transaction)
            }
        }
    }

    // MARK: - Reasoner Operations

    /// Create an OWL reasoner for an ontology
    ///
    /// - Parameters:
    ///   - iri: The ontology IRI
    ///   - configuration: Reasoner configuration (default: standard settings)
    /// - Returns: Configured OWLReasoner
    /// - Throws: OntologyError.notFound if ontology doesn't exist
    ///
    /// **Example**:
    /// ```swift
    /// let reasoner = try await context.ontology.reasoner(for: "http://example.org/family")
    ///
    /// // Check consistency
    /// let isConsistent = reasoner.isConsistent()
    ///
    /// // Query class hierarchy
    /// let superClasses = reasoner.superClasses(of: "ex:Employee")
    ///
    /// // Check subsumption
    /// let subsumes = reasoner.subsumes(
    ///     superClass: .named("ex:Person"),
    ///     subClass: .named("ex:Employee")
    /// )
    /// ```
    public func reasoner(
        for iri: String,
        configuration: OWLReasoner.Configuration = .default
    ) async throws -> OWLReasoner {
        guard let ontology = try await get(iri: iri) else {
            throw OntologyError.notFound(iri)
        }
        return OWLReasoner(ontology: ontology, configuration: configuration)
    }

    // MARK: - Query Operations

    /// Query class hierarchy from stored materialized index
    ///
    /// - Parameters:
    ///   - classIRI: The class IRI
    ///   - ontologyIRI: The ontology IRI
    /// - Returns: Set of superclass IRIs
    public func getSuperClasses(
        of classIRI: String,
        in ontologyIRI: String
    ) async throws -> Set<String> {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.getSuperClasses(
                of: classIRI,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }

    /// Query subclass hierarchy from stored materialized index
    ///
    /// - Parameters:
    ///   - classIRI: The class IRI
    ///   - ontologyIRI: The ontology IRI
    /// - Returns: Set of subclass IRIs
    public func getSubClasses(
        of classIRI: String,
        in ontologyIRI: String
    ) async throws -> Set<String> {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.getSubClasses(
                of: classIRI,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }

    /// Query property hierarchy
    ///
    /// - Parameters:
    ///   - propertyIRI: The property IRI
    ///   - ontologyIRI: The ontology IRI
    /// - Returns: Set of superproperty IRIs
    public func getSuperProperties(
        of propertyIRI: String,
        in ontologyIRI: String
    ) async throws -> Set<String> {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.getSuperProperties(
                of: propertyIRI,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }

    /// Check if a property is transitive
    ///
    /// - Parameters:
    ///   - propertyIRI: The property IRI
    ///   - ontologyIRI: The ontology IRI
    /// - Returns: true if the property is transitive
    public func isTransitive(
        property propertyIRI: String,
        in ontologyIRI: String
    ) async throws -> Bool {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.isTransitive(
                property: propertyIRI,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }

    /// Get inverse property
    ///
    /// - Parameters:
    ///   - propertyIRI: The property IRI
    ///   - ontologyIRI: The ontology IRI
    /// - Returns: Inverse property IRI if exists
    public func getInverse(
        of propertyIRI: String,
        in ontologyIRI: String
    ) async throws -> String? {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.getInverse(
                of: propertyIRI,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }

    /// Get property chains for a target property
    ///
    /// - Parameters:
    ///   - propertyIRI: The target property IRI
    ///   - ontologyIRI: The ontology IRI
    /// - Returns: Array of property chains
    public func getPropertyChains(
        for propertyIRI: String,
        in ontologyIRI: String
    ) async throws -> [[String]] {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.getPropertyChains(
                for: propertyIRI,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }
}

// MARK: - OntologyError

/// Errors for ontology operations
public enum OntologyError: Error, CustomStringConvertible {
    /// Ontology not found
    case notFound(String)

    /// Invalid ontology format
    case invalidFormat(String)

    /// Reasoning error
    case reasoningFailed(String)

    public var description: String {
        switch self {
        case .notFound(let iri):
            return "Ontology not found: \(iri)"
        case .invalidFormat(let message):
            return "Invalid ontology format: \(message)"
        case .reasoningFailed(let message):
            return "Reasoning failed: \(message)"
        }
    }
}
