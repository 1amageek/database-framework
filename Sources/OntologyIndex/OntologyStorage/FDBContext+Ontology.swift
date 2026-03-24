// FDBContext+Ontology.swift
// GraphIndex - FDBContext extension for ontology management
//
// Provides high-level API for ontology CRUD operations via FDBContext.
//
// Reference: W3C OWL 2 https://www.w3.org/TR/owl2-syntax/

import Foundation
import StorageKit
import Graph
import Core
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
            // loadOntology is idempotent — it clears existing data internally
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
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.reconstruct(iri: iri, transaction: transaction)
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

    // MARK: - Schema Validation

    /// Validate all @OWLClass / @OWLObjectProperty / @OWLDataProperty IRIs in a schema
    /// against the OntologyStore.
    ///
    /// Checks that every macro-bound IRI references a class or property
    /// that exists in the specified ontology with the correct type.
    ///
    /// - Parameters:
    ///   - schema: The schema to validate
    ///   - ontologyIRI: The ontology IRI to validate against
    /// - Throws: OntologyValidationError.validationFailed if any IRI is not found
    ///
    /// **Example**:
    /// ```swift
    /// let schema = Schema([Employee.self, Assignment.self])
    /// try await context.ontology.validateSchema(schema, ontologyIRI: "http://example.org/onto")
    /// ```
    public func validateSchema(_ schema: Schema, ontologyIRI: String) async throws {
        let ontologyStore = store()
        let validator = OntologyIRIValidator(store: ontologyStore)

        let errors: [OntologyValidationError] = try await context.indexQueryContext.withTransaction { transaction in
            var collected: [OntologyValidationError] = []
            for entity in schema.entities {
                // Validate @OWLClass IRI
                if let classIRI = entity.ontologyClassIRI {
                    do {
                        try await validator.validateClass(classIRI, in: ontologyIRI, transaction: transaction)
                    } catch let error as OntologyValidationError {
                        collected.append(error)
                    }
                    // Non-validation errors (FDB connection, etc.) propagate immediately
                }

                // Validate @OWLObjectProperty IRI
                if let propIRI = entity.objectPropertyIRI {
                    do {
                        try await validator.validateObjectProperty(propIRI, in: ontologyIRI, transaction: transaction)
                    } catch let error as OntologyValidationError {
                        collected.append(error)
                    }
                    // Non-validation errors propagate immediately
                }

                // Validate @OWLDataProperty IRIs
                // Source 1: Runtime type (when Persistable type is linked)
                // Source 2: Schema.Entity.dataPropertyIRIs (when deserialized from wire format)
                let dataPropertyIRIs: [String]
                if let type = entity.persistableType {
                    if let owlClass = type as? any OWLClassEntity.Type {
                        dataPropertyIRIs = owlClass.ontologyPropertyDescriptors.map(\.iri)
                    } else if let owlObjProp = type as? any OWLObjectPropertyEntity.Type {
                        dataPropertyIRIs = owlObjProp.ontologyPropertyDescriptors.map(\.iri)
                    } else {
                        dataPropertyIRIs = []
                    }
                } else if let wireIRIs = entity.dataPropertyIRIs {
                    // E-1 fallback: use wire-format IRIs when persistableType is nil
                    dataPropertyIRIs = wireIRIs
                } else {
                    dataPropertyIRIs = []
                }
                for iri in dataPropertyIRIs {
                    do {
                        try await validator.validateDataProperty(iri, in: ontologyIRI, transaction: transaction)
                    } catch let error as OntologyValidationError {
                        collected.append(error)
                    }
                    // Non-validation errors propagate immediately
                }
            }
            return collected
        }

        if !errors.isEmpty {
            throw OntologyValidationError.validationFailed(errors: errors)
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
