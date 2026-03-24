// OntologyIRIValidator.swift
// GraphIndex - Validates @OWLClass / @OWLObjectProperty IRIs against OntologyStore

import Foundation
import StorageKit
import Graph
import Core
import DatabaseEngine

/// Validates that @OWLClass and @OWLObjectProperty IRIs
/// exist in the OntologyStore as defined classes/properties.
///
/// **Design**: Macros are bindings to OntologyStore concepts.
/// This validator ensures the bindings reference valid IRIs.
///
/// **Usage**:
/// ```swift
/// let validator = OntologyIRIValidator(store: ontologyStore)
/// try await validator.validateClass(
///     "http://example.org/onto#Employee",
///     in: "http://example.org/onto",
///     transaction: tx
/// )
/// ```
public struct OntologyIRIValidator: Sendable {
    private let store: OntologyStore

    public init(store: OntologyStore) {
        self.store = store
    }

    /// Validate that a class IRI exists in the OntologyStore
    ///
    /// - Parameters:
    ///   - classIRI: The OWL class IRI (from @OWLClass macro)
    ///   - ontologyIRI: The ontology to check against
    ///   - transaction: The FDB transaction
    /// - Throws: OntologyValidationError.classNotFound if the IRI is not defined
    public func validateClass(
        _ classIRI: String,
        in ontologyIRI: String,
        transaction: any Transaction
    ) async throws {
        let classDef = try await store.getClass(
            classIRI,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )
        guard classDef != nil else {
            throw OntologyValidationError.classNotFound(
                iri: classIRI,
                ontologyIRI: ontologyIRI
            )
        }
    }

    /// Validate that an object property IRI exists in the OntologyStore
    /// and is actually an owl:ObjectProperty (not a DataProperty).
    ///
    /// - Parameters:
    ///   - propertyIRI: The OWL property IRI (from @OWLObjectProperty macro)
    ///   - ontologyIRI: The ontology to check against
    ///   - transaction: The FDB transaction
    /// - Throws: OntologyValidationError.propertyNotFound if the IRI is not defined,
    ///           OntologyValidationError.propertyTypeMismatch if the IRI is not an ObjectProperty
    public func validateObjectProperty(
        _ propertyIRI: String,
        in ontologyIRI: String,
        transaction: any Transaction
    ) async throws {
        let propDef = try await store.getProperty(
            propertyIRI,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )
        guard let propDef else {
            throw OntologyValidationError.propertyNotFound(
                iri: propertyIRI,
                ontologyIRI: ontologyIRI
            )
        }
        guard propDef.type == .objectProperty else {
            throw OntologyValidationError.propertyTypeMismatch(
                iri: propertyIRI,
                expected: .objectProperty,
                actual: propDef.type,
                ontologyIRI: ontologyIRI
            )
        }
    }

    /// Validate that a data property IRI exists in the OntologyStore
    /// and is actually an owl:DatatypeProperty (not an ObjectProperty).
    ///
    /// - Parameters:
    ///   - propertyIRI: The OWL property IRI (from @OWLDataProperty macro)
    ///   - ontologyIRI: The ontology to check against
    ///   - transaction: The FDB transaction
    /// - Throws: OntologyValidationError.propertyNotFound if the IRI is not defined,
    ///           OntologyValidationError.propertyTypeMismatch if the IRI is not a DataProperty
    public func validateDataProperty(
        _ propertyIRI: String,
        in ontologyIRI: String,
        transaction: any Transaction
    ) async throws {
        let propDef = try await store.getProperty(
            propertyIRI,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )
        guard let propDef else {
            throw OntologyValidationError.propertyNotFound(
                iri: propertyIRI,
                ontologyIRI: ontologyIRI
            )
        }
        guard propDef.type == .dataProperty else {
            throw OntologyValidationError.propertyTypeMismatch(
                iri: propertyIRI,
                expected: .dataProperty,
                actual: propDef.type,
                ontologyIRI: ontologyIRI
            )
        }
    }
}

/// Errors from ontology IRI validation
public enum OntologyValidationError: Error, Sendable, CustomStringConvertible {
    /// Class IRI not found in the OntologyStore
    case classNotFound(iri: String, ontologyIRI: String)

    /// Property IRI not found in the OntologyStore
    case propertyNotFound(iri: String, ontologyIRI: String)

    /// Property exists but has wrong type (e.g. DataProperty used as ObjectProperty)
    case propertyTypeMismatch(iri: String, expected: StoredPropertyType, actual: StoredPropertyType, ontologyIRI: String)

    /// Multiple validation failures
    case validationFailed(errors: [OntologyValidationError])

    public var description: String {
        switch self {
        case .classNotFound(let iri, let ontologyIRI):
            return "OWL class '\(iri)' not found in ontology '\(ontologyIRI)'. " +
                   "Ensure the class is defined in the OntologyStore before referencing it with @OWLClass."
        case .propertyNotFound(let iri, let ontologyIRI):
            return "OWL property '\(iri)' not found in ontology '\(ontologyIRI)'. " +
                   "Ensure the property is defined in the OntologyStore before referencing it with @OWLDataProperty or @OWLObjectProperty."
        case .propertyTypeMismatch(let iri, let expected, let actual, let ontologyIRI):
            let macroName = expected == .objectProperty ? "@OWLObjectProperty" : "@OWLDataProperty"
            return "OWL property '\(iri)' in ontology '\(ontologyIRI)' is \(actual.rawValue), " +
                   "but \(macroName) requires \(expected.rawValue)."
        case .validationFailed(let errors):
            return "Ontology validation failed with \(errors.count) error(s):\n" +
                   errors.map { "  - \($0.description)" }.joined(separator: "\n")
        }
    }
}
