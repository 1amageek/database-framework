// OntologyIRIValidationTests.swift
// Integration tests for OntologyIRIValidator
//
// Validates that @OWLClass / @OWLObjectProperty IRI bindings
// are checked against the OntologyStore.

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Models

@Persistable
@OWLClass("http://test.org/onto#Employee")
struct ValEmployee {
    #Directory<ValEmployee>("test", "validation", "employee")

    var id: String = ULID().ulidString

    @OWLDataProperty("http://test.org/onto#name")
    var name: String = ""
}

@Persistable
@OWLObjectProperty("http://test.org/onto#worksOn", from: "employeeID", to: "projectID")
struct ValAssignment {
    #Directory<ValAssignment>("test", "validation", "assignment")

    var id: String = ULID().ulidString
    var employeeID: String = ""
    var projectID: String = ""
}

@Persistable
@OWLClass("http://test.org/onto#NonExistentClass")
struct ValBadClass {
    #Directory<ValBadClass>("test", "validation", "badclass")

    var id: String = ULID().ulidString
    var name: String = ""
}

@Persistable
@OWLObjectProperty("http://test.org/onto#nonExistentProp", from: "fromID", to: "toID")
struct ValBadRelation {
    #Directory<ValBadRelation>("test", "validation", "badrel")

    var id: String = ULID().ulidString
    var fromID: String = ""
    var toID: String = ""
}

/// Uses a DataProperty IRI with @OWLObjectProperty — should fail type check
@Persistable
@OWLObjectProperty("http://test.org/onto#name", from: "srcID", to: "dstID")
struct ValDataPropAsObjectProp {
    #Directory<ValDataPropAsObjectProp>("test", "validation", "typemismatch")

    var id: String = ULID().ulidString
    var srcID: String = ""
    var dstID: String = ""
}

// MARK: - Tests

@Suite("Ontology IRI Validation", .serialized)
struct OntologyIRIValidationTests {

    private static let ontologyIRI = "http://test.org/onto"

    // MARK: - Helpers

    private func setupContext() async throws -> FDBContext {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema(
            [ValEmployee.self, ValAssignment.self, ValBadClass.self,
             ValBadRelation.self, ValDataPropAsObjectProp.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        return container.newContext()
    }

    private func loadTestOntology(context: FDBContext) async throws {
        // Clean up first
        try await context.ontology.delete(iri: Self.ontologyIRI)

        // Load an ontology with known classes and properties
        var ontology = OWLOntology(iri: Self.ontologyIRI)
        ontology.classes = [
            OWLClass(iri: "http://test.org/onto#Person"),
            OWLClass(iri: "http://test.org/onto#Employee"),
            OWLClass(iri: "http://test.org/onto#Project"),
        ]
        ontology.objectProperties = [
            OWLObjectProperty(iri: "http://test.org/onto#worksOn"),
        ]
        ontology.dataProperties = [
            OWLDataProperty(iri: "http://test.org/onto#name"),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("http://test.org/onto#Employee"), sup: .named("http://test.org/onto#Person")),
        ]
        try await context.ontology.load(ontology)
    }

    // MARK: - Class Validation

    @Test("Valid class IRI passes validation")
    func validClassIRIPasses() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let store = OntologyStore(subspace: OntologySubspace(base: Subspace(prefix: Array("O".utf8))))
        let validator = OntologyIRIValidator(store: store)

        try await context.indexQueryContext.withTransaction { transaction in
            try await validator.validateClass(
                "http://test.org/onto#Employee",
                in: Self.ontologyIRI,
                transaction: transaction
            )
        }
    }

    @Test("Invalid class IRI throws classNotFound")
    func invalidClassIRIThrows() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let store = OntologyStore(subspace: OntologySubspace(base: Subspace(prefix: Array("O".utf8))))
        let validator = OntologyIRIValidator(store: store)

        try await context.indexQueryContext.withTransaction { transaction in
            do {
                try await validator.validateClass(
                    "http://test.org/onto#NonExistentClass",
                    in: Self.ontologyIRI,
                    transaction: transaction
                )
                Issue.record("Expected classNotFound error")
            } catch let error as OntologyValidationError {
                switch error {
                case .classNotFound(let iri, _):
                    #expect(iri == "http://test.org/onto#NonExistentClass")
                default:
                    Issue.record("Expected classNotFound, got \(error)")
                }
            }
        }
    }

    // MARK: - Property Validation

    @Test("Valid object property IRI passes validation")
    func validObjectPropertyIRIPasses() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let store = OntologyStore(subspace: OntologySubspace(base: Subspace(prefix: Array("O".utf8))))
        let validator = OntologyIRIValidator(store: store)

        try await context.indexQueryContext.withTransaction { transaction in
            try await validator.validateObjectProperty(
                "http://test.org/onto#worksOn",
                in: Self.ontologyIRI,
                transaction: transaction
            )
        }
    }

    @Test("Invalid property IRI throws propertyNotFound")
    func invalidPropertyIRIThrows() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let store = OntologyStore(subspace: OntologySubspace(base: Subspace(prefix: Array("O".utf8))))
        let validator = OntologyIRIValidator(store: store)

        try await context.indexQueryContext.withTransaction { transaction in
            do {
                try await validator.validateObjectProperty(
                    "http://test.org/onto#nonExistentProp",
                    in: Self.ontologyIRI,
                    transaction: transaction
                )
                Issue.record("Expected propertyNotFound error")
            } catch let error as OntologyValidationError {
                switch error {
                case .propertyNotFound(let iri, _):
                    #expect(iri == "http://test.org/onto#nonExistentProp")
                default:
                    Issue.record("Expected propertyNotFound, got \(error)")
                }
            }
        }
    }

    @Test("DataProperty IRI used as ObjectProperty throws propertyTypeMismatch")
    func dataPropertyAsObjectPropertyThrows() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let store = OntologyStore(subspace: OntologySubspace(base: Subspace(prefix: Array("O".utf8))))
        let validator = OntologyIRIValidator(store: store)

        try await context.indexQueryContext.withTransaction { transaction in
            do {
                // "name" is a DataProperty, not an ObjectProperty
                try await validator.validateObjectProperty(
                    "http://test.org/onto#name",
                    in: Self.ontologyIRI,
                    transaction: transaction
                )
                Issue.record("Expected propertyTypeMismatch error")
            } catch let error as OntologyValidationError {
                switch error {
                case .propertyTypeMismatch(let iri, let expected, let actual, _):
                    #expect(iri == "http://test.org/onto#name")
                    #expect(expected == .objectProperty)
                    #expect(actual == .dataProperty)
                default:
                    Issue.record("Expected propertyTypeMismatch, got \(error)")
                }
            }
        }
    }

    // MARK: - Schema Validation

    @Test("Schema validation passes for valid IRIs")
    func schemaValidationPasses() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValEmployee.self, ValAssignment.self],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Schema validation fails for invalid class IRI")
    func schemaValidationFailsForBadClass() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValBadClass.self],
            version: Schema.Version(1, 0, 0)
        )

        do {
            try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
            Issue.record("Expected validation failure")
        } catch let error as OntologyValidationError {
            switch error {
            case .validationFailed(let errors):
                #expect(errors.count == 1)
                if case .classNotFound(let iri, _) = errors.first {
                    #expect(iri == "http://test.org/onto#NonExistentClass")
                } else {
                    Issue.record("Expected classNotFound error in errors array")
                }
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }

    @Test("Schema validation fails for invalid property IRI")
    func schemaValidationFailsForBadProperty() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValBadRelation.self],
            version: Schema.Version(1, 0, 0)
        )

        do {
            try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
            Issue.record("Expected validation failure")
        } catch let error as OntologyValidationError {
            switch error {
            case .validationFailed(let errors):
                #expect(errors.count == 1)
                if case .propertyNotFound(let iri, _) = errors.first {
                    #expect(iri == "http://test.org/onto#nonExistentProp")
                } else {
                    Issue.record("Expected propertyNotFound error in errors array")
                }
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }

    @Test("Schema validation detects DataProperty used as ObjectProperty")
    func schemaValidationDetectsTypeMismatch() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValDataPropAsObjectProp.self],
            version: Schema.Version(1, 0, 0)
        )

        do {
            try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
            Issue.record("Expected validation failure")
        } catch let error as OntologyValidationError {
            switch error {
            case .validationFailed(let errors):
                #expect(errors.count == 1)
                if case .propertyTypeMismatch(let iri, let expected, let actual, _) = errors.first {
                    #expect(iri == "http://test.org/onto#name")
                    #expect(expected == .objectProperty)
                    #expect(actual == .dataProperty)
                } else {
                    Issue.record("Expected propertyTypeMismatch, got \(errors)")
                }
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }

    @Test("Schema validation collects multiple errors")
    func schemaValidationCollectsMultipleErrors() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValBadClass.self, ValBadRelation.self],
            version: Schema.Version(1, 0, 0)
        )

        do {
            try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
            Issue.record("Expected validation failure")
        } catch let error as OntologyValidationError {
            switch error {
            case .validationFailed(let errors):
                #expect(errors.count == 2)
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }

    @Test("Schema with no ontology annotations passes validation")
    func schemaWithNoOntologyPasses() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [OntologyTestDummy.self],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Empty schema passes validation")
    func emptySchemaPassesValidation() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            entities: [],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Validation against non-existent ontology IRI reports errors")
    func validationAgainstNonExistentOntology() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValEmployee.self],
            version: Schema.Version(1, 0, 0)
        )

        do {
            try await context.ontology.validateSchema(
                schema, ontologyIRI: "http://does.not/exist"
            )
            Issue.record("Expected validation failure")
        } catch let error as OntologyValidationError {
            switch error {
            case .validationFailed(let errors):
                #expect(errors.count == 1)
                if case .classNotFound(_, let ontologyIRI) = errors.first {
                    #expect(ontologyIRI == "http://does.not/exist")
                } else {
                    Issue.record("Expected classNotFound, got \(errors)")
                }
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }
}
