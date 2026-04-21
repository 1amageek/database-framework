#if FOUNDATION_DB
// OntologyIRIValidationTests.swift
// Integration tests for OntologyIRIValidator
//
// Validates that @OWLClass / @OWLObjectProperty IRI bindings
// are checked against the OntologyStore.

import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex
@testable import OntologyIndex

// MARK: - Test Models

@Persistable
@OWLClass("http://test.org/onto#Employee")
struct ValEmployee {
    #Directory<ValEmployee>("ontology_iri_validation_tests", "employee")

    var id: String = ULID().ulidString

    @OWLDataProperty("http://test.org/onto#name")
    var name: String = ""
}

@Persistable
@OWLObjectProperty("http://test.org/onto#worksOn", from: "employeeID", to: "projectID")
struct ValAssignment {
    #Directory<ValAssignment>("ontology_iri_validation_tests", "assignment")

    var id: String = ULID().ulidString
    var employeeID: String = ""
    var projectID: String = ""
}

@Persistable
@OWLClass("http://test.org/onto#NonExistentClass")
struct ValBadClass {
    #Directory<ValBadClass>("ontology_iri_validation_tests", "badclass")

    var id: String = ULID().ulidString
    var name: String = ""
}

@Persistable
@OWLObjectProperty("http://test.org/onto#nonExistentProp", from: "fromID", to: "toID")
struct ValBadRelation {
    #Directory<ValBadRelation>("ontology_iri_validation_tests", "badrel")

    var id: String = ULID().ulidString
    var fromID: String = ""
    var toID: String = ""
}

/// Uses a DataProperty IRI with @OWLObjectProperty — should fail type check
@Persistable
@OWLObjectProperty("http://test.org/onto#name", from: "srcID", to: "dstID")
struct ValDataPropAsObjectProp {
    #Directory<ValDataPropAsObjectProp>("ontology_iri_validation_tests", "typemismatch")

    var id: String = ULID().ulidString
    var srcID: String = ""
    var dstID: String = ""
}

/// Has @OWLDataProperty with an IRI not defined in OntologyStore
@Persistable
@OWLClass("http://test.org/onto#Employee")
struct ValBadDataProperty {
    #Directory<ValBadDataProperty>("ontology_iri_validation_tests", "baddataprop")

    var id: String = ULID().ulidString

    @OWLDataProperty("http://test.org/onto#nonExistentDataProp")
    var nickname: String = ""
}

/// Has @OWLDataProperty that uses an ObjectProperty IRI — should fail type check
@Persistable
@OWLClass("http://test.org/onto#Employee")
struct ValObjPropAsDataProp {
    #Directory<ValObjPropAsDataProp>("ontology_iri_validation_tests", "objasdata")

    var id: String = ULID().ulidString

    @OWLDataProperty("http://test.org/onto#worksOn")
    var dept: String = ""
}

// MARK: - Tests

@Suite("Ontology IRI Validation", .serialized, .heartbeat)
struct OntologyIRIValidationTests {

    private static let ontologyIRI = "http://test.org/ontology-iri-validation"

    // MARK: - Helpers

    private func setupContext() async throws -> FDBContext {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBTestSetup.shared.makeEngine()
        if try await database.directoryService.exists(path: ["ontology_iri_validation_tests"]) {
            try await database.directoryService.remove(path: ["ontology_iri_validation_tests"])
        }
        let schema = Schema(
            [ValEmployee.self, ValAssignment.self, ValBadClass.self,
             ValBadRelation.self, ValDataPropAsObjectProp.self,
             ValBadDataProperty.self, ValObjPropAsDataProp.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled,
        )
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
                // ValEmployee has @OWLClass + @OWLDataProperty, both fail against non-existent ontology
                #expect(errors.count == 2)
                let classErrors = errors.filter {
                    if case .classNotFound = $0 { return true }
                    return false
                }
                let propErrors = errors.filter {
                    if case .propertyNotFound = $0 { return true }
                    return false
                }
                #expect(classErrors.count == 1)
                #expect(propErrors.count == 1)
                if case .classNotFound(_, let ontologyIRI) = classErrors.first {
                    #expect(ontologyIRI == "http://does.not/exist")
                }
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }

    // MARK: - Data Property Validation

    @Test("Valid data property IRI passes schema validation")
    func validDataPropertyPasses() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        // ValEmployee has @OWLDataProperty("http://test.org/onto#name") which exists
        let schema = Schema(
            [ValEmployee.self],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Invalid data property IRI fails schema validation")
    func invalidDataPropertyFails() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValBadDataProperty.self],
            version: Schema.Version(1, 0, 0)
        )

        do {
            try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
            Issue.record("Expected validation failure")
        } catch let error as OntologyValidationError {
            switch error {
            case .validationFailed(let errors):
                // classIRI is valid (Employee), but data property IRI is not found
                #expect(errors.count == 1)
                if case .propertyNotFound(let iri, _) = errors.first {
                    #expect(iri == "http://test.org/onto#nonExistentDataProp")
                } else {
                    Issue.record("Expected propertyNotFound, got \(errors)")
                }
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }

    @Test("ObjectProperty IRI used as @OWLDataProperty throws propertyTypeMismatch")
    func objectPropertyAsDataPropertyThrows() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        let schema = Schema(
            [ValObjPropAsDataProp.self],
            version: Schema.Version(1, 0, 0)
        )

        do {
            try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
            Issue.record("Expected validation failure")
        } catch let error as OntologyValidationError {
            switch error {
            case .validationFailed(let errors):
                // classIRI is valid (Employee), but worksOn is ObjectProperty not DataProperty
                #expect(errors.count == 1)
                if case .propertyTypeMismatch(let iri, let expected, let actual, _) = errors.first {
                    #expect(iri == "http://test.org/onto#worksOn")
                    #expect(expected == .dataProperty)
                    #expect(actual == .objectProperty)
                } else {
                    Issue.record("Expected propertyTypeMismatch, got \(errors)")
                }
            default:
                Issue.record("Expected validationFailed, got \(error)")
            }
        }
    }

    @Test("Valid schema with both class and data property IRIs passes")
    func schemaWithClassAndDataPropertyPasses() async throws {
        let context = try await setupContext()
        try await loadTestOntology(context: context)

        // ValEmployee: @OWLClass("...#Employee") + @OWLDataProperty("...#name")
        // ValAssignment: @OWLObjectProperty("...#worksOn")
        let schema = Schema(
            [ValEmployee.self, ValAssignment.self],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }
}
#endif
