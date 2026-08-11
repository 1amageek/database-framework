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
import DatabaseKit
import DatabaseTypes
import DatabaseRuntime
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex
@testable import OntologyIndex

// MARK: - Test Models

@Persistable
@OWLClass(
    "http://test.org/onto#Employee",
    individualIRIBase: "http://test.org/individual/"
)
struct ValEmployee {
    #Directory<ValEmployee>("ontology_iri_validation_tests", "employee")

    var id: String = UUID().uuidString

    @OWLDataProperty("http://test.org/onto#name")
    var name: String = ""
}

@Persistable
@OWLObjectProperty("http://test.org/onto#worksOn", from: "employeeID", to: "projectID")
struct ValAssignment {
    #Directory<ValAssignment>("ontology_iri_validation_tests", "assignment")

    var id: String = UUID().uuidString
    var employeeID: String = ""
    var projectID: String = ""
}

@Persistable
@OWLClass(
    "http://test.org/onto#NonExistentClass",
    individualIRIBase: "http://test.org/individual/"
)
struct ValBadClass {
    #Directory<ValBadClass>("ontology_iri_validation_tests", "badclass")

    var id: String = UUID().uuidString
    var name: String = ""
}

@Persistable
@OWLObjectProperty("http://test.org/onto#nonExistentProp", from: "fromID", to: "toID")
struct ValBadRelation {
    #Directory<ValBadRelation>("ontology_iri_validation_tests", "badrel")

    var id: String = UUID().uuidString
    var fromID: String = ""
    var toID: String = ""
}

/// Uses a DataProperty IRI with @OWLObjectProperty — should fail type check
@Persistable
@OWLObjectProperty("http://test.org/onto#name", from: "srcID", to: "dstID")
struct ValDataPropAsObjectProp {
    #Directory<ValDataPropAsObjectProp>("ontology_iri_validation_tests", "typemismatch")

    var id: String = UUID().uuidString
    var srcID: String = ""
    var dstID: String = ""
}

/// Has @OWLDataProperty with an IRI not defined in OntologyStore
@Persistable
@OWLClass(
    "http://test.org/onto#Employee",
    individualIRIBase: "http://test.org/individual/"
)
struct ValBadDataProperty {
    #Directory<ValBadDataProperty>("ontology_iri_validation_tests", "baddataprop")

    var id: String = UUID().uuidString

    @OWLDataProperty("http://test.org/onto#nonExistentDataProp")
    var nickname: String = ""
}

/// Has @OWLDataProperty that uses an ObjectProperty IRI — should fail type check
@Persistable
@OWLClass(
    "http://test.org/onto#Employee",
    individualIRIBase: "http://test.org/individual/"
)
struct ValObjPropAsDataProp {
    #Directory<ValObjPropAsDataProp>("ontology_iri_validation_tests", "objasdata")

    var id: String = UUID().uuidString

    @OWLDataProperty("http://test.org/onto#worksOn")
    var dept: String = ""
}

// MARK: - Tests

@Suite("Ontology IRI Validation", .serialized, .foundationDBScenario, .heartbeat)
struct OntologyIRIValidationTests {

    private static let ontologyIRI = "http://test.org/ontology-iri-validation"

    // MARK: - Helpers

    private func setupContext() async throws -> DatabaseContext {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try ValEmployee.schemaEntity,
                try ValAssignment.schemaEntity,
                try ValBadClass.schemaEntity,
                try ValBadRelation.schemaEntity,
                try ValDataPropAsObjectProp.schemaEntity,
                try ValBadDataProperty.schemaEntity,
                try ValObjPropAsDataProp.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(ValEmployee.self), try DatabaseFrameworkRuntime.entity(ValAssignment.self), try DatabaseFrameworkRuntime.entity(ValBadClass.self), try DatabaseFrameworkRuntime.entity(ValBadRelation.self), try DatabaseFrameworkRuntime.entity(ValDataPropAsObjectProp.self), try DatabaseFrameworkRuntime.entity(ValBadDataProperty.self), try DatabaseFrameworkRuntime.entity(ValObjPropAsDataProp.self)]),
            security: .testingDisabled,
        )
        try await container.resetTestBaseData()
        return container.testBaseContext()
    }

    private func loadTestOntology(context: DatabaseContext) async throws {
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
        try await context.ontology.load(
            ontology,
            at: Timestamp(secondsSinceUnixEpoch: 1_000)
        )
    }

    private func ontologyStore(
        context: DatabaseContext
    ) async throws -> OntologyStore {
        try await context.withDataOperation {
            let root = try context.requireOperationDataRoot().root
                .subspace("data")
                .subspace("database-framework")
                .subspace("ontology-index")
            return OntologyStore(
                subspace: OntologySubspace(base: root)
            )
        }
    }

    // MARK: - Class Validation

    @Test("Valid class IRI passes validation")
    func validClassIRIPasses() async throws {
        let context = try await setupContext()
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let store = try await ontologyStore(context: context)
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let store = try await ontologyStore(context: context)
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let store = try await ontologyStore(context: context)
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let store = try await ontologyStore(context: context)
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let store = try await ontologyStore(context: context)
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [
                try ValEmployee.schemaEntity,
                try ValAssignment.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Schema validation fails for invalid class IRI")
    func schemaValidationFailsForBadClass() async throws {
        let context = try await setupContext()
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [try ValBadClass.schemaEntity],
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [try ValBadRelation.schemaEntity],
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [try ValDataPropAsObjectProp.schemaEntity],
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [
                try ValBadClass.schemaEntity,
                try ValBadRelation.schemaEntity,
            ],
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [try OntologyPersistenceEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Empty schema passes validation")
    func emptySchemaPassesValidation() async throws {
        let context = try await setupContext()
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Validation against non-existent ontology IRI reports errors")
    func validationAgainstNonExistentOntology() async throws {
        let context = try await setupContext()
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [try ValEmployee.schemaEntity],
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        // ValEmployee has @OWLDataProperty("http://test.org/onto#name") which exists
        let schema = try Schema(
            entities: [try ValEmployee.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }

    @Test("Invalid data property IRI fails schema validation")
    func invalidDataPropertyFails() async throws {
        let context = try await setupContext()
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [try ValBadDataProperty.schemaEntity],
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        let schema = try Schema(
            entities: [try ValObjPropAsDataProp.schemaEntity],
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
        defer { await context.container.shutdown() }
        try await loadTestOntology(context: context)

        // ValEmployee: @OWLClass("...#Employee") + @OWLDataProperty("...#name")
        // ValAssignment: @OWLObjectProperty("...#worksOn")
        let schema = try Schema(
            entities: [
                try ValEmployee.schemaEntity,
                try ValAssignment.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        try await context.ontology.validateSchema(schema, ontologyIRI: Self.ontologyIRI)
    }
}
#endif
