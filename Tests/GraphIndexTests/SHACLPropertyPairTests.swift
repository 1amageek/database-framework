import DatabaseKit
import TestSupport
import DatabaseRuntime
import DatabaseTypes
import Foundation
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("SHACL property pair contracts", .serialized, .heartbeat)
struct SHACLPropertyPairTests {
    @Persistable
    struct Statement {
        #Directory<Statement>("shacl_property_pair_tests", "statements")

        var id: String = Foundation.UUID().uuidString
        var subject: RDFTerm = .iri(.xsdString)
        var predicate: RDFTerm = .iri(.xsdString)
        var object: RDFTerm = .iri(.xsdString)

        #Index(
            .rdfDataset,
            from: \Statement.subject,
            edge: \Statement.predicate,
            to: \Statement.object
        )
    }

    private static let rdfType =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    private static let xsdDecimal =
        "http://www.w3.org/2001/XMLSchema#decimal"

    private func setupContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [try Statement.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(Statement.self)]),
            security: .testingDisabled
        )
    }

    private func makeStatement(
        subject: String,
        predicate: String,
        object: RDFTerm
    ) throws -> Statement {
        var statement = Statement()
        statement.subject = try .iri(validating: subject)
        statement.predicate = try .iri(validating: predicate)
        statement.object = object
        return statement
    }

    @Test("lessThan compares RDF literal values without dropping terms")
    func lessThanUsesRDFValueSemantics() async throws {
        let container = try await setupContainer()
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        for statement in [
            try makeStatement(
                subject: "ex:Alice",
                predicate: Self.rdfType,
                object: .iri(validating: "ex:Person")
            ),
            try makeStatement(
                subject: "ex:Alice",
                predicate: "ex:start",
                object: RDFTerm.integer(2)
            ),
            try makeStatement(
                subject: "ex:Alice",
                predicate: "ex:end",
                object: RDFTerm.integer(10)
            ),
            try makeStatement(
                subject: "ex:Bob",
                predicate: Self.rdfType,
                object: .iri(validating: "ex:Person")
            ),
            try makeStatement(
                subject: "ex:Bob",
                predicate: "ex:start",
                object: RDFTerm.integer(10)
            ),
            try makeStatement(
                subject: "ex:Bob",
                predicate: "ex:end",
                object: RDFTerm.integer(2)
            ),
            try makeStatement(
                subject: "ex:Eve",
                predicate: Self.rdfType,
                object: .iri(validating: "ex:Person")
            ),
            try makeStatement(
                subject: "ex:Eve",
                predicate: "ex:start",
                object: .literal(
                    try RDFLiteral(
                        lexicalForm: "9007199254740992.1",
                        datatype: Self.xsdDecimal
                    )
                )
            ),
            try makeStatement(
                subject: "ex:Eve",
                predicate: "ex:end",
                object: .literal(
                    try RDFLiteral(
                        lexicalForm: "9007199254740992.2",
                        datatype: Self.xsdDecimal
                    )
                )
            ),
        ] {
            try context.insert(statement)
        }
        try await context.save()

        let shapesGraph = SHACLShapesGraph(
            iri: "ex:PropertyPairShapes",
            shapes: [
                .node(NodeShape(
                    identifier: try .iri(
                        validating: "ex:PropertyPairShape"
                    ),
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate(
                                try RDFPredicateIRI("ex:start")
                            ),
                            constraints: [
                                .lessThan(
                                    .predicate(
                                        try RDFPredicateIRI("ex:end")
                                    )
                                ),
                            ]
                        ),
                    ]
                )),
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            Statement.self,
            against: "ex:PropertyPairShapes"
        )
        let violations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:LessThanConstraintComponent"
        }

        #expect(report.conforms == false)
        #expect(violations.count == 1)
        #expect(
            violations.first?.focusNode
                == (try .iri(validating: "ex:Bob"))
        )
        #expect(violations.first?.value == RDFTerm.integer(10))
    }
}
