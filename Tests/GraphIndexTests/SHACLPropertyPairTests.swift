import Core
import DatabaseRuntime
import DatabaseValue
import Graph
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("SHACL property pair contracts", .serialized, .heartbeat)
struct SHACLPropertyPairTests {
    @Persistable
    struct Statement {
        #Directory<Statement>("shacl_property_pair_tests", "statements")

        var id: String = ULID().ulidString
        var subject: DatabaseRDFTerm = .iri("urn:subject")
        var predicate: DatabaseRDFTerm = .iri("urn:predicate")
        var object: DatabaseRDFTerm = .iri("urn:object")

        #Index(RDFQuadIndexKind<Statement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object
        ))
    }

    private static let rdfType =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    private static let xsdDecimal =
        "http://www.w3.org/2001/XMLSchema#decimal"

    private func setupContainer() async throws -> DBContainer {
        let schema = Schema(
            [Statement.self],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }

    private func makeStatement(
        subject: String,
        predicate: String,
        object: DatabaseRDFTerm
    ) -> Statement {
        var statement = Statement()
        statement.subject = .iri(subject)
        statement.predicate = .iri(predicate)
        statement.object = object
        return statement
    }

    @Test("lessThan compares RDF literal values without dropping terms")
    func lessThanUsesRDFValueSemantics() async throws {
        let container = try await setupContainer()

        let context = container.newContext()
        for statement in [
            makeStatement(
                subject: "ex:Alice",
                predicate: Self.rdfType,
                object: .iri("ex:Person")
            ),
            makeStatement(
                subject: "ex:Alice",
                predicate: "ex:start",
                object: RDFTerm.integer(2)
            ),
            makeStatement(
                subject: "ex:Alice",
                predicate: "ex:end",
                object: RDFTerm.integer(10)
            ),
            makeStatement(
                subject: "ex:Bob",
                predicate: Self.rdfType,
                object: .iri("ex:Person")
            ),
            makeStatement(
                subject: "ex:Bob",
                predicate: "ex:start",
                object: RDFTerm.integer(10)
            ),
            makeStatement(
                subject: "ex:Bob",
                predicate: "ex:end",
                object: RDFTerm.integer(2)
            ),
            makeStatement(
                subject: "ex:Eve",
                predicate: Self.rdfType,
                object: .iri("ex:Person")
            ),
            makeStatement(
                subject: "ex:Eve",
                predicate: "ex:start",
                object: .literal(
                    try DatabaseRDFLiteral(
                        lexicalForm: "9007199254740992.1",
                        datatype: Self.xsdDecimal
                    )
                )
            ),
            makeStatement(
                subject: "ex:Eve",
                predicate: "ex:end",
                object: .literal(
                    try DatabaseRDFLiteral(
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
                    identifier: .iri("ex:PropertyPairShape"),
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:start"),
                            constraints: [
                                .lessThan(.predicate("ex:end")),
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
        #expect(violations.first?.focusNode == .iri("ex:Bob"))
        #expect(violations.first?.value == RDFTerm.integer(10))
    }
}
