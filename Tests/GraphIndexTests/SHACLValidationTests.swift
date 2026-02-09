// SHACLValidationTests.swift
// End-to-end integration tests for SHACL validation against FoundationDB
//
// These tests validate the complete SHACL validation path:
//   User Code -> FDBContext.shacl -> SHACLValidator -> SHACLTargetResolver/SHACLConstraintEvaluator -> FDB
//
// All literal values are stored using RDFTerm N-Triples encoding (W3C RDF 1.1):
//   IRI:       "ex:Alice"                    → stored as-is
//   Literal:   RDFTerm.string("Alice").encoded → stored as "\"Alice\""
//   Typed:     RDFTerm.integer(30).encoded    → stored as "\"30\"^^xsd:integer"
//   Lang:      RDFTerm.langString("hi", language: "en").encoded → "\"hi\"@en"
//   BlankNode: "_:b1"                         → stored as-is
//
// Reference: W3C SHACL https://www.w3.org/TR/shacl/

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

/// RDF-like statement for SHACL validation testing
@Persistable
struct SHACLTestStatement {
    #Directory<SHACLTestStatement>("test", "shacl", "statements")

    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index(GraphIndexKind<SHACLTestStatement>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        strategy: .hexastore
    ))
}

// MARK: - Test Suite

@Suite("SHACL Validation Tests", .serialized)
struct SHACLValidationTests {

    // MARK: - Setup Helpers

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema([SHACLTestStatement.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        do {
            try await directoryLayer.remove(path: ["test", "shacl", "statements"])
        } catch {
            // Directory may not exist on first run
        }
        try await container.newContext().shacl.deleteAllShapesGraphs()
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: SHACLTestStatement.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in SHACLTestStatement.indexDescriptors {
            let maxAttempts = 3
            for attempt in 1...maxAttempts {
                let currentState = try await indexStateManager.state(of: descriptor.name)

                switch currentState {
                case .disabled:
                    do {
                        try await indexStateManager.enable(descriptor.name)
                        try await indexStateManager.makeReadable(descriptor.name)
                        break
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .writeOnly:
                    do {
                        try await indexStateManager.makeReadable(descriptor.name)
                        break
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .readable:
                    break
                }
            }
        }
    }

    private func insertStatements(_ statements: [SHACLTestStatement], context: FDBContext) async throws {
        for statement in statements {
            context.insert(statement)
        }
        try await context.save()
    }

    private func makeStatement(subject: String, predicate: String, object: String) -> SHACLTestStatement {
        var stmt = SHACLTestStatement()
        stmt.subject = subject
        stmt.predicate = predicate
        stmt.object = object
        return stmt
    }

    /// Insert a standard person dataset for SHACL tests.
    /// Literal values use RDFTerm encoding; IRI values (rdf:type objects) are stored as-is.
    private func insertPersonData(context: FDBContext) async throws {
        try await insertStatements([
            // Alice is a Person with name and email
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Alice", predicate: "ex:email", object: RDFTerm.string("alice@example.com").encoded),
            // Bob is a Person with name
            makeStatement(subject: "ex:Bob", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Bob", predicate: "ex:name", object: RDFTerm.string("Bob").encoded),
            // Carol is a Person with name and age
            makeStatement(subject: "ex:Carol", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Carol", predicate: "ex:name", object: RDFTerm.string("Carol").encoded),
            makeStatement(subject: "ex:Carol", predicate: "ex:age", object: RDFTerm.integer(30).encoded),
        ], context: context)
    }

    /// Create a basic PersonShape that requires ex:name with minCount(1)
    private func makePersonShapesGraph() -> SHACLShapesGraph {
        SHACLShapesGraph(
            iri: "ex:PersonShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:PersonShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.minCount(1)]
                        )
                    ]
                ))
            ],
            prefixes: .standard
        )
    }

    // MARK: - Shapes Store CRUD

    @Test("Load and get shapes graph")
    func testLoadAndGetShapesGraph() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()
        let shapesGraph = makePersonShapesGraph()

        // Load shapes graph
        try await context.shacl.loadShapes(shapesGraph)

        // Retrieve and verify
        let loaded = try await context.shacl.getShapesGraph(iri: "ex:PersonShapes")
        #expect(loaded != nil)
        #expect(loaded?.iri == "ex:PersonShapes")
        #expect(loaded?.shapes.count == 1)

        if case .node(let nodeShape) = loaded?.shapes.first {
            #expect(nodeShape.iri == "ex:PersonShape")
            #expect(nodeShape.targets.count == 1)
            #expect(nodeShape.propertyShapes.count == 1)
        } else {
            Issue.record("Expected a node shape")
        }

        try await cleanup(container: container)
    }

    @Test("List shapes graphs")
    func testListShapesGraphs() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Load multiple shapes graphs
        let graph1 = SHACLShapesGraph(iri: "ex:Shapes1", shapes: [])
        let graph2 = SHACLShapesGraph(iri: "ex:Shapes2", shapes: [])
        let graph3 = SHACLShapesGraph(iri: "ex:Shapes3", shapes: [])

        try await context.shacl.loadShapes(graph1)
        try await context.shacl.loadShapes(graph2)
        try await context.shacl.loadShapes(graph3)

        // List all
        let iris = try await context.shacl.listShapesGraphs()
        #expect(iris.count == 3)
        #expect(iris.contains("ex:Shapes1"))
        #expect(iris.contains("ex:Shapes2"))
        #expect(iris.contains("ex:Shapes3"))

        try await cleanup(container: container)
    }

    @Test("Delete shapes graph")
    func testDeleteShapesGraph() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Load a shapes graph
        let shapesGraph = makePersonShapesGraph()
        try await context.shacl.loadShapes(shapesGraph)

        // Verify it exists
        let before = try await context.shacl.getShapesGraph(iri: "ex:PersonShapes")
        #expect(before != nil)

        // Delete it
        try await context.shacl.deleteShapesGraph(iri: "ex:PersonShapes")

        // Verify it is gone
        let after = try await context.shacl.getShapesGraph(iri: "ex:PersonShapes")
        #expect(after == nil)

        try await cleanup(container: container)
    }

    // MARK: - Basic Validation - Conforming

    @Test("Conforming data graph produces no violations")
    func testConformingDataGraph() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: all Persons have at least one ex:name (RDFTerm-encoded literals)
        try await insertPersonData(context: context)

        // Load shapes: Person must have ex:name (minCount 1)
        let shapesGraph = makePersonShapesGraph()
        try await context.shacl.loadShapes(shapesGraph)

        // Validate
        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:PersonShapes"
        )

        #expect(report.conforms == true)
        #expect(report.violations.isEmpty)

        try await cleanup(container: container)
    }

    // MARK: - Cardinality Constraints

    @Test("minCount violation when required property is missing")
    func testMinCountViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Dave is a Person but has NO ex:name
        try await insertStatements([
            makeStatement(subject: "ex:Dave", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Dave", predicate: "ex:age", object: RDFTerm.integer(25).encoded),
        ], context: context)

        // Shape: Person must have ex:name (minCount 1)
        let shapesGraph = makePersonShapesGraph()
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:PersonShapes"
        )

        #expect(report.conforms == false)
        #expect(report.violations.count >= 1)

        let violation = report.violations.first
        #expect(violation?.focusNode == "ex:Dave")
        #expect(violation?.sourceConstraintComponent == "sh:MinCountConstraintComponent")

        try await cleanup(container: container)
    }

    @Test("maxCount violation when too many values")
    func testMaxCountViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Eve is a Person with TWO names (RDFTerm-encoded literals)
        try await insertStatements([
            makeStatement(subject: "ex:Eve", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Eve", predicate: "ex:name", object: RDFTerm.string("Eve").encoded),
            makeStatement(subject: "ex:Eve", predicate: "ex:name", object: RDFTerm.string("Evelyn").encoded),
        ], context: context)

        // Shape: Person must have exactly one ex:name (minCount 1, maxCount 1)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:StrictPersonShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:StrictPersonShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.minCount(1), .maxCount(1)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:StrictPersonShapes"
        )

        #expect(report.conforms == false)
        let maxViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:MaxCountConstraintComponent"
        }
        #expect(maxViolations.count >= 1)
        #expect(maxViolations.first?.focusNode == "ex:Eve")

        try await cleanup(container: container)
    }

    // MARK: - Value Type Constraints

    @Test("sh:class constraint violation")
    func testClassConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data (rdf:type objects are IRIs — no RDFTerm encoding needed)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:knows", object: "ex:Bob"),
            // Bob is an Organization, not a Person
            makeStatement(subject: "ex:Bob", predicate: "rdf:type", object: "ex:Organization"),
        ], context: context)

        // Shape: ex:knows values must be instances of ex:Person
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:KnowsShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:KnowsPersonShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:knows"),
                            constraints: [.class_("ex:Person")]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:KnowsShapes"
        )

        #expect(report.conforms == false)
        let classViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:ClassConstraintComponent"
        }
        #expect(classViolations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:nodeKind constraint violation: blank node where IRI expected")
    func testNodeKindConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice knows a blank node (stored with _: prefix)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:knows", object: "_:blank1"),
        ], context: context)

        // Shape: ex:knows values must be IRIs (sh:nodeKind sh:IRI)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:NodeKindShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:IRIOnlyShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:knows"),
                            constraints: [.nodeKind(.iri)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:NodeKindShapes"
        )

        // _:blank1 is a blank node, not an IRI, so it should produce a violation
        #expect(report.conforms == false)
        let nodeKindViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:NodeKindConstraintComponent"
        }
        #expect(nodeKindViolations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:nodeKind distinguishes IRI from literal")
    func testNodeKindIRIvsLiteral() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has a name (literal) and knows Bob (IRI)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Alice", predicate: "ex:knows", object: "ex:Bob"),
        ], context: context)

        // Shape 1: ex:name must be a literal
        let literalShapes = SHACLShapesGraph(
            iri: "ex:LiteralShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:NameLiteralShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.nodeKind(.literal)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(literalShapes)

        let report1 = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:LiteralShapes"
        )
        // Name is a literal → no violation
        #expect(report1.conforms == true)

        // Shape 2: ex:name must be an IRI (should fail since name is a literal)
        try await context.shacl.deleteShapesGraph(iri: "ex:LiteralShapes")

        let iriShapes = SHACLShapesGraph(
            iri: "ex:IRIShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:NameIRIShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.nodeKind(.iri)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(iriShapes)

        let report2 = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:IRIShapes"
        )
        // Name is a literal, not IRI → violation
        #expect(report2.conforms == false)
        let violations = report2.violations.filter {
            $0.sourceConstraintComponent == "sh:NodeKindConstraintComponent"
        }
        #expect(violations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:datatype validates literal datatype")
    func testDatatypeConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has a string name and integer age
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Alice", predicate: "ex:age", object: RDFTerm.integer(30).encoded),
        ], context: context)

        // Shape: ex:name must be xsd:string, ex:age must be xsd:integer
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:DatatypeShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:DatatypePersonShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.datatype("xsd:string")]
                        ),
                        PropertyShape(
                            path: .predicate("ex:age"),
                            constraints: [.datatype("xsd:integer")]
                        ),
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:DatatypeShapes"
        )
        // Both match their datatypes → no violation
        #expect(report.conforms == true)

        // Now test wrong datatype: expect ex:name to be xsd:integer (should fail)
        try await context.shacl.deleteShapesGraph(iri: "ex:DatatypeShapes")

        let wrongShapes = SHACLShapesGraph(
            iri: "ex:WrongDatatypeShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:WrongDatatypeShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.datatype("xsd:integer")]
                        ),
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(wrongShapes)

        let report2 = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:WrongDatatypeShapes"
        )
        // Name is xsd:string, expected xsd:integer → violation
        #expect(report2.conforms == false)
        let dtViolations = report2.violations.filter {
            $0.sourceConstraintComponent == "sh:DatatypeConstraintComponent"
        }
        #expect(dtViolations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:datatype rejects IRI where literal expected")
    func testDatatypeRejectsIRI() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: ex:knows value is an IRI (not a literal)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:knows", object: "ex:Bob"),
        ], context: context)

        // Shape: ex:knows must be xsd:string (impossible for an IRI)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:DatatypeIRIShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:DatatypeIRIShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:knows"),
                            constraints: [.datatype("xsd:string")]
                        ),
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:DatatypeIRIShapes"
        )
        // IRI is not a literal → datatype violation
        #expect(report.conforms == false)
        let dtViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:DatatypeConstraintComponent"
        }
        #expect(dtViolations.count >= 1)

        try await cleanup(container: container)
    }

    // MARK: - String Constraints

    @Test("sh:minLength violation with empty string literal")
    func testMinLengthViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has an empty name (RDFTerm-encoded empty literal)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("").encoded),
        ], context: context)

        // Shape: ex:name must have minLength 1
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:MinLengthShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:NameLengthShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.minLength(1)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:MinLengthShapes"
        )

        #expect(report.conforms == false)
        let lengthViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:MinLengthConstraintComponent"
        }
        #expect(lengthViolations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:pattern violation with non-matching email literal")
    func testPatternViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has an invalid email (RDFTerm-encoded literal)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:email", object: RDFTerm.string("not-an-email").encoded),
        ], context: context)

        // Shape: ex:email must match email-like pattern
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:PatternShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:EmailPatternShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:email"),
                            constraints: [.pattern("^[^@]+@[^@]+\\.[^@]+$", flags: nil)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:PatternShapes"
        )

        #expect(report.conforms == false)
        let patternViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:PatternConstraintComponent"
        }
        #expect(patternViolations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:languageIn validates language tags on literals")
    func testLanguageInConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has labels in English (allowed) and Japanese (not allowed)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "rdfs:label",
                          object: RDFTerm.langString("Alice", language: "en").encoded),
            makeStatement(subject: "ex:Alice", predicate: "rdfs:label",
                          object: RDFTerm.langString("アリス", language: "ja").encoded),
        ], context: context)

        // Shape: rdfs:label must have language in ["en", "de", "fr"]
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:LangShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:LabelLangShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("rdfs:label"),
                            constraints: [.languageIn(["en", "de", "fr"])]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:LangShapes"
        )

        // "en" is allowed, "ja" is not → one violation
        #expect(report.conforms == false)
        let langViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:LanguageInConstraintComponent"
        }
        #expect(langViolations.count == 1)

        try await cleanup(container: container)
    }

    // MARK: - Value Range Constraints

    @Test("sh:minInclusive and sh:maxInclusive validate numeric range")
    func testValueRangeConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has age 30 (valid), Bob has age 200 (too high)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:age", object: RDFTerm.integer(30).encoded),
            makeStatement(subject: "ex:Bob", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Bob", predicate: "ex:age", object: RDFTerm.integer(200).encoded),
        ], context: context)

        // Shape: ex:age must be between 0 and 150 (inclusive)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:RangeShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:AgeRangeShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:age"),
                            constraints: [
                                .minInclusive(.integer(0)),
                                .maxInclusive(.integer(150)),
                            ]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:RangeShapes"
        )

        // Alice's age 30 is valid, Bob's age 200 > 150 → violation
        #expect(report.conforms == false)
        let rangeViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:MaxInclusiveConstraintComponent"
        }
        #expect(rangeViolations.count >= 1)
        #expect(rangeViolations.first?.focusNode == "ex:Bob")

        try await cleanup(container: container)
    }

    // MARK: - sh:closed

    @Test("sh:closed violation with unexpected properties")
    func testClosedShapeViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has name (declared) and age (unexpected)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Alice", predicate: "ex:age", object: RDFTerm.integer(30).encoded),
        ], context: context)

        // Shape: closed shape only allows rdf:type and ex:name
        // Note: sh:closed automatically includes predicates from declared property shapes (W3C SHACL 4.8.1)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:ClosedShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:ClosedPersonShape",
                    targets: [.class_("ex:Person")],
                    constraints: [.closed(ignoredProperties: ["rdf:type"])],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.minCount(1)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:ClosedShapes"
        )

        #expect(report.conforms == false)
        let closedViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:ClosedConstraintComponent"
        }
        #expect(closedViolations.count >= 1)

        // The violation should reference ex:age as the unexpected property
        let hasAgeViolation = closedViolations.contains { result in
            if case .predicate(let pred) = result.resultPath {
                return pred == "ex:age"
            }
            return false
        }
        #expect(hasAgeViolation)

        try await cleanup(container: container)
    }

    @Test("sh:closed with ignoredProperties produces no violation")
    func testClosedShapeWithIgnored() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has name (declared), age (in ignoredProperties), and rdf:type (also ignored)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Alice", predicate: "ex:age", object: RDFTerm.integer(30).encoded),
        ], context: context)

        // Shape: closed shape, but ex:age and rdf:type are in ignoredProperties
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:ClosedIgnoredShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:ClosedIgnoredPersonShape",
                    targets: [.class_("ex:Person")],
                    constraints: [.closed(ignoredProperties: ["rdf:type", "ex:age"])],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.minCount(1)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:ClosedIgnoredShapes"
        )

        #expect(report.conforms == true)
        #expect(report.violations.isEmpty)

        try await cleanup(container: container)
    }

    // MARK: - sh:hasValue and sh:in

    @Test("sh:hasValue violation when required value is absent")
    func testHasValueViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has rdf:type ex:Person but NOT ex:Agent (both are IRIs)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
        ], context: context)

        // Shape: every Person must have rdf:type ex:Agent as well
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:HasValueShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:AgentRequiredShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("rdf:type"),
                            constraints: [.hasValue(.iri("ex:Agent"))]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:HasValueShapes"
        )

        #expect(report.conforms == false)
        let hasValueViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:HasValueConstraintComponent"
        }
        #expect(hasValueViolations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:in constraint violation when value not in allowed list")
    func testInConstraintViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has status "ex:draft" (IRI) which is not in the allowed list
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:status", object: "ex:draft"),
        ], context: context)

        // Shape: ex:status must be one of [ex:active, ex:inactive] (IRIs)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:InConstraintShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:StatusShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:status"),
                            constraints: [.in_([.iri("ex:active"), .iri("ex:inactive")])]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:InConstraintShapes"
        )

        #expect(report.conforms == false)
        let inViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:InConstraintComponent"
        }
        #expect(inViolations.count >= 1)

        try await cleanup(container: container)
    }

    @Test("sh:hasValue with literal value")
    func testHasValueWithLiteral() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has status literal "active"
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:status", object: RDFTerm.string("active").encoded),
        ], context: context)

        // Shape: ex:status must have value "active" (literal)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:HasValueLiteralShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:StatusLiteralShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:status"),
                            constraints: [.hasValue(.string("active"))]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:HasValueLiteralShapes"
        )

        // Literal "active" matches → conforms
        #expect(report.conforms == true)

        try await cleanup(container: container)
    }

    // MARK: - Target Resolution

    @Test("sh:targetNode targets a specific node")
    func testTargetNode() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice and Bob both have names (RDFTerm-encoded literals)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Alice", predicate: "ex:email", object: RDFTerm.string("alice@example.com").encoded),
            makeStatement(subject: "ex:Bob", predicate: "ex:name", object: RDFTerm.string("Bob").encoded),
            // Bob has no email
        ], context: context)

        // Shape: only ex:Alice must have ex:email (targetNode)
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:TargetNodeShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:AliceEmailShape",
                    targets: [.node("ex:Alice")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:email"),
                            constraints: [.minCount(1)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:TargetNodeShapes"
        )

        // Alice has email, so no violation. Bob is not targeted.
        #expect(report.conforms == true)
        #expect(report.violations.isEmpty)

        try await cleanup(container: container)
    }

    @Test("sh:targetClass targets all instances of a class")
    func testTargetClass() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: two Persons, one Animal (RDFTerm-encoded literals)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Bob", predicate: "rdf:type", object: "ex:Person"),
            // Bob has NO name -> will violate
            makeStatement(subject: "ex:Fido", predicate: "rdf:type", object: "ex:Animal"),
            // Fido has no name, but Animals are not targeted
        ], context: context)

        // Shape: all Persons must have ex:name
        let shapesGraph = makePersonShapesGraph()
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:PersonShapes"
        )

        // Alice conforms, Bob does not, Fido is not targeted
        #expect(report.conforms == false)
        #expect(report.violations.count == 1)
        #expect(report.violations.first?.focusNode == "ex:Bob")

        try await cleanup(container: container)
    }

    @Test("sh:targetSubjectsOf targets subjects with given predicate")
    func testTargetSubjectsOf() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice and Bob have emails (RDFTerm-encoded literals), Carol does not
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "ex:email", object: RDFTerm.string("alice@example.com").encoded),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Bob", predicate: "ex:email", object: RDFTerm.string("bob@example.com").encoded),
            // Bob has email but no name -> violation
            makeStatement(subject: "ex:Carol", predicate: "ex:name", object: RDFTerm.string("Carol").encoded),
            // Carol has no email, so she is NOT targeted
        ], context: context)

        // Shape: any subject that has ex:email must also have ex:name
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:SubjectsOfShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:EmailHolderShape",
                    targets: [.subjectsOf("ex:email")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.minCount(1)]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:SubjectsOfShapes"
        )

        // Alice conforms (has both email and name)
        // Bob violates (has email but no name)
        // Carol is not targeted (no email)
        #expect(report.conforms == false)
        let nameViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:MinCountConstraintComponent"
        }
        #expect(nameViolations.count == 1)
        #expect(nameViolations.first?.focusNode == "ex:Bob")

        try await cleanup(container: container)
    }

    @Test("Deactivated shape produces no validation results")
    func testDeactivatedShapeSkipped() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Dave is a Person with no name (would normally violate)
        try await insertStatements([
            makeStatement(subject: "ex:Dave", predicate: "rdf:type", object: "ex:Person"),
        ], context: context)

        // Shape: deactivated PersonShape
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:DeactivatedShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:DeactivatedPersonShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [.minCount(1)]
                        )
                    ],
                    deactivated: true  // This shape is deactivated
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:DeactivatedShapes"
        )

        // Deactivated shape should produce no results
        #expect(report.conforms == true)
        #expect(report.results.isEmpty)

        try await cleanup(container: container)
    }

    // MARK: - Logical Constraints

    @Test("sh:or constraint allows one of multiple shapes")
    func testOrConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has string name (valid), Bob has integer "name" (valid via or), Carol has no name
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:identifier",
                          object: RDFTerm.string("alice123").encoded),
            makeStatement(subject: "ex:Bob", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Bob", predicate: "ex:identifier",
                          object: RDFTerm.integer(42).encoded),
            makeStatement(subject: "ex:Carol", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Carol", predicate: "ex:identifier",
                          object: RDFTerm.boolean(true).encoded),
        ], context: context)

        // Shape: ex:identifier must be either xsd:string or xsd:integer
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:OrShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:IdentifierShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:identifier"),
                            constraints: [
                                .or([
                                    .node(NodeShape(constraints: [.datatype("xsd:string")])),
                                    .node(NodeShape(constraints: [.datatype("xsd:integer")])),
                                ])
                            ]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:OrShapes"
        )

        // Alice (string) and Bob (integer) conform; Carol (boolean) violates
        #expect(report.conforms == false)
        let orViolations = report.violations.filter {
            $0.focusNode == "ex:Carol"
        }
        #expect(orViolations.count >= 1)

        // Alice and Bob should have no violations
        let aliceViolations = report.violations.filter { $0.focusNode == "ex:Alice" }
        let bobViolations = report.violations.filter { $0.focusNode == "ex:Bob" }
        #expect(aliceViolations.isEmpty)
        #expect(bobViolations.isEmpty)

        try await cleanup(container: container)
    }

    @Test("sh:not constraint rejects matching shape")
    func testNotConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has name "Alice" (string), Bob has name with integer datatype
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Bob", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Bob", predicate: "ex:name",
                          object: RDFTerm.literal(OWLLiteral(lexicalForm: "42", datatype: "xsd:integer")).encoded),
        ], context: context)

        // Shape: ex:name must NOT be xsd:integer
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:NotShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:NameNotIntegerShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("ex:name"),
                            constraints: [
                                .not(.node(NodeShape(constraints: [.datatype("xsd:integer")])))
                            ]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:NotShapes"
        )

        // Alice's name is xsd:string → passes sh:not(xsd:integer)
        // Bob's name is xsd:integer → fails sh:not(xsd:integer)
        #expect(report.conforms == false)
        let notViolations = report.violations.filter { $0.focusNode == "ex:Bob" }
        #expect(notViolations.count >= 1)

        let aliceViolations = report.violations.filter { $0.focusNode == "ex:Alice" }
        #expect(aliceViolations.isEmpty)

        try await cleanup(container: container)
    }

    // MARK: - Unique Language

    @Test("sh:uniqueLang violation when duplicate language tags")
    func testUniqueLangConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data: Alice has two English labels (duplicate lang tag)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "rdfs:label",
                          object: RDFTerm.langString("Alice", language: "en").encoded),
            makeStatement(subject: "ex:Alice", predicate: "rdfs:label",
                          object: RDFTerm.langString("Ali", language: "en").encoded),
            makeStatement(subject: "ex:Alice", predicate: "rdfs:label",
                          object: RDFTerm.langString("Alicia", language: "es").encoded),
        ], context: context)

        // Shape: rdfs:label must have unique language tags
        let shapesGraph = SHACLShapesGraph(
            iri: "ex:UniqueLangShapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:UniqueLangShape",
                    targets: [.class_("ex:Person")],
                    propertyShapes: [
                        PropertyShape(
                            path: .predicate("rdfs:label"),
                            constraints: [.uniqueLang]
                        )
                    ]
                ))
            ]
        )
        try await context.shacl.loadShapes(shapesGraph)

        let report = try await context.shacl.validate(
            SHACLTestStatement.self,
            against: "ex:UniqueLangShapes"
        )

        // Two "en" labels → uniqueLang violation
        #expect(report.conforms == false)
        let uniqueLangViolations = report.violations.filter {
            $0.sourceConstraintComponent == "sh:UniqueLangConstraintComponent"
        }
        #expect(uniqueLangViolations.count >= 1)

        try await cleanup(container: container)
    }

    // MARK: - FDBContext+SHACL API

    @Test("validateNode API validates a specific node against a shape")
    func testValidateNodeAgainstShape() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert data (RDFTerm-encoded literals)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
            makeStatement(subject: "ex:Alice", predicate: "ex:name", object: RDFTerm.string("Alice").encoded),
            makeStatement(subject: "ex:Bob", predicate: "rdf:type", object: "ex:Person"),
            // Bob has no name
        ], context: context)

        // Load shapes
        let shapesGraph = makePersonShapesGraph()
        try await context.shacl.loadShapes(shapesGraph)

        // Validate only Alice
        let aliceReport = try await context.shacl.validateNode(
            SHACLTestStatement.self,
            nodeIRI: "ex:Alice",
            against: "ex:PersonShape",
            in: "ex:PersonShapes"
        )
        #expect(aliceReport.conforms == true)

        // Validate only Bob
        let bobReport = try await context.shacl.validateNode(
            SHACLTestStatement.self,
            nodeIRI: "ex:Bob",
            against: "ex:PersonShape",
            in: "ex:PersonShapes"
        )
        #expect(bobReport.conforms == false)
        #expect(bobReport.violations.count >= 1)
        #expect(bobReport.violations.first?.focusNode == "ex:Bob")

        try await cleanup(container: container)
    }

    // MARK: - Shapes Graph Not Found

    @Test("Validate against nonexistent shapes graph throws error")
    func testValidateNonexistentShapesGraph() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = container.newContext()

        // Insert some data (doesn't matter what)
        try await insertStatements([
            makeStatement(subject: "ex:Alice", predicate: "rdf:type", object: "ex:Person"),
        ], context: context)

        // Validate against a shapes graph that does not exist
        do {
            _ = try await context.shacl.validate(
                SHACLTestStatement.self,
                against: "ex:NonexistentShapes"
            )
            Issue.record("Expected SHACLError.shapesGraphNotFound to be thrown")
        } catch let error as SHACLError {
            if case .shapesGraphNotFound(let iri) = error {
                #expect(iri == "ex:NonexistentShapes")
            } else {
                Issue.record("Expected shapesGraphNotFound, got \(error)")
            }
        }

        try await cleanup(container: container)
    }
}
