// SHACLTypeTests.swift
// Pure unit tests for SHACL types, PrefixMap, and RDFTerm
//
// These tests do NOT require FoundationDB. They verify
// Codable round-trips, computed properties, and convenience APIs
// for the Graph module's SHACL types, PrefixMap, and RDFTerm.

import Testing
import Foundation
import Graph

// MARK: - RDFTerm Tests

@Suite("RDFTerm Tests")
struct RDFTermTests {

    // MARK: - Encoding

    @Test("IRI encodes as-is")
    func testIRIEncoding() {
        let term = RDFTerm.iri("ex:Alice")
        #expect(term.encoded == "ex:Alice")

        let fullIRI = RDFTerm.iri("http://example.org/Person")
        #expect(fullIRI.encoded == "http://example.org/Person")
    }

    @Test("Plain literal encodes with quotes")
    func testPlainLiteralEncoding() {
        let term = RDFTerm.string("Alice")
        #expect(term.encoded == "\"Alice\"")
    }

    @Test("Typed literal encodes with ^^ datatype")
    func testTypedLiteralEncoding() {
        let term = RDFTerm.literal(OWLLiteral(lexicalForm: "30", datatype: "xsd:integer"))
        #expect(term.encoded == "\"30\"^^xsd:integer")
    }

    @Test("xsd:string literal uses compact form (no ^^xsd:string suffix)")
    func testXSDStringCompactForm() {
        // xsd:string uses compact form per W3C N-Triples convention
        let term = RDFTerm.string("hello")
        #expect(term.encoded == "\"hello\"")
    }

    @Test("Language-tagged literal encodes with @ tag")
    func testLanguageTagEncoding() {
        let term = RDFTerm.literal(OWLLiteral(lexicalForm: "hello", datatype: "rdf:langString", language: "en"))
        #expect(term.encoded == "\"hello\"@en")

        let withLang = RDFTerm.langString("bonjour", language: "fr")
        #expect(withLang.encoded == "\"bonjour\"@fr")
    }

    @Test("Blank node encodes with _: prefix")
    func testBlankNodeEncoding() {
        let term = RDFTerm.blankNode("b1")
        #expect(term.encoded == "_:b1")
    }

    @Test("Special characters are escaped in literals")
    func testEscaping() {
        let term = RDFTerm.string("line1\nline2\ttab \"quoted\" \\slash")
        let encoded = term.encoded
        #expect(encoded == "\"line1\\nline2\\ttab \\\"quoted\\\" \\\\slash\"")
    }

    // MARK: - Decoding

    @Test("Decode IRI (no special prefix)")
    func testDecodeIRI() {
        let term = RDFTerm.decode("ex:Alice")
        if case .iri(let value) = term {
            #expect(value == "ex:Alice")
        } else {
            Issue.record("Expected .iri, got \(term)")
        }
    }

    @Test("Decode blank node")
    func testDecodeBlankNode() {
        let term = RDFTerm.decode("_:b1")
        if case .blankNode(let id) = term {
            #expect(id == "b1")
        } else {
            Issue.record("Expected .blankNode, got \(term)")
        }
    }

    @Test("Decode plain literal")
    func testDecodePlainLiteral() {
        let term = RDFTerm.decode("\"Alice\"")
        if case .literal(let owl) = term {
            #expect(owl.lexicalForm == "Alice")
            #expect(owl.datatype == "xsd:string")
            #expect(owl.language == nil)
        } else {
            Issue.record("Expected .literal, got \(term)")
        }
    }

    @Test("Decode typed literal")
    func testDecodeTypedLiteral() {
        let term = RDFTerm.decode("\"30\"^^xsd:integer")
        if case .literal(let owl) = term {
            #expect(owl.lexicalForm == "30")
            #expect(owl.datatype == "xsd:integer")
            #expect(owl.language == nil)
        } else {
            Issue.record("Expected .literal, got \(term)")
        }
    }

    @Test("Decode language-tagged literal")
    func testDecodeLanguageTagged() {
        let term = RDFTerm.decode("\"hello\"@en")
        if case .literal(let owl) = term {
            #expect(owl.lexicalForm == "hello")
            #expect(owl.datatype == "rdf:langString")
            #expect(owl.language == "en")
        } else {
            Issue.record("Expected .literal, got \(term)")
        }
    }

    @Test("Decode literal with escaped characters")
    func testDecodeEscaped() {
        let term = RDFTerm.decode("\"line1\\nline2\\ttab \\\"quoted\\\" \\\\slash\"")
        if case .literal(let owl) = term {
            #expect(owl.lexicalForm == "line1\nline2\ttab \"quoted\" \\slash")
        } else {
            Issue.record("Expected .literal, got \(term)")
        }
    }

    // MARK: - Round-trip

    @Test("Encode-decode round-trip for all term types")
    func testRoundTrip() {
        let terms: [RDFTerm] = [
            .iri("ex:Alice"),
            .iri("http://example.org/Person"),
            .string("Alice"),
            .literal(OWLLiteral(lexicalForm: "30", datatype: "xsd:integer")),
            .literal(OWLLiteral(lexicalForm: "3.14", datatype: "xsd:decimal")),
            .literal(OWLLiteral(lexicalForm: "true", datatype: "xsd:boolean")),
            .literal(OWLLiteral(lexicalForm: "hello", datatype: "rdf:langString", language: "en")),
            .blankNode("b1"),
            .blankNode("node42"),
        ]

        for original in terms {
            let encoded = original.encoded
            let decoded = RDFTerm.decode(encoded)
            #expect(original == decoded, "Round-trip failed for \(original): encoded=\(encoded), decoded=\(decoded)")
        }
    }

    @Test("Round-trip with special characters in literals")
    func testRoundTripSpecialChars() {
        let terms: [RDFTerm] = [
            .string("line1\nline2"),
            .string("tab\there"),
            .string("\"quoted\""),
            .string("back\\slash"),
            .string("mix\n\t\"\\"),
            .string(""),  // empty string
        ]

        for original in terms {
            let encoded = original.encoded
            let decoded = RDFTerm.decode(encoded)
            #expect(original == decoded, "Round-trip failed for special chars: encoded=\(encoded)")
        }
    }

    // MARK: - Convenience Constructors

    @Test("Convenience constructors produce correct terms")
    func testConvenienceConstructors() {
        let str = RDFTerm.string("hello")
        #expect(str == .literal(OWLLiteral(lexicalForm: "hello", datatype: "xsd:string")))

        let int = RDFTerm.integer(42)
        #expect(int == .literal(OWLLiteral(lexicalForm: "42", datatype: "xsd:integer")))

        let dec = RDFTerm.decimal(3.14)
        if case .literal(let owl) = dec {
            #expect(owl.datatype == "xsd:decimal")
            #expect(owl.lexicalForm == "3.14")
        } else {
            Issue.record("Expected .literal for .decimal()")
        }

        let bool = RDFTerm.boolean(true)
        #expect(bool == .literal(OWLLiteral(lexicalForm: "true", datatype: "xsd:boolean")))

        let lang = RDFTerm.langString("hello", language: "en")
        #expect(lang == .literal(OWLLiteral(lexicalForm: "hello", datatype: "rdf:langString", language: "en")))
    }

    // MARK: - Codable

    @Test("Codable round-trip for RDFTerm")
    func testCodableRoundTrip() throws {
        let terms: [RDFTerm] = [
            .iri("ex:Alice"),
            .string("hello"),
            .literal(OWLLiteral(lexicalForm: "30", datatype: "xsd:integer")),
            .langString("hi", language: "en"),
            .blankNode("b1"),
        ]

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        for original in terms {
            let data = try encoder.encode(original)
            let decoded = try decoder.decode(RDFTerm.self, from: data)
            #expect(original == decoded)
        }
    }
}

// MARK: - PrefixMap Tests

@Suite("PrefixMap Tests")
struct PrefixMapTests {

    @Test("Expand prefixed name to full IRI")
    func testExpandPrefixed() {
        let prefixes = PrefixMap.standard
        let expanded = prefixes.expand("sh:NodeShape")
        #expect(expanded == "http://www.w3.org/ns/shacl#NodeShape")
    }

    @Test("Expand full IRI unchanged when it contains //")
    func testExpandFullIRI() {
        let prefixes = PrefixMap.standard
        let fullIRI = "http://example.org/foo"
        let expanded = prefixes.expand(fullIRI)
        #expect(expanded == fullIRI)
    }

    @Test("Expand with unknown prefix returns input unchanged")
    func testExpandUnknownPrefix() {
        let prefixes = PrefixMap.standard
        let input = "unknown:Foo"
        let expanded = prefixes.expand(input)
        #expect(expanded == input)
    }

    @Test("Compact full SHACL IRI to prefixed form")
    func testCompactFullIRI() {
        let prefixes = PrefixMap.standard
        let compacted = prefixes.compact("http://www.w3.org/ns/shacl#NodeShape")
        #expect(compacted == "sh:NodeShape")
    }

    @Test("Compact unknown IRI returns input unchanged")
    func testCompactNoMatch() {
        let prefixes = PrefixMap.standard
        let unknownIRI = "http://unknown.example.org/Thing"
        let compacted = prefixes.compact(unknownIRI)
        #expect(compacted == unknownIRI)
    }

    @Test("Compact selects longest matching namespace prefix")
    func testCompactLongestMatch() {
        var prefixes = PrefixMap()
        prefixes.register(prefix: "ex", namespace: "http://example.org/")
        prefixes.register(prefix: "exvocab", namespace: "http://example.org/vocab/")

        let compacted = prefixes.compact("http://example.org/vocab/Term")
        #expect(compacted == "exvocab:Term")
    }

    @Test("Register custom prefix and expand")
    func testRegisterAndExpand() {
        var prefixes = PrefixMap()
        prefixes.register(prefix: "myns", namespace: "http://myapp.example.com/ns/")

        let expanded = prefixes.expand("myns:Widget")
        #expect(expanded == "http://myapp.example.com/ns/Widget")
    }

    @Test("Merge two PrefixMaps and both work")
    func testMerged() {
        let first = PrefixMap(["a": "http://a.example.org/"])
        let second = PrefixMap(["b": "http://b.example.org/"])

        let merged = first.merged(with: second)
        #expect(merged.expand("a:Foo") == "http://a.example.org/Foo")
        #expect(merged.expand("b:Bar") == "http://b.example.org/Bar")
    }

    @Test("Standard prefixes contain rdf, rdfs, owl, xsd, sh")
    func testStandardPrefixes() {
        let standard = PrefixMap.standard
        let prefixes = standard.prefixes

        #expect(prefixes.contains("rdf"))
        #expect(prefixes.contains("rdfs"))
        #expect(prefixes.contains("owl"))
        #expect(prefixes.contains("xsd"))
        #expect(prefixes.contains("sh"))
    }

    @Test("Codable round-trip preserves PrefixMap equality")
    func testCodableRoundTrip() throws {
        var original = PrefixMap.standard
        original.register(prefix: "test", namespace: "http://test.example.org/")

        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        let data = try encoder.encode(original)
        let decoded = try JSONDecoder().decode(PrefixMap.self, from: data)

        #expect(original == decoded)
    }
}

// MARK: - SHACLPath Tests

@Suite("SHACLPath Tests")
struct SHACLPathTests {

    @Test("Predicate path reports isPredicatePath and predicateIRI")
    func testPredicatePath() {
        let path: SHACLPath = .predicate("ex:name")
        #expect(path.isPredicatePath == true)
        #expect(path.predicateIRI == "ex:name")
    }

    @Test("Inverse path is not a predicate path")
    func testInversePath() {
        let path: SHACLPath = .inverse(.predicate("ex:knows"))
        #expect(path.isPredicatePath == false)
        #expect(path.predicateIRI == nil)
    }

    @Test("Sequence path collects all referenced predicates")
    func testReferencedPredicates() {
        let path: SHACLPath = .sequence([
            .predicate("ex:parent"),
            .predicate("ex:name"),
            .inverse(.predicate("ex:child"))
        ])
        let predicates = path.referencedPredicates
        #expect(predicates == Set(["ex:parent", "ex:name", "ex:child"]))
    }

    @Test("Codable round-trip for various path types")
    func testCodableRoundTrip() throws {
        let paths: [SHACLPath] = [
            .predicate("ex:name"),
            .inverse(.predicate("ex:knows")),
            .sequence([.predicate("ex:parent"), .predicate("ex:name")]),
            .alternative([.predicate("rdfs:label"), .predicate("skos:prefLabel")]),
            .zeroOrMore(.predicate("ex:knows")),
            .oneOrMore(.predicate("ex:parent")),
            .zeroOrOne(.predicate("ex:nickname")),
        ]

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        for original in paths {
            let data = try encoder.encode(original)
            let decoded = try decoder.decode(SHACLPath.self, from: data)
            #expect(original == decoded)
        }
    }
}

// MARK: - SHACLConstraint Tests

@Suite("SHACLConstraint Tests")
struct SHACLConstraintTests {

    @Test("Each constraint type returns correct W3C component IRI")
    func testComponentIRIs() {
        let cases: [(SHACLConstraint, String)] = [
            (.class_("ex:Person"), "sh:ClassConstraintComponent"),
            (.datatype("xsd:string"), "sh:DatatypeConstraintComponent"),
            (.nodeKind(.iri), "sh:NodeKindConstraintComponent"),
            (.minCount(1), "sh:MinCountConstraintComponent"),
            (.maxCount(5), "sh:MaxCountConstraintComponent"),
            (.minExclusive(.integer(0)), "sh:MinExclusiveConstraintComponent"),
            (.maxExclusive(.integer(100)), "sh:MaxExclusiveConstraintComponent"),
            (.minInclusive(.integer(1)), "sh:MinInclusiveConstraintComponent"),
            (.maxInclusive(.integer(99)), "sh:MaxInclusiveConstraintComponent"),
            (.minLength(1), "sh:MinLengthConstraintComponent"),
            (.maxLength(255), "sh:MaxLengthConstraintComponent"),
            (.pattern("^[a-z]+$", flags: "i"), "sh:PatternConstraintComponent"),
            (.languageIn(["en", "de"]), "sh:LanguageInConstraintComponent"),
            (.uniqueLang, "sh:UniqueLangConstraintComponent"),
            (.equals(.predicate("ex:name")), "sh:EqualsConstraintComponent"),
            (.disjoint(.predicate("ex:other")), "sh:DisjointConstraintComponent"),
            (.lessThan(.predicate("ex:upper")), "sh:LessThanConstraintComponent"),
            (.lessThanOrEquals(.predicate("ex:max")), "sh:LessThanOrEqualsConstraintComponent"),
            (.closed(ignoredProperties: ["rdf:type"]), "sh:ClosedConstraintComponent"),
            (.hasValue(.iri("ex:Active")), "sh:HasValueConstraintComponent"),
            (.in_([.iri("ex:A"), .iri("ex:B")]), "sh:InConstraintComponent"),
        ]

        for (constraint, expectedIRI) in cases {
            #expect(constraint.componentIRI == expectedIRI)
        }
    }

    @Test("Codable round-trip for constraints including indirect cases")
    func testCodableRoundTrip() throws {
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        let constraints: [SHACLConstraint] = [
            .class_("ex:Person"),
            .datatype("xsd:string"),
            .nodeKind(.literal),
            .minCount(1),
            .maxCount(10),
            .minExclusive(.decimal(0.5)),
            .maxInclusive(.integer(100)),
            .minLength(3),
            .maxLength(50),
            .pattern("^\\d+$", flags: nil),
            .languageIn(["en", "fr", "de"]),
            .uniqueLang,
            .equals(.predicate("ex:sameAs")),
            .disjoint(.predicate("ex:differentFrom")),
            .lessThan(.predicate("ex:upper")),
            .lessThanOrEquals(.predicate("ex:ceiling")),
            .closed(ignoredProperties: ["rdf:type"]),
            .hasValue(.string("active")),
            .in_([.integer(1), .integer(2), .integer(3)]),
            // Indirect cases referencing SHACLShape
            .not(.node(NodeShape(
                iri: "ex:ForbiddenShape",
                constraints: [.maxCount(0)]
            ))),
            .and([
                .node(NodeShape(constraints: [.minCount(1)])),
                .node(NodeShape(constraints: [.datatype("xsd:string")])),
            ]),
            .or([
                .node(NodeShape(constraints: [.datatype("xsd:string")])),
                .node(NodeShape(constraints: [.datatype("xsd:integer")])),
            ]),
            .xone([
                .node(NodeShape(constraints: [.hasValue(.boolean(true))])),
                .node(NodeShape(constraints: [.hasValue(.boolean(false))])),
            ]),
            .node(NodeShape(
                iri: "ex:AddressShape",
                constraints: [.minCount(1)]
            )),
            .qualifiedValueShape(
                shape: .node(NodeShape(constraints: [.datatype("xsd:string")])),
                min: 1,
                max: 3
            ),
        ]

        for original in constraints {
            let data = try encoder.encode(original)
            let decoded = try decoder.decode(SHACLConstraint.self, from: data)
            #expect(original == decoded)
        }
    }

    @Test("RDFTerm convenience constructors produce correct OWLLiteral")
    func testRDFTermConvenience() {
        let strVal = RDFTerm.string("hello")
        let intVal = RDFTerm.integer(42)
        let decVal = RDFTerm.decimal(3.14)
        let boolVal = RDFTerm.boolean(true)

        // Verify they produce .literal variants with correct OWLLiteral
        if case .literal(let lit) = strVal {
            #expect(lit.lexicalForm == "hello")
            #expect(lit.datatype == "xsd:string")
        } else {
            Issue.record("Expected .literal for .string()")
        }

        if case .literal(let lit) = intVal {
            #expect(lit.lexicalForm == "42")
            #expect(lit.datatype == "xsd:integer")
        } else {
            Issue.record("Expected .literal for .integer()")
        }

        if case .literal(let lit) = decVal {
            #expect(lit.lexicalForm == "3.14")
            #expect(lit.datatype == "xsd:decimal")
        } else {
            Issue.record("Expected .literal for .decimal()")
        }

        if case .literal(let lit) = boolVal {
            #expect(lit.lexicalForm == "true")
            #expect(lit.datatype == "xsd:boolean")
        } else {
            Issue.record("Expected .literal for .boolean()")
        }
    }
}

// MARK: - SHACLShapesGraph Tests

@Suite("SHACLShapesGraph Tests")
struct SHACLShapesGraphTests {

    @Test("Active shapes excludes deactivated shapes")
    func testActiveShapes() {
        let graph = SHACLShapesGraph(
            iri: "http://example.org/shapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:ActiveShape",
                    targets: [.class_("ex:Person")],
                    deactivated: false
                )),
                .node(NodeShape(
                    iri: "ex:InactiveShape",
                    targets: [.class_("ex:Animal")],
                    deactivated: true
                )),
                .property(PropertyShape(
                    iri: "ex:ActiveProp",
                    path: .predicate("ex:name"),
                    deactivated: false
                )),
                .property(PropertyShape(
                    iri: "ex:InactiveProp",
                    path: .predicate("ex:age"),
                    deactivated: true
                )),
            ]
        )

        let active = graph.activeShapes
        #expect(active.count == 2)
        #expect(active.allSatisfy { !$0.isDeactivated })
    }

    @Test("findShape(iri:) returns the matching shape")
    func testFindShapeByIRI() {
        let targetShape = NodeShape(
            iri: "ex:PersonShape",
            targets: [.class_("ex:Person")],
            constraints: [.minCount(1)]
        )
        let graph = SHACLShapesGraph(
            iri: "http://example.org/shapes",
            shapes: [
                .node(targetShape),
                .node(NodeShape(iri: "ex:OtherShape")),
            ]
        )

        let found = graph.findShape(iri: "ex:PersonShape")
        #expect(found != nil)
        #expect(found?.iri == "ex:PersonShape")

        let notFound = graph.findShape(iri: "ex:NonExistent")
        #expect(notFound == nil)
    }

    @Test("targetClassIRIs collects all target class IRIs")
    func testTargetClassIRIs() {
        let graph = SHACLShapesGraph(
            iri: "http://example.org/shapes",
            shapes: [
                .node(NodeShape(
                    iri: "ex:Shape1",
                    targets: [.class_("ex:Person"), .class_("ex:Agent")]
                )),
                .node(NodeShape(
                    iri: "ex:Shape2",
                    targets: [.class_("ex:Organization"), .node("ex:SpecificNode")]
                )),
                .property(PropertyShape(
                    path: .predicate("ex:name"),
                    targets: [.class_("ex:Person")]
                )),
            ]
        )

        let classIRIs = graph.targetClassIRIs
        #expect(classIRIs == Set(["ex:Person", "ex:Agent", "ex:Organization"]))
    }

    @Test("nodeShapes and propertyShapes computed properties")
    func testNodeAndPropertyShapeAccess() {
        let ns1 = NodeShape(iri: "ex:NS1")
        let ns2 = NodeShape(iri: "ex:NS2")
        let ps1 = PropertyShape(path: .predicate("ex:name"))
        let ps2 = PropertyShape(path: .predicate("ex:age"))

        let graph = SHACLShapesGraph(
            iri: "http://example.org/shapes",
            shapes: [
                .node(ns1),
                .property(ps1),
                .node(ns2),
                .property(ps2),
            ]
        )

        #expect(graph.nodeShapes.count == 2)
        #expect(graph.propertyShapes.count == 2)
        #expect(graph.nodeShapes[0].iri == "ex:NS1")
        #expect(graph.nodeShapes[1].iri == "ex:NS2")
    }

    @Test("Codable round-trip for full shapes graph")
    func testCodableRoundTrip() throws {
        let original = SHACLShapesGraph(
            iri: "http://example.org/shapes/person",
            shapes: [
                .node(NodeShape(
                    iri: "ex:PersonShape",
                    targets: [.class_("ex:Person")],
                    constraints: [.nodeKind(.blankNodeOrIRI)],
                    propertyShapes: [
                        PropertyShape(
                            iri: "ex:PersonNameShape",
                            path: .predicate("ex:name"),
                            constraints: [.minCount(1), .datatype("xsd:string"), .maxLength(200)]
                        ),
                        PropertyShape(
                            path: .predicate("ex:age"),
                            constraints: [
                                .minInclusive(.integer(0)),
                                .maxInclusive(.integer(150)),
                                .datatype("xsd:integer"),
                            ]
                        ),
                    ],
                    severity: .violation,
                    messages: ["Person must have a name"]
                )),
                .property(PropertyShape(
                    iri: "ex:EmailShape",
                    path: .predicate("ex:email"),
                    constraints: [
                        .pattern("^[^@]+@[^@]+$", flags: nil),
                        .nodeKind(.literal),
                    ],
                    severity: .warning,
                    messages: ["Email should be valid"]
                )),
            ],
            prefixes: .standard,
            entailment: .rdfs
        )

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(SHACLShapesGraph.self, from: data)
        #expect(original == decoded)
    }
}

// MARK: - SHACLValidationReport Tests

@Suite("SHACLValidationReport Tests")
struct SHACLValidationReportTests {

    @Test("No violations means conforms is true")
    func testConforms() {
        let report = SHACLValidationReport(results: [])
        #expect(report.conforms == true)

        // Even with warnings/infos, conforms should be true (only violations matter)
        let reportWithWarning = SHACLValidationReport(results: [
            SHACLValidationResult(
                focusNode: "ex:Alice",
                sourceConstraintComponent: "sh:MinCountConstraintComponent",
                resultSeverity: .warning
            ),
            SHACLValidationResult(
                focusNode: "ex:Bob",
                sourceConstraintComponent: "sh:DatatypeConstraintComponent",
                resultSeverity: .info
            ),
        ])
        #expect(reportWithWarning.conforms == true)
    }

    @Test("Violations present means conforms is false")
    func testNotConforms() {
        let report = SHACLValidationReport(results: [
            SHACLValidationResult(
                focusNode: "ex:Alice",
                resultPath: .predicate("ex:name"),
                sourceConstraintComponent: "sh:MinCountConstraintComponent",
                resultMessage: ["Missing required property ex:name"],
                resultSeverity: .violation
            ),
        ])
        #expect(report.conforms == false)
    }

    @Test("violations filter returns only severity == .violation")
    func testViolationsFilter() {
        let report = SHACLValidationReport(results: [
            SHACLValidationResult(
                focusNode: "ex:Alice",
                sourceConstraintComponent: "sh:MinCountConstraintComponent",
                resultSeverity: .violation
            ),
            SHACLValidationResult(
                focusNode: "ex:Bob",
                sourceConstraintComponent: "sh:DatatypeConstraintComponent",
                resultSeverity: .warning
            ),
            SHACLValidationResult(
                focusNode: "ex:Carol",
                sourceConstraintComponent: "sh:NodeKindConstraintComponent",
                resultSeverity: .violation
            ),
            SHACLValidationResult(
                focusNode: "ex:Dave",
                sourceConstraintComponent: "sh:MaxCountConstraintComponent",
                resultSeverity: .info
            ),
        ])

        #expect(report.violations.count == 2)
        #expect(report.violations.allSatisfy { $0.resultSeverity == .violation })
    }

    @Test("warnings and infos filter correctly")
    func testWarningsAndInfos() {
        let report = SHACLValidationReport(results: [
            SHACLValidationResult(
                focusNode: "ex:A",
                sourceConstraintComponent: "sh:MinCountConstraintComponent",
                resultSeverity: .violation
            ),
            SHACLValidationResult(
                focusNode: "ex:B",
                sourceConstraintComponent: "sh:PatternConstraintComponent",
                resultSeverity: .warning
            ),
            SHACLValidationResult(
                focusNode: "ex:C",
                sourceConstraintComponent: "sh:MaxLengthConstraintComponent",
                resultSeverity: .warning
            ),
            SHACLValidationResult(
                focusNode: "ex:D",
                sourceConstraintComponent: "sh:DatatypeConstraintComponent",
                resultSeverity: .info
            ),
        ])

        #expect(report.warnings.count == 2)
        #expect(report.warnings.allSatisfy { $0.resultSeverity == .warning })
        #expect(report.infos.count == 1)
        #expect(report.infos.allSatisfy { $0.resultSeverity == .info })
    }

    @Test("Merged report combines results from two reports")
    func testMerged() {
        let report1 = SHACLValidationReport(results: [
            SHACLValidationResult(
                focusNode: "ex:Alice",
                sourceConstraintComponent: "sh:MinCountConstraintComponent",
                resultSeverity: .violation
            ),
        ])
        let report2 = SHACLValidationReport(results: [
            SHACLValidationResult(
                focusNode: "ex:Bob",
                sourceConstraintComponent: "sh:DatatypeConstraintComponent",
                resultSeverity: .warning
            ),
            SHACLValidationResult(
                focusNode: "ex:Carol",
                sourceConstraintComponent: "sh:MaxCountConstraintComponent",
                resultSeverity: .violation
            ),
        ])

        let merged = report1.merged(with: report2)
        #expect(merged.results.count == 3)
        // merged has violations from both reports
        #expect(merged.conforms == false)
        #expect(merged.violations.count == 2)
        #expect(merged.warnings.count == 1)
    }

    @Test("resultsByFocusNode groups results correctly")
    func testResultsByFocusNode() {
        let report = SHACLValidationReport(results: [
            SHACLValidationResult(
                focusNode: "ex:Alice",
                resultPath: .predicate("ex:name"),
                sourceConstraintComponent: "sh:MinCountConstraintComponent",
                resultSeverity: .violation
            ),
            SHACLValidationResult(
                focusNode: "ex:Alice",
                resultPath: .predicate("ex:email"),
                sourceConstraintComponent: "sh:PatternConstraintComponent",
                resultSeverity: .warning
            ),
            SHACLValidationResult(
                focusNode: "ex:Bob",
                resultPath: .predicate("ex:age"),
                sourceConstraintComponent: "sh:DatatypeConstraintComponent",
                resultSeverity: .violation
            ),
        ])

        let grouped = report.resultsByFocusNode
        #expect(grouped.count == 2)
        #expect(grouped["ex:Alice"]?.count == 2)
        #expect(grouped["ex:Bob"]?.count == 1)
    }
}
