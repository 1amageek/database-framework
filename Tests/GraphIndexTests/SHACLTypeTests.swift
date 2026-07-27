#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("Prefix map semantics", .heartbeat)
struct PrefixMapTests {
    @Test("Registered prefixes expand and the longest namespace compacts")
    func expansionAndCompaction() {
        var prefixes = PrefixMap.standard
        prefixes.register(prefix: "ex", namespace: "https://example.com/")
        prefixes.register(
            prefix: "vocab",
            namespace: "https://example.com/vocabulary/"
        )

        #expect(
            prefixes.expand("sh:NodeShape")
                == "http://www.w3.org/ns/shacl#NodeShape"
        )
        #expect(
            prefixes.expand("ex:Person")
                == "https://example.com/Person"
        )
        #expect(
            prefixes.compact("https://example.com/vocabulary/Person")
                == "vocab:Person"
        )
    }

    @Test("Unknown prefixes and absolute IRIs remain unchanged")
    func unknownAndAbsoluteValuesRemainUnchanged() {
        let prefixes = PrefixMap.standard

        #expect(prefixes.expand("unknown:Person") == "unknown:Person")
        #expect(
            prefixes.expand("https://example.com/Person")
                == "https://example.com/Person"
        )
        #expect(
            prefixes.compact("https://example.com/Person")
                == "https://example.com/Person"
        )
    }

    @Test("Merging replaces conflicting mappings with the incoming map")
    func mergePrecedence() {
        let original = PrefixMap([
            "ex": "https://original.example/",
            "stable": "https://stable.example/",
        ])
        let incoming = PrefixMap([
            "ex": "https://incoming.example/",
        ])

        let merged = original.merged(with: incoming)

        #expect(
            merged.expand("ex:Value")
                == "https://incoming.example/Value"
        )
        #expect(
            merged.expand("stable:Value")
                == "https://stable.example/Value"
        )
    }
}

@Suite("SHACL path semantics", .heartbeat)
struct SHACLPathTests {
    @Test("Predicate paths expose their typed predicate")
    func predicatePath() throws {
        let predicate = try RDFPredicateIRI("https://example.com/name")
        let path = SHACLPath.predicate(predicate)

        #expect(path.isPredicatePath)
        #expect(path.predicateIRI == predicate)
        #expect(path.referencedPredicates == Set([predicate]))
    }

    @Test("Compound paths collect every referenced predicate")
    func compoundPathReferences() throws {
        let parent = try RDFPredicateIRI("https://example.com/parent")
        let name = try RDFPredicateIRI("https://example.com/name")
        let child = try RDFPredicateIRI("https://example.com/child")
        let sequence = try SHACLPathList([
            .predicate(parent),
            .predicate(name),
            .inverse(.predicate(child)),
        ])
        let path = SHACLPath.sequence(sequence)

        #expect(!path.isPredicatePath)
        #expect(path.predicateIRI == nil)
        #expect(path.referencedPredicates == Set([parent, name, child]))
    }

    @Test("Sequence and alternative paths require at least two members")
    func pathListCardinality() throws {
        let predicate = try RDFPredicateIRI("https://example.com/name")

        #expect(throws: SHACLPathError.insufficientMembers(actual: 0)) {
            _ = try SHACLPathList([])
        }
        #expect(throws: SHACLPathError.insufficientMembers(actual: 1)) {
            _ = try SHACLPathList([.predicate(predicate)])
        }
    }
}

@Suite("SHACL constraint semantics", .heartbeat)
struct SHACLConstraintTests {
    @Test("Every constraint reports its W3C component identifier")
    func componentIdentifiers() throws {
        let predicate = SHACLPath.predicate(
            try RDFPredicateIRI("https://example.com/value")
        )
        let value = RDFTerm.string("value")
        let nestedShape = SHACLShape.node(
            NodeShape(constraints: [.minCount(1)])
        )
        let cases: [(SHACLConstraint, String)] = [
            (.class_("ex:Person"), "sh:ClassConstraintComponent"),
            (.datatype("xsd:string"), "sh:DatatypeConstraintComponent"),
            (.nodeKind(.iri), "sh:NodeKindConstraintComponent"),
            (.minCount(1), "sh:MinCountConstraintComponent"),
            (.maxCount(1), "sh:MaxCountConstraintComponent"),
            (.minExclusive(value), "sh:MinExclusiveConstraintComponent"),
            (.maxExclusive(value), "sh:MaxExclusiveConstraintComponent"),
            (.minInclusive(value), "sh:MinInclusiveConstraintComponent"),
            (.maxInclusive(value), "sh:MaxInclusiveConstraintComponent"),
            (.minLength(1), "sh:MinLengthConstraintComponent"),
            (.maxLength(1), "sh:MaxLengthConstraintComponent"),
            (.pattern("value", flags: nil), "sh:PatternConstraintComponent"),
            (.languageIn(["en"]), "sh:LanguageInConstraintComponent"),
            (.uniqueLang, "sh:UniqueLangConstraintComponent"),
            (.equals(predicate), "sh:EqualsConstraintComponent"),
            (.disjoint(predicate), "sh:DisjointConstraintComponent"),
            (.lessThan(predicate), "sh:LessThanConstraintComponent"),
            (
                .lessThanOrEquals(predicate),
                "sh:LessThanOrEqualsConstraintComponent"
            ),
            (.not(nestedShape), "sh:NotConstraintComponent"),
            (.and([nestedShape]), "sh:AndConstraintComponent"),
            (.or([nestedShape]), "sh:OrConstraintComponent"),
            (.xone([nestedShape]), "sh:XoneConstraintComponent"),
            (
                .node(NodeShape()),
                "sh:NodeConstraintComponent"
            ),
            (
                .qualifiedValueShape(
                    shape: nestedShape,
                    min: 1,
                    max: 2
                ),
                "sh:QualifiedValueShapeConstraintComponent"
            ),
            (
                .closed(ignoredProperties: []),
                "sh:ClosedConstraintComponent"
            ),
            (.hasValue(value), "sh:HasValueConstraintComponent"),
            (.in_([value]), "sh:InConstraintComponent"),
        ]

        for (constraint, expectedIdentifier) in cases {
            #expect(constraint.componentIRI == expectedIdentifier)
        }
    }

    @Test("Invalid regular expressions fail with the typed SHACL error")
    func invalidRegularExpression() {
        do {
            _ = try SHACLRegularExpression(pattern: "[", flags: nil)
            Issue.record("Expected invalidPattern")
        } catch let error as SHACLError {
            guard case .invalidPattern(let expression, let reason) = error
            else {
                Issue.record("Expected invalidPattern, received \(error)")
                return
            }
            #expect(expression == "[")
            #expect(!reason.isEmpty)
        } catch {
            Issue.record("Expected SHACLError, received \(error)")
        }
    }
}

@Suite("SHACL shapes graph semantics", .heartbeat)
struct SHACLShapesGraphTests {
    @Test("Graph projections preserve shape identity and activation")
    func graphProjections() throws {
        let activeIdentifier = RDFTerm.iri(
            try RDFIRI("https://example.com/ActiveShape")
        )
        let inactiveIdentifier = RDFTerm.iri(
            try RDFIRI("https://example.com/InactiveShape")
        )
        let propertyIdentifier = RDFTerm.iri(
            try RDFIRI("https://example.com/PropertyShape")
        )
        let predicate = SHACLPath.predicate(
            try RDFPredicateIRI("https://example.com/name")
        )
        let graph = SHACLShapesGraph(
            iri: "https://example.com/shapes",
            shapes: [
                .node(
                    NodeShape(
                        identifier: activeIdentifier,
                        targets: [.class_("https://example.com/Person")]
                    )
                ),
                .node(
                    NodeShape(
                        identifier: inactiveIdentifier,
                        deactivated: true
                    )
                ),
                .property(
                    PropertyShape(
                        identifier: propertyIdentifier,
                        path: predicate
                    )
                ),
            ]
        )

        #expect(graph.nodeShapes.count == 2)
        #expect(graph.propertyShapes.count == 1)
        #expect(graph.activeShapes.count == 2)
        #expect(graph.findShape(identifier: activeIdentifier) != nil)
        #expect(graph.findShape(identifier: inactiveIdentifier) != nil)
        #expect(
            graph.targetClassIRIs
                == Set(["https://example.com/Person"])
        )
    }
}

@Suite("SHACL validation report semantics", .heartbeat)
struct SHACLValidationReportTests {
    @Test("Only violations make a report nonconforming")
    func conformanceUsesViolationSeverity() throws {
        let focusNode = RDFTerm.iri(
            try RDFIRI("https://example.com/Alice")
        )
        let warning = SHACLValidationResult(
            focusNode: focusNode,
            sourceConstraintComponent: "sh:PatternConstraintComponent",
            resultSeverity: .warning
        )
        let conformingReport = SHACLValidationReport(results: [warning])
        let violation = SHACLValidationResult(
            focusNode: focusNode,
            sourceConstraintComponent: "sh:MinCountConstraintComponent",
            resultSeverity: .violation
        )
        let nonconformingReport = SHACLValidationReport(
            results: [warning, violation]
        )

        #expect(conformingReport.conforms)
        #expect(!nonconformingReport.conforms)
        #expect(nonconformingReport.violations.count == 1)
        #expect(
            nonconformingReport.violations.first?.resultSeverity
                == .violation
        )
        #expect(nonconformingReport.warnings.count == 1)
        #expect(
            nonconformingReport.warnings.first?.resultSeverity
                == .warning
        )
        #expect(nonconformingReport.infos.isEmpty)
        #expect(nonconformingReport.resultsByFocusNode[focusNode]?.count == 2)
    }
}
#endif
