import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@testable import GraphIndex
import Testing

@Suite("SHACL shapes graph storage format")
struct SHACLShapesGraphStorageFormatTests {
    @Test("Round trip preserves every SHACL shape family")
    func roundTrip() throws {
        let predicate = try RDFPredicateIRI("https://example.com/name")
        let node = RDFTerm.iri(try RDFIRI("https://example.com/Person"))
        let property = PropertyShape(
            identifier: .blankNode(try RDFBlankNodeIdentifier("property")),
            path: .sequence(
                try SHACLPathList([
                    .predicate(predicate),
                    .inverse(.predicate(predicate)),
                ])
            ),
            targets: [.subjectsOf(predicate.rawValue)],
            constraints: [
                .datatype("http://www.w3.org/2001/XMLSchema#string"),
                .pattern("^[A-Z]", flags: "i"),
                .languageIn(["en", "ja"]),
                .qualifiedValueShape(
                    shape: .node(
                        NodeShape(
                            constraints: [.hasValue(node)]
                        )
                    ),
                    min: 1,
                    max: 3
                ),
                .in_([node]),
            ],
            severity: .warning,
            messages: ["Name constraint"],
            deactivated: false,
            name: "name",
            shapeDescription: "A display name",
            order: 1.5,
            group: "https://example.com/group",
            defaultValue: node
        )
        let graph = SHACLShapesGraph(
            iri: "https://example.com/shapes",
            shapes: [
                .node(
                    NodeShape(
                        identifier: node,
                        targets: [.class_("https://example.com/Person")],
                        constraints: [
                            .closed(ignoredProperties: [predicate.rawValue]),
                            .and([.property(property)]),
                        ],
                        propertyShapes: [property],
                        severity: .info,
                        messages: ["Person shape"]
                    )
                ),
                .property(property),
            ],
            prefixes: PrefixMap([
                "ex": "https://example.com/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            ]),
            entailment: .owl
        )

        let encoded = try SHACLShapesGraphStorageFormat.encode(graph)
        let decoded = try SHACLShapesGraphStorageFormat.decode(encoded)

        #expect(decoded == graph)
        #expect(
            try SHACLShapesGraphStorageFormat.encode(decoded) == encoded
        )
    }

    @Test("Invalid header and trailing bytes are rejected")
    func invalidFrames() throws {
        let graph = SHACLShapesGraph(iri: "https://example.com/shapes")
        let encoded = try SHACLShapesGraphStorageFormat.encode(graph)

        var invalidMagic = Array(encoded)
        invalidMagic[0] ^= 0xff
        #expect(throws: StorageFrameError.invalidMagic) {
            try SHACLShapesGraphStorageFormat.decode(
                ByteString(invalidMagic)
            )
        }

        var trailing = Array(encoded)
        trailing.append(0)
        #expect(throws: StorageFrameError.trailingBytes) {
            try SHACLShapesGraphStorageFormat.decode(ByteString(trailing))
        }
    }

    @Test("Nested shapes obey storage depth limits")
    func nestingLimit() throws {
        var shape = SHACLShape.node(NodeShape())
        for _ in 0..<8 {
            shape = .node(NodeShape(constraints: [.not(shape)]))
        }
        let graph = SHACLShapesGraph(
            iri: "https://example.com/deep",
            shapes: [shape]
        )
        let limits = try StorageFrameLimits(
            maximumFrameBytes: 1_048_576,
            maximumStringBytes: 65_536,
            maximumByteStringBytes: 1_048_576,
            maximumCollectionCount: 1_024,
            maximumNestingDepth: 4
        )

        #expect {
            try SHACLShapesGraphStorageFormat.encode(
                graph,
                limits: limits
            )
        } throws: {
            guard case StorageFrameError.nestingTooDeep = $0 else {
                return false
            }
            return true
        }
    }
}
