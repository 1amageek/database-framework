import DatabaseValue
import Graph
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("RDF quad index physical codec")
struct RDFQuadIndexPhysicalCodecTests {
    @Test("All six orderings match tuple bytes and decode the same named quad")
    func allOrderingsMatchReferenceBytes() throws {
        let base = Subspace(prefix: Tuple("rdf-physical-codec").pack())
        let codec = RDFQuadIndexPhysicalCodec(baseSubspace: base)
        let quad = namedQuad()
        let writePlan = try RDFQuadIndexWritePlan(quad: quad)
        var visited: [GraphIndexOrdering] = []

        try writePlan.forEachEntry { entry in
            let key = try codec.encode(entry)
            let expected = try referenceKey(
                for: quad,
                ordering: entry.ordering,
                base: base
            )
            #expect(key == expected)
            #expect(try codec.decodeQuad(
                key: key,
                ordering: entry.ordering
            ) == quad)
            visited.append(entry.ordering)
        }

        #expect(visited == [.spo, .pos, .osp, .gspo, .gpos, .gosp])
    }

    @Test("Default graph marker round-trips through every ordering")
    func defaultGraphRoundTrips() throws {
        let base = Subspace(prefix: Tuple("rdf-default-graph-codec").pack())
        let codec = RDFQuadIndexPhysicalCodec(baseSubspace: base)
        let quad = defaultQuad()

        try RDFQuadIndexWritePlan(quad: quad).forEachEntry { entry in
            let key = try codec.encode(entry)
            #expect(try codec.decodeQuad(
                key: key,
                ordering: entry.ordering
            ) == quad)
        }
    }

    @Test("Direct range boundaries match tuple-layer reference boundaries")
    func directRangesMatchReference() throws {
        let base = Subspace(prefix: Tuple("rdf-range-codec").pack())
        let codec = RDFQuadIndexPhysicalCodec(baseSubspace: base)
        let quad = namedQuad()

        for ordering in canonicalOrderings {
            let components = try referenceComponents(
                for: quad,
                ordering: ordering
            )
            let plans = try componentPlans(for: quad, ordering: ordering)
            for count in 0...4 {
                var prefix = RDFQuadIndexPrefixWritePlan()
                for component in plans.prefix(count) {
                    try prefix.append(component)
                }

                let actual = try codec.range(
                    prefix: prefix,
                    ordering: ordering
                )
                let expected = try referenceRange(
                    components: Array(components.prefix(count)),
                    ordering: ordering,
                    base: base
                )
                #expect(actual.begin == expected.begin)
                #expect(actual.end == expected.end)
            }
        }
    }

    @Test("Malformed physical keys preserve their exact failure category")
    func malformedKeysAreTyped() throws {
        let base = Subspace(prefix: Tuple("rdf-malformed-codec").pack())
        let codec = RDFQuadIndexPhysicalCodec(baseSubspace: base)
        let subject = try encoded(.iri("https://example.com/subject"), role: .subject)
        let predicate = try encoded(.iri("https://example.com/predicate"), role: .predicate)
        let object = try encoded(.literal(DatabaseRDFLiteral(
            lexicalForm: "value",
            datatype: .xsdString
        )), role: .object)
        let graph = try encoded(.iri("https://example.com/graph"), role: .graphName)
        let spo = base.subspace(Int64(2))

        let truncated = spo.pack(Tuple(subject, predicate, object))
        #expect(throws: RDFQuadIndexPhysicalCodecError.truncatedComponent(
            position: 3
        )) {
            try codec.decodeQuad(key: truncated, ordering: .spo)
        }

        let wrongType = spo.pack(Tuple("not-bytes", predicate, object, graph))
        #expect(throws: RDFQuadIndexPhysicalCodecError.unexpectedTupleType(
            position: 0,
            actualTypeCode: TupleTypeCode.string.rawValue
        )) {
            try codec.decodeQuad(key: wrongType, ordering: .spo)
        }

        let literalPredicate = try encoded(
            .literal(DatabaseRDFLiteral(
                lexicalForm: "not-an-iri",
                datatype: .xsdString
            )),
            role: .term
        )
        let wrongRole = spo.pack(Tuple(
            subject,
            literalPredicate,
            object,
            graph
        ))
        #expect(throws: RDFQuadIndexPhysicalCodecError.invalidComponent(
            .predicate,
            .invalidRole(expected: .predicate, actual: .literal)
        )) {
            try codec.decodeQuad(key: wrongRole, ordering: .spo)
        }

        let trailing = spo.pack(Tuple(
            subject,
            predicate,
            object,
            graph,
            Bytes([0x01])
        ))
        do {
            _ = try codec.decodeQuad(key: trailing, ordering: .spo)
            Issue.record("Expected trailing tuple data to fail")
        } catch let error {
            guard case .unexpectedTrailingTupleData = error else {
                Issue.record("Unexpected error: \(error)")
                return
            }
        }

        let foreignBase = RDFQuadIndexPhysicalCodec(
            baseSubspace: Subspace(prefix: Tuple("foreign").pack())
        )
        #expect(throws: RDFQuadIndexPhysicalCodecError.prefixMismatch(.spo)) {
            try foreignBase.decodeQuad(key: truncated, ordering: .spo)
        }
    }

    @Test("Real transaction scans select SPO POS OSP and graph-first variants")
    func realTransactionScansEveryOrdering() async throws {
        let base = Subspace(prefix: Tuple("rdf-scanner-e2e").pack())
        let codec = RDFQuadIndexPhysicalCodec(baseSubspace: base)
        let engine = InMemoryEngine()
        let defaultQuad = defaultQuad()
        let namedQuad = namedQuad()

        try await engine.withTransaction(configuration: .batch) { transaction in
            for quad in [defaultQuad, namedQuad] {
                try RDFQuadIndexWritePlan(quad: quad).forEachEntry { entry in
                    try transaction.setValue([], for: codec.encode(entry))
                }
            }
        }

        let source = RDFDatasetSource(
            entityName: "Event",
            indexName: "rdf",
            indexSubspace: base,
            coverage: .dataset
        )
        let scanner = IndexedRDFDatasetScanner(sources: [source])
        let namedGraph = try RDFGraphName(#require(namedQuad.graph))

        try await expectScan(
            scanner,
            engine: engine,
            subject: defaultQuad.subject,
            predicate: nil,
            object: nil,
            graphScope: .defaultGraph,
            expected: defaultQuad
        )
        try await expectScan(
            scanner,
            engine: engine,
            subject: nil,
            predicate: defaultQuad.predicate,
            object: nil,
            graphScope: .defaultGraph,
            expected: defaultQuad
        )
        try await expectScan(
            scanner,
            engine: engine,
            subject: nil,
            predicate: nil,
            object: defaultQuad.object,
            graphScope: .defaultGraph,
            expected: defaultQuad
        )
        try await expectScan(
            scanner,
            engine: engine,
            subject: namedQuad.subject,
            predicate: nil,
            object: nil,
            graphScope: .named(namedGraph),
            expected: namedQuad
        )
        try await expectScan(
            scanner,
            engine: engine,
            subject: nil,
            predicate: namedQuad.predicate,
            object: nil,
            graphScope: .named(namedGraph),
            expected: namedQuad
        )
        try await expectScan(
            scanner,
            engine: engine,
            subject: nil,
            predicate: nil,
            object: namedQuad.object,
            graphScope: .named(namedGraph),
            expected: namedQuad
        )
    }

    private var canonicalOrderings: [GraphIndexOrdering] {
        [.spo, .pos, .osp, .gspo, .gpos, .gosp]
    }

    private func defaultQuad() -> RDFQuad {
        RDFQuad(
            subject: .iri("https://example.com/default-subject"),
            predicate: .iri("https://example.com/default-predicate"),
            object: .literal(DatabaseRDFLiteral(
                lexicalForm: "default-value",
                datatype: .xsdString
            )),
            graph: nil
        )
    }

    private func namedQuad() -> RDFQuad {
        RDFQuad(
            subject: .blankNode("named-subject"),
            predicate: .iri("https://example.com/named-predicate"),
            object: .tripleTerm(
                subject: .iri("https://example.com/quoted-subject"),
                predicate: .iri("https://example.com/quoted-predicate"),
                object: .literal(DatabaseRDFLiteral(
                    lexicalForm: "quoted-value",
                    datatype: .xsdString
                ))
            ),
            graph: .iri("https://example.com/named-graph")
        )
    }

    private func componentPlans(
        for quad: RDFQuad,
        ordering: GraphIndexOrdering
    ) throws -> [RDFQuadIndexComponentWritePlan] {
        let subject = try RDFQuadIndexComponentWritePlan(
            term: quad.subject,
            role: .subject
        )
        let predicate = try RDFQuadIndexComponentWritePlan(
            term: quad.predicate,
            role: .predicate
        )
        let object = try RDFQuadIndexComponentWritePlan(
            term: quad.object,
            role: .object
        )
        let graph: RDFQuadIndexComponentWritePlan
        if let value = quad.graph {
            graph = try RDFQuadIndexComponentWritePlan(
                term: value,
                role: .graphName
            )
        } else {
            graph = .defaultGraph
        }
        switch ordering {
        case .spo: return [subject, predicate, object, graph]
        case .pos: return [predicate, object, subject, graph]
        case .osp: return [object, subject, predicate, graph]
        case .gspo: return [graph, subject, predicate, object]
        case .gpos: return [graph, predicate, object, subject]
        case .gosp: return [graph, object, subject, predicate]
        case .out, .in, .sop, .pso, .ops:
            throw RDFQuadIndexPhysicalCodecError.unsupportedOrdering(ordering)
        }
    }

    private func referenceKey(
        for quad: RDFQuad,
        ordering: GraphIndexOrdering,
        base: Subspace
    ) throws -> Bytes {
        let components = try referenceComponents(for: quad, ordering: ordering)
        return base.subspace(subspaceKey(for: ordering)).pack(
            Tuple(components.map { $0 as any TupleElement })
        )
    }

    private func referenceComponents(
        for quad: RDFQuad,
        ordering: GraphIndexOrdering
    ) throws -> [Bytes] {
        let subject = try encoded(quad.subject, role: .subject)
        let predicate = try encoded(quad.predicate, role: .predicate)
        let object = try encoded(quad.object, role: .object)
        let graph: Bytes
        if let value = quad.graph {
            graph = try encoded(value, role: .graphName)
        } else {
            graph = Bytes(
                retaining: RDFQuadIndexPhysicalLayout.defaultGraphDiscriminator
            )
        }
        switch ordering {
        case .spo: return [subject, predicate, object, graph]
        case .pos: return [predicate, object, subject, graph]
        case .osp: return [object, subject, predicate, graph]
        case .gspo: return [graph, subject, predicate, object]
        case .gpos: return [graph, predicate, object, subject]
        case .gosp: return [graph, object, subject, predicate]
        case .out, .in, .sop, .pso, .ops:
            throw RDFQuadIndexPhysicalCodecError.unsupportedOrdering(ordering)
        }
    }

    private func referenceRange(
        components: [Bytes],
        ordering: GraphIndexOrdering,
        base: Subspace
    ) throws -> (begin: Bytes, end: Bytes) {
        let orderingSubspace = base.subspace(subspaceKey(for: ordering))
        guard !components.isEmpty else {
            return orderingSubspace.range()
        }
        let packed = orderingSubspace.pack(
            Tuple(components.map { $0 as any TupleElement })
        )
        if components.count == 4 {
            return (packed, try strinc(packed))
        }
        return Subspace(prefix: packed).range()
    }

    private func subspaceKey(for ordering: GraphIndexOrdering) -> Int64 {
        switch ordering {
        case .spo: 2
        case .pos: 3
        case .osp: 4
        case .gspo: 8
        case .gpos: 9
        case .gosp: 10
        case .out, .in, .sop, .pso, .ops:
            preconditionFailure("Unsupported canonical RDF ordering")
        }
    }

    private func encoded(
        _ term: DatabaseRDFTerm,
        role: DatabaseRDFTermRole
    ) throws -> Bytes {
        Bytes(retaining: try DatabaseRDFTermCodec.encode(
            term,
            role: role
        ))
    }

    private func expectScan(
        _ scanner: IndexedRDFDatasetScanner,
        engine: InMemoryEngine,
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphScope: RDFGraphScanScope,
        expected: RDFQuad
    ) async throws {
        try await engine.withTransaction { transaction in
            let result = try await scanner.scan(
                subject: subject,
                predicate: predicate,
                object: object,
                graphScope: graphScope,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: DatabaseWorkMeter(budget: .init())
            )
            #expect(result.count == 1)
            #expect(result.first?.quad == expected)
            #expect(result.physicalScanCount == 1)
        }
    }
}
