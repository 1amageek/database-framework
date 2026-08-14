import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import GraphIndex

@Suite("Malformed RDF graph index")
struct MalformedRDFGraphIndexTests {
    @Test("Malformed RDF key bytes fail deterministically")
    func malformedRDFBytesFail() async throws {
        let predicate = RDFTerm.iri(.xsdString)
        let object = try RDFTerm.iri(validating: "urn:node:B")
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("malformed-rdf-graph").pack())
        let malformed = subspace.subspace(Int64(8)).pack(
            Tuple([
                RDFQuadIndexPhysicalLayout.defaultGraphDiscriminator,
                ByteString([0x01]),
                try RDFTermStorageFormat.encode(predicate),
                try RDFTermStorageFormat.encode(object),
            ])
        )
        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try transaction.setValue([], for: malformed)
        }
        let scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: .quadStore,
            graphTarget: .defaultGraph
        )

        do {
            try await StorageTransactionExecutor(engine: engine).withTransaction(
                configuration: .default,
                clock: TestProcessMonotonicClock()
            ) { transaction in
                var cursor = scanner.scanAllEdges(
                    edgeLabel: nil,
                    transaction: transaction
                ).makeCursor()
                while try await cursor.next() != nil {}
            }
            Issue.record("Expected malformed RDF bytes to fail")
        } catch let error as GraphIndexError {
            guard case .invalidRDFEncoding(let reason) = error else {
                Issue.record("Unexpected graph index error: \(error)")
                return
            }
            #expect(reason == .truncated)
        } catch {
            Issue.record("Unexpected error type: \(error)")
        }
    }
}
