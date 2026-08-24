import DatabaseKit
import DatabaseTypes
import Testing

@testable import VectorIndex

@Persistable
private struct VectorFusionInputItem {
    var id: String
    var embedding: Vector
}

@Suite("Vector Fusion input")
struct SimilarFusionInputTests {
    @Test("Similar lowers to an immutable canonical index input")
    func lowersToCanonicalInput() throws {
        let query = try Similar(
            VectorFusionInputItem.fields.embedding,
            dimensions: 3
        )
        .index(named: "vector_embedding")
        .nearest(to: [1, 2, 3], k: 7)
        .metric(.euclidean)

        let input = query.fusionInput
        #expect(input.scoring == .annotation(
            name: "distance",
            order: .lowerIsBetter
        ))
        #expect(input.requirement == .unrestricted)
        #expect(input.limit == 7)
        guard case .index(let source) = input.operation else {
            Issue.record("Similar must lower to an index operation")
            return
        }
        #expect(source.selection == .named(
            name: "vector_embedding",
            type: .vector
        ))
        #expect(source.referencedFields == [
            VectorFusionInputItem.fields.embedding.identity,
        ])
        #expect(source.parameters[VectorReadParameter.fieldName] == .string(
            "embedding"
        ))
        #expect(source.parameters[VectorReadParameter.dimensions] == .int64(3))
        #expect(source.parameters[VectorReadParameter.metric] == .string(
            VectorDistanceMetric.euclidean.rawValue
        ))
        #expect(source.parameters[VectorReadParameter.queryVector] != nil)
    }

    @Test("Similar rejects a negative result count")
    func rejectsNegativeResultCount() throws {
        let query = Similar(
            VectorFusionInputItem.fields.embedding,
            dimensions: 3
        )
        #expect {
            _ = try query.nearest(to: [1, 2, 3], k: -1)
        } throws: { error in
            error as? VectorFusionInputError == .invalidResultCount(-1)
        }
    }
}
