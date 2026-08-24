import DatabaseKit
import DatabaseTypes
import Testing

@testable import GraphIndex

@Persistable
private struct ConnectedFusionResultItem {
    var id: String
    var vertexID: String
}

@Persistable
private struct ConnectedFusionEdge {
    var id: String
}

@Suite("Connected Fusion input")
struct ConnectedFusionInputTests {
    @Test("Connected preserves cross-entity traversal semantics in QueryIR")
    func lowersToCanonicalInput() throws {
        let partitions = try FieldObject([
            (key: "tenant", value: .string("a")),
        ])
        let input = Connected(
            ConnectedFusionResultItem.fields.vertexID,
            from: "origin",
            through: ConnectedFusionEdge.self,
            indexNamed: "property_graph",
            partitions: partitions
        )
        .via("follows")
        .direction(.both)
        .hops(3)
        .limit(9)
        .fusionInput

        #expect(input.scoring == .annotation(
            name: "hops",
            order: .lowerIsBetter
        ))
        #expect(input.limit == 9)
        guard case .connected(let source) = input.operation else {
            Issue.record("Connected must lower to a connected operation")
            return
        }
        #expect(source.edgeEntity == ConnectedFusionEdge.persistableType)
        #expect(source.edgePartitions == partitions)
        #expect(source.selection == .named(
            name: "property_graph",
            type: .graph(.property)
        ))
        #expect(source.resultField.name == "vertexID")
        #expect(source.origin == "origin")
        #expect(source.edgeLabel == "follows")
        #expect(source.direction == .both)
        #expect(source.maximumHops == 3)
    }
}
