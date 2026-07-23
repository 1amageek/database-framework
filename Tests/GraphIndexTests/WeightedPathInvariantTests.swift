import Testing
@testable import GraphIndex

@Suite("Weighted path invariants")
struct WeightedPathInvariantTests {
    @Test("missing edge labels are not omitted during reconstruction")
    func missingEdgeLabelFails() {
        let result = SingleSourceResult(
            source: "A",
            distances: ["A": 0, "B": 1],
            parents: ["B": "A"],
            edgeLabels: [:],
            nodesExplored: 2,
            durationNs: 0
        )

        #expect(throws: WeightedShortestPathError.self) {
            _ = try result.pathTo("B")
        }
    }

    @Test("missing parent distances are not replaced with zero")
    func missingParentDistanceFails() {
        let result = SingleSourceResult(
            source: "A",
            distances: ["B": 1],
            parents: ["B": "A"],
            edgeLabels: ["B": "edge"],
            nodesExplored: 2,
            durationNs: 0
        )

        #expect(throws: WeightedShortestPathError.self) {
            _ = try result.pathTo("B")
        }
    }

    @Test("valid parent state reconstructs every label and weight")
    func validStateReconstructsPath() throws {
        let result = SingleSourceResult(
            source: "A",
            distances: ["A": 0, "B": 2, "C": 5],
            parents: ["B": "A", "C": "B"],
            edgeLabels: ["B": "ab", "C": "bc"],
            nodesExplored: 3,
            durationNs: 0
        )

        let path = try #require(try result.pathTo("C"))
        #expect(path.nodeIDs == ["A", "B", "C"])
        #expect(path.edgeLabels == ["ab", "bc"])
        #expect(path.weights == [2, 3])
    }
}
