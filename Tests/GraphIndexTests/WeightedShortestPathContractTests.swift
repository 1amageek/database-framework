import Testing
@testable import GraphIndex

@Suite("Weighted shortest path contract")
struct WeightedShortestPathContractTests {
    @Test("negative weight error reports offending edge")
    func negativeWeightErrorDescriptionIncludesEdge() {
        let error = WeightedShortestPathError.negativeWeight(
            source: "a",
            target: "b",
            edgeLabel: "cost",
            weight: -1
        )

        #expect(error.description.contains("a -> b"))
        #expect(error.description.contains("weight=-1.0"))
    }
}
