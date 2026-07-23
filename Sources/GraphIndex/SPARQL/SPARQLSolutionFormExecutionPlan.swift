/// The terminal solution sequence consumed by ASK, CONSTRUCT, and DESCRIBE.
public struct SPARQLSolutionFormExecutionPlan: Sendable, Equatable {
    public let ordered: SPARQLOrderedSolutionPlan
    public let slice: SPARQLSlice

    package init(
        ordered: consuming SPARQLOrderedSolutionPlan,
        slice: SPARQLSlice
    ) {
        self.ordered = consume ordered
        self.slice = slice
    }
}
