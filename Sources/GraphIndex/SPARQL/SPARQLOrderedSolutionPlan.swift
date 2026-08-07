/// Query-form-independent SPARQL solution pipeline through ORDER BY.
///
/// The executable dataset is retained with the algebra so a compiled plan
/// cannot be evaluated against a silently different dataset selection.
public struct SPARQLOrderedSolutionPlan: Sendable {
    public let dataset: SPARQLExecutionDataset
    public let algebra: ExecutionPattern
    public let orderKeys: [SPARQLOrderKeyPlan]
    public let visibleVariables: [String]

    package init(
        dataset: SPARQLExecutionDataset,
        algebra: consuming ExecutionPattern,
        orderKeys: consuming [SPARQLOrderKeyPlan],
        visibleVariables: consuming [String]
    ) {
        self.dataset = dataset
        self.algebra = consume algebra
        self.orderKeys = consume orderKeys
        self.visibleVariables = consume visibleVariables
    }
}
