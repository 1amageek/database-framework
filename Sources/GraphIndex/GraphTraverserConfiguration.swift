/// Limits for one transaction-scoped graph traversal.
package struct GraphTraverserConfiguration: Sendable {
    package static let `default` = GraphTraverserConfiguration(
        maximumDepth: 3,
        maximumNodes: 10_000
    )

    package let maximumDepth: Int
    package let maximumNodes: Int

    package init(
        maximumDepth: Int,
        maximumNodes: Int
    ) {
        self.maximumDepth = maximumDepth
        self.maximumNodes = maximumNodes
    }
}
