/// One node emitted by a breadth-first traversal.
package struct GraphTraversalStep: Sendable, Equatable {
    package let depth: Int
    package let node: GraphIdentity
}
