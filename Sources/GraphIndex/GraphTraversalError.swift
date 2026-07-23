import DatabaseEngine

/// Failures that prevent a graph traversal from returning a complete result.
package enum GraphTraversalError: Error, Sendable, Equatable {
    case invalidMaximumDepth(Int)
    case invalidMaximumNodes(Int)
    case maximumNodesReached(explored: Int, limit: Int)
    case incomplete(LimitReason)
}
