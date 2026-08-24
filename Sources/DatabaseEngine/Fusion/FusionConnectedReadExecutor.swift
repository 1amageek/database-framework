import DatabaseKit

/// Feature-owned property-graph traversal invoked by canonical Fusion.
///
/// DatabaseEngine owns cross-entity mapping, candidates, score ordering,
/// transaction lifetime, and output validation. The graph feature owns only
/// property-graph physical planning and shortest-hop traversal.
package protocol FusionConnectedReadExecutor: Sendable {
    var indexType: IndexType { get }

    func validate(
        _ request: FusionConnectedValidationRequest
    ) throws

    func execute(
        _ request: FusionConnectedReadRequest,
        output: FusionConnectedMatchSink
    ) async throws -> FusionInputCoverage
}
