import DatabaseKit

/// Feature-owned physical reader invoked only by canonical Fusion execution.
///
/// DatabaseEngine owns the transaction, candidate domain, output validation,
/// and score composition. A feature module owns only its physical index
/// algorithm and layout.
package protocol FusionIndexReadExecutor: Sendable {
    var indexType: IndexType { get }

    /// Validates feature-specific parameters before any physical read starts.
    func validate(
        _ request: FusionIndexValidationRequest
    ) throws

    /// Reads the admitted index without an incoming candidate restriction.
    func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage

    /// Reads the admitted index after restricting work to Engine-owned
    /// candidates. The feature can observe only canonical primary keys.
    func executeRestricted(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage
}
