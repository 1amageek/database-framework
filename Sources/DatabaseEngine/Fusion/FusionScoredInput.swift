import DatabaseKit

/// One scored observation list in canonical plan order.
struct FusionScoredInput: Sendable {
    let scoring: FusionScoring
    let result: FusionIndexReadResult
}
