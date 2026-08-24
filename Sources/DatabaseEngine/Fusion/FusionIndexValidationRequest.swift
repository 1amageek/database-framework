import DatabaseKit

/// Schema-bound input available during Fusion preflight.
package struct FusionIndexValidationRequest: Sendable {
    package let source: FusionIndexSource
    package let scoring: FusionScoring?
    package let descriptor: IndexDescriptor

    init(
        source: FusionIndexSource,
        scoring: FusionScoring?,
        descriptor: IndexDescriptor
    ) {
        self.source = source
        self.scoring = scoring
        self.descriptor = descriptor
    }
}
