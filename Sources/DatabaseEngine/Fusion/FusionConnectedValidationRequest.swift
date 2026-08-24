import DatabaseKit

/// Schema-resolved contract for one Connected physical reader.
package struct FusionConnectedValidationRequest: Sendable {
    package let source: FusionConnectedSource
    package let scoring: FusionScoring?
    package let descriptor: IndexDescriptor

    package init(
        source: FusionConnectedSource,
        scoring: FusionScoring?,
        descriptor: IndexDescriptor
    ) {
        self.source = source
        self.scoring = scoring
        self.descriptor = descriptor
    }
}
