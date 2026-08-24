import DatabaseKit

/// One schema-admitted physical Fusion read.
package struct FusionIndexReadRequest: Sendable {
    package let source: FusionIndexSource
    package let scoring: FusionScoring?
    package let limit: Int
    package let access: any FusionIndexReadAccess
    package let workMeter: DatabaseWorkMeter

    init(
        source: FusionIndexSource,
        scoring: FusionScoring?,
        limit: Int,
        access: any FusionIndexReadAccess,
        workMeter: DatabaseWorkMeter
    ) {
        self.source = source
        self.scoring = scoring
        self.limit = limit
        self.access = access
        self.workMeter = workMeter
    }
}
