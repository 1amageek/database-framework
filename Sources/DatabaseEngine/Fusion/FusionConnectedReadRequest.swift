import DatabaseKit

/// One admitted property-graph read for a Connected Fusion input.
package struct FusionConnectedReadRequest: Sendable {
    package let source: FusionConnectedSource
    package let access: any FusionIndexReadAccess
    package let workMeter: DatabaseWorkMeter

    package init(
        source: FusionConnectedSource,
        access: any FusionIndexReadAccess,
        workMeter: DatabaseWorkMeter
    ) {
        self.source = source
        self.access = access
        self.workMeter = workMeter
    }
}
