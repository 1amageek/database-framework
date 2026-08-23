#if DATABASE_MULTI_BASE
import DatabaseKit

/// Immutable Composition metadata exposed while Framework retains every
/// authorized member lease for the surrounding callback.
///
/// This value carries no transaction, storage root, engine, or lease
/// authority. Retaining it after the callback cannot extend a Base generation
/// lifetime.
@_spi(DatabaseExecution)
public struct DatabaseCompositionReadDescriptor: Sendable {
    public let selection: CompositionSelection
    public let resolution: CompositionResolution
    public let namedRecord: DatabaseCompositionRecord?
    public let basePlacementGenerations: [Base.ID: UInt64]

    package init(lease: DatabaseCompositionLease) {
        self.selection = lease.selection
        self.resolution = lease.resolution
        self.namedRecord = lease.namedRecord
        self.basePlacementGenerations = lease.basePlacementGenerations
    }
}
#endif
