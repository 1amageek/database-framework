import DatabaseKit
import DatabaseTypes

/// Selects the physical index generations retired after a schema transition.
///
/// Migration and host orchestration may request cleanup, while the layout being
/// cleared remains owned by DatabaseEngine.
@_spi(DatabaseExecution)
public enum DatabaseIndexStorageRetirement: Sendable, Hashable {
    /// Retires every physical generation owned by one removed logical name.
    case allGenerations

    /// Retires one exact definition and provider-layout generation.
    case physicalGeneration(
        definitionFingerprint: SchemaFingerprint,
        layoutFingerprint: ByteString
    )
}
