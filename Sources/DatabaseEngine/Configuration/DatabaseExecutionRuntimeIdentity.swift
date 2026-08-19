/// Application-controlled identity for executable behavior that cannot be
/// derived from canonical schema metadata.
///
/// Keep `identifier` stable for one application runtime and increment
/// `revision` whenever authorization policies, entity adapters, mutation
/// maintainers, or query executors change behavior without a schema change.
public struct DatabaseExecutionRuntimeIdentity: Sendable, Hashable {
    public static let maximumIdentifierUTF8ByteCount = 512

    public let identifier: String
    public let revision: UInt64

    public init(identifier: String, revision: UInt64) {
        self.identifier = identifier
        self.revision = revision
    }
}
