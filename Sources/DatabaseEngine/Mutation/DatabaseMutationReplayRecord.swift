import DatabaseTypes

@_spi(DatabaseExecution)
public struct DatabaseMutationReplayRecord: Sendable, Hashable {
    public let discriminator: ByteString
    public let requestFingerprint: ByteString
    public let outcomeFingerprint: ByteString
    public let outcome: ByteString

    public init(
        discriminator: ByteString,
        requestFingerprint: ByteString,
        outcomeFingerprint: ByteString,
        outcome: ByteString
    ) {
        self.discriminator = discriminator
        self.requestFingerprint = requestFingerprint
        self.outcomeFingerprint = outcomeFingerprint
        self.outcome = outcome
    }
}
