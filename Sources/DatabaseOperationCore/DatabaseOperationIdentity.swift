public struct DatabaseOperationIdentity: Sendable, Hashable {
    public let version: String

    public init(version: String) {
        self.version = version
    }
}
