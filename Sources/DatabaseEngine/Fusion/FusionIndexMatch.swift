import DatabaseTypes

/// One feature-owned physical match admitted by DatabaseEngine.
struct FusionIndexMatch: Sendable {
    let primaryKey: ByteString
    let numericSignal: Double?
}
