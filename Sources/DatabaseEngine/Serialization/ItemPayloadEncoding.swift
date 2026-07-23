/// Canonical payload encoding stored in an item envelope.
public enum ItemPayloadEncoding: UInt8, Sendable, Equatable {
    /// Payload bytes are stored without transformation.
    case identity = 0
}
