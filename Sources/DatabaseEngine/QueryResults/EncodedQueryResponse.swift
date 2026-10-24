import DatabaseTypes
import Synchronization

/// An authoritatively closed encoded response, retained until terminal publication.
@_spi(DatabaseExecution)
public final class EncodedQueryResponse: Sendable {
    private let payload: Mutex<EncodedQueryPayload?>
    public let byteCount: Int

    init(_ payload: EncodedQueryPayload) {
        self.byteCount = payload.bytes.count
        self.payload = Mutex(payload)
    }

    public func promoteToOutput() throws -> ByteString {
        let selected = try payload.withLock { payload in
            guard let selected = payload else { throw SQLBlockProjectionError.alreadyPromoted }
            payload = nil
            return selected
        }
        defer { withExtendedLifetime(selected.claim) {} }
        return selected.bytes
    }
}
