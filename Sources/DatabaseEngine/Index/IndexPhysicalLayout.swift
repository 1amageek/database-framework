import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire

/// Provider-owned description of one persisted index layout.
///
/// Logical index meaning remains in `IndexDescriptor`. This value contains
/// only parameters that change persisted bytes or require rebuilding them.
/// Query-only tuning must not be included.
public struct IndexPhysicalLayout: Sendable, Hashable {
    public let name: String
    public let revision: UInt32
    public let parameters: FieldObject

    public let fingerprint: ByteString

    public init(
        name: String,
        revision: UInt32,
        parameters: FieldObject = FieldObject()
    ) throws(IndexPhysicalLayoutError) {
        guard !name.isEmpty else {
            throw .emptyName
        }
        guard revision > 0 else {
            throw .invalidRevision(revision)
        }

        let fingerprint: ByteString
        do {
            var accumulator = SHA256Accumulator()
            try DatabaseWireWriter.emit(
                consume: { accumulator.update($0) }
            ) { writer throws(DatabaseWireError) in
                try writer.writeString("database-framework.index-layout")
                try writer.writeString(name)
                writer.writeUInt32(revision)
                try parameters.encode(into: &writer)
            }
            fingerprint = accumulator.finalize()
        } catch {
            throw .invalidParameters
        }

        self.name = name
        self.revision = revision
        self.parameters = parameters
        self.fingerprint = fingerprint
    }
}

public enum IndexPhysicalLayoutError: Error, Sendable, Equatable {
    case emptyName
    case invalidRevision(UInt32)
    case invalidParameters
}
