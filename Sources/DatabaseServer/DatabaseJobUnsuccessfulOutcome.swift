import DatabaseWire

public enum DatabaseJobUnsuccessfulOutcome:
    DatabaseWireValue,
    Sendable,
    Hashable {
    case failed(DatabaseRemoteError)
    case cancelled

    public func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        switch self {
        case .failed(let error):
            writer.writeUInt8(1)
            try error.encode(into: &writer)
        case .cancelled:
            writer.writeUInt8(2)
        }
    }

    public init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        switch try reader.readUInt8() {
        case 1:
            self = .failed(try DatabaseRemoteError(from: &reader))
        case 2:
            self = .cancelled
        case let tag:
            throw .invalidValueTag(tag)
        }
    }
}
