import DatabaseTypes
import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseQueryPageCursor: Sendable, Hashable {
    private static let formatVersion: UInt8 = 3

    let baseID: Base.ID
    let readPosition: DomainReadPoint.Position
    let placementGeneration: UInt64
    let continuation: ByteString

    func encode(limits: DatabaseWireLimits) throws -> ByteString {
        do {
            return try DatabaseWireWriter.encode(limits: limits) {
                (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                writer.writeUInt8(Self.formatVersion)
                try writer.writeString(baseID.value)
                switch readPosition {
                case .version(let version):
                    writer.writeUInt8(1)
                    writer.writeUInt64(version)
                case .opaque(let identifier):
                    writer.writeUInt8(2)
                    try writer.writeBytes(identifier)
                }
                writer.writeUInt64(placementGeneration)
                try writer.writeBytes(continuation)
            }
        } catch {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
    }

    static func decode(
        _ bytes: ByteString,
        limits: DatabaseWireLimits
    ) throws -> Self {
        do {
            var reader = DatabaseWireReader(bytes, limits: limits)
            guard try reader.readUInt8() == Self.formatVersion else {
                throw DatabaseQueryExecutionError.invalidContinuation
            }
            let baseID = try Base.ID(reader.readString())
            let readPosition: DomainReadPoint.Position
            switch try reader.readUInt8() {
            case 1:
                readPosition = .version(try reader.readUInt64())
            case 2:
                let identifier = try reader.readBytes()
                guard !identifier.isEmpty else {
                    throw DatabaseQueryExecutionError.invalidContinuation
                }
                readPosition = .opaque(identifier)
            default:
                throw DatabaseQueryExecutionError.invalidContinuation
            }
            let placementGeneration = try reader.readUInt64()
            let continuation = try reader.readBytes()
            guard !continuation.isEmpty else {
                throw DatabaseQueryExecutionError.invalidContinuation
            }
            try reader.ensureFullyRead()
            return Self(
                baseID: baseID,
                readPosition: readPosition,
                placementGeneration: placementGeneration,
                continuation: continuation
            )
        } catch let error as DatabaseQueryExecutionError {
            throw error
        } catch {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
    }
}
