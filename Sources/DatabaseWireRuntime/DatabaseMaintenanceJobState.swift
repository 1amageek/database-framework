import DatabaseTypes

public struct DatabaseMaintenanceJobState:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    enum Value: Sendable, Hashable {
        case migrations
        case indexRebuild(started: Bool)
        case compaction(continuation: ByteString?)
    }

    private static let formatVersion: UInt8 = 1

    let value: Value

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue
    {
        let fields: [(key: String, value: FieldValue)]
        switch value {
        case .migrations:
            fields = [
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "kind", value: .string("migrations")),
            ]
        case .indexRebuild(let started):
            fields = [
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "kind", value: .string("indexRebuild")),
                (key: "started", value: .bool(started)),
            ]
        case .compaction(let continuation):
            fields = [
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "kind", value: .string("compaction")),
                (
                    key: "continuation",
                    value: continuation.map(FieldValue.bytes) ?? .null
                ),
            ]
        }
        do {
            return .object(try FieldObject(fields))
        } catch {
            throw .invalidValue("Maintenance state fields are not unique")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields["version"]?.uint8Value == Self.formatVersion,
              let kind = fields["kind"]?.stringValue else {
            throw .invalidValue("Invalid maintenance state header")
        }
        switch kind {
        case "migrations":
            value = .migrations
        case "indexRebuild":
            guard let started = fields["started"]?.boolValue else {
                throw .invalidValue("Invalid index rebuild state")
            }
            value = .indexRebuild(started: started)
        case "compaction":
            guard let continuation = fields["continuation"] else {
                throw .invalidValue("Invalid compaction state")
            }
            if continuation.isNull {
                value = .compaction(continuation: nil)
            } else if let bytes = continuation.bytesValue {
                value = .compaction(continuation: bytes)
            } else {
                throw .invalidValue("Invalid compaction continuation")
            }
        default:
            throw .invalidValue("Unknown maintenance state kind")
        }
    }

    init(value: Value) {
        self.value = value
    }
}
