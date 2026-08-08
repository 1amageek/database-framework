import DatabaseTypes

public struct DatabaseSchemaApplyJobState:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    private static let formatVersion: UInt8 = 1

    let targetOffset: UInt64
    let nextPartitionContinuation: ByteString?
    let activePartitions: FieldObject?
    let activePartitionIsLast: Bool
    let activeBuildStarted: Bool

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        do {
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "targetOffset", value: .uint64(targetOffset)),
                (
                    key: "nextPartitionContinuation",
                    value: nextPartitionContinuation.map(FieldValue.bytes)
                        ?? .null
                ),
                (
                    key: "activePartitions",
                    value: activePartitions.map(FieldValue.object) ?? .null
                ),
                (
                    key: "activePartitionIsLast",
                    value: .bool(activePartitionIsLast)
                ),
                (
                    key: "activeBuildStarted",
                    value: .bool(activeBuildStarted)
                ),
            ]))
        } catch {
            throw .invalidValue("Schema apply job state is not canonical")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields.count == 6,
              fields["version"]?.uint8Value == Self.formatVersion,
              let targetOffset = fields["targetOffset"]?.uint64Value,
              let nextValue = fields["nextPartitionContinuation"],
              let activeValue = fields["activePartitions"],
              let activePartitionIsLast =
                fields["activePartitionIsLast"]?.boolValue,
              let activeBuildStarted =
                fields["activeBuildStarted"]?.boolValue else {
            throw .invalidValue("Invalid schema apply job state")
        }
        let nextPartitionContinuation: ByteString?
        if nextValue.isNull {
            nextPartitionContinuation = nil
        } else if let bytes = nextValue.bytesValue {
            nextPartitionContinuation = bytes
        } else {
            throw .invalidValue("Invalid schema partition continuation")
        }
        let activePartitions: FieldObject?
        if activeValue.isNull {
            activePartitions = nil
        } else if let partitions = activeValue.objectValue {
            activePartitions = partitions
        } else {
            throw .invalidValue("Invalid active schema build partition")
        }
        guard activePartitions != nil || !activeBuildStarted else {
            throw .invalidValue("Started schema build has no active partition")
        }
        self.targetOffset = targetOffset
        self.nextPartitionContinuation = nextPartitionContinuation
        self.activePartitions = activePartitions
        self.activePartitionIsLast = activePartitionIsLast
        self.activeBuildStarted = activeBuildStarted
    }

    init(
        targetOffset: UInt64 = 0,
        nextPartitionContinuation: ByteString? = nil,
        activePartitions: FieldObject? = nil,
        activePartitionIsLast: Bool = false,
        activeBuildStarted: Bool = false
    ) {
        self.targetOffset = targetOffset
        self.nextPartitionContinuation = nextPartitionContinuation
        self.activePartitions = activePartitions
        self.activePartitionIsLast = activePartitionIsLast
        self.activeBuildStarted = activeBuildStarted
    }
}
