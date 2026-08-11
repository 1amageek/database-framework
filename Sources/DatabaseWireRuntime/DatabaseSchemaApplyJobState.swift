import DatabaseTypes

public struct DatabaseSchemaApplyJobState:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    enum Phase: UInt8, Sendable, Hashable {
        case staging
        case publishing
        case installing
        case building
        case finishing
    }

    private static let formatVersion: UInt8 = 4

    let phase: Phase
    let dataTargetOffset: UInt64
    let indexOffset: UInt64
    let nextPartitionContinuation: ByteString?
    let activePartitions: FieldObject?
    let activePartitionIsLast: Bool
    let activeBuildStarted: Bool

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        do {
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "phase", value: .uint8(phase.rawValue)),
                (key: "dataTargetOffset", value: .uint64(dataTargetOffset)),
                (key: "indexOffset", value: .uint64(indexOffset)),
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
              fields.count == 8,
              fields["version"]?.uint8Value == Self.formatVersion,
              let rawPhase = fields["phase"]?.uint8Value,
              let phase = Phase(rawValue: rawPhase),
              let dataTargetOffset = fields["dataTargetOffset"]?.uint64Value,
              let indexOffset = fields["indexOffset"]?.uint64Value,
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
        guard activePartitions != nil || !activeBuildStarted,
              phase == .building
                || (indexOffset == 0
                    && nextPartitionContinuation == nil
                    && activePartitions == nil
                    && !activePartitionIsLast
                    && !activeBuildStarted) else {
            throw .invalidValue("Schema apply phase contains build state")
        }
        self.phase = phase
        self.dataTargetOffset = dataTargetOffset
        self.indexOffset = indexOffset
        self.nextPartitionContinuation = nextPartitionContinuation
        self.activePartitions = activePartitions
        self.activePartitionIsLast = activePartitionIsLast
        self.activeBuildStarted = activeBuildStarted
    }

    init(
        phase: Phase = .staging,
        dataTargetOffset: UInt64 = 0,
        indexOffset: UInt64 = 0,
        nextPartitionContinuation: ByteString? = nil,
        activePartitions: FieldObject? = nil,
        activePartitionIsLast: Bool = false,
        activeBuildStarted: Bool = false
    ) {
        self.phase = phase
        self.dataTargetOffset = dataTargetOffset
        self.indexOffset = indexOffset
        self.nextPartitionContinuation = nextPartitionContinuation
        self.activePartitions = activePartitions
        self.activePartitionIsLast = activePartitionIsLast
        self.activeBuildStarted = activeBuildStarted
    }
}
