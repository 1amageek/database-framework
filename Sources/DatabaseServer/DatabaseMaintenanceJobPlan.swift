import DatabaseKit
import DatabaseTypes

public struct DatabaseMaintenanceJobPlan:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    enum Invocation: Sendable, Hashable {
        case migrations(
            targetVersion: SchemaVersion,
            totalStageCount: UInt64,
            maximumStagesPerSlice: UInt64
        )
        case indexRebuild(
            entity: String,
            index: String,
            partitions: FieldObject,
            schemaVersion: SchemaVersion,
            maximumWorkUnits: UInt64
        )
        case compaction(maximumWorkUnits: UInt64)
    }

    private static let formatVersion: UInt8 = 1

    let invocation: Invocation

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue
    {
        var fields: [(key: String, value: FieldValue)] = [
            (key: "version", value: .uint8(Self.formatVersion)),
        ]
        switch invocation {
        case .migrations(
            let targetVersion,
            let totalStageCount,
            let maximumStagesPerSlice
        ):
            fields.append((key: "kind", value: .string("migrations")))
            fields.append(
                (key: "targetVersion", value: Self.value(targetVersion))
            )
            fields.append(
                (key: "totalStageCount", value: .uint64(totalStageCount))
            )
            fields.append(
                (
                    key: "maximumStagesPerSlice",
                    value: .uint64(maximumStagesPerSlice)
                )
            )
        case .indexRebuild(
            let entity,
            let index,
            let partitions,
            let schemaVersion,
            let maximumWorkUnits
        ):
            fields.append((key: "kind", value: .string("indexRebuild")))
            fields.append((key: "entity", value: .string(entity)))
            fields.append((key: "index", value: .string(index)))
            fields.append((key: "partitions", value: .object(partitions)))
            fields.append(
                (key: "schemaVersion", value: Self.value(schemaVersion))
            )
            fields.append(
                (key: "maximumWorkUnits", value: .uint64(maximumWorkUnits))
            )
        case .compaction(let maximumWorkUnits):
            fields.append((key: "kind", value: .string("compaction")))
            fields.append(
                (key: "maximumWorkUnits", value: .uint64(maximumWorkUnits))
            )
        }
        do {
            return .object(try FieldObject(consume fields))
        } catch {
            throw .invalidValue("Maintenance plan fields are not unique")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields["version"]?.uint8Value == Self.formatVersion,
              let kind = fields["kind"]?.stringValue else {
            throw .invalidValue("Invalid maintenance plan header")
        }
        switch kind {
        case "migrations":
            guard let targetVersion = Self.schemaVersion(
                    fields["targetVersion"]
                  ),
                  let totalStageCount =
                    fields["totalStageCount"]?.uint64Value,
                  let maximumStagesPerSlice =
                    fields["maximumStagesPerSlice"]?.uint64Value else {
                throw .invalidValue("Invalid migration plan")
            }
            invocation = .migrations(
                targetVersion: targetVersion,
                totalStageCount: totalStageCount,
                maximumStagesPerSlice: maximumStagesPerSlice
            )
        case "indexRebuild":
            guard let entity = fields["entity"]?.stringValue,
                  let index = fields["index"]?.stringValue,
                  let partitions = fields["partitions"]?.objectValue,
                  let schemaVersion = Self.schemaVersion(
                    fields["schemaVersion"]
                  ),
                  let maximumWorkUnits =
                    fields["maximumWorkUnits"]?.uint64Value else {
                throw .invalidValue("Invalid index rebuild plan")
            }
            invocation = .indexRebuild(
                entity: entity,
                index: index,
                partitions: partitions,
                schemaVersion: schemaVersion,
                maximumWorkUnits: maximumWorkUnits
            )
        case "compaction":
            guard let maximumWorkUnits =
                    fields["maximumWorkUnits"]?.uint64Value else {
                throw .invalidValue("Invalid compaction plan")
            }
            invocation = .compaction(maximumWorkUnits: maximumWorkUnits)
        default:
            throw .invalidValue("Unknown maintenance plan kind")
        }
    }

    init(invocation: Invocation) {
        self.invocation = invocation
    }

    private static func value(_ version: SchemaVersion) -> FieldValue {
        .array([
            .uint32(version.major),
            .uint32(version.minor),
            .uint32(version.patch),
        ])
    }

    private static func schemaVersion(
        _ value: FieldValue?
    ) -> SchemaVersion? {
        guard let components = value?.arrayValue,
              components.count == 3,
              let major = components[0].uint32Value,
              let minor = components[1].uint32Value,
              let patch = components[2].uint32Value else {
            return nil
        }
        return SchemaVersion(major, minor, patch)
    }
}
