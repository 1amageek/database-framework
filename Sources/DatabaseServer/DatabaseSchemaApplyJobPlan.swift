import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseSchemaApplyJobPlan:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    struct Target: Sendable, Hashable {
        let entity: String
        let index: String
        let usesDynamicDirectory: Bool
    }

    private static let formatVersion: UInt8 = 1

    let previousFingerprint: SchemaFingerprint
    let targetFingerprint: SchemaFingerprint
    let schemaVersion: Schema.Version
    let targets: [Target]
    let maximumWorkUnitsPerSlice: UInt64

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        let encodedTargets: [FieldValue]
        do {
            encodedTargets = try targets.map { target in
                .object(try FieldObject([
                    (key: "entity", value: .string(target.entity)),
                    (key: "index", value: .string(target.index)),
                    (
                        key: "usesDynamicDirectory",
                        value: .bool(target.usesDynamicDirectory)
                    ),
                ]))
            }
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (
                    key: "previousFingerprint",
                    value: .bytes(previousFingerprint.bytes)
                ),
                (
                    key: "targetFingerprint",
                    value: .bytes(targetFingerprint.bytes)
                ),
                (
                    key: "schemaVersion",
                    value: Self.value(schemaVersion)
                ),
                (key: "targets", value: .array(encodedTargets)),
                (
                    key: "maximumWorkUnitsPerSlice",
                    value: .uint64(maximumWorkUnitsPerSlice)
                ),
            ]))
        } catch {
            throw .invalidValue("Schema apply job plan is not canonical")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields.count == 6,
              fields["version"]?.uint8Value == Self.formatVersion,
              let previousBytes = fields["previousFingerprint"]?.bytesValue,
              let targetBytes = fields["targetFingerprint"]?.bytesValue,
              let schemaVersion = Self.schemaVersion(fields["schemaVersion"]),
              let targetValues = fields["targets"]?.arrayValue,
              let maximumWorkUnitsPerSlice =
                fields["maximumWorkUnitsPerSlice"]?.uint64Value,
              maximumWorkUnitsPerSlice > 0 else {
            throw .invalidValue("Invalid schema apply job plan header")
        }
        let previousFingerprint: SchemaFingerprint
        let targetFingerprint: SchemaFingerprint
        do {
            previousFingerprint = try SchemaFingerprint(previousBytes)
            targetFingerprint = try SchemaFingerprint(targetBytes)
        } catch {
            throw .invalidValue("Invalid schema apply job fingerprint")
        }
        var targets: [Target] = []
        targets.reserveCapacity(targetValues.count)
        for value in targetValues {
            guard let target = value.objectValue,
                  target.count == 3,
                  let entity = target["entity"]?.stringValue,
                  !entity.isEmpty,
                  let index = target["index"]?.stringValue,
                  !index.isEmpty,
                  let usesDynamicDirectory =
                    target["usesDynamicDirectory"]?.boolValue else {
                throw .invalidValue("Invalid schema apply index target")
            }
            targets.append(
                Target(
                    entity: entity,
                    index: index,
                    usesDynamicDirectory: usesDynamicDirectory
                )
            )
        }
        guard !targets.isEmpty,
              Set(targets).count == targets.count else {
            throw .invalidValue("Schema apply index targets are empty or duplicated")
        }
        self.previousFingerprint = previousFingerprint
        self.targetFingerprint = targetFingerprint
        self.schemaVersion = schemaVersion
        self.targets = targets
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }

    init(
        previousFingerprint: SchemaFingerprint,
        targetFingerprint: SchemaFingerprint,
        schemaVersion: Schema.Version,
        targets: [Target],
        maximumWorkUnitsPerSlice: UInt64
    ) {
        self.previousFingerprint = previousFingerprint
        self.targetFingerprint = targetFingerprint
        self.schemaVersion = schemaVersion
        self.targets = targets
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }

    private static func value(_ version: Schema.Version) -> FieldValue {
        .array([
            .uint32(version.major),
            .uint32(version.minor),
            .uint32(version.patch),
        ])
    }

    private static func schemaVersion(
        _ value: FieldValue?
    ) -> Schema.Version? {
        guard let components = value?.arrayValue,
              components.count == 3,
              let major = components[0].uint32Value,
              let minor = components[1].uint32Value,
              let patch = components[2].uint32Value else {
            return nil
        }
        return Schema.Version(major, minor, patch)
    }
}
