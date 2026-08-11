import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

public struct DatabaseSchemaApplyJobPlan:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    struct DataTarget: Sendable, Hashable {
        let resource: Security.Resource
        let generation: UInt64
    }

    struct IndexTarget: Sendable, Hashable {
        let entity: String
        let index: String
        let usesDynamicDirectory: Bool
    }

    private static let formatVersion: UInt8 = 3

    let previousFingerprint: SchemaFingerprint
    let targetFingerprint: SchemaFingerprint
    let schemaVersion: Schema.Version
    let idempotencyKey: String
    let manifestBytes: ByteString
    let dataTargets: [DataTarget]
    let indexes: [IndexTarget]
    let maximumWorkUnitsPerSlice: UInt64

    var manifest: SchemaManifest {
        get throws {
            try SchemaManifest(canonicalBytes: manifestBytes)
        }
    }

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        do {
            let encodedDataTargets = try dataTargets.map { target in
                let kind: UInt8
                let baseID: FieldValue
                switch target.resource {
                case .database:
                    kind = 0
                    baseID = .null
                case .base(let id):
                    kind = 1
                    baseID = .string(id.value)
                }
                return FieldValue.object(try FieldObject([
                    (key: "kind", value: .uint8(kind)),
                    (key: "baseID", value: baseID),
                    (key: "generation", value: .uint64(target.generation)),
                ]))
            }
            let encodedIndexes = try indexes.map { target in
                FieldValue.object(try FieldObject([
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
                (key: "schemaVersion", value: Self.value(schemaVersion)),
                (key: "idempotencyKey", value: .string(idempotencyKey)),
                (key: "manifest", value: .bytes(manifestBytes)),
                (key: "dataTargets", value: .array(encodedDataTargets)),
                (key: "indexes", value: .array(encodedIndexes)),
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
              fields.count == 9,
              fields["version"]?.uint8Value == Self.formatVersion,
              let previousBytes = fields["previousFingerprint"]?.bytesValue,
              let targetBytes = fields["targetFingerprint"]?.bytesValue,
              let schemaVersion = Self.schemaVersion(fields["schemaVersion"]),
              let idempotencyKey = fields["idempotencyKey"]?.stringValue,
              !idempotencyKey.isEmpty,
              let manifestBytes = fields["manifest"]?.bytesValue,
              let dataTargetValues = fields["dataTargets"]?.arrayValue,
              let indexValues = fields["indexes"]?.arrayValue,
              let maximumWorkUnitsPerSlice =
                fields["maximumWorkUnitsPerSlice"]?.uint64Value,
              maximumWorkUnitsPerSlice > 0 else {
            throw .invalidValue("Invalid schema apply job plan header")
        }
        let previousFingerprint: SchemaFingerprint
        let targetFingerprint: SchemaFingerprint
        let manifest: SchemaManifest
        do {
            previousFingerprint = try SchemaFingerprint(previousBytes)
            targetFingerprint = try SchemaFingerprint(targetBytes)
            manifest = try SchemaManifest(canonicalBytes: manifestBytes)
            guard try manifest.fingerprint() == targetFingerprint,
                  manifest.schema.version == schemaVersion else {
                throw DatabaseSchemaApplyJobError.corruptedPlan
            }
        } catch {
            throw .invalidValue("Invalid schema apply job manifest")
        }

        var dataTargets: [DataTarget] = []
        dataTargets.reserveCapacity(dataTargetValues.count)
        for value in dataTargetValues {
            guard let fields = value.objectValue,
                  fields.count == 3,
                  let kind = fields["kind"]?.uint8Value,
                  let baseIDValue = fields["baseID"],
                  let generation = fields["generation"]?.uint64Value else {
                throw .invalidValue("Invalid schema apply data target")
            }
            let resource: Security.Resource
            switch kind {
            case 0 where baseIDValue.isNull:
                resource = .database
            case 1:
                guard let identifier = baseIDValue.stringValue else {
                    throw .invalidValue("Invalid schema apply data target")
                }
                do {
                    resource = .base(try Base.ID(identifier))
                } catch {
                    throw .invalidValue("Invalid schema apply data target")
                }
            default:
                throw .invalidValue("Invalid schema apply data target")
            }
            dataTargets.append(
                DataTarget(resource: resource, generation: generation)
            )
        }
        guard dataTargets == dataTargets.sorted(by: Self.dataTargetLessThan),
              Set(dataTargets.map { $0.resource }).count == dataTargets.count else {
            throw .invalidValue("Schema apply data targets are not canonical")
        }

        var indexes: [IndexTarget] = []
        indexes.reserveCapacity(indexValues.count)
        for value in indexValues {
            guard let fields = value.objectValue,
                  fields.count == 3,
                  let entity = fields["entity"]?.stringValue,
                  !entity.isEmpty,
                  let index = fields["index"]?.stringValue,
                  !index.isEmpty,
                  let dynamic = fields["usesDynamicDirectory"]?.boolValue else {
                throw .invalidValue("Invalid schema apply index target")
            }
            indexes.append(
                IndexTarget(
                    entity: entity,
                    index: index,
                    usesDynamicDirectory: dynamic
                )
            )
        }
        guard indexes == indexes.sorted(by: Self.indexLessThan),
              Set(indexes).count == indexes.count else {
            throw .invalidValue("Schema apply index targets are not canonical")
        }

        self.previousFingerprint = previousFingerprint
        self.targetFingerprint = targetFingerprint
        self.schemaVersion = schemaVersion
        self.idempotencyKey = idempotencyKey
        self.manifestBytes = manifestBytes
        self.dataTargets = dataTargets
        self.indexes = indexes
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }

    init(
        previousFingerprint: SchemaFingerprint,
        targetFingerprint: SchemaFingerprint,
        manifest: SchemaManifest,
        idempotencyKey: String,
        dataTargets: [DataTarget],
        indexes: [IndexTarget],
        maximumWorkUnitsPerSlice: UInt64
    ) throws {
        self.previousFingerprint = previousFingerprint
        self.targetFingerprint = targetFingerprint
        self.schemaVersion = manifest.schema.version
        self.idempotencyKey = idempotencyKey
        self.manifestBytes = try manifest.canonicalBytes()
        self.dataTargets = dataTargets.sorted(by: Self.dataTargetLessThan)
        self.indexes = indexes.sorted(by: Self.indexLessThan)
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }

    private static func indexLessThan(
        _ lhs: IndexTarget,
        _ rhs: IndexTarget
    ) -> Bool {
        (lhs.entity, lhs.index) < (rhs.entity, rhs.index)
    }

    private static func dataTargetLessThan(
        _ lhs: DataTarget,
        _ rhs: DataTarget
    ) -> Bool {
        switch (lhs.resource, rhs.resource) {
        case (.database, .database):
            return false
        case (.database, .base):
            return true
        case (.base, .database):
            return false
        case (.base(let lhsID), .base(let rhsID)):
            return lhsID < rhsID
        }
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
