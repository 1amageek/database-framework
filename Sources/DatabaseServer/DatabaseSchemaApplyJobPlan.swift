import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseSchemaApplyJobPlan:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    struct BaseTarget: Sendable, Hashable {
        let id: Base.ID
        let placementGeneration: UInt64
    }

    struct IndexTarget: Sendable, Hashable {
        let entity: String
        let index: String
        let usesDynamicDirectory: Bool
    }

    private static let formatVersion: UInt8 = 2

    let previousFingerprint: SchemaFingerprint
    let targetFingerprint: SchemaFingerprint
    let schemaVersion: Schema.Version
    let idempotencyKey: String
    let manifestBytes: ByteString
    let bases: [BaseTarget]
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
            let encodedBases = try bases.map { base in
                FieldValue.object(try FieldObject([
                    (key: "id", value: .string(base.id.value)),
                    (
                        key: "placementGeneration",
                        value: .uint64(base.placementGeneration)
                    ),
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
                (key: "bases", value: .array(encodedBases)),
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
              let baseValues = fields["bases"]?.arrayValue,
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

        var bases: [BaseTarget] = []
        bases.reserveCapacity(baseValues.count)
        for value in baseValues {
            guard let fields = value.objectValue,
                  fields.count == 2,
                  let identifier = fields["id"]?.stringValue,
                  let generation = fields["placementGeneration"]?.uint64Value else {
                throw .invalidValue("Invalid schema apply Base target")
            }
            let id: Base.ID
            do {
                id = try Base.ID(identifier)
            } catch {
                throw .invalidValue("Invalid schema apply Base target")
            }
            bases.append(
                BaseTarget(id: id, placementGeneration: generation)
            )
        }
        guard bases == bases.sorted(by: { $0.id < $1.id }),
              Set(bases.map { $0.id }).count == bases.count else {
            throw .invalidValue("Schema apply Base targets are not canonical")
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
        self.bases = bases
        self.indexes = indexes
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }

    init(
        previousFingerprint: SchemaFingerprint,
        targetFingerprint: SchemaFingerprint,
        manifest: SchemaManifest,
        idempotencyKey: String,
        bases: [BaseTarget],
        indexes: [IndexTarget],
        maximumWorkUnitsPerSlice: UInt64
    ) throws {
        self.previousFingerprint = previousFingerprint
        self.targetFingerprint = targetFingerprint
        self.schemaVersion = manifest.schema.version
        self.idempotencyKey = idempotencyKey
        self.manifestBytes = try manifest.canonicalBytes()
        self.bases = bases.sorted { $0.id < $1.id }
        self.indexes = indexes.sorted(by: Self.indexLessThan)
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }

    private static func indexLessThan(
        _ lhs: IndexTarget,
        _ rhs: IndexTarget
    ) -> Bool {
        (lhs.entity, lhs.index) < (rhs.entity, rhs.index)
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
