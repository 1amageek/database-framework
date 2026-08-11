#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes

public struct DatabaseBaseLifecycleJobState:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    enum Phase: UInt8, Sendable, Hashable {
        case simple = 0
        case movePrepare = 1
        case moveCopy = 2
        case moveVerifySource = 3
        case moveVerifyDestination = 4
        case moveCutover = 5
        case moveCleanup = 6
        case deletePrepare = 7
        case deleteClear = 8
        case deleteFinish = 9
    }

    private static let formatVersion: UInt8 = 3

    let phase: Phase
    let descriptor: DatabaseBasePlacementMoveDescriptor?
    let continuation: ByteString?
    let digest: ByteString?
    let keyCount: UInt64
    let byteCount: UInt64
    let sourceDigest: ByteString?
    let sourceKeyCount: UInt64
    let sourceByteCount: UInt64

    init(
        phase: Phase = .simple,
        descriptor: DatabaseBasePlacementMoveDescriptor? = nil,
        continuation: ByteString? = nil,
        digest: ByteString? = nil,
        keyCount: UInt64 = 0,
        byteCount: UInt64 = 0,
        sourceDigest: ByteString? = nil,
        sourceKeyCount: UInt64 = 0,
        sourceByteCount: UInt64 = 0
    ) {
        self.phase = phase
        self.descriptor = descriptor
        self.continuation = continuation
        self.digest = digest
        self.keyCount = keyCount
        self.byteCount = byteCount
        self.sourceDigest = sourceDigest
        self.sourceKeyCount = sourceKeyCount
        self.sourceByteCount = sourceByteCount
    }

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        do {
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "phase", value: .uint8(phase.rawValue)),
                (
                    key: "descriptor",
                    value: try descriptor.map(Self.value) ?? .null
                ),
                (
                    key: "continuation",
                    value: continuation.map(FieldValue.bytes) ?? .null
                ),
                (
                    key: "digest",
                    value: digest.map(FieldValue.bytes) ?? .null
                ),
                (key: "keyCount", value: .uint64(keyCount)),
                (key: "byteCount", value: .uint64(byteCount)),
                (
                    key: "sourceDigest",
                    value: sourceDigest.map(FieldValue.bytes) ?? .null
                ),
                (key: "sourceKeyCount", value: .uint64(sourceKeyCount)),
                (key: "sourceByteCount", value: .uint64(sourceByteCount)),
            ]))
        } catch let error as PersistentJobPayloadError {
            throw error
        } catch {
            throw .invalidValue("Base lifecycle state is not canonical")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields.count == 10,
              fields["version"]?.uint8Value == Self.formatVersion,
              let rawPhase = fields["phase"]?.uint8Value,
              let phase = Phase(rawValue: rawPhase),
              let descriptorValue = fields["descriptor"],
              let continuationValue = fields["continuation"],
              let digestValue = fields["digest"],
              let keyCount = fields["keyCount"]?.uint64Value,
              let byteCount = fields["byteCount"]?.uint64Value,
              let sourceDigestValue = fields["sourceDigest"],
              let sourceKeyCount = fields["sourceKeyCount"]?.uint64Value,
              let sourceByteCount = fields["sourceByteCount"]?.uint64Value
        else {
            throw .invalidValue("Invalid Base lifecycle state")
        }
        let descriptor: DatabaseBasePlacementMoveDescriptor?
        if descriptorValue.isNull {
            descriptor = nil
        } else {
            descriptor = try Self.descriptor(descriptorValue)
        }
        let continuation = try Self.optionalBytes(
            continuationValue,
            field: "placement continuation"
        )
        let digest = try Self.optionalDigest(
            digestValue,
            field: "placement digest"
        )
        let sourceDigest = try Self.optionalDigest(
            sourceDigestValue,
            field: "source placement digest"
        )
        switch phase {
        case .simple, .movePrepare, .deletePrepare, .deleteClear,
             .deleteFinish:
            guard descriptor == nil,
                  continuation == nil,
                  digest == nil,
                  keyCount == 0,
                  byteCount == 0,
                  sourceDigest == nil,
                  sourceKeyCount == 0,
                  sourceByteCount == 0 else {
                throw .invalidValue("Invalid initial Base lifecycle state")
            }
        case .moveCopy, .moveVerifySource, .moveCutover, .moveCleanup:
            guard descriptor != nil else {
                throw .invalidValue("Base placement state has no descriptor")
            }
        case .moveVerifyDestination:
            guard descriptor != nil, sourceDigest != nil else {
                throw .invalidValue("Base placement verification is incomplete")
            }
        }
        self.phase = phase
        self.descriptor = descriptor
        self.continuation = continuation
        self.digest = digest
        self.keyCount = keyCount
        self.byteCount = byteCount
        self.sourceDigest = sourceDigest
        self.sourceKeyCount = sourceKeyCount
        self.sourceByteCount = sourceByteCount
    }

    private static func value(
        _ descriptor: DatabaseBasePlacementMoveDescriptor
    ) throws -> FieldValue {
        .object(try FieldObject([
            (key: "baseID", value: .string(descriptor.baseID.value)),
            (
                key: "sourcePlacementID",
                value: .string(descriptor.sourcePlacementID.value)
            ),
            (
                key: "sourceDomainID",
                value: .string(descriptor.sourceDomainID.value)
            ),
            (
                key: "sourceNamespacePath",
                value: .array(descriptor.sourceNamespacePath.map {
                    .string($0)
                })
            ),
            (
                key: "sourcePlacementGeneration",
                value: .uint64(descriptor.sourcePlacementGeneration)
            ),
            (
                key: "movingRevision",
                value: .uint64(descriptor.movingRevision)
            ),
            (
                key: "destinationPlacementID",
                value: .string(descriptor.destinationPlacementID.value)
            ),
            (
                key: "destinationDomainID",
                value: .string(descriptor.destinationDomainID.value)
            ),
            (
                key: "destinationNamespacePath",
                value: .array(descriptor.destinationNamespacePath.map {
                    .string($0)
                })
            ),
            (
                key: "destinationPlacementGeneration",
                value: .uint64(descriptor.destinationPlacementGeneration)
            ),
            (
                key: "sourceRootPrefix",
                value: descriptor.sourceRootPrefix.map(FieldValue.bytes)
                    ?? .null
            ),
            (
                key: "destinationRootPrefix",
                value: descriptor.destinationRootPrefix.map(FieldValue.bytes)
                    ?? .null
            ),
        ]))
    }

    private static func descriptor(
        _ value: FieldValue
    ) throws(PersistentJobPayloadError) -> DatabaseBasePlacementMoveDescriptor {
        guard let fields = value.objectValue,
              fields.count == 12,
              let baseID = fields["baseID"]?.stringValue,
              let sourcePlacementID = fields["sourcePlacementID"]?.stringValue,
              let sourceDomainID = fields["sourceDomainID"]?.stringValue,
              let sourcePathValues = fields["sourceNamespacePath"]?.arrayValue,
              let sourceGeneration = fields["sourcePlacementGeneration"]?
                .uint64Value,
              let movingRevision = fields["movingRevision"]?.uint64Value,
              let destinationPlacementID = fields["destinationPlacementID"]?
                .stringValue,
              let destinationDomainID = fields["destinationDomainID"]?
                .stringValue,
              let destinationPathValues = fields["destinationNamespacePath"]?
                .arrayValue,
              let destinationGeneration = fields[
                "destinationPlacementGeneration"
              ]?.uint64Value,
              let sourceRootValue = fields["sourceRootPrefix"],
              let destinationRootValue = fields["destinationRootPrefix"] else {
            throw .invalidValue("Invalid Base placement descriptor")
        }
        do {
            let sourcePath = try Self.path(sourcePathValues)
            let destinationPath = try Self.path(destinationPathValues)
            let sourceRootPrefix = try Self.optionalBytes(
                sourceRootValue,
                field: "source root prefix"
            )
            let destinationRootPrefix = try Self.optionalBytes(
                destinationRootValue,
                field: "destination root prefix"
            )
            guard sourceRootPrefix?.isEmpty == false,
                  destinationRootPrefix?.isEmpty == false else {
                throw PersistentJobPayloadError.invalidValue(
                    "Base placement roots are not prepared"
                )
            }
            return DatabaseBasePlacementMoveDescriptor(
                baseID: try Base.ID(baseID),
                sourcePlacementID: try Base.Placement.ID(sourcePlacementID),
                sourceDomainID: try DatabaseStorageDomain.ID(sourceDomainID),
                sourceNamespacePath: sourcePath,
                sourcePlacementGeneration: sourceGeneration,
                movingRevision: movingRevision,
                destinationPlacementID: try Base.Placement.ID(
                    destinationPlacementID
                ),
                destinationDomainID: try DatabaseStorageDomain.ID(
                    destinationDomainID
                ),
                destinationNamespacePath: destinationPath,
                destinationPlacementGeneration: destinationGeneration,
                sourceRootPrefix: sourceRootPrefix,
                destinationRootPrefix: destinationRootPrefix
            )
        } catch {
            throw .invalidValue("Invalid Base placement identity")
        }
    }

    private static func path(
        _ values: [FieldValue]
    ) throws(PersistentJobPayloadError) -> [String] {
        guard !values.isEmpty else {
            throw .invalidValue("Base placement path is empty")
        }
        var result: [String] = []
        result.reserveCapacity(values.count)
        for value in values {
            guard let component = value.stringValue, !component.isEmpty else {
                throw PersistentJobPayloadError.invalidValue(
                    "Invalid Base placement path"
                )
            }
            result.append(component)
        }
        return result
    }

    private static func optionalBytes(
        _ value: FieldValue,
        field: String
    ) throws(PersistentJobPayloadError) -> ByteString? {
        if value.isNull { return nil }
        guard let bytes = value.bytesValue else {
            throw .invalidValue("Invalid \(field)")
        }
        return bytes
    }

    private static func optionalDigest(
        _ value: FieldValue,
        field: String
    ) throws(PersistentJobPayloadError) -> ByteString? {
        let bytes = try optionalBytes(value, field: field)
        guard bytes == nil || bytes?.count == 32 else {
            throw .invalidValue("Invalid \(field)")
        }
        return bytes
    }
}

#endif
