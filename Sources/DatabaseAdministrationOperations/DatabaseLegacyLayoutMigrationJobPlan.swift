import DatabaseJobRuntime
#if DATABASE_ADMINISTRATION_OPERATIONS_ENABLED
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseLegacyLayoutMigrationJobPlan:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    private static let formatVersion: UInt8 = 1

    package let baseID: Base.ID
    package let placementID: Base.Placement.ID
    package let initialGrants: [Security.Grant]
    package let expectedLayoutFingerprint: DatabaseLayoutFingerprint
    package let expectedRevision: UInt64

    package init(
        baseID: Base.ID,
        placementID: Base.Placement.ID,
        initialGrants: [Security.Grant],
        expectedLayoutFingerprint: DatabaseLayoutFingerprint,
        expectedRevision: UInt64
    ) {
        self.baseID = baseID
        self.placementID = placementID
        self.initialGrants = initialGrants
        self.expectedLayoutFingerprint = expectedLayoutFingerprint
        self.expectedRevision = expectedRevision
    }

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        do {
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "baseID", value: .string(baseID.value)),
                (key: "placementID", value: .string(placementID.value)),
                (
                    key: "initialGrants",
                    value: .array(try initialGrants.map(Self.value))
                ),
                (
                    key: "layoutFingerprint",
                    value: .bytes(expectedLayoutFingerprint.bytes)
                ),
                (key: "expectedRevision", value: .uint64(expectedRevision)),
            ]))
        } catch let error as PersistentJobPayloadError {
            throw error
        } catch {
            throw .invalidValue("Legacy migration plan is not canonical")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields.count == 6,
              fields["version"]?.uint8Value == Self.formatVersion,
              let baseValue = fields["baseID"]?.stringValue,
              let placementValue = fields["placementID"]?.stringValue,
              let grantValues = fields["initialGrants"]?.arrayValue,
              let fingerprint = fields["layoutFingerprint"]?.bytesValue,
              fingerprint.count == DatabaseLayoutFingerprint.byteCount,
              let expectedRevision = fields["expectedRevision"]?.uint64Value
        else {
            throw .invalidValue("Invalid legacy migration plan")
        }
        let decodedBaseID: Base.ID
        let decodedPlacementID: Base.Placement.ID
        let decodedGrants: [Security.Grant]
        let decodedFingerprint: DatabaseLayoutFingerprint
        do {
            decodedBaseID = try Base.ID(baseValue)
            decodedPlacementID = try Base.Placement.ID(placementValue)
            decodedGrants = try grantValues.map {
                try Self.grant($0, baseID: decodedBaseID)
            }
            decodedFingerprint = try DatabaseLayoutFingerprint(
                fingerprint
            )
        } catch let error as PersistentJobPayloadError {
            throw error
        } catch {
            throw .invalidValue("Invalid legacy migration identity")
        }
        guard expectedRevision == 0,
              !decodedGrants.isEmpty,
              decodedGrants.contains(where: {
                  $0.resource == .base(decodedBaseID)
                      && $0.access.contains(.administer)
              }),
              decodedGrants.allSatisfy({
                  $0.resource == .base(decodedBaseID)
              })
        else {
            throw .invalidValue("Invalid legacy migration grants")
        }
        self.baseID = decodedBaseID
        self.placementID = decodedPlacementID
        self.initialGrants = decodedGrants
        self.expectedLayoutFingerprint = decodedFingerprint
        self.expectedRevision = expectedRevision
    }

    private static func value(_ grant: Security.Grant) throws -> FieldValue {
        let subjectKind: UInt8
        let subject: String
        switch grant.subject {
        case .principal(let value):
            subjectKind = 0
            subject = value
        case .principalRole(let value):
            subjectKind = 1
            subject = value
        }
        return .object(try FieldObject([
            (key: "subjectKind", value: .uint8(subjectKind)),
            (key: "subject", value: .string(subject)),
            (key: "access", value: .uint8(grant.access.rawValue)),
        ]))
    }

    private static func grant(
        _ value: FieldValue,
        baseID: Base.ID
    ) throws(PersistentJobPayloadError) -> Security.Grant {
        guard let fields = value.objectValue,
              fields.count == 3,
              let subjectKind = fields["subjectKind"]?.uint8Value,
              let subjectValue = fields["subject"]?.stringValue,
              !subjectValue.isEmpty,
              let accessValue = fields["access"]?.uint8Value else {
            throw .invalidValue("Invalid legacy migration Grant")
        }
        let subject: Security.Subject
        switch subjectKind {
        case 0: subject = .principal(subjectValue)
        case 1: subject = .principalRole(subjectValue)
        default:
            throw .invalidValue("Invalid legacy migration Grant subject")
        }
        let access = Security.Access(rawValue: accessValue)
        guard !access.isEmpty, access.containsOnlyKnownPermissions else {
            throw .invalidValue("Invalid legacy migration Grant access")
        }
        return Security.Grant(
            subject: subject,
            resource: .base(baseID),
            access: access
        )
    }
}

#endif
