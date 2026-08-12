import DatabaseJobRuntime
#if DATABASE_ADMINISTRATION_OPERATIONS_ENABLED
import DatabaseKit
import DatabaseTypes

public struct DatabaseBaseLifecycleJobPlan:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    package enum Action: UInt8, Sendable, Hashable {
        case create = 0
        case retire = 1
        case activate = 2
        case delete = 3
        case move = 4
    }

    private static let formatVersion: UInt8 = 1

    package let action: Action
    package let baseID: Base.ID
    package let placementID: Base.Placement.ID?
    package let initialGrants: [Security.Grant]
    package let expectedRevision: UInt64

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        do {
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "action", value: .uint8(action.rawValue)),
                (key: "baseID", value: .string(baseID.value)),
                (
                    key: "placementID",
                    value: placementID.map { .string($0.value) } ?? .null
                ),
                (
                    key: "initialGrants",
                    value: .array(try initialGrants.map(Self.value))
                ),
                (key: "expectedRevision", value: .uint64(expectedRevision)),
            ]))
        } catch let error as PersistentJobPayloadError {
            throw error
        } catch {
            throw .invalidValue("Base lifecycle plan is not canonical")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields.count == 6,
              fields["version"]?.uint8Value == Self.formatVersion,
              let rawAction = fields["action"]?.uint8Value,
              let action = Action(rawValue: rawAction),
              let baseValue = fields["baseID"]?.stringValue,
              let placementValue = fields["placementID"],
              let grantValues = fields["initialGrants"]?.arrayValue,
              let expectedRevision = fields["expectedRevision"]?.uint64Value
        else {
            throw .invalidValue("Invalid Base lifecycle plan")
        }
        do {
            let baseID = try Base.ID(baseValue)
            self.baseID = baseID
            if placementValue.isNull {
                self.placementID = nil
            } else if let value = placementValue.stringValue {
                self.placementID = try Base.Placement.ID(value)
            } else {
                throw PersistentJobPayloadError.invalidValue(
                    "Invalid Base placement"
                )
            }
            self.initialGrants = try grantValues.map {
                try Self.grant($0, baseID: baseID)
            }
        } catch let error as PersistentJobPayloadError {
            throw error
        } catch {
            throw .invalidValue("Invalid Base lifecycle identity")
        }
        guard (action == .create || action == .move)
                == (placementID != nil),
              action != .create || !initialGrants.isEmpty,
              action == .create || initialGrants.isEmpty else {
            throw .invalidValue("Invalid Base lifecycle action payload")
        }
        self.action = action
        self.expectedRevision = expectedRevision
    }

    package init(
        action: Action,
        baseID: Base.ID,
        placementID: Base.Placement.ID? = nil,
        initialGrants: [Security.Grant] = [],
        expectedRevision: UInt64
    ) {
        self.action = action
        self.baseID = baseID
        self.placementID = placementID
        self.initialGrants = initialGrants
        self.expectedRevision = expectedRevision
    }

    private static func value(
        _ grant: Security.Grant
    ) throws -> FieldValue {
        let subjectKind: UInt8
        let subjectValue: String
        switch grant.subject {
        case .principal(let identifier):
            subjectKind = 0
            subjectValue = identifier
        case .principalRole(let role):
            subjectKind = 1
            subjectValue = role
        }
        return .object(try FieldObject([
            (key: "subjectKind", value: .uint8(subjectKind)),
            (key: "subject", value: .string(subjectValue)),
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
            throw .invalidValue("Invalid Base lifecycle Grant")
        }
        let subject: Security.Subject
        switch subjectKind {
        case 0:
            subject = .principal(subjectValue)
        case 1:
            subject = .principalRole(subjectValue)
        default:
            throw .invalidValue("Invalid Base lifecycle Grant subject")
        }
        let access = Security.Access(rawValue: accessValue)
        guard !access.isEmpty, access.containsOnlyKnownPermissions else {
            throw .invalidValue("Invalid Base lifecycle Grant access")
        }
        return Security.Grant(
            subject: subject,
            resource: .base(baseID),
            access: access
        )
    }
}

#endif
