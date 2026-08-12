import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseOperationTargetKinds: OptionSet, Sendable, Hashable {
    public let rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    public static let database = Self(rawValue: 1 << 0)
    public static let base = Self(rawValue: 1 << 1)
    public static let composition = Self(rawValue: 1 << 2)

    public func accepts(_ target: DatabaseOperationTarget) -> Bool {
        switch target {
        case .database:
            return contains(.database)
        case .base:
            return contains(.base)
        case .composition:
            return contains(.composition)
        }
    }
}

public enum DatabaseOperationTransactionKind: Sendable, Hashable {
    case none
    case read
    case write
}

public enum DatabaseBaseAdmissionKind: Sendable, Hashable {
    case activeData
    case administration
    case lifecycleJob
}

public struct DatabaseOperationRequirement: Sendable, Hashable {
    public let acceptedTargets: DatabaseOperationTargetKinds
    public let access: Security.Access
    public let transaction: DatabaseOperationTransactionKind
    public let baseAdmission: DatabaseBaseAdmissionKind
    public let permitsMigrationRequiredLayout: Bool

    public init(
        acceptedTargets: DatabaseOperationTargetKinds,
        access: Security.Access,
        transaction: DatabaseOperationTransactionKind,
        baseAdmission: DatabaseBaseAdmissionKind = .activeData,
        permitsMigrationRequiredLayout: Bool = false
    ) {
        self.acceptedTargets = acceptedTargets
        self.access = access
        self.transaction = transaction
        self.baseAdmission = baseAdmission
        self.permitsMigrationRequiredLayout = permitsMigrationRequiredLayout
    }

    package static func canonical(
        for identifier: DatabaseOperationIdentifier
    ) -> Self {
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        let grantAndJobTargets: DatabaseOperationTargetKinds = [
            .database,
            .base,
        ]
        let queryTargets: DatabaseOperationTargetKinds = [
            .base,
            .composition,
        ]
        let dataTargets: DatabaseOperationTargetKinds = .base
        #else
        let grantAndJobTargets: DatabaseOperationTargetKinds = .database
        let queryTargets: DatabaseOperationTargetKinds = .database
        let dataTargets: DatabaseOperationTargetKinds = .database
        #endif
        switch identifier {
        case .capabilitiesDescribe, .schemaDescribe:
            return Self(
                acceptedTargets: .database,
                access: .read,
                transaction: .read,
                permitsMigrationRequiredLayout: true
            )
        case .schemaExecute, .baseExecute, .compositionExecute:
            return Self(
                acceptedTargets: .database,
                access: .administer,
                transaction: .write
            )
        case .grantExecute:
            return Self(
                acceptedTargets: grantAndJobTargets,
                access: .administer,
                transaction: .write,
                baseAdmission: .administration
            )
        case .queryExecute:
            return Self(
                acceptedTargets: queryTargets,
                access: .read,
                transaction: .read
            )
        case .mutationExecute:
            return Self(
                acceptedTargets: dataTargets,
                access: .write,
                transaction: .write
            )
        case .graphAlgorithm:
            return Self(
                acceptedTargets: dataTargets,
                access: .read,
                transaction: .read
            )
        case .ontologyExecute, .shaclExecute, .commandExecute,
             .maintenanceExecute:
            return Self(
                acceptedTargets: dataTargets,
                access: .administer,
                transaction: .write
            )
        case .jobStart:
            return Self(
                acceptedTargets: grantAndJobTargets,
                access: .administer,
                transaction: .write,
                baseAdmission: .administration
            )
        case .jobCancel:
            return Self(
                acceptedTargets: grantAndJobTargets,
                access: .administer,
                transaction: .write,
                baseAdmission: .lifecycleJob,
                permitsMigrationRequiredLayout: true
            )
        case .jobStatus, .jobResult:
            return Self(
                acceptedTargets: grantAndJobTargets,
                access: .administer,
                transaction: .read,
                baseAdmission: .lifecycleJob,
                permitsMigrationRequiredLayout: true
            )
        }
    }
}
