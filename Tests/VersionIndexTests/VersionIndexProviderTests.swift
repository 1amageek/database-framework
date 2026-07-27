#if FOUNDATION_DB
import Testing
import TestHeartbeat
import DatabaseKit
import DatabaseTypes
import StorageKit
@testable import DatabaseEngine
@testable import VersionIndex

struct VersionIndexEntity: Persistable {
    typealias ID = String
    var id: String
    var title: String

    static var persistableType: String { "VersionIndexEntity" }
    static var allFields: [String] { ["id", "title"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "title": return title
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<VersionIndexEntity, Value>) -> String {
        switch keyPath {
        case \VersionIndexEntity.id: return "id"
        case \VersionIndexEntity.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<VersionIndexEntity>) -> String {
        switch keyPath {
        case \VersionIndexEntity.id: return "id"
        case \VersionIndexEntity.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<VersionIndexEntity> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

func versionIndexMetadata(
    strategy: VersionHistoryStrategy
) -> IndexKindMetadata {
    let strategyMetadata: [String: FieldValue]
    switch strategy {
    case .keepAll:
        strategyMetadata = ["strategy": .string("keepAll")]
    case .keepLast(let count):
        strategyMetadata = [
            "strategy": .string("keepLast"),
            "strategyCount": .int64(Int64(count)),
        ]
    case .keepForDuration(let duration):
        strategyMetadata = [
            "strategy": .string("keepForDuration"),
            "strategyDurationSeconds": .float64(duration),
        ]
    }

    return IndexKindMetadata(
        identifier: "version",
        subspaceStructure: .hierarchical,
        fields: [
            IndexFieldMetadata(
                identity: FieldIdentity(name: "id", number: 1)
            )
        ],
        metadata: strategyMetadata
    )
}

@Suite("Version index provider tests", .heartbeat)
struct VersionIndexProviderTests {
    @Test("Provider creates a maintainer from canonical version metadata")
    func providerCreatesMaintainer() throws {
        let metadata = versionIndexMetadata(strategy: .keepLast(3))
        let definition = try IndexDefinition(metadata: metadata)
        #expect(definition == .version(strategy: .keepLast(3)))

        let index = Index(
            name: "VersionIndexEntity_id",
            kind: metadata,
            rootExpression: FieldKeyExpression(fieldName: "id")
        )
        let maintainer: any IndexMaintainer<VersionIndexEntity> = try
            VersionIndexMaintainerProvider().makeIndexMaintainer(
                index: index,
                subspace: Subspace(),
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: []
            )
        #expect(maintainer is VersionIndexMaintainer<VersionIndexEntity>)
    }
}
#endif
