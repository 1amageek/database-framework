import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire

/// Stable physical identity for one schema-owned index declaration.
public struct DatabaseIndexStorageIdentity: Sendable, Hashable {
    public let name: String
    public let definitionFingerprint: SchemaFingerprint
    public let layoutFingerprint: ByteString

    public init(
        name: String,
        definitionFingerprint: SchemaFingerprint,
        layoutFingerprint: ByteString
    ) throws(DatabaseIndexStorageIdentityError) {
        guard !name.isEmpty else {
            throw DatabaseIndexStorageIdentityError.indexNotDeclared(name)
        }
        guard layoutFingerprint.count == SHA256Accumulator.digestByteCount
        else {
            throw DatabaseIndexStorageIdentityError
                .physicalLayoutNotResolved(name)
        }
        self.name = name
        self.definitionFingerprint = definitionFingerprint
        self.layoutFingerprint = layoutFingerprint
    }

    package static func resolve(
        named name: String,
        in schema: Schema,
        physicalLayout: IndexPhysicalLayout
    ) throws -> DatabaseIndexStorageIdentity {
        try DatabaseIndexStorageIdentity(
            name: name,
            definitionFingerprint: try definitionFingerprint(
                named: name,
                in: schema
            ),
            layoutFingerprint: physicalLayout.fingerprint
        )
    }

    package static func definitionFingerprint(
        named name: String,
        in schema: Schema
    ) throws -> SchemaFingerprint {
        let concrete = schema.indexDescriptors.filter { $0.name == name }
        if concrete.count == 1, let descriptor = concrete.first {
            return try SchemaManifest.indexFingerprint(descriptor)
        }
        guard concrete.isEmpty else {
            throw DatabaseIndexStorageIdentityError.ambiguousIndexName(name)
        }
        let groups = schema.polymorphicGroups.filter { group in
            group.indexes.contains { $0.name == name }
        }
        guard groups.count == 1, let group = groups.first,
              let declaration = group.indexes.first(where: { $0.name == name })
        else {
            throw groups.isEmpty
                ? DatabaseIndexStorageIdentityError.indexNotDeclared(name)
                : DatabaseIndexStorageIdentityError.ambiguousIndexName(name)
        }
        let memberDescriptors = group.memberTypeNames.compactMap {
            memberTypeName in
            schema.polymorphicIndexDescriptors(
                identifier: group.identifier,
                memberTypeName: memberTypeName
            ).first { $0.name == name }
        }
        guard memberDescriptors.count == group.memberTypeNames.count else {
            throw DatabaseIndexStorageIdentityError
                .incompletePolymorphicProjection(
                    group: group.identifier,
                    index: name
                )
        }
        return try SchemaManifest.polymorphicIndexFingerprint(
            groupIdentifier: group.identifier,
            declaration: declaration,
            memberDescriptors: memberDescriptors
        )
    }
}

public enum DatabaseIndexStorageIdentityError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    case indexNotDeclared(String)
    case ambiguousIndexName(String)
    case incompletePolymorphicProjection(group: String, index: String)
    case physicalLayoutNotResolved(String)

    public var description: String {
        switch self {
        case .indexNotDeclared(let name):
            return "Index '\(name)' is not declared in the schema"
        case .ambiguousIndexName(let name):
            return "Index name '\(name)' resolves to more than one declaration"
        case .incompletePolymorphicProjection(let group, let index):
            return "Polymorphic index '\(index)' does not project every member of group '\(group)'"
        case .physicalLayoutNotResolved(let name):
            return "Index '\(name)' has no resolved physical layout"
        }
    }
}
