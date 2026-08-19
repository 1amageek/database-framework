import DatabaseKit
import DatabaseTypes

/// Framework-owned physical work derived from a schema generation change.
@_spi(DatabaseExecution)
public struct DatabaseIndexTransitionPlan: Sendable, Hashable {
    public struct Target: Sendable, Hashable {
        public let scope: DatabaseIndexStorageScope
        public let identity: DatabaseIndexStorageIdentity

        public init(
            scope: DatabaseIndexStorageScope,
            identity: DatabaseIndexStorageIdentity
        ) throws(DatabaseIndexStorageScopeError) {
            try scope.validate()
            self.scope = scope
            self.identity = identity
        }
    }

    public let builds: [Target]
    public let retirements: [Target]

    init(
        currentSchema: Schema,
        currentLayoutFingerprints: [String: ByteString],
        targetSchema: Schema,
        targetPhysicalLayouts: [String: IndexPhysicalLayout]
    ) throws {
        let current = try Self.targets(
            schema: currentSchema,
            layoutFingerprints: currentLayoutFingerprints
        )
        let target = try Self.targets(
            schema: targetSchema,
            layoutFingerprints: targetPhysicalLayouts.mapValues({
                $0.fingerprint
            })
        )
        let targetByScope = Dictionary(
            uniqueKeysWithValues: target.map { (TargetKey($0), $0) }
        )
        let currentByScope = Dictionary(
            uniqueKeysWithValues: current.map { (TargetKey($0), $0) }
        )
        self.builds = target.filter { candidate in
            currentByScope[TargetKey(candidate)]?.identity
                != candidate.identity
        }.sorted(by: Target.stableLessThan)
        self.retirements = current.filter { candidate in
            targetByScope[TargetKey(candidate)]?.identity
                != candidate.identity
        }.sorted(by: Target.stableLessThan)
    }

    private static func targets(
        schema: Schema,
        layoutFingerprints: [String: ByteString]
    ) throws -> [Target] {
        var result: [Target] = []
        result.reserveCapacity(
            schema.indexDescriptors.count
                + schema.polymorphicGroups.reduce(0) {
                    $0 + $1.indexes.count
                }
        )
        for entity in schema.entities {
            for descriptor in entity.indexDescriptors {
                result.append(try Target(
                    scope: .entity(
                        name: entity.name,
                        directoryComponents: entity.directoryComponents
                    ),
                    identity: identity(
                        named: descriptor.name,
                        schema: schema,
                        layoutFingerprints: layoutFingerprints
                    )
                ))
            }
        }
        for group in schema.polymorphicGroups {
            for declaration in group.indexes {
                result.append(try Target(
                    scope: .polymorphicGroup(
                        identifier: group.identifier,
                        directoryPath: try group.resolvedDirectoryPath()
                    ),
                    identity: identity(
                        named: declaration.name,
                        schema: schema,
                        layoutFingerprints: layoutFingerprints
                    )
                ))
            }
        }
        guard Set(result.map(TargetKey.init)).count == result.count else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "index transition contains duplicate scoped identities"
            )
        }
        return result
    }

    private static func identity(
        named name: String,
        schema: Schema,
        layoutFingerprints: [String: ByteString]
    ) throws -> DatabaseIndexStorageIdentity {
        guard let layoutFingerprint = layoutFingerprints[name] else {
            throw DatabaseIndexStorageIdentityError
                .physicalLayoutNotResolved(name)
        }
        return try DatabaseIndexStorageIdentity(
            name: name,
            definitionFingerprint: DatabaseIndexStorageIdentity
                .definitionFingerprint(named: name, in: schema),
            layoutFingerprint: layoutFingerprint
        )
    }

    private struct TargetKey: Hashable {
        let scope: DatabaseIndexStorageScope
        let index: String

        init(_ target: Target) {
            scope = target.scope
            index = target.identity.name
        }
    }
}

extension DatabaseIndexTransitionPlan.Target {
    public static func stableLessThan(
        _ lhs: Self,
        _ rhs: Self
    ) -> Bool {
        (
            lhs.scope.stableOrderingKey,
            lhs.identity.name,
            lhs.identity.definitionFingerprint.bytes,
            lhs.identity.layoutFingerprint
        ) < (
            rhs.scope.stableOrderingKey,
            rhs.identity.name,
            rhs.identity.definitionFingerprint.bytes,
            rhs.identity.layoutFingerprint
        )
    }
}

extension DBContainer {
    /// Derives exact build and retirement identities from two immutable schema
    /// generations. The server coordinates the returned work without
    /// reinterpreting index definitions.
    @_spi(DatabaseExecution)
    public func planIndexTransition(
        to target: Schema,
        preparedGeneration: DatabasePreparedSchemaGeneration
    ) throws -> DatabaseIndexTransitionPlan {
        let current = acquirePublishedSchemaLease()
        return try DatabaseIndexTransitionPlan(
            currentSchema: current.schema,
            currentLayoutFingerprints: current.indexPhysicalLayouts.mapValues({
                $0.fingerprint
            }),
            targetSchema: target,
            targetPhysicalLayouts: preparedGeneration.indexPhysicalLayouts
        )
    }
}
