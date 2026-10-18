import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire

/// Validated index execution contract for one immutable schema generation.
@_spi(DatabaseExecution)
public struct DatabasePreparedSchemaGeneration: Sendable {
    public let indexPhysicalLayouts: [String: IndexPhysicalLayout]
    public let indexPhysicalFingerprint: ByteString
    public let executionRuntimeFingerprint: ByteString

    init(
        schemaFingerprint: SchemaFingerprint,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        securityConfiguration: SecurityConfiguration,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        sqlCatalogDigest: ByteString,
        sqlIndexIdentities: [DatabaseIndexStorageIdentity] = []
    ) throws {
        guard sqlCatalogDigest.count == SHA256Accumulator.digestByteCount else {
            throw DatabaseSchemaPublicationError.corruptedState("SQL catalog digest has an invalid length")
        }
        var policies: [(name: String, options: FieldObject)] = []
        policies.reserveCapacity(
            runtimeConfiguration.indexConfigurations.count
        )
        for configuration in runtimeConfiguration.indexConfigurations
        where
            indexPhysicalLayouts[configuration.indexName] != nil
        {
            policies.append(
                (
                    name: configuration.indexName,
                    options: try configuration.executionOptions
                ))
        }
        policies.sort {
            if $0.name != $1.name {
                return $0.name < $1.name
            }
            return $0.options < $1.options
        }

        var physicalAccumulator = SHA256Accumulator()
        try DatabaseWireWriter.emit(
            consume: { physicalAccumulator.update($0) }
        ) { writer throws(DatabaseWireError) in
            try writer.writeString(
                "database-framework.index-physical-generation"
            )
            try writer.writeBytes(schemaFingerprint.bytes)
            let names = indexPhysicalLayouts.keys.sorted()
            try writer.writeCount(names.count)
            for name in names {
                guard let layout = indexPhysicalLayouts[name] else {
                    throw DatabaseWireError.invalidSchemaManifest(
                        "Physical index layout is missing"
                    )
                }
                try writer.writeString(name)
                try writer.writeBytes(layout.fingerprint)
            }
            if !sqlIndexIdentities.isEmpty {
                try writer.writeString("sql-index-generations")
                let identities = sqlIndexIdentities.sorted { $0.name < $1.name }
                try writer.writeCount(identities.count)
                for identity in identities {
                    try writer.writeString(identity.name)
                    try writer.writeBytes(identity.definitionFingerprint.bytes)
                    try writer.writeBytes(identity.layoutFingerprint)
                }
            }
        }
        let indexPhysicalFingerprint = physicalAccumulator.finalize()

        var runtimeAccumulator = SHA256Accumulator()
        try DatabaseWireWriter.emit(
            consume: { runtimeAccumulator.update($0) }
        ) { writer throws(DatabaseWireError) in
            try writer.writeString(
                "database-framework.execution-runtime-generation"
            )
            try writer.writeBytes(indexPhysicalFingerprint)
            try writer.writeBytes(sqlCatalogDigest)
            try writer.writeString(
                runtimeConfiguration.executionIdentity.identifier
            )
            writer.writeUInt64(
                runtimeConfiguration.executionIdentity.revision
            )
            try writer.writeString(
                securityConfiguration.executionIdentityComponent
            )
            try writer.writeCount(policies.count)
            for policy in policies {
                try writer.writeString(policy.name)
                try policy.options.encode(into: &writer)
            }
        }

        self.indexPhysicalLayouts = indexPhysicalLayouts
        self.indexPhysicalFingerprint = indexPhysicalFingerprint
        self.executionRuntimeFingerprint = runtimeAccumulator.finalize()
    }
}
