import DatabaseEngine
import DatabaseValue
import DatabaseWire
import QueryIR
import StorageKit

/// Canonical physical codec for persisted SQL/PGQ property graph definitions.
package struct PropertyGraphDefinitionCatalogCodec: Sendable {
    private let subspace: Subspace
    private let definitionLimits: DatabaseWireLimits

    package init(
        subspace: Subspace,
        definitionLimits: DatabaseWireLimits
    ) {
        self.subspace = subspace
        self.definitionLimits = definitionLimits
    }

    package func key(
        for graphName: String
    ) throws(PropertyGraphDefinitionCatalogError) -> Bytes {
        guard !graphName.isEmpty else {
            throw .emptyGraphName
        }

        var tupleByteCount = 2
        for byte in graphName.utf8 {
            let width = byte == 0 ? 2 : 1
            let (next, overflow) = tupleByteCount.addingReportingOverflow(width)
            guard !overflow else {
                throw .keyTooLarge(
                    actual: Int.max,
                    maximum: databaseMaximumKeySize
                )
            }
            tupleByteCount = next
        }

        let (keyByteCount, overflow) = subspace.prefix.count
            .addingReportingOverflow(tupleByteCount)
        guard !overflow, keyByteCount <= databaseMaximumKeySize else {
            throw .keyTooLarge(
                actual: overflow ? Int.max : keyByteCount,
                maximum: databaseMaximumKeySize
            )
        }

        let key = subspace.pack(
            encodedTupleByteCount: tupleByteCount
        ) { sink in
            graphName.encodeTuple(to: &sink)
        }
        assert(key.count == keyByteCount)
        return key
    }

    package func encode(
        _ definition: CreateGraphStatement
    ) throws(PropertyGraphDefinitionCatalogError) -> Bytes {
        let encoded: DatabaseBytes
        do {
            encoded = try QueryIRWireCodec.encode(
                .createGraph(definition),
                limits: definitionLimits
            )
        } catch let error {
            throw .definitionCannotBeRepresented(error)
        }
        guard encoded.count <= databaseMaximumValueSize else {
            throw .definitionTooLarge(
                actual: encoded.count,
                maximum: databaseMaximumValueSize
            )
        }
        return Bytes(retaining: encoded)
    }

    package func decode(
        _ value: Bytes,
        expectedGraphName: String
    ) throws(PropertyGraphDefinitionCatalogError) -> CreateGraphStatement {
        guard value.count <= databaseMaximumValueSize else {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .valueTooLarge(
                    actual: value.count,
                    maximum: databaseMaximumValueSize
                )
            )
        }

        let encoded = DatabaseBytes(retaining: value)
        let statement: QueryStatement
        do {
            statement = try QueryIRWireCodec.decode(
                encoded,
                limits: definitionLimits
            )
        } catch let error {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .decodingFailed(error)
            )
        }

        guard case .createGraph(let definition) = statement else {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .unexpectedStatement
            )
        }
        guard definition.graphName == expectedGraphName else {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .graphNameMismatch(actual: definition.graphName)
            )
        }
        guard !definition.ifNotExists else {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .containsCreationCondition
            )
        }

        let canonicalEncoding: DatabaseBytes
        do {
            canonicalEncoding = try QueryIRWireCodec.encode(
                statement,
                limits: definitionLimits
            )
        } catch let error {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .canonicalizationFailed(error)
            )
        }
        guard canonicalEncoding == encoded else {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .nonCanonicalEncoding
            )
        }
        return definition
    }

    private func invalidStoredDefinition(
        named graphName: String,
        violation: PropertyGraphDefinitionCatalogError.StoredDefinitionViolation
    ) -> PropertyGraphDefinitionCatalogError {
        .invalidStoredDefinition(
            graphName: graphName,
            violation: violation
        )
    }
}
