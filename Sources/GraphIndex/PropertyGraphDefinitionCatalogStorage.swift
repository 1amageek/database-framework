import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

/// Owns catalog keys and persisted values for SQL/PGQ graph definitions.
package struct PropertyGraphDefinitionCatalogStorage: Sendable {
    private let subspace: Subspace
    private let limits: StorageFrameLimits

    package init(
        subspace: Subspace,
        limits: StorageFrameLimits
    ) {
        self.subspace = subspace
        self.limits = limits
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
        let encoded: ByteString
        do {
            encoded = try PropertyGraphDefinitionStorageFormat.encode(
                definition,
                limits: limits
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

        let encoded = ByteString(retaining: value)
        let definition: CreateGraphStatement
        do {
            definition = try PropertyGraphDefinitionStorageFormat.decode(
                encoded,
                limits: limits
            )
        } catch let error {
            throw invalidStoredDefinition(
                named: expectedGraphName,
                violation: .decodingFailed(error)
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

        let canonicalEncoding: ByteString
        do {
            canonicalEncoding = try PropertyGraphDefinitionStorageFormat
                .encode(
                    definition,
                    limits: limits
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
