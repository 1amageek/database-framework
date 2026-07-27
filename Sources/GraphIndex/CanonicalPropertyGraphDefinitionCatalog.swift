import DatabaseEngine
import DatabaseKit
import StorageKit

/// Persistent database-wide SQL/PGQ property graph definition catalog.
public struct CanonicalPropertyGraphDefinitionCatalog:
    PropertyGraphDefinitionCatalog
{
    /// Database-wide v1 property graph definition namespace.
    ///
    /// This namespace is intentionally disjoint from the RDF graph store:
    /// SQL/PGQ definitions describe graph projections over relational sources,
    /// while RDF named graphs own RDF dataset contents.
    public static let defaultRootSubspace = Subspace(
        prefix: Tuple([
            "_database-framework",
            "property-graph-definition-catalog",
            Int64(1),
        ]).pack()
    )

    private let storage: PropertyGraphDefinitionCatalogStorage

    public init(
        rootSubspace: Subspace = Self.defaultRootSubspace,
        storageLimits: StorageFrameLimits = .default
    ) {
        self.storage = PropertyGraphDefinitionCatalogStorage(
            subspace: rootSubspace.subspace(Int64(1)),
            limits: storageLimits
        )
    }

    public func definition(
        named graphName: String,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> CreateGraphStatement? {
        let key = try storage.key(for: graphName)
        try workMeter.consume(at: .storageRow)
        guard let value = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return nil
        }
        return try storage.decode(value, expectedGraphName: graphName)
    }

    @discardableResult
    public func create(
        _ definition: CreateGraphStatement,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> PropertyGraphDefinitionCreation {
        let canonicalDefinition = CreateGraphStatement(
            graphName: definition.graphName,
            vertexTables: definition.vertexTables,
            edgeTables: definition.edgeTables
        )
        let key = try storage.key(for: definition.graphName)
        let encodedDefinition = try storage.encode(canonicalDefinition)

        try workMeter.consume(at: .storageRow)
        if let existingValue = try await transaction.getValue(
            for: key,
            snapshot: false
        ) {
            _ = try storage.decode(
                existingValue,
                expectedGraphName: definition.graphName
            )
            guard definition.ifNotExists else {
                throw PropertyGraphDefinitionCatalogError.graphAlreadyExists(
                    definition.graphName
                )
            }
            return .retainedExistingDefinition
        }

        try workMeter.consume(at: .storageWrite)
        try transaction.setValue(encodedDefinition, for: key)
        return .created
    }

    public func dropDefinition(
        named graphName: String,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws {
        let key = try storage.key(for: graphName)
        try workMeter.consume(at: .storageRow)
        guard let existingValue = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            throw PropertyGraphDefinitionCatalogError.graphNotFound(graphName)
        }
        _ = try storage.decode(existingValue, expectedGraphName: graphName)

        try workMeter.consume(at: .storageWrite)
        try transaction.clear(key: key)
    }
}
