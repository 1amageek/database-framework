import DatabaseEngine
import DatabaseWire
import QueryIR
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

    private let codec: PropertyGraphDefinitionCatalogCodec

    public init(
        rootSubspace: Subspace = Self.defaultRootSubspace,
        definitionLimits: DatabaseWireLimits = .default
    ) {
        self.codec = PropertyGraphDefinitionCatalogCodec(
            subspace: rootSubspace.subspace(Int64(1)),
            definitionLimits: definitionLimits
        )
    }

    public func definition(
        named graphName: String,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> CreateGraphStatement? {
        let key = try codec.key(for: graphName)
        try workMeter.consume(at: .storageRow)
        guard let value = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return nil
        }
        return try codec.decode(value, expectedGraphName: graphName)
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
        let key = try codec.key(for: definition.graphName)
        let encodedDefinition = try codec.encode(canonicalDefinition)

        try workMeter.consume(at: .storageRow)
        if let existingValue = try await transaction.getValue(
            for: key,
            snapshot: false
        ) {
            _ = try codec.decode(
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
        let key = try codec.key(for: graphName)
        try workMeter.consume(at: .storageRow)
        guard let existingValue = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            throw PropertyGraphDefinitionCatalogError.graphNotFound(graphName)
        }
        _ = try codec.decode(existingValue, expectedGraphName: graphName)

        try workMeter.consume(at: .storageWrite)
        try transaction.clear(key: key)
    }
}
