// IndexHandlerRegistry.swift
// fdb-cli - Centralized registry for IndexHandler creation
//
// Eliminates code duplication across DataCommands, AdminCommands, and FindCommands.

import Foundation

/// Central registry for IndexHandler creation
///
/// **Design Principles**:
/// - Single Source of Truth: All IndexHandler creation logic is centralized here
/// - Compile-time Safety: Enum exhaustiveness checking is maintained
/// - Testability: Factory functions can be validated
///
/// **Usage**:
/// ```swift
/// // Create a single handler
/// let handler = try IndexHandlerRegistry.createHandler(
///     for: indexDef.kind,
///     definition: indexDef,
///     schemaName: "User"
/// )
///
/// // Create handlers for all indexes in a schema
/// let handlers = IndexHandlerRegistry.createHandlers(for: schema)
/// ```
public enum IndexHandlerRegistry {

    /// Type definition for IndexHandler factory
    public typealias HandlerFactory = @Sendable (IndexDefinition, String) -> any IndexHandler

    /// Factories for all index kinds
    ///
    /// **Compile-time Safety**:
    /// Gaps can be detected by comparing with IndexKind.allCases
    private static let factories: [IndexKind: HandlerFactory] = [
        .scalar: { ScalarIndexHandler(indexDefinition: $0, schemaName: $1) },
        .bitmap: { BitmapIndexHandler(indexDefinition: $0, schemaName: $1) },
        .rank: { RankIndexHandler(indexDefinition: $0, schemaName: $1) },
        .vector: { VectorIndexHandler(indexDefinition: $0, schemaName: $1) },
        .fulltext: { FullTextIndexHandler(indexDefinition: $0, schemaName: $1) },
        .spatial: { SpatialIndexHandler(indexDefinition: $0, schemaName: $1) },
        .graph: { GraphIndexHandler(indexDefinition: $0, schemaName: $1) },
        .aggregation: { AggregationIndexHandler(indexDefinition: $0, schemaName: $1) },
        .version: { VersionIndexHandler(indexDefinition: $0, schemaName: $1) },
        .leaderboard: { LeaderboardIndexHandler(indexDefinition: $0, schemaName: $1) },
        .relationship: { RelationshipIndexHandler(indexDefinition: $0, schemaName: $1) },
        .permuted: { PermutedIndexHandler(indexDefinition: $0, schemaName: $1) },
    ]

    // MARK: - Public API

    /// Create a single IndexHandler
    ///
    /// - Parameters:
    ///   - kind: Index kind
    ///   - definition: Index definition
    ///   - schemaName: Schema name
    /// - Returns: The corresponding IndexHandler
    /// - Throws: IndexHandlerError.unregisteredKind if the kind is not registered
    public static func createHandler(
        for kind: IndexKind,
        definition: IndexDefinition,
        schemaName: String
    ) throws -> any IndexHandler {
        guard let factory = factories[kind] else {
            throw IndexHandlerError.unregisteredKind(kind)
        }
        return factory(definition, schemaName)
    }

    /// Create IndexHandlers for all indexes in a schema
    ///
    /// - Parameters:
    ///   - schema: Dynamic schema
    /// - Returns: Array of IndexHandlers (failed creations are skipped)
    public static func createHandlers(for schema: DynamicSchema) -> [any IndexHandler] {
        schema.indexes.compactMap { indexDef in
            try? createHandler(
                for: indexDef.kind,
                definition: indexDef,
                schemaName: schema.name
            )
        }
    }

    /// Get all registered index kinds
    public static var registeredKinds: Set<IndexKind> {
        Set(factories.keys)
    }

    /// Validate that all IndexKind cases are registered
    ///
    /// Use in tests to detect missing registrations when new IndexKind cases are added
    public static func validateAllKindsRegistered() -> [IndexKind] {
        IndexKind.allCases.filter { !factories.keys.contains($0) }
    }
}

// MARK: - Errors

public enum IndexHandlerError: Error, CustomStringConvertible {
    case unregisteredKind(IndexKind)

    public var description: String {
        switch self {
        case .unregisteredKind(let kind):
            return "IndexHandlerRegistry: unregistered index kind '\(kind.rawValue)'"
        }
    }
}
