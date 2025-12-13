// EntityIndexBuilder.swift
// FDBIndexing - Entity-specific index building support
//
// Provides runtime type dispatch for OnlineIndexer instantiation.

import FoundationDB
import Core
import Synchronization

/// Protocol for Persistable types that support runtime index building
///
/// This protocol enables OnlineIndexer to be instantiated with a concrete type
/// at runtime. It captures the concrete type in a closure during Schema creation.
///
/// **Design**:
/// - All Codable Persistable types automatically conform via protocol extension
/// - The static method captures the concrete Self type
/// - MigrationContext uses this to build indexes during migrations
public protocol IndexBuildableEntity: Persistable {
    /// Build index entries for all records of this entity type
    ///
    /// This method creates an OnlineIndexer with the concrete type and builds the index.
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - storeSubspace: Store root subspace (parent of R/I/B/M)
    ///   - index: Index definition
    ///   - indexStateManager: Index state manager
    ///   - batchSize: Number of items per batch
    ///   - configurations: Index configurations for runtime parameters (HNSW, full-text, etc.)
    /// - Throws: Error if index building fails
    static func buildEntityIndex(
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int,
        configurations: [any IndexConfiguration]
    ) async throws
}

// MARK: - Default Implementation for Codable Persistable

extension Persistable where Self: Codable {
    /// Build index entries for all records of this entity type
    ///
    /// This implementation creates an OnlineIndexer with the concrete type.
    public static func buildEntityIndex(
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int,
        configurations: [any IndexConfiguration]
    ) async throws {
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)

        // Create IndexMaintainer based on IndexKind (passing configurations)
        let indexMaintainer = try createIndexMaintainer(
            for: index,
            indexSubspace: indexSubspace,
            configurations: configurations
        )

        // Create and run OnlineIndexer
        let indexer = OnlineIndexer<Self>(
            database: database,
            storeSubspace: storeSubspace,
            itemType: Self.persistableType,
            index: index,
            indexMaintainer: indexMaintainer,
            indexStateManager: indexStateManager,
            batchSize: batchSize
        )

        try await indexer.buildIndex(clearFirst: false)
    }

    /// Create appropriate IndexMaintainer for the index kind
    ///
    /// Uses `IndexKindMaintainable` protocol to bridge IndexKind (metadata)
    /// with IndexMaintainer (runtime). This allows third-party IndexKinds
    /// to provide their own maintainer implementations.
    ///
    /// **Design**: IndexMaintainer implementors are responsible for adding
    /// `IndexKindMaintainable` conformance to their corresponding IndexKind.
    private static func createIndexMaintainer(
        for index: Index,
        indexSubspace: Subspace,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Self> {
        // Default id expression (assumes "id" field)
        let idExpression = FieldKeyExpression(fieldName: "id")

        // Use IndexKindMaintainable protocol to create maintainer
        // This allows third-party IndexKinds to provide their own implementations
        if let maintainable = index.kind as? any IndexKindMaintainable {
            return maintainable.makeIndexMaintainer(
                index: index,
                subspace: indexSubspace,
                idExpression: idExpression,
                configurations: configurations
            )
        } else {
            let kindIdentifier = type(of: index.kind).identifier
            throw EntityIndexBuilderError.indexKindNotMaintainable(
                kindIdentifier: kindIdentifier,
                indexName: index.name
            )
        }
    }
}

// MARK: - IndexBuilderRegistry

/// Registry for index builder closures that capture concrete types
///
/// This registry stores closures created at Schema initialization time that
/// can build indexes for specific entity types. Each closure captures its
/// concrete Persistable type, allowing OnlineIndexer to be instantiated
/// with the correct type at runtime.
///
/// **Design**: Uses `final class: Sendable` + `Mutex` pattern for high throughput
/// (not actor, which would serialize execution and reduce throughput).
///
/// **Usage**:
/// ```swift
/// // At Schema creation (captures concrete type)
/// IndexBuilderRegistry.shared.register(User.self)
///
/// // At migration time (uses captured type)
/// try await IndexBuilderRegistry.shared.buildIndex(
///     entityName: "User",
///     database: database,
///     configurations: container.indexConfigurations.flatMap(\.value),
///     ...
/// )
/// ```
public final class IndexBuilderRegistry: Sendable {
    /// Shared instance
    public static let shared = IndexBuilderRegistry()

    /// Type alias for index builder closure
    public typealias IndexBuilder = @Sendable (
        _ database: any DatabaseProtocol,
        _ storeSubspace: Subspace,
        _ index: Index,
        _ indexStateManager: IndexStateManager,
        _ batchSize: Int,
        _ configurations: [any IndexConfiguration]
    ) async throws -> Void

    /// Mutable state protected by Mutex
    private struct State: Sendable {
        var builders: [String: IndexBuilder] = [:]
    }

    /// State protected by Mutex (Synchronization module)
    private let state: Mutex<State>

    private init() {
        self.state = Mutex(State())
    }

    /// Register a Persistable type for index building
    ///
    /// This method captures the concrete type in a closure that can be called
    /// at runtime to build indexes.
    ///
    /// - Parameter type: The Persistable type to register
    public func register<T: Persistable & Codable>(_ type: T.Type) {
        state.withLock { state in
            state.builders[T.persistableType] = { database, storeSubspace, index, stateManager, batchSize, configurations in
                try await T.buildEntityIndex(
                    database: database,
                    storeSubspace: storeSubspace,
                    index: index,
                    indexStateManager: stateManager,
                    batchSize: batchSize,
                    configurations: configurations
                )
            }
        }
    }

    /// Build index for an entity by name
    ///
    /// - Parameters:
    ///   - entityName: The entity type name
    ///   - database: Database instance
    ///   - storeSubspace: Store root subspace (parent of R/I/B/M)
    ///   - index: Index definition
    ///   - indexStateManager: Index state manager
    ///   - batchSize: Number of items per batch
    ///   - configurations: Index configurations for runtime parameters
    /// - Throws: Error if entity not registered or build fails
    public func buildIndex(
        entityName: String,
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int,
        configurations: [any IndexConfiguration]
    ) async throws {
        // Get builder synchronously (Mutex lock scope is minimal)
        let builder = try state.withLock { state -> IndexBuilder in
            guard let builder = state.builders[entityName] else {
                throw EntityIndexBuilderError.entityNotRegistered(entityName: entityName)
            }
            return builder
        }

        // Execute builder outside of lock scope (I/O should not hold lock)
        try await builder(database, storeSubspace, index, indexStateManager, batchSize, configurations)
    }

    /// Check if an entity is registered
    public func isRegistered(_ entityName: String) -> Bool {
        state.withLock { state in
            state.builders[entityName] != nil
        }
    }

    /// Clear all registrations (for testing)
    public func clearAll() {
        state.withLock { state in
            state.builders.removeAll()
        }
    }
}

// MARK: - EntityIndexBuilder Helper

/// Helper namespace for runtime index building
///
/// Provides static methods to build indexes using:
/// 1. The IndexBuilderRegistry (if entity is registered)
/// 2. Direct type dispatch (if type is known at compile time)
/// 3. Existential type dispatch via `_EntityIndexBuildable` (for Schema.Entity.persistableType)
public struct EntityIndexBuilder {
    /// Build index for an entity using the registry
    ///
    /// - Parameters:
    ///   - entityName: The entity type name
    ///   - database: Database instance
    ///   - storeSubspace: Store root subspace (parent of R/I/B/M)
    ///   - index: Index definition
    ///   - indexStateManager: Index state manager
    ///   - batchSize: Number of items per batch
    ///   - configurations: Index configurations for runtime parameters
    /// - Throws: Error if entity not registered or build fails
    public static func buildIndex(
        entityName: String,
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int = 100,
        configurations: [any IndexConfiguration] = []
    ) async throws {
        try await IndexBuilderRegistry.shared.buildIndex(
            entityName: entityName,
            database: database,
            storeSubspace: storeSubspace,
            index: index,
            indexStateManager: indexStateManager,
            batchSize: batchSize,
            configurations: configurations
        )
    }

    /// Build index with direct type (for compile-time known types)
    ///
    /// - Parameters:
    ///   - type: The concrete Persistable type
    ///   - database: Database instance
    ///   - storeSubspace: Store root subspace (parent of R/I/B/M)
    ///   - index: Index definition
    ///   - indexStateManager: Index state manager
    ///   - batchSize: Number of items per batch
    ///   - configurations: Index configurations for runtime parameters
    /// - Throws: Error if build fails
    public static func buildIndex<T: Persistable & Codable>(
        for type: T.Type,
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int = 100,
        configurations: [any IndexConfiguration] = []
    ) async throws {
        try await T.buildEntityIndex(
            database: database,
            storeSubspace: storeSubspace,
            index: index,
            indexStateManager: indexStateManager,
            batchSize: batchSize,
            configurations: configurations
        )
    }

    /// Build index using existential type dispatch
    ///
    /// This method enables building indexes for types stored as `any Persistable.Type`
    /// (e.g., in Schema.Entity.persistableType). It uses multiple fallback strategies:
    ///
    /// 1. **IndexBuilderRegistry** (preferred): If the type was registered via
    ///    `IndexBuilderRegistry.shared.register(Type.self)`, use the registered closure.
    ///    This is the most reliable approach as it captures the concrete type at registration.
    ///
    /// 2. **_EntityIndexBuildable protocol**: Fall back to protocol-based dispatch.
    ///    Note: This may not work for all types due to Swift's protocol extension limitations.
    ///
    /// - Parameters:
    ///   - persistableType: The Persistable metatype (from Schema.Entity.persistableType)
    ///   - database: Database instance
    ///   - storeSubspace: Store root subspace (parent of R/I/B/M)
    ///   - index: Index definition
    ///   - indexStateManager: Index state manager
    ///   - batchSize: Number of items per batch
    ///   - configurations: Index configurations for runtime parameters
    /// - Throws: `EntityIndexBuilderError.typeNotBuildable` if the type doesn't support index building
    public static func buildIndex(
        forPersistableType persistableType: any Persistable.Type,
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int = 100,
        configurations: [any IndexConfiguration] = []
    ) async throws {
        let entityName = persistableType.persistableType

        // Strategy 1: Try IndexBuilderRegistry first (most reliable)
        if IndexBuilderRegistry.shared.isRegistered(entityName) {
            try await IndexBuilderRegistry.shared.buildIndex(
                entityName: entityName,
                database: database,
                storeSubspace: storeSubspace,
                index: index,
                indexStateManager: indexStateManager,
                batchSize: batchSize,
                configurations: configurations
            )
            return
        }

        // Strategy 2: Fall back to _EntityIndexBuildable protocol dispatch
        if let buildableType = persistableType as? any _EntityIndexBuildable.Type {
            try await buildableType._buildIndex(
                database: database,
                storeSubspace: storeSubspace,
                index: index,
                indexStateManager: indexStateManager,
                batchSize: batchSize,
                configurations: configurations
            )
            return
        }

        // Neither strategy worked
        throw EntityIndexBuilderError.typeNotBuildable(
            typeName: entityName,
            reason: "Type is not registered in IndexBuilderRegistry and does not conform to _EntityIndexBuildable. " +
                    "Ensure the type is Codable and was created via Schema([Type.self]) or manually registered."
        )
    }
}

// MARK: - _EntityIndexBuildable Protocol

/// Internal protocol for existential type dispatch in index building
///
/// This protocol enables calling `buildEntityIndex` through an existential type
/// (`any Persistable.Type`). All `Persistable & Codable` types automatically
/// conform via the protocol extension below.
///
/// **Why this is needed**:
/// Swift's existential dispatch doesn't call specialized protocol extensions.
/// When you have `type: any Persistable.Type` and call `type.buildEntityIndex()`,
/// Swift uses the default implementation, not the specialized `where Self: Codable` one.
///
/// This protocol provides a workaround:
/// 1. Define `_EntityIndexBuildable` with `_buildIndex` method
/// 2. Make `Persistable & Codable` types conform automatically
/// 3. Cast `any Persistable.Type` to `any _EntityIndexBuildable.Type` to check conformance
/// 4. Call `_buildIndex` through the existential, which dispatches to the concrete implementation
public protocol _EntityIndexBuildable: Persistable {
    /// Build index entries for this entity type
    ///
    /// This is the existential-callable version of `buildEntityIndex`.
    static func _buildIndex(
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int,
        configurations: [any IndexConfiguration]
    ) async throws
}

/// Automatic conformance for all Codable Persistable types
///
/// This extension makes all `Persistable & Codable` types conform to
/// `_EntityIndexBuildable`, enabling existential type dispatch for index building.
extension Persistable where Self: Codable {
    public static func _buildIndex(
        database: any DatabaseProtocol,
        storeSubspace: Subspace,
        index: Index,
        indexStateManager: IndexStateManager,
        batchSize: Int,
        configurations: [any IndexConfiguration]
    ) async throws {
        try await Self.buildEntityIndex(
            database: database,
            storeSubspace: storeSubspace,
            index: index,
            indexStateManager: indexStateManager,
            batchSize: batchSize,
            configurations: configurations
        )
    }
}

// MARK: - Errors

/// Errors from EntityIndexBuilder
public enum EntityIndexBuilderError: Error, CustomStringConvertible {
    /// Index kind does not conform to IndexKindMaintainable
    ///
    /// This error occurs when an IndexKind is used that hasn't been bridged
    /// to its corresponding IndexMaintainer via `IndexKindMaintainable` conformance.
    ///
    /// **Resolution**: The IndexMaintainer implementor must add `IndexKindMaintainable`
    /// conformance to the IndexKind. See `ScalarIndexKind` extension in
    /// `ScalarIndexMaintainer.swift` for an example.
    case indexKindNotMaintainable(kindIdentifier: String, indexName: String)

    /// Entity not registered in IndexBuilderRegistry
    case entityNotRegistered(entityName: String)

    /// Type does not support index building
    ///
    /// This error occurs when trying to build an index for a type that doesn't
    /// conform to `_EntityIndexBuildable` (i.e., it's not `Persistable & Codable`).
    ///
    /// **Common causes**:
    /// - Entity created with manual initializer (not from Persistable.Type)
    /// - Type doesn't conform to Codable
    case typeNotBuildable(typeName: String, reason: String)

    public var description: String {
        switch self {
        case .indexKindNotMaintainable(let kindIdentifier, let indexName):
            return "Index kind '\(kindIdentifier)' for index '\(indexName)' does not conform to IndexKindMaintainable. " +
                   "The IndexMaintainer implementor must add IndexKindMaintainable conformance to the IndexKind."
        case .entityNotRegistered(let entityName):
            return "Entity '\(entityName)' is not registered in IndexBuilderRegistry. " +
                   "This usually means the type is not Codable or Schema was created without calling registerForIndexBuilding()."
        case .typeNotBuildable(let typeName, let reason):
            return "Cannot build index for type '\(typeName)': \(reason). " +
                   "Ensure the type conforms to both Persistable and Codable."
        }
    }
}

// MARK: - Auto-Registration for Codable Types

/// Specialized implementation of `registerForIndexBuilding` for Codable Persistable types
///
/// This extension provides automatic registration with `IndexBuilderRegistry` for all
/// `Persistable & Codable` types. When `Schema.init` calls `type.registerForIndexBuilding()`,
/// Codable types will automatically be registered for OnlineIndexer support.
///
/// **Design**: This follows the same pattern as `IndexKindMaintainable` - the specialized
/// implementation is provided in FDBIndexing (server-only) while the protocol is in FDBModel
/// (all platforms).
extension Persistable where Self: Codable {
    /// Register this type for index building during migrations
    ///
    /// This specialized implementation automatically registers Codable Persistable types
    /// with `IndexBuilderRegistry`, enabling OnlineIndexer support during migrations.
    ///
    /// **Called by**: `Schema.init` for each type
    /// **Manual call not needed**: Registration is automatic when using Schema
    public static func registerForIndexBuilding() {
        IndexBuilderRegistry.shared.register(Self.self)
    }
}
