// VectorIndexConfiguration.swift
// VectorIndex - Runtime configuration for vector indexes
//
// Provides IndexConfiguration implementation to select HNSW vs Flat search at runtime.

import Foundation
import Core
import Vector

// MARK: - Internal Protocol for Type-Safe Casting

/// Internal protocol for type-safe vector index configuration access
///
/// **Purpose**: Enables type-safe casting in `makeIndexMaintainer` without Mirror reflection.
/// The underscore prefix indicates this is an implementation detail, not public API.
///
/// **Usage in VectorIndexKind+Maintainable**:
/// ```swift
/// if let config = matchingConfig as? _VectorIndexConfiguration {
///     switch config.algorithm { ... }
/// }
/// ```
public protocol _VectorIndexConfiguration: IndexConfiguration {
    /// Vector search algorithm selection
    var algorithm: VectorAlgorithm { get }

    /// Subspace key for data isolation (inherited from IndexConfiguration)
    var subspaceKey: String? { get }
}

// MARK: - Vector Index Configuration

/// Runtime configuration for VectorIndexKind
///
/// **Purpose**: Select vector search algorithm at container initialization.
///
/// **Algorithm Selection**:
/// - **Flat scan**: Default, O(n), 100% recall, no memory overhead
/// - **HNSW**: O(log n), ~95-99% recall, requires graph in memory
///
/// **Usage Example**:
/// ```swift
/// // Define model with vector index
/// @Persistable
/// struct Product {
///     var id: Int64
///     @Index(type: VectorIndexKind(dimensions: 384, metric: .cosine))
///     var embedding: [Float]
/// }
///
/// // Configure HNSW at runtime
/// let config = VectorIndexConfiguration<Product>(
///     keyPath: \.embedding,
///     algorithm: .hnsw(.default)
/// )
///
/// let container = try FDBContainer(
///     for: schema,
///     indexConfigurations: [config]
/// )
/// ```
///
/// **When to use HNSW**:
/// - >10,000 vectors
/// - Latency-sensitive searches
/// - High-throughput requirements
///
/// **When to use Flat (default)**:
/// - <10,000 vectors
/// - 100% recall required
/// - Memory-constrained environments
/// - Development/testing
///
/// **Note**: `@unchecked Sendable` is used because `KeyPath` is immutable and thread-safe.
public struct VectorIndexConfiguration<Model: Persistable>: _VectorIndexConfiguration, @unchecked Sendable {
    /// Must match VectorIndexKind.identifier
    public static var kindIdentifier: String { "vector" }

    /// Type-erased keyPath for protocol conformance
    public var keyPath: AnyKeyPath { _keyPath }

    /// Model type name for index name generation
    public var modelTypeName: String { String(describing: Model.self) }

    // MARK: - Private Storage

    private let _keyPath: KeyPath<Model, [Float]>

    // MARK: - Configuration Properties

    /// Vector search algorithm selection
    public let algorithm: VectorAlgorithm

    /// Optional subspace key for data isolation
    ///
    /// When specified, creates a separate subspace for this configuration's data.
    /// Useful for maintaining multiple algorithm variants (e.g., "hnsw", "flat").
    public let subspaceKey: String?

    // MARK: - Initialization

    /// Create vector index configuration
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - algorithm: Search algorithm to use (default: .auto)
    ///   - subspaceKey: Optional key for subspace isolation (default: nil)
    public init(
        keyPath: KeyPath<Model, [Float]>,
        algorithm: VectorAlgorithm = .auto(.default),
        subspaceKey: String? = nil
    ) {
        self._keyPath = keyPath
        self.algorithm = algorithm
        self.subspaceKey = subspaceKey
    }

    /// Create configuration with HNSW algorithm
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - hnswParameters: HNSW algorithm parameters
    ///   - subspaceKey: Optional key for subspace isolation (default: nil)
    public init(
        keyPath: KeyPath<Model, [Float]>,
        hnsw hnswParameters: VectorHNSWParameters,
        subspaceKey: String? = nil
    ) {
        self._keyPath = keyPath
        self.algorithm = .hnsw(hnswParameters)
        self.subspaceKey = subspaceKey
    }
}

// MARK: - Vector Algorithm Selection

/// Vector search algorithm selection
///
/// **Auto (default)**: Automatic selection based on dataset size
/// - <1K vectors → Flat (exact, no overhead)
/// - >=1K vectors → HNSW (fast approximate)
///
/// **Flat**: Brute-force linear scan
/// - Time complexity: O(n)
/// - Space complexity: O(1) extra
/// - Recall: 100% (exact)
/// - Best for: <10K vectors, exact results required
///
/// **HNSW**: Hierarchical Navigable Small World graph
/// - Time complexity: O(log n)
/// - Space complexity: O(n × M × log n) for graph
/// - Recall: ~95-99% (approximate)
/// - Best for: >10K vectors, speed matters
public enum VectorAlgorithm: Sendable {
    /// Automatic algorithm selection
    ///
    /// Selects the best algorithm based on dataset characteristics:
    /// - Small datasets (<flatThreshold): Flat for exact results
    /// - Larger datasets (>=flatThreshold): HNSW for fast approximate search
    ///
    /// **This is the recommended default for most use cases.**
    case auto(VectorAutoParameters)

    /// Flat scan (brute-force, exact results)
    case flat

    /// HNSW graph (approximate, fast)
    case hnsw(VectorHNSWParameters)

    /// Default algorithm: auto with default parameters
    public static var `default`: VectorAlgorithm { .auto(.default) }
}

// MARK: - Auto Selection Parameters

/// Parameters for automatic algorithm selection
///
/// **Thresholds**:
/// - `flatThreshold`: Use Flat for datasets smaller than this (default: 1000)
/// - Above `flatThreshold`: Use HNSW
///
/// **Selection Logic**:
/// ```
/// if vectorCount < flatThreshold:
///     return Flat (exact, fast for small data)
/// else:
///     return HNSW (fast approximate)
/// ```
public struct VectorAutoParameters: Sendable, Codable, Hashable {
    /// Threshold below which Flat algorithm is used (default: 1000)
    ///
    /// For datasets smaller than this, brute-force search is fast enough
    /// and provides exact results.
    public let flatThreshold: Int

    /// HNSW parameters to use when HNSW is selected
    public let hnswParameters: VectorHNSWParameters

    /// Create custom auto selection parameters
    ///
    /// - Parameters:
    ///   - flatThreshold: Use Flat below this count (default: 1000)
    ///   - hnswParameters: HNSW parameters (default: .default)
    public init(
        flatThreshold: Int = 1000,
        hnswParameters: VectorHNSWParameters = .default
    ) {
        precondition(flatThreshold > 0, "flatThreshold must be positive")

        self.flatThreshold = flatThreshold
        self.hnswParameters = hnswParameters
    }

    /// Default parameters
    ///
    /// - flatThreshold: 1000
    /// - hnswParameters: .default
    public static let `default` = VectorAutoParameters()

    /// High recall parameters (prefer accuracy over speed)
    ///
    /// - flatThreshold: 5000 (use exact search for larger datasets)
    /// - hnswParameters: .highRecall
    public static let highRecall = VectorAutoParameters(
        flatThreshold: 5000,
        hnswParameters: .highRecall
    )

    /// Fast parameters (prefer speed over accuracy)
    ///
    /// - flatThreshold: 500 (switch to approximate earlier)
    /// - hnswParameters: .fast
    public static let fast = VectorAutoParameters(
        flatThreshold: 500,
        hnswParameters: .fast
    )

    /// Select the appropriate algorithm based on vector count
    ///
    /// - Parameters:
    ///   - vectorCount: Current number of vectors in the index
    ///   - dimensions: Vector dimensions (unused, kept for API compatibility)
    /// - Returns: The selected algorithm
    public func selectAlgorithm(vectorCount: Int, dimensions: Int) -> VectorAlgorithm {
        if vectorCount < flatThreshold {
            return .flat
        } else {
            return .hnsw(hnswParameters)
        }
    }
}

// MARK: - HNSW Parameters

/// HNSW algorithm parameters for VectorIndexConfiguration
///
/// **Parameters Guide**:
/// - **m**: Maximum bi-directional links per node (default: 16)
///   - Range: 5-64
///   - Higher → better recall, more memory, slower insertion
/// - **efConstruction**: Size of dynamic candidate list during construction (default: 200)
///   - Range: 100-500
///   - Higher → better graph quality, slower build
/// - **efSearch**: Size of dynamic candidate list during search (default: 50)
///   - Range: k to 500 (must be >= k, the number of results)
///   - Higher → better recall, slower search
///
/// **Presets**:
/// - `.default`: Balanced (m=16, efConstruction=200, efSearch=50)
/// - `.highRecall`: Better quality (m=32, efConstruction=400, efSearch=100)
/// - `.fast`: Faster build (m=8, efConstruction=100, efSearch=30)
public struct VectorHNSWParameters: Sendable, Codable, Hashable {
    /// Maximum bi-directional links per node per layer
    public let m: Int

    /// Size of dynamic candidate list during construction
    public let efConstruction: Int

    /// Size of dynamic candidate list during search (default ef for queries)
    ///
    /// **Recommendation**: efSearch >= k (k = number of results)
    /// - For recall ~90%: efSearch ≈ k * 1.5
    /// - For recall ~95%: efSearch ≈ k * 2
    /// - For recall ~99%: efSearch ≈ k * 3
    public let efSearch: Int

    /// Create custom HNSW parameters
    ///
    /// - Parameters:
    ///   - m: Maximum links per node (default: 16)
    ///   - efConstruction: Construction candidate list size (default: 200)
    ///   - efSearch: Search candidate list size (default: 50)
    public init(m: Int = 16, efConstruction: Int = 200, efSearch: Int = 50) {
        self.m = m
        self.efConstruction = efConstruction
        self.efSearch = efSearch
    }

    /// Default balanced parameters
    ///
    /// - m: 16
    /// - efConstruction: 200
    /// - efSearch: 50
    public static let `default` = VectorHNSWParameters(m: 16, efConstruction: 200, efSearch: 50)

    /// High recall parameters (slower build)
    ///
    /// - m: 32
    /// - efConstruction: 400
    /// - efSearch: 100
    public static let highRecall = VectorHNSWParameters(m: 32, efConstruction: 400, efSearch: 100)

    /// Fast build parameters (lower recall)
    ///
    /// - m: 8
    /// - efConstruction: 100
    /// - efSearch: 30
    public static let fast = VectorHNSWParameters(m: 8, efConstruction: 100, efSearch: 30)
}
