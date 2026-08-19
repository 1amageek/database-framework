// VectorIndexConfiguration.swift
// VectorIndex - Runtime configuration for vector indexes
//
// Provides IndexRuntimeConfiguration implementation for explicit vector index layouts.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes

// MARK: - Vector Index Configuration

/// Runtime configuration for a declared vector index.
///
/// **Purpose**: Select the vector search algorithm for one schema generation.
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
///     #Index(.vector(
///         name: "Product_embedding",
///         embedding: \Product.embedding,
///         dimensions: 384,
///         metric: .cosine
///     ))
///
///     var id: Int64
///     var embedding: Vector
/// }
///
/// // Configure HNSW at runtime
/// let config = VectorIndexConfiguration(
///     indexName: "Product_embedding",
///     algorithm: .hnsw(.default)
/// )
///
/// let runtime = try DatabaseFrameworkRuntime.configuration(
///     executionIdentity: DatabaseExecutionRuntimeIdentity(
///         identifier: "application",
///         revision: 1
///     ),
///     entityRuntimes: [
///         try DatabaseFrameworkRuntime.entity(Product.self),
///     ],
///     indexConfigurations: [config]
/// )
///
/// let container = try await DBContainer.open(
///     for: schema,
///     configuration: configuration,
///     runtimeConfiguration: runtime
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
public struct VectorIndexConfiguration: IndexRuntimeConfiguration {
    /// Must match the canonical vector index identifier.
    public static var indexType: IndexType { .vector }

    /// Explicit name of the declared vector index.
    public let indexName: String

    // MARK: - Configuration Properties

    /// Vector search algorithm selection
    public let algorithm: VectorAlgorithm

    public var executionOptions: FieldObject {
        get throws {
            var fields: [(key: String, value: FieldValue)] = []
            switch algorithm {
            case .flat:
                fields.append(("algorithm", .string("flat")))
            case .hnsw(let parameters):
                fields.append(("algorithm", .string("hnsw")))
                fields.append(("efConstruction", .int64(Int64(parameters.efConstruction))))
                fields.append(("efSearch", .int64(Int64(parameters.efSearch))))
                fields.append(("m", .int64(Int64(parameters.m))))
            case .ivf(let parameters):
                guard parameters.nlist > 0,
                      parameters.nprobe > 0,
                      parameters.nprobe <= parameters.nlist,
                      parameters.kmeansIterations > 0 else {
                    throw VectorIndexError.invalidArgument(
                        "Vector IVF parameters must be positive and nprobe must not exceed nlist"
                    )
                }
                fields.append(("algorithm", .string("ivf")))
                fields.append(("kmeansIterations", .int64(Int64(parameters.kmeansIterations))))
                fields.append(("nlist", .int64(Int64(parameters.nlist))))
                fields.append(("nprobe", .int64(Int64(parameters.nprobe))))
            case .pq(let parameters):
                fields.append(("algorithm", .string("pq")))
                fields.append(("m", .int64(Int64(parameters.m))))
                fields.append(("niter", .int64(Int64(parameters.niter))))
            }
            return try FieldObject(fields)
        }
    }

    // MARK: - Initialization

    /// Create vector index configuration
    ///
    /// - Parameters:
    ///   - indexName: Explicit name of the declared vector index
    ///   - algorithm: Search algorithm to use (default: .flat)
    public init(
        indexName: String,
        algorithm: VectorAlgorithm = .flat
    ) {
        self.indexName = indexName
        self.algorithm = algorithm
    }

    /// Create configuration with HNSW algorithm
    ///
    /// - Parameters:
    ///   - indexName: Explicit name of the declared vector index
    ///   - hnswParameters: HNSW algorithm parameters
    public init(
        indexName: String,
        hnsw hnswParameters: VectorHNSWParameters
    ) {
        self.indexName = indexName
        self.algorithm = .hnsw(hnswParameters)
    }
}

// MARK: - Vector Algorithm Selection

/// Vector search algorithm selection
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
///
/// **IVF**: Inverted File Index with K-means clustering
/// - Time complexity: O(nprobe × n/nlist)
/// - Space complexity: O(n) for vectors + O(k × d) for centroids
/// - Recall: ~80-95% (depends on nprobe)
/// - Best for: >100K vectors, memory-constrained environments
/// - Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search", 2011
public enum VectorAlgorithm: Sendable {
    /// Flat scan (brute-force, exact results)
    case flat

    /// HNSW graph (approximate, fast)
    case hnsw(VectorHNSWParameters)

    /// IVF (Inverted File Index) with K-means clustering
    ///
    /// Partitions vector space into clusters. At query time, only
    /// searches the nearest clusters. Good for very large datasets
    /// with limited memory.
    case ivf(VectorIVFParameters)

    /// PQ (Product Quantization) for vector compression
    ///
    /// Compresses vectors by splitting into subspaces and encoding
    /// each subspace with a single byte. Trades accuracy for memory.
    /// - Compression: d × 4 bytes → m bytes (typical m=8, 16x-64x compression)
    /// - Best for: Memory-constrained environments, billion-scale datasets
    case pq(VectorPQParameters)

    /// Default algorithm: exact flat scan.
    public static var `default`: VectorAlgorithm { .flat }
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
public struct VectorHNSWParameters: Sendable, Hashable {
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

// MARK: - IVF Parameters

/// IVF algorithm parameters for VectorIndexConfiguration
///
/// **Parameters Guide**:
/// - **nlist**: Number of clusters (partitions)
///   - Rule of thumb: sqrt(n) to 4*sqrt(n) where n = dataset size
///   - Range: 16-4096
/// - **nprobe**: Number of clusters to search
///   - Range: 1 to nlist
///   - Higher = better recall, slower search
///
/// **Trade-offs**:
/// - nlist too small: Each cluster is large, slow search
/// - nlist too large: More overhead, some clusters may be empty
/// - nprobe too small: Miss relevant vectors, low recall
/// - nprobe too large: Approaches brute force, no speedup
///
/// **Presets**:
/// - `.default`: Balanced (nlist=100, nprobe=10)
/// - `.highRecall`: Better quality (nlist=100, nprobe=25)
/// - `.fast`: Faster search (nlist=256, nprobe=5)
///
/// **Reference**: Jégou et al., "Product Quantization for Nearest Neighbor Search", 2011
public struct VectorIVFParameters: Sendable, Hashable {
    /// Number of clusters (inverted lists)
    public let nlist: Int

    /// Number of clusters to probe during search
    public let nprobe: Int

    /// K-means training iterations
    public let kmeansIterations: Int

    /// Create IVF parameters
    ///
    /// - Parameters:
    ///   - nlist: Number of clusters (default: 100)
    ///   - nprobe: Clusters to search (default: 10)
    ///   - kmeansIterations: Training iterations (default: 20)
    public init(
        nlist: Int = 100,
        nprobe: Int = 10,
        kmeansIterations: Int = 20
    ) throws(VectorIndexError) {
        guard nlist > 0,
              nprobe > 0,
              nprobe <= nlist,
              kmeansIterations > 0 else {
            throw .invalidArgument(
                "IVF parameters must be positive and nprobe must not exceed nlist"
            )
        }
        self.nlist = nlist
        self.nprobe = nprobe
        self.kmeansIterations = kmeansIterations
    }

    private init(
        validatedNlist nlist: Int,
        nprobe: Int,
        kmeansIterations: Int = 20
    ) {
        self.nlist = nlist
        self.nprobe = nprobe
        self.kmeansIterations = kmeansIterations
    }

    /// Default balanced parameters
    public static let `default` = VectorIVFParameters(
        validatedNlist: 100,
        nprobe: 10
    )

    /// High recall parameters
    public static let highRecall = VectorIVFParameters(
        validatedNlist: 100,
        nprobe: 25
    )

    /// Fast search parameters
    public static let fast = VectorIVFParameters(
        validatedNlist: 256,
        nprobe: 5
    )

    /// Small dataset parameters (< 10K vectors)
    public static let small = VectorIVFParameters(
        validatedNlist: 32,
        nprobe: 8
    )

    /// Large dataset parameters (> 100K vectors)
    public static let large = VectorIVFParameters(
        validatedNlist: 512,
        nprobe: 16
    )
}

// MARK: - PQ Parameters

/// PQ algorithm parameters for VectorIndexConfiguration
///
/// **Parameters Guide**:
/// - **m**: Number of subquantizers (subspaces)
///   - Must divide vector dimensions evenly
///   - Common values: 4, 8, 16, 32
///   - Higher M = better accuracy, larger codes
///
/// **Compression**:
/// - m=4: 4 bytes per vector
/// - m=8: 8 bytes per vector (default)
/// - m=16: 16 bytes per vector
/// - m=32: 32 bytes per vector
///
/// **Trade-offs**:
/// - More subquantizers → better accuracy, lower compression
/// - Fewer subquantizers → higher compression, lower accuracy
///
/// **Presets**:
/// - `.default`: Balanced (m=8)
/// - `.highCompression`: Maximum compression (m=4)
/// - `.highAccuracy`: Better accuracy (m=16)
///
/// **Reference**: Jégou et al., "Product Quantization for Nearest Neighbor Search", 2011
public struct VectorPQParameters: Sendable, Hashable {
    /// Number of subquantizers
    ///
    /// Must divide vector dimensions evenly.
    public let m: Int

    /// K-means training iterations per subspace
    public let niter: Int

    /// Create PQ parameters
    ///
    /// - Parameters:
    ///   - m: Number of subquantizers (default: 8)
    ///   - niter: Training iterations (default: 25)
    public init(
        m: Int = 8,
        niter: Int = 25
    ) throws(VectorIndexError) {
        guard m > 0, niter > 0 else {
            throw .invalidArgument("PQ parameters must be positive")
        }
        self.m = m
        self.niter = niter
    }

    private init(validatedM m: Int, niter: Int = 25) {
        self.m = m
        self.niter = niter
    }

    /// Default balanced parameters (m=8)
    public static let `default` = VectorPQParameters(validatedM: 8)

    /// High compression parameters (m=4)
    ///
    /// - 4 bytes per vector
    /// - Maximum compression, lower accuracy
    public static let highCompression = VectorPQParameters(validatedM: 4)

    /// High accuracy parameters (m=16)
    ///
    /// - 16 bytes per vector
    /// - Better accuracy, larger codes
    public static let highAccuracy = VectorPQParameters(validatedM: 16)

    /// Very high accuracy parameters (m=32)
    ///
    /// - 32 bytes per vector
    /// - Best PQ accuracy
    public static let veryHighAccuracy = VectorPQParameters(validatedM: 32)
}
