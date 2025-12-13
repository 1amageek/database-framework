// AlgorithmConfiguration.swift
// FDBIndexing - Runtime algorithm configuration
//
// Allows runtime selection of index algorithms without changing model definitions.

/// Runtime algorithm configuration for indexes
///
/// **Purpose**: Separate model definition (data structure) from runtime optimization (algorithm)
///
/// **Design Principle**:
/// - **Model definition**: Specifies WHAT to index (fields, dimensions, metric)
/// - **Runtime configuration**: Specifies HOW to index (algorithm, parameters)
///
/// **Benefits**:
/// - ✅ Environment-based algorithm selection (dev vs prod)
/// - ✅ Memory-based adaptation (HNSW vs flatScan)
/// - ✅ Performance tuning without model changes
/// - ✅ A/B testing different algorithms
///
/// **Example**:
/// ```swift
/// // Model: Only data structure
/// @Persistable
/// struct Product {
///     var id: Int64
///     #Index([\.embedding], type: VectorIndexKind(dimensions: 384, metric: .cosine))
///     var embedding: [Float32]
/// }
///
/// // Runtime: Select algorithm per environment
/// #if DEBUG
/// let config: AlgorithmConfiguration = .vectorFlatScan  // Fast startup
/// #else
/// let config: AlgorithmConfiguration = .vectorHNSW(.default)  // High performance
/// #endif
///
/// let schema = Schema(
///     [Product.self],
///     indexConfigurations: IndexConfigurationBuilder()
///         .configure(Product.self, \.embedding, algorithm: config)
///         .build()
/// )
/// ```
public enum AlgorithmConfiguration: Sendable {
    // MARK: - Vector Algorithms

    /// Vector flat scan: Brute force linear search
    /// - Time: O(n)
    /// - Memory: O(n * d) (just vectors)
    /// - Recall: 100% (exact)
    /// - Best for: <10K vectors, development, low memory
    case vectorFlatScan

    /// Vector HNSW: Hierarchical Navigable Small World graph
    /// - Time: O(log n) search
    /// - Memory: O(n * d + n * M * log(n)) (vectors + graph)
    /// - Recall: ~95-99% (approximate)
    /// - Best for: >10K vectors, production, high memory
    case vectorHNSW(HNSWParameters)

    /// Vector IVF: Inverted File with clustering
    /// - Time: O(n/k + k) where k = nlist
    /// - Memory: O(n * d + k * d) (vectors + centroids)
    /// - Recall: ~90-95% (approximate)
    /// - Best for: >100K vectors, medium memory
    case vectorIVF(IVFParameters)

    // MARK: - Spatial Algorithms

    /// Spatial index with level-based precision
    /// - level: 0-30 for 2D, 0-20 for 3D
    /// - Higher level → higher precision, more cells
    /// - S2: level 15-17 typical for city-level
    /// - Morton: level 20-25 typical for meter-level
    case spatial(level: Int)

    // MARK: - Full-Text Algorithms

    /// Full-text standard analyzer
    /// - Whitespace tokenization
    /// - Lowercase normalization
    /// - No stemming
    case fullTextStandard

    /// Full-text advanced analyzer
    /// - Language-specific stemming
    /// - Custom stopwords
    /// - More sophisticated tokenization
    case fullTextAdvanced(stemming: Bool, stopwords: [String])

    // Future: Add more as needed
    // - case graphTraversal(maxDepth: Int)
    // - case timeSeriesCompression(algorithm: String)
}

/// HNSW algorithm parameters
///
/// **Parameters Guide**:
/// - **m**: Max edges per layer (5-48, default: 16)
///   - Higher → better recall, slower insertion, more memory
/// - **efConstruction**: Construction candidate list size (100-500, default: 200)
///   - Higher → better graph quality, slower insertion
/// - **efSearch**: Search candidate list size (50-200, default: 100)
///   - Higher → better recall, slower search
///   - Should be >= k (number of results)
public struct HNSWParameters: Sendable, Codable, Hashable {
    public let m: Int
    public let efConstruction: Int
    public let efSearch: Int

    public init(m: Int = 16, efConstruction: Int = 200, efSearch: Int = 100) {
        self.m = m
        self.efConstruction = efConstruction
        self.efSearch = efSearch
    }

    /// Default HNSW parameters (balanced)
    public static let `default` = HNSWParameters(m: 16, efConstruction: 200, efSearch: 100)

    /// High recall parameters (slower, more memory)
    public static let highRecall = HNSWParameters(m: 32, efConstruction: 400, efSearch: 200)

    /// Fast parameters (lower recall, less memory)
    public static let fast = HNSWParameters(m: 8, efConstruction: 100, efSearch: 50)
}

/// IVF algorithm parameters
///
/// **Parameters Guide**:
/// - **nlist**: Number of clusters (sqrt(n), default: 100)
///   - Example: 100 for 10K vectors, 1000 for 1M vectors
/// - **nprobe**: Number of clusters to probe (1-20, default: 10)
///   - Higher → better recall, slower search
public struct IVFParameters: Sendable, Codable, Hashable {
    public let nlist: Int
    public let nprobe: Int

    public init(nlist: Int = 100, nprobe: Int = 10) {
        self.nlist = nlist
        self.nprobe = nprobe
    }

    /// Default IVF parameters
    public static let `default` = IVFParameters(nlist: 100, nprobe: 10)

    /// High recall parameters (more probes)
    public static let highRecall = IVFParameters(nlist: 100, nprobe: 20)

    /// Fast parameters (fewer probes)
    public static let fast = IVFParameters(nlist: 100, nprobe: 5)
}
