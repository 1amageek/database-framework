// IVFParameters.swift
// VectorIndex - Parameters for IVF (Inverted File Index) algorithm
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import Foundation

/// Parameters for IVF (Inverted File Index) algorithm
///
/// **Algorithm Overview**:
/// IVF partitions the vector space into nlist clusters using K-means.
/// At query time, only the nprobe nearest clusters are searched,
/// reducing search complexity from O(n) to O(nprobe × n/nlist).
///
/// **Key Parameters**:
/// - **nlist**: Number of clusters (partitions)
///   - Rule of thumb: sqrt(n) to 4*sqrt(n) where n = dataset size
///   - More clusters = smaller search scope but longer training
///
/// - **nprobe**: Number of clusters to search
///   - Higher = better recall, slower search
///   - Typical: 1-10 for speed, 10-50 for accuracy
///
/// **Trade-offs**:
/// - nlist too small: Each cluster is large, slow search
/// - nlist too large: More overhead, some clusters may be empty
/// - nprobe too small: Miss relevant vectors, low recall
/// - nprobe too large: Approaches brute force, no speedup
///
/// **Usage**:
/// ```swift
/// let params = IVFParameters(
///     nlist: 100,   // 100 clusters
///     nprobe: 10    // Search 10 nearest clusters
/// )
/// ```
///
/// **Performance Characteristics**:
/// - Training time: O(n × nlist × iterations)
/// - Insert time: O(nlist) for centroid lookup
/// - Query time: O(nprobe × n/nlist) on average
public struct IVFParameters: Sendable, Codable, Hashable {
    /// Number of clusters (inverted lists)
    ///
    /// **Guidelines**:
    /// - n < 10K: nlist = 16-64
    /// - n < 100K: nlist = 64-256
    /// - n < 1M: nlist = 256-1024
    /// - n > 1M: nlist = 1024-4096
    public let nlist: Int

    /// Number of clusters to probe during search
    ///
    /// **Guidelines**:
    /// - Fast search: nprobe = 1-5
    /// - Balanced: nprobe = 10-20
    /// - High recall: nprobe = nlist / 4 or more
    public let nprobe: Int

    /// K-means training iterations
    public let kmeansIterations: Int

    /// Create IVF parameters
    ///
    /// - Parameters:
    ///   - nlist: Number of clusters (default: 100)
    ///   - nprobe: Number of clusters to search (default: 10)
    ///   - kmeansIterations: K-means iterations (default: 20)
    public init(
        nlist: Int = 100,
        nprobe: Int = 10,
        kmeansIterations: Int = 20
    ) {
        precondition(nlist > 0, "nlist must be positive")
        precondition(nprobe > 0, "nprobe must be positive")
        precondition(nprobe <= nlist, "nprobe cannot exceed nlist")
        precondition(kmeansIterations > 0, "kmeansIterations must be positive")

        self.nlist = nlist
        self.nprobe = nprobe
        self.kmeansIterations = kmeansIterations
    }

    // MARK: - Presets

    /// Default balanced parameters
    ///
    /// - nlist: 100
    /// - nprobe: 10
    /// - kmeansIterations: 20
    public static let `default` = IVFParameters(nlist: 100, nprobe: 10)

    /// Fast search parameters (lower recall)
    ///
    /// - nlist: 256
    /// - nprobe: 5
    public static let fast = IVFParameters(nlist: 256, nprobe: 5)

    /// High recall parameters (slower search)
    ///
    /// - nlist: 100
    /// - nprobe: 25
    public static let highRecall = IVFParameters(nlist: 100, nprobe: 25)

    /// Small dataset parameters (< 10K vectors)
    ///
    /// - nlist: 32
    /// - nprobe: 8
    public static let small = IVFParameters(nlist: 32, nprobe: 8)

    /// Large dataset parameters (> 100K vectors)
    ///
    /// - nlist: 512
    /// - nprobe: 16
    public static let large = IVFParameters(nlist: 512, nprobe: 16)

    // MARK: - Auto Parameters

    /// Create parameters based on estimated dataset size
    ///
    /// Uses heuristics from FAISS library:
    /// - nlist ≈ 4 × sqrt(n) capped at 4096
    /// - nprobe ≈ sqrt(nlist) for ~90% recall
    ///
    /// - Parameter estimatedVectorCount: Expected number of vectors
    /// - Returns: Optimized IVF parameters
    public static func auto(estimatedVectorCount n: Int) -> IVFParameters {
        let nlist: Int
        if n < 10_000 {
            nlist = max(16, min(64, Int(sqrt(Double(n)))))
        } else if n < 100_000 {
            nlist = max(64, min(256, Int(sqrt(Double(n)) * 2)))
        } else if n < 1_000_000 {
            nlist = max(256, min(1024, Int(sqrt(Double(n)) * 4)))
        } else {
            nlist = min(4096, Int(sqrt(Double(n)) * 4))
        }

        // nprobe for ~90% recall
        let nprobe = max(1, Int(sqrt(Double(nlist))))

        return IVFParameters(nlist: nlist, nprobe: nprobe)
    }
}
