// BM25Parameters.swift
// FullTextIndex - BM25 scoring parameters

// MARK: - BM25Parameters

/// Parameters for BM25 scoring.
///
/// **Reference**: Lucene BM25Similarity
public struct BM25Parameters: Sendable, Hashable {
    /// Term saturation parameter.
    ///
    /// Controls how quickly term frequency saturates.
    /// Higher values mean term frequency continues to matter.
    /// Default: 1.2 (Lucene default)
    public let k1: Float

    /// Length normalization parameter.
    ///
    /// Controls the impact of document length on score.
    /// 0 means no length normalization; 1 means full length normalization.
    /// Default: 0.75 (Lucene default)
    public let b: Float

    public init(k1: Float = 1.2, b: Float = 0.75) {
        self.k1 = k1
        self.b = b
    }

    /// Lucene defaults.
    public static let `default` = BM25Parameters()

    /// No length normalization.
    public static let noLengthNorm = BM25Parameters(k1: 1.2, b: 0)

    /// Strong length normalization.
    public static let strongLengthNorm = BM25Parameters(k1: 1.2, b: 1.0)
}
