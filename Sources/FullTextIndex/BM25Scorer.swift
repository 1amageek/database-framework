// BM25Scorer.swift
// FullTextIndex - BM25 scoring calculator
//
// Reference: Robertson & Zaragoza, "The Probabilistic Relevance Framework: BM25 and Beyond"
// Foundations and Trends in Information Retrieval, 2009

import Foundation

// MARK: - BM25 Statistics

/// BM25 corpus statistics required for scoring
///
/// These statistics are maintained by `FullTextIndexMaintainer` and
/// stored in the index for efficient retrieval.
public struct BM25Statistics: Sendable {
    /// Total number of documents in the corpus
    public let totalDocuments: Int64

    /// Sum of all document lengths (for computing avgDL)
    public let totalLength: Int64

    /// Average document length
    public var averageDocumentLength: Double {
        guard totalDocuments > 0 else { return 0 }
        return Double(totalLength) / Double(totalDocuments)
    }

    public init(totalDocuments: Int64, totalLength: Int64) {
        self.totalDocuments = totalDocuments
        self.totalLength = totalLength
    }
}

// MARK: - BM25 Scorer

/// BM25 scoring calculator
///
/// Calculates BM25 scores for documents based on term frequencies and corpus statistics.
///
/// **Formula**:
/// ```
/// BM25(D, Q) = Σ IDF(qi) × (tf(qi, D) × (k1 + 1)) / (tf(qi, D) + k1 × (1 - b + b × |D|/avgDL))
/// ```
///
/// **IDF (Inverse Document Frequency)**:
/// ```
/// IDF(q) = log((N - df(q) + 0.5) / (df(q) + 0.5))
/// ```
///
/// Note: IDF can be negative when df > N/2 (term appears in majority of documents).
/// This is intentional: very common terms carry less information.
///
/// **Usage**:
/// ```swift
/// let scorer = BM25Scorer(params: .default, statistics: stats)
/// let score = scorer.score(
///     termFrequencies: ["swift": 3, "concurrency": 1],
///     documentFrequencies: ["swift": 100, "concurrency": 50],
///     docLength: 500
/// )
/// ```
public struct BM25Scorer: Sendable {

    /// BM25 parameters (k1, b)
    public let params: BM25Parameters

    /// Total number of documents in corpus
    public let totalDocuments: Int64

    /// Average document length
    public let averageDocumentLength: Double

    /// Initialize with parameters and corpus statistics
    ///
    /// - Parameters:
    ///   - params: BM25 parameters (k1, b)
    ///   - statistics: Corpus statistics (N, avgDL)
    public init(params: BM25Parameters, statistics: BM25Statistics) {
        self.params = params
        self.totalDocuments = statistics.totalDocuments
        self.averageDocumentLength = statistics.averageDocumentLength
    }

    /// Initialize with explicit values
    ///
    /// - Parameters:
    ///   - params: BM25 parameters (k1, b)
    ///   - totalDocuments: Total number of documents in corpus
    ///   - averageDocumentLength: Average document length
    public init(params: BM25Parameters, totalDocuments: Int64, averageDocumentLength: Double) {
        self.params = params
        self.totalDocuments = totalDocuments
        self.averageDocumentLength = averageDocumentLength
    }

    // MARK: - IDF Calculation

    /// Calculate IDF (Inverse Document Frequency) for a term
    ///
    /// Uses standard BM25 IDF formula:
    /// ```
    /// IDF(q) = log((N - df + 0.5) / (df + 0.5))
    /// ```
    ///
    /// **Note**: Returns negative value when df > N/2 (term in majority of docs).
    /// This is intentional: very common terms carry less information.
    ///
    /// - Parameter documentFrequency: Number of documents containing this term
    /// - Returns: IDF value (can be negative for very common terms)
    public func idf(documentFrequency df: Int64) -> Double {
        guard totalDocuments > 0 else { return 0 }

        let N = Double(totalDocuments)
        let dfDouble = Double(df)

        let numerator = N - dfDouble + 0.5
        let denominator = dfDouble + 0.5

        // Standard BM25 IDF (can be negative when df > N/2)
        return log(numerator / denominator)
    }

    // MARK: - Score Calculation

    /// Calculate BM25 score for a document
    ///
    /// - Parameters:
    ///   - termFrequencies: Map of query term -> frequency in this document
    ///   - documentFrequencies: Map of query term -> number of documents containing term
    ///   - docLength: Length of this document (number of tokens)
    /// - Returns: BM25 score (higher is better match)
    public func score(
        termFrequencies: [String: Int],
        documentFrequencies: [String: Int64],
        docLength: Int
    ) -> Double {
        guard averageDocumentLength > 0 else { return 0 }

        var totalScore = 0.0

        let k1 = Double(params.k1)
        let b = Double(params.b)

        for (term, tf) in termFrequencies {
            guard let df = documentFrequencies[term], tf > 0 else { continue }

            let idfValue = idf(documentFrequency: df)

            // TF normalization with length normalization
            // (tf × (k1 + 1)) / (tf + k1 × (1 - b + b × |D|/avgDL))
            let tfDouble = Double(tf)
            let lengthRatio = Double(docLength) / averageDocumentLength
            let denominator = tfDouble + k1 * (1 - b + b * lengthRatio)
            let tfNormalized = (tfDouble * (k1 + 1)) / denominator

            totalScore += idfValue * tfNormalized
        }

        return totalScore
    }

    /// Calculate BM25 score for a single term
    ///
    /// Useful when scoring terms individually.
    ///
    /// - Parameters:
    ///   - termFrequency: Frequency of term in document
    ///   - documentFrequency: Number of documents containing term
    ///   - docLength: Length of document
    /// - Returns: BM25 contribution for this term
    public func scoreForTerm(
        termFrequency tf: Int,
        documentFrequency df: Int64,
        docLength: Int
    ) -> Double {
        guard averageDocumentLength > 0, tf > 0 else { return 0 }

        let idfValue = idf(documentFrequency: df)

        let k1 = Double(params.k1)
        let b = Double(params.b)

        let tfDouble = Double(tf)
        let lengthRatio = Double(docLength) / averageDocumentLength
        let denominator = tfDouble + k1 * (1 - b + b * lengthRatio)
        let tfNormalized = (tfDouble * (k1 + 1)) / denominator

        return idfValue * tfNormalized
    }
}
