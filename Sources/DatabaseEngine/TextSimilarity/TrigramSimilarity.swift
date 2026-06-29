// TrigramSimilarity.swift
// DatabaseEngine - Trigram-based text similarity computation
//
// Provides Jaccard similarity over character trigrams (3-grams).
// Used by GraphIndex (SPARQL FILTER) and ScalarIndex (Filter) for fuzzy text matching.

import Foundation

/// Trigram-based text similarity.
///
/// Computes Sørensen–Dice coefficient over 3-character sliding windows.
/// Case-insensitive. Returns 1.0 for identical strings, 0.0 for no overlap.
/// Dice is more tolerant of length differences than Jaccard.
///
/// ```swift
/// TrigramSimilarity.score("Google", "Google LLC")  // 0.667
/// TrigramSimilarity.score("Apple", "Zebra")        // 0.0
/// ```
public enum TrigramSimilarity {

    /// Compute trigram Dice similarity between two strings.
    ///
    /// Dice = 2 * |A ∩ B| / (|A| + |B|)
    ///
    /// - Parameters:
    ///   - a: First string
    ///   - b: Second string
    /// - Returns: Similarity score in [0.0, 1.0]
    public static func score(_ a: String, _ b: String) -> Double {
        let trigramsA = trigrams(a.lowercased())
        let trigramsB = trigrams(b.lowercased())
        guard !trigramsA.isEmpty || !trigramsB.isEmpty else { return 1.0 }
        let sum = trigramsA.count + trigramsB.count
        guard sum > 0 else { return 0.0 }
        let intersection = trigramsA.intersection(trigramsB).count
        return 2.0 * Double(intersection) / Double(sum)
    }

    /// Extract trigrams (3-character sliding windows) from a string.
    private static func trigrams(_ s: String) -> Set<String> {
        guard s.count >= 3 else { return s.isEmpty ? [] : [s] }
        var result = Set<String>()
        var i = s.startIndex
        while i < s.endIndex {
            let end = s.index(i, offsetBy: 3, limitedBy: s.endIndex) ?? s.endIndex
            if s.distance(from: i, to: end) == 3 {
                result.insert(String(s[i..<end]))
            }
            i = s.index(after: i)
        }
        return result
    }
}
