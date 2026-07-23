// SpatialScanBudget.swift
// SpatialIndex - Candidate scan budgeting for exact-filtered spatial queries

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

internal enum SpatialScanBudget {
    static let minimumCandidateLimit = 256
    static let candidateOverscanFactor = 32
    static let maximumCandidateLimit = 10_000

    static func candidateLimit(forFetchLimit fetchLimit: Int?) -> Int? {
        guard let fetchLimit, fetchLimit > 0 else {
            return nil
        }

        let lowerBound = max(fetchLimit, minimumCandidateLimit)
        let upperBound = max(fetchLimit, maximumCandidateLimit)
        let multiplied = fetchLimit > upperBound / candidateOverscanFactor
            ? upperBound
            : fetchLimit * candidateOverscanFactor
        return min(max(multiplied, lowerBound), upperBound)
    }

    static func rangeReadLimit(totalLimit: Int?, emittedCount: Int) -> Int {
        guard let totalLimit else {
            return 0
        }

        guard emittedCount < totalLimit else {
            return 1
        }

        return max(totalLimit - emittedCount + 1, 1)
    }
}
