import DatabaseEngine
import DatabaseTypes

/// Builds one retained property-path relation with prospective admission.
struct SPARQLPropertyPathMatchBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<SPARQLPropertyPathMatch>
    private let maximumResults: Int?

    private init(
        storage: consuming DatabaseRetainedArrayBuilder<
            SPARQLPropertyPathMatch
        >,
        maximumResults: Int?
    ) {
        self.storage = storage
        self.maximumResults = maximumResults
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .pathExpansion,
        maximumResults: Int,
        expectedCount: Int = 0
    ) throws -> SPARQLPropertyPathMatchBuilder {
        guard maximumResults >= 0 else {
            throw SPARQLPropertyPathMatchStorageError
                .invalidMaximumResults(maximumResults)
        }
        guard expectedCount >= 0 else {
            throw DatabaseRetainedArrayLayoutError
                .invalidRequiredCount(expectedCount)
        }

        return try makeStorage(
            workMeter: workMeter,
            stage: stage,
            maximumResults: maximumResults,
            expectedCount: expectedCount
        )
    }

    /// Builds intermediate path algebra storage. Request work and retained
    /// memory budgets bound this relation; the final-result limit is applied
    /// only after endpoint compatibility is known.
    static func makeIntermediate(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .pathExpansion,
        expectedCount: Int = 0
    ) throws -> SPARQLPropertyPathMatchBuilder {
        guard expectedCount >= 0 else {
            throw DatabaseRetainedArrayLayoutError
                .invalidRequiredCount(expectedCount)
        }
        return try makeStorage(
            workMeter: workMeter,
            stage: stage,
            maximumResults: nil,
            expectedCount: expectedCount
        )
    }

    private static func makeStorage(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        maximumResults: Int?,
        expectedCount: Int
    ) throws -> SPARQLPropertyPathMatchBuilder {
        let storage = try DatabaseRetainedArrayBuilder<
            SPARQLPropertyPathMatch
        >(
            workMeter: workMeter,
            stage: stage,
            layout: try SPARQLPropertyPathMatchRetainedFootprint
                .retainedArrayLayout(),
            expectedCount: maximumResults.map {
                min(expectedCount, $0)
            } ?? expectedCount
        )
        return SPARQLPropertyPathMatchBuilder(
            storage: storage,
            maximumResults: maximumResults
        )
    }

    /// Reopens a unique relation without copying its Array buffer.
    static func resuming(
        _ matches: consuming SPARQLPropertyPathMatches,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .pathExpansion,
        maximumResults: Int
    ) throws -> SPARQLPropertyPathMatchBuilder {
        guard maximumResults >= 0 else {
            throw SPARQLPropertyPathMatchStorageError
                .invalidMaximumResults(maximumResults)
        }
        guard matches.count <= maximumResults else {
            throw SPARQLQueryError.propertyPathResultLimitExceeded(
                maximum: maximumResults
            )
        }

        switch consume matches {
        case .empty:
            return try make(
                workMeter: workMeter,
                stage: stage,
                maximumResults: maximumResults
            )
        case .unique(let retained):
            return SPARQLPropertyPathMatchBuilder(
                storage: retained.resumeBuilding(at: stage),
                maximumResults: maximumResults
            )
        }
    }

    /// Reopens intermediate path algebra storage without imposing the final
    /// compatible-binding count on an internal frontier.
    static func resumeIntermediate(
        _ matches: consuming SPARQLPropertyPathMatches,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .pathExpansion
    ) throws -> SPARQLPropertyPathMatchBuilder {
        switch consume matches {
        case .empty:
            return try makeIntermediate(
                workMeter: workMeter,
                stage: stage
            )
        case .unique(let retained):
            return SPARQLPropertyPathMatchBuilder(
                storage: retained.resumeBuilding(at: stage),
                maximumResults: nil
            )
        }
    }

    var count: Int { storage.count }
    var isEmpty: Bool { storage.isEmpty }

    /// Verifies the result limit without changing any retained reservation.
    borrowing func checkAppendAllowed() throws {
        guard let maximumResults else { return }
        guard storage.count < maximumResults else {
            throw SPARQLQueryError.propertyPathResultLimitExceeded(
                maximum: maximumResults
            )
        }
    }

    mutating func append(
        start: borrowing RDFTerm,
        end: borrowing RDFTerm,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        try checkAppendAllowed()
        let footprint = try SPARQLPropertyPathMatchRetainedFootprint.measure(
            start: start,
            end: end
        )
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
        let match = SPARQLPropertyPathMatch(
            start: copy start,
            end: copy end
        )
        storage.append(match, using: admission)
    }

    mutating func append(
        _ match: consuming SPARQLPropertyPathMatch,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        try checkAppendAllowed()
        let footprint = try SPARQLPropertyPathMatchRetainedFootprint.measure(
            of: match
        )
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
        storage.append(match, using: admission)
    }

    mutating func appendBorrowed(
        _ match: borrowing SPARQLPropertyPathMatch,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        try checkAppendAllowed()
        let footprint = try SPARQLPropertyPathMatchRetainedFootprint.measure(
            of: match
        )
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
        let retainedMatch = copy match
        storage.append(retainedMatch, using: admission)
    }

    mutating func appendBorrowed(
        contentsOf matches: borrowing SPARQLPropertyPathMatches,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        for index in 0..<matches.count {
            try matches.withElement(at: index) { match in
                try appendBorrowed(match, at: stage)
            }
        }
    }

    consuming func finish() -> SPARQLPropertyPathMatches {
        guard !storage.isEmpty else { return .empty }
        return .unique(storage.finish())
    }
}
