import DatabaseEngine
import StorageKit

/// Reads autocomplete entries through one request-owned retained buffer.
/// Cursor rows are admitted before decoding and are promoted only at the
/// public result boundary.
struct AutocompleteIndexReader: Sendable {
    let subspace: Subspace
    let minPrefixLength: Int

    func suggestions(
        field: String,
        prefix: String,
        limit: Int,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [AutocompleteSuggestion] {
        guard limit >= 0 else {
            throw AutocompleteError.invalidLimit(limit)
        }
        let normalizedPrefix = normalize(prefix)
        guard normalizedPrefix.count >= minPrefixLength, limit > 0 else {
            return []
        }

        let prefixSubspace = subspace
            .subspace("suggestions")
            .subspace(field)
            .subspace(normalizedPrefix)
        let (begin, end) = prefixSubspace.range()
        var builder = try makeBuilder(
            workMeter: workMeter,
            expectedCount: limit
        )
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: try workMeter.storageReadLimitWithSentinel(),
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        do {
            while let (key, value) = try await cursor.next() {
                try Task.checkCancellation()
                guard prefixSubspace.contains(key) else { break }
                let admission = try builder.prepareAppend(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: UInt64(key.count + value.count + 128)
                    ),
                    at: .indexScan
                )
                let term = try FullTextStorageDecoder
                    .autocompleteSuggestionTerm(
                        from: key,
                        in: prefixSubspace,
                        field: field,
                        prefix: normalizedPrefix
                    )
                let score = try ByteConversion.bytesToInt64(value)
                guard score > 0 else { continue }
                builder.append(
                    AutocompleteSuggestion(term: term, score: score),
                    using: admission
                )
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        try Task.checkCancellation()
        return promote(
            builder: consume builder,
            limit: limit
        )
    }

    func popularTerms(
        field: String,
        limit: Int,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [AutocompleteSuggestion] {
        guard limit >= 0 else {
            throw AutocompleteError.invalidLimit(limit)
        }
        guard limit > 0 else { return [] }

        let termsSubspace = subspace.subspace("terms").subspace(field)
        let (begin, end) = termsSubspace.range()
        var builder = try makeBuilder(
            workMeter: workMeter,
            expectedCount: limit
        )
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: try workMeter.storageReadLimitWithSentinel(),
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        do {
            while let (key, value) = try await cursor.next() {
                try Task.checkCancellation()
                guard termsSubspace.contains(key) else { break }
                let admission = try builder.prepareAppend(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: UInt64(key.count + value.count + 128)
                    ),
                    at: .indexScan
                )
                let term = try FullTextStorageDecoder.autocompleteTerm(
                    from: key,
                    in: termsSubspace,
                    field: field
                )
                let score = try ByteConversion.bytesToInt64(value)
                guard score > 0 else { continue }
                builder.append(
                    AutocompleteSuggestion(term: term, score: score),
                    using: admission
                )
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        try Task.checkCancellation()
        return promote(
            builder: consume builder,
            limit: limit
        )
    }

    private func makeBuilder(
        workMeter: DatabaseWorkMeter,
        expectedCount: Int
    ) throws -> DatabaseRetainedArrayBuilder<AutocompleteSuggestion> {
        try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                AutocompleteSuggestion.self
            ),
            expectedCount: expectedCount
        )
    }

    private func promote(
        builder: consuming DatabaseRetainedArrayBuilder<AutocompleteSuggestion>,
        limit: Int
    ) -> [AutocompleteSuggestion] {
        var retained = builder.finish()
        retained = retained.sortingElements { lhs, rhs in
            if lhs.score == rhs.score {
                return lhs.term < rhs.term
            }
            return lhs.score > rhs.score
        }
        if retained.count > limit {
            retained = retained.retainingSubrange(0..<limit)
        }
        return retained.promoteToOutput()
    }

    private func normalize(_ text: String) -> String {
        String(
            FullTextTextUtilities.trimmingWhitespace(text.lowercased())
        )
    }
}
