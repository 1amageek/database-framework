import DatabaseEngine
import StorageKit

struct AutocompleteIndexReader: Sendable {
    let subspace: Subspace
    let minPrefixLength: Int

    func suggestions(
        field: String,
        prefix: String,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [AutocompleteSuggestion] {
        let normalizedPrefix = normalize(prefix)
        guard normalizedPrefix.count >= minPrefixLength else {
            return []
        }

        let prefixSubspace = subspace
            .subspace("suggestions")
            .subspace(field)
            .subspace(normalizedPrefix)
        let (begin, end) = prefixSubspace.range()
        var suggestions: [AutocompleteSuggestion] = []

        let entries = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        for (key, value) in entries {
            guard prefixSubspace.contains(key) else { break }
            let term = try FullTextStorageDecoder.autocompleteSuggestionTerm(
                from: key,
                in: prefixSubspace,
                field: field,
                prefix: normalizedPrefix
            )
            let score = try ByteConversion.bytesToInt64(value)
            if score > 0 {
                suggestions.append(
                    AutocompleteSuggestion(term: term, score: score)
                )
            }
        }
        suggestions.sort {
            if $0.score == $1.score {
                return $0.term < $1.term
            }
            return $0.score > $1.score
        }
        return Array(suggestions.prefix(limit))
    }

    func popularTerms(
        field: String,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [AutocompleteSuggestion] {
        let termsSubspace = subspace.subspace("terms").subspace(field)
        let (begin, end) = termsSubspace.range()
        var terms: [AutocompleteSuggestion] = []

        let entries = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        for (key, value) in entries {
            guard termsSubspace.contains(key) else { break }
            let term = try FullTextStorageDecoder.autocompleteTerm(
                from: key,
                in: termsSubspace,
                field: field
            )
            let score = try ByteConversion.bytesToInt64(value)
            if score > 0 {
                terms.append(AutocompleteSuggestion(term: term, score: score))
            }
        }
        terms.sort {
            if $0.score == $1.score {
                return $0.term < $1.term
            }
            return $0.score > $1.score
        }
        return Array(terms.prefix(limit))
    }

    private func normalize(_ text: String) -> String {
        String(
            FullTextTextUtilities.trimmingWhitespace(text.lowercased())
        )
    }
}
