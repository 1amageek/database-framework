import DatabaseTypes
import StorageKit

enum FullTextStorageDecoder {
    static func termFrequency(
        from value: ByteString,
        positionsStored: Bool,
        term: String
    ) throws -> Int {
        var decoded = try postingCursor(
            from: value,
            term: term
        )
        try validatePostingTail(
            cursor: &decoded.cursor,
            expectedPositionCount: positionsStored ? decoded.termFrequency : 0,
            term: term,
            consume: { _ in }
        )
        return decoded.termFrequency
    }

    static func posting(
        from value: ByteString,
        positionsStored: Bool,
        term: String
    ) throws -> (termFrequency: Int, positions: [Int]) {
        try posting(
            from: value,
            positionsStored: positionsStored,
            term: term,
            reservingPositionsWith: { _ in }
        )
    }

    static func posting(
        from value: ByteString,
        positionsStored: Bool,
        term: String,
        reservingPositionsWith reservePositions: (Int) throws -> Void
    ) throws -> (termFrequency: Int, positions: [Int]) {
        var decoded = try postingCursor(
            from: value,
            term: term
        )
        let positionCount = positionsStored ? decoded.termFrequency : 0
        try reservePositions(positionCount)
        var positions: [Int] = []
        positions.reserveCapacity(positionCount)
        try validatePostingTail(
            cursor: &decoded.cursor,
            expectedPositionCount: positionCount,
            term: term
        ) { position in
            positions.append(position)
        }
        return (
            termFrequency: decoded.termFrequency,
            positions: positions
        )
    }

    private static func postingCursor(
        from value: ByteString,
        term: String
    ) throws -> (cursor: TupleCursor, termFrequency: Int) {
        var cursor = TupleCursor(bytes: value)
        let rawFrequency: Int64
        do {
            guard let decodedFrequency = try cursor.next() as? Int64 else {
                throw FullTextStorageError.corruptedPosting(term: term)
            }
            rawFrequency = decodedFrequency
        } catch {
            throw FullTextStorageError.corruptedPosting(term: term)
        }

        guard let termFrequency = Int(exactly: rawFrequency),
              termFrequency > 0 else {
            throw FullTextStorageError.corruptedPosting(term: term)
        }
        return (cursor: cursor, termFrequency: termFrequency)
    }

    private static func validatePostingTail(
        cursor: inout TupleCursor,
        expectedPositionCount: Int,
        term: String,
        consume: (Int) throws -> Void
    ) throws {
        var previousPosition: Int?
        var decodedPositionCount = 0
        do {
            while let element = try cursor.next() {
                guard decodedPositionCount < expectedPositionCount,
                      let rawPosition = element as? Int64,
                  let position = Int(exactly: rawPosition),
                  position >= 0,
                  previousPosition.map({ position > $0 }) ?? true else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
                try consume(position)
                previousPosition = position
                decodedPositionCount += 1
            }
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedPosting(term: term)
        }
        guard decodedPositionCount == expectedPositionCount else {
            throw FullTextStorageError.corruptedPosting(term: term)
        }
    }

    static func documentMetadata(
        from value: ByteString
    ) throws -> (uniqueTermCount: Int64, docLength: Int64) {
        do {
            let tuple = try Tuple.unpack(from: value)
            guard tuple.count == 2,
                  let uniqueTermCount = tuple[0] as? Int64,
                  let docLength = tuple[1] as? Int64,
                  uniqueTermCount >= 0,
                  docLength >= 0 else {
                throw FullTextStorageError.corruptedDocumentMetadata
            }
            return (uniqueTermCount: uniqueTermCount, docLength: docLength)
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedDocumentMetadata
        }
    }

    static func facetValue(
        from key: ByteString,
        in subspace: Subspace,
        field: String
    ) throws -> String {
        do {
            let tuple = try subspace.unpack(key)
            guard let value = tuple[0] as? String else {
                throw FullTextStorageError.corruptedFacetKey(field: field)
            }
            return value
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedFacetKey(field: field)
        }
    }

    static func documentFacetValues(from value: ByteString, field: String) throws -> [String] {
        do {
            let tuple = try Tuple.unpack(from: value)
            var values: [String] = []
            values.reserveCapacity(tuple.count)
            for index in 0..<tuple.count {
                guard let value = tuple[index] as? String else {
                    throw FullTextStorageError.corruptedDocumentFacetValues(field: field)
                }
                values.append(value)
            }
            return values
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedDocumentFacetValues(field: field)
        }
    }

    static func autocompleteSuggestionTerm(
        from key: ByteString,
        in subspace: Subspace,
        field: String,
        prefix: String
    ) throws -> String {
        do {
            let tuple = try subspace.unpack(key)
            guard let term = tuple[0] as? String else {
                throw FullTextStorageError.corruptedAutocompleteSuggestionKey(
                    field: field,
                    prefix: prefix
                )
            }
            return term
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedAutocompleteSuggestionKey(
                field: field,
                prefix: prefix
            )
        }
    }

    static func autocompleteTerm(
        from key: ByteString,
        in subspace: Subspace,
        field: String
    ) throws -> String {
        do {
            let tuple = try subspace.unpack(key)
            guard let term = tuple[0] as? String else {
                throw FullTextStorageError.corruptedAutocompleteTermKey(field: field)
            }
            return term
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedAutocompleteTermKey(field: field)
        }
    }
}
