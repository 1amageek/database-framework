import DatabaseEngine
import DatabaseTypes
import StorageKit

enum FullTextStorageDecoder {
    /// Decodes only the posting frequency without materializing a Tuple or a
    /// positions array.
    static func postingFrequency(
        from value: ByteString,
        positionsStored: Bool,
        term: String,
        workMeter: DatabaseWorkMeter
    ) throws -> Int {
        do {
            var cursor = TupleCursor(bytes: value)
            guard let frequency = Int(exactly: try cursor.requireInt64()),
                  frequency > 0,
                  (!positionsStored || frequency <= value.count) else {
                throw FullTextStorageError.corruptedPosting(term: term)
            }
            var positionCount = 0
            var previousPosition: Int64?
            while !cursor.isAtEnd {
                guard positionCount < frequency else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
                try workMeter.consume(at: .indexScan)
                let position = try cursor.requireInt64()
                guard positionsStored,
                      position >= 0,
                      previousPosition.map({ $0 < position }) ?? true else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
                previousPosition = position
                positionCount += 1
            }
            guard (!positionsStored && positionCount == 0)
                    || (positionsStored && positionCount == frequency) else {
                throw FullTextStorageError.corruptedPosting(term: term)
            }
            return frequency
        } catch let error as FullTextStorageError {
            throw error
        } catch is TupleError {
            throw FullTextStorageError.corruptedPosting(term: term)
        }
    }

    /// Decodes positions after the caller has admitted storage proportional to
    /// the encoded value. The cursor borrows the immutable value bytes.
    static func postingPositions(
        from value: ByteString,
        term: String,
        workMeter: DatabaseWorkMeter
    ) throws -> [Int] {
        var positions: [Int] = []
        positions.reserveCapacity(value.count)
        _ = try postingPositions(
            from: value,
            term: term,
            into: &positions,
            workMeter: workMeter
        )
        return positions
    }

    /// Fills caller-owned scratch whose capacity was admitted before this
    /// function. Existing capacity is retained across posting decodes.
    static func postingPositions(
        from value: ByteString,
        term: String,
        into positions: inout [Int],
        workMeter: DatabaseWorkMeter
    ) throws -> Int {
        do {
            var cursor = TupleCursor(bytes: value)
            guard let frequency = Int(exactly: try cursor.requireInt64()),
                  frequency > 0,
                  frequency <= value.count else {
                throw FullTextStorageError.corruptedPosting(term: term)
            }
            positions.removeAll(keepingCapacity: true)
            var previousPosition: Int?
            while !cursor.isAtEnd {
                guard positions.count < frequency else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
                try workMeter.consume(at: .indexScan)
                guard let position = Int(exactly: try cursor.requireInt64()),
                      position >= 0,
                      previousPosition.map({ $0 < position }) ?? true else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
                positions.append(position)
                previousPosition = position
            }
            guard positions.count == frequency else {
                throw FullTextStorageError.corruptedPosting(term: term)
            }
            return frequency
        } catch let error as FullTextStorageError {
            throw error
        } catch is TupleError {
            throw FullTextStorageError.corruptedPosting(term: term)
        }
    }

    /// Decodes fixed-width document metadata without allocating a Tuple.
    static func documentMetadataCursor(
        from value: ByteString
    ) throws -> (uniqueTermCount: Int64, docLength: Int64) {
        do {
            var cursor = TupleCursor(bytes: value)
            let uniqueTermCount = try cursor.requireInt64()
            let docLength = try cursor.requireInt64()
            guard
                  cursor.isAtEnd,
                  uniqueTermCount >= 0,
                  docLength >= 0 else {
                throw FullTextStorageError.corruptedDocumentMetadata
            }
            return (uniqueTermCount, docLength)
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedDocumentMetadata
        }
    }

    static func posting(
        from value: ByteString,
        positionsStored: Bool,
        term: String
    ) throws -> (termFrequency: Int, positions: [Int]) {
        do {
            let tuple = try Tuple.unpack(from: value)
            guard let rawFrequency = tuple.first as? Int64,
                  let termFrequency = Int(exactly: rawFrequency),
                  termFrequency > 0 else {
                throw FullTextStorageError.corruptedPosting(term: term)
            }

            if positionsStored {
                guard tuple.count == termFrequency + 1 else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
            } else {
                guard tuple.count == 1 else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
            }

            var positions: [Int] = []
            positions.reserveCapacity(tuple.count - 1)
            var previousPosition: Int?
            for index in 1..<tuple.count {
                guard let rawPosition = tuple[index] as? Int64,
                      let position = Int(exactly: rawPosition),
                      position >= 0,
                      previousPosition.map({ $0 < position }) ?? true else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
                positions.append(position)
                previousPosition = position
            }
            return (termFrequency: termFrequency, positions: positions)
        } catch let error as FullTextStorageError {
            throw error
        } catch {
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
