import StorageKit

enum FullTextStorageDecoder {
    static func posting(
        from value: Bytes,
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
            for index in 1..<tuple.count {
                guard let rawPosition = tuple[index] as? Int64,
                      let position = Int(exactly: rawPosition),
                      position >= 0 else {
                    throw FullTextStorageError.corruptedPosting(term: term)
                }
                positions.append(position)
            }
            return (termFrequency: termFrequency, positions: positions)
        } catch let error as FullTextStorageError {
            throw error
        } catch {
            throw FullTextStorageError.corruptedPosting(term: term)
        }
    }

    static func documentMetadata(
        from value: Bytes
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
        from key: Bytes,
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

    static func documentFacetValues(from value: Bytes, field: String) throws -> [String] {
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
        from key: Bytes,
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
        from key: Bytes,
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
