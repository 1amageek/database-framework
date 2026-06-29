import StorageKit

enum FullTextStorageDecoder {
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
