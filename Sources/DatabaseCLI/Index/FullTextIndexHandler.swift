import Foundation
import FoundationDB

/// Handler for full-text indexes (BM25 ranking, phrase search)
///
/// Storage layout:
/// - terms/<term>/<docId> = positions (if enabled) or empty
/// - docfreq/<term> = document frequency count
/// - doclen/<docId> = document length
public struct FullTextIndexHandler: IndexHandler, Sendable {
    public let indexDefinition: IndexDefinition
    public let schemaName: String

    public init(indexDefinition: IndexDefinition, schemaName: String) {
        self.indexDefinition = indexDefinition
        self.schemaName = schemaName
    }

    public func updateIndex(
        oldItem: [String: Any]?,
        newItem: [String: Any]?,
        id: String,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws {
        guard let config = indexDefinition.config,
              case .fulltext(let ftConfig) = config else {
            return
        }

        let field = indexDefinition.fields.first ?? ""
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .fulltext,
            indexName: indexDefinition.name
        )

        let termsSubspace = indexSubspace.subspace(Tuple(["terms"]))
        let docFreqSubspace = indexSubspace.subspace(Tuple(["docfreq"]))
        let docLenSubspace = indexSubspace.subspace(Tuple(["doclen"]))

        let oldText = oldItem?[field] as? String
        let newText = newItem?[field] as? String

        // Remove old terms
        if let text = oldText {
            let oldTerms = tokenize(text, config: ftConfig)
            let uniqueTerms = Set(oldTerms.map { $0.term })

            for term in uniqueTerms {
                let termKey = termsSubspace.pack(Tuple([term, id]))
                transaction.clear(key: termKey)

                // Decrement document frequency (read-modify-write)
                let freqKey = docFreqSubspace.pack(Tuple([term]))
                if let existingBytes = try await transaction.getValue(for: freqKey, snapshot: false) {
                    let currentCount = unpackInt64(existingBytes)
                    let newCount = max(0, currentCount - 1)
                    if newCount > 0 {
                        transaction.setValue(packInt64(newCount), for: freqKey)
                    } else {
                        transaction.clear(key: freqKey)
                    }
                }
            }

            // Remove document length
            let lenKey = docLenSubspace.pack(Tuple([id]))
            transaction.clear(key: lenKey)
        }

        // Add new terms
        if let text = newText {
            let newTerms = tokenize(text, config: ftConfig)
            let uniqueTerms = Set(newTerms.map { $0.term })

            for term in uniqueTerms {
                let positions = newTerms.filter { $0.term == term }.map { $0.position }

                let termKey = termsSubspace.pack(Tuple([term, id]))
                if ftConfig.storePositions {
                    let posData = packPositions(positions)
                    transaction.setValue(posData, for: termKey)
                } else {
                    transaction.setValue([], for: termKey)
                }

                // Increment document frequency (read-modify-write)
                let freqKey = docFreqSubspace.pack(Tuple([term]))
                let currentCount: Int64
                if let existingBytes = try await transaction.getValue(for: freqKey, snapshot: false) {
                    currentCount = unpackInt64(existingBytes)
                } else {
                    currentCount = 0
                }
                transaction.setValue(packInt64(currentCount + 1), for: freqKey)
            }

            // Store document length
            let lenKey = docLenSubspace.pack(Tuple([id]))
            transaction.setValue(packInt64(Int64(newTerms.count)), for: lenKey)
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let config = indexDefinition.config,
              case .fulltext(let ftConfig) = config else {
            return []
        }

        guard let textQuery = query as? FullTextQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .fulltext,
            indexName: indexDefinition.name
        )
        let termsSubspace = indexSubspace.subspace(Tuple(["terms"]))

        // Tokenize query
        let queryTerms = tokenize(textQuery.text, config: ftConfig)
        guard !queryTerms.isEmpty else { return [] }

        // For simple search: find documents containing all terms
        var documentSets: [Set<String>] = []

        for term in Set(queryTerms.map { $0.term }) {
            let termSubspace = termsSubspace.subspace(Tuple([term]))
            let (begin, end) = termSubspace.range()

            var docIds = Set<String>()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                if let tuple = try? termSubspace.unpack(key),
                   let docId = tuple[0] as? String {
                    docIds.insert(docId)
                }
            }
            documentSets.append(docIds)
        }

        // Intersect all document sets (AND logic)
        guard var resultSet = documentSets.first else { return [] }
        for set in documentSets.dropFirst() {
            resultSet = resultSet.intersection(set)
        }

        return Array(resultSet.prefix(limit))
    }

    // MARK: - Tokenization

    private struct Token {
        let term: String
        let position: Int
    }

    private func tokenize(_ text: String, config: FullTextIndexConfig) -> [Token] {
        var tokens: [Token] = []
        var position = 0

        let words = text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }

        for word in words {
            let processedTerms: [String]

            switch config.tokenizer {
            case .simple:
                processedTerms = [word]
            case .stem:
                processedTerms = [stem(word)]
            case .ngram:
                let k = config.ngramK ?? 3
                processedTerms = ngrams(word, k: k)
            case .keyword:
                processedTerms = [word]
            }

            for term in processedTerms {
                tokens.append(Token(term: term, position: position))
            }
            position += 1
        }

        return tokens
    }

    private func stem(_ word: String) -> String {
        // Simple Porter-like stemming
        var stem = word

        // Remove common suffixes
        let suffixes = ["ing", "ed", "es", "s", "ly", "ment", "ness", "tion", "ation"]
        for suffix in suffixes {
            if stem.hasSuffix(suffix) && stem.count > suffix.count + 2 {
                stem = String(stem.dropLast(suffix.count))
                break
            }
        }

        return stem
    }

    private func ngrams(_ word: String, k: Int) -> [String] {
        guard word.count >= k else { return [word] }

        var result: [String] = []
        let chars = Array(word)

        for i in 0...(chars.count - k) {
            result.append(String(chars[i..<i+k]))
        }

        return result
    }

    // MARK: - Serialization

    private func packPositions(_ positions: [Int]) -> FDB.Bytes {
        var bytes: [UInt8] = []
        for pos in positions {
            var p = Int32(pos)
            let posBytes = withUnsafeBytes(of: &p) { Array($0) }
            bytes.append(contentsOf: posBytes)
        }
        return bytes
    }

    private func packInt64(_ value: Int64) -> FDB.Bytes {
        var v = value
        return withUnsafeBytes(of: &v) { Array($0) }
    }

    private func unpackInt64(_ bytes: FDB.Bytes) -> Int64 {
        guard bytes.count >= 8 else { return 0 }
        return bytes.withUnsafeBytes { $0.load(as: Int64.self) }
    }
}

// MARK: - Full Text Query

public struct FullTextQuery {
    public let text: String
    public let phrase: Bool
    public let fuzzy: Int?

    public init(text: String, phrase: Bool = false, fuzzy: Int? = nil) {
        self.text = text
        self.phrase = phrase
        self.fuzzy = fuzzy
    }
}
