import DatabaseEngine
import DatabaseKit
import StorageKit

public struct AutocompleteQueryBuilder<Item: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private var field: FieldIdentity?
    private var searchPrefix = ""
    private var fetchLimit = 10

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    public func field(_ field: Field<Item, String>) -> Self {
        selecting(field.identity)
    }

    public func field(_ field: Field<Item, String?>) -> Self {
        selecting(field.identity)
    }

    public func field(_ field: Field<Item, [String]>) -> Self {
        selecting(field.identity)
    }

    public func field(_ field: Field<Item, [String]?>) -> Self {
        selecting(field.identity)
    }

    public func prefix(_ prefix: String) -> Self {
        var copy = self
        copy.searchPrefix = prefix
        return copy
    }

    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.fetchLimit = count
        return copy
    }

    public func execute() async throws -> [AutocompleteSuggestion] {
        let resolved = try resolveIndex()
        guard fetchLimit >= 0 else {
            throw AutocompleteError.invalidLimit(fetchLimit)
        }
        return try await queryContext.withReadableIndex(
            named: resolved.descriptor.name,
            indexType: resolved.descriptor.type,
            for: Item.self,
            authorization: IndexReadAuthorization(
                limit: fetchLimit,
                offset: nil,
                orderBy: nil
            )
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            let reader = AutocompleteIndexReader(
                subspace: readableIndex.subspace,
                minPrefixLength: resolved.configuration.minPrefixLength
            )
            return try await reader.suggestions(
                field: resolved.field.name,
                prefix: searchPrefix,
                limit: fetchLimit,
                transaction: transaction
            )
        }
    }

    public func popularTerms() async throws -> [AutocompleteSuggestion] {
        let resolved = try resolveIndex()
        guard fetchLimit >= 0 else {
            throw AutocompleteError.invalidLimit(fetchLimit)
        }
        return try await queryContext.withReadableIndex(
            named: resolved.descriptor.name,
            indexType: resolved.descriptor.type,
            for: Item.self,
            authorization: IndexReadAuthorization(
                limit: fetchLimit,
                offset: nil,
                orderBy: nil
            )
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            let reader = AutocompleteIndexReader(
                subspace: readableIndex.subspace,
                minPrefixLength: resolved.configuration.minPrefixLength
            )
            return try await reader.popularTerms(
                field: resolved.field.name,
                limit: fetchLimit,
                transaction: transaction
            )
        }
    }

    private func selecting(_ identity: FieldIdentity) -> Self {
        var copy = self
        copy.field = identity
        return copy
    }

    private func resolveIndex() throws -> (
        descriptor: IndexDescriptor,
        configuration: AutocompleteIndexConfiguration,
        field: FieldIdentity
    ) {
        guard let field else {
            throw AutocompleteError.noFieldSpecified
        }
        let matches = queryContext.indexDescriptors(for: Item.self).filter {
            $0.type == .text(.autocomplete)
                && $0.fieldIdentities.contains(field)
        }
        guard let descriptor = matches.first else {
            throw AutocompleteError.indexNotFound(
                entity: Item.persistableType,
                field: field.name
            )
        }
        guard matches.count == 1 else {
            throw AutocompleteError.ambiguousIndex(
                entity: Item.persistableType,
                field: field.name
            )
        }
        return (
            descriptor,
            try AutocompleteIndexConfiguration(definition: descriptor.declaration.definition),
            field
        )
    }
}

extension DatabaseContext {
    public func autocomplete<Item: Persistable>(
        _ type: Item.Type
    ) -> AutocompleteQueryBuilder<Item> {
        AutocompleteQueryBuilder(queryContext: indexQueryContext)
    }
}

public enum AutocompleteError: Error, Sendable {
    case noFieldSpecified
    case indexNotFound(entity: String, field: String)
    case ambiguousIndex(entity: String, field: String)
    case invalidLimit(Int)
    case invalidIndexConfiguration
}
