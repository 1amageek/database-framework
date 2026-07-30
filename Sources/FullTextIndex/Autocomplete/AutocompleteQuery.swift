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
        let subspace = try await queryContext.indexSubspace(for: Item.self)
            .subspace(resolved.descriptor.name)
        let reader = AutocompleteIndexReader(
            subspace: subspace,
            minPrefixLength: resolved.configuration.minPrefixLength
        )
        return try await queryContext.withTransaction { transaction in
            try await reader.suggestions(
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
        let subspace = try await queryContext.indexSubspace(for: Item.self)
            .subspace(resolved.descriptor.name)
        let reader = AutocompleteIndexReader(
            subspace: subspace,
            minPrefixLength: resolved.configuration.minPrefixLength
        )
        return try await queryContext.withTransaction { transaction in
            try await reader.popularTerms(
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
        let matches = try Item.indexDescriptors.filter {
            $0.kindIdentifier == "autocomplete"
                && $0.kind.fields.contains(where: { $0.identity == field })
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
            try AutocompleteIndexConfiguration(metadata: descriptor.kind),
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
