import DatabaseKit
import DatabaseTypes

/// A decoded polymorphic row returned from a logical polymorphic query.
public struct PolymorphicQueryResult: Sendable {
    public let model: PersistedModel
    public let typeName: String
    public let typeCode: Int64
    public let row: QueryRow

    public init(
        model: PersistedModel,
        typeName: String,
        typeCode: Int64,
        row: QueryRow
    ) {
        self.model = model
        self.typeName = typeName
        self.typeCode = typeCode
        self.row = row
    }

    public var fields: [String: FieldValue] { row.fields }
    public var annotations: [String: FieldValue] { row.annotations }

    public func decode<Concrete: Persistable>(
        as type: Concrete.Type
    ) throws -> Concrete {
        guard typeName == Concrete.persistableType else {
            throw PolymorphicQueryError.unexpectedType(
                expected: Concrete.persistableType,
                actual: typeName
            )
        }
        return try model.decode(as: type)
    }

    /// Decodes this result only when its concrete persisted type matches.
    public func decodedModel<Concrete: Persistable>(
        as type: Concrete.Type
    ) throws -> Concrete? {
        guard typeName == Concrete.persistableType else { return nil }
        return try model.decode(as: type)
    }
}

/// A decoded page returned from a polymorphic query execution.
public struct PolymorphicQueryPage: Sendable {
    public let results: [PolymorphicQueryResult]
    public let continuation: QueryContinuation?
    public let metadata: [String: FieldValue]

    public init(
        results: [PolymorphicQueryResult],
        continuation: QueryContinuation?,
        metadata: [String: FieldValue]
    ) {
        self.results = results
        self.continuation = continuation
        self.metadata = metadata
    }
}

public enum PolymorphicQueryError: Error, Sendable, CustomStringConvertible {
    case missingTypeName
    case unknownType(String)
    case unexpectedType(expected: String, actual: String)

    public var description: String {
        switch self {
        case .missingTypeName:
            return "Polymorphic query row is missing the _typeName annotation."
        case .unknownType(let typeName):
            return "Polymorphic query row references unknown schema type '\(typeName)'."
        case .unexpectedType(let expected, let actual):
            return "Expected polymorphic type '\(expected)' but received '\(actual)'."
        }
    }
}

/// Developer-facing builder for querying a polymorphic logical source.
///
/// Use any concrete type that conforms to the polymorphic protocol. Swift cannot
/// pass existential protocol metatypes such as `Document.self` into this generic
/// API, but every conforming type maps to the same logical group.
public struct PolymorphicQuery<Member: Persistable & Polymorphable>: Sendable {
    private let context: DatabaseContext
    private let groupIdentifier: String
    private var limit: UInt64?
    private var offset: UInt64?
    private var orderBy: [SortKey] = []
    private var options: ReadExecutionOptions = .default

    internal init(context: DatabaseContext, groupIdentifier: String) {
        self.context = context
        self.groupIdentifier = groupIdentifier
    }

    /// The logical polymorphic group identifier.
    public var identifier: String { groupIdentifier }

    /// The current result limit, if configured.
    public var limitCount: UInt64? { limit }

    /// The current result offset, if configured.
    public var offsetCount: UInt64? { offset }

    /// The current ORDER BY keys, if configured.
    public var sortKeys: [SortKey]? { orderBy.isEmpty ? nil : orderBy }

    /// The current canonical read options.
    public var executionOptions: ReadExecutionOptions { options }

    /// Limit the number of results returned by the logical source.
    public func limit(_ count: UInt64) -> Self {
        var copy = self
        copy.limit = count
        return copy
    }

    /// Skip the first N results from the logical source.
    public func offset(_ count: UInt64) -> Self {
        var copy = self
        copy.offset = count
        return copy
    }

    /// Append an ORDER BY clause using a compiled field on the member type.
    ///
    /// The field should be shared across the polymorphic
    /// group. The runtime still validates against actual row data.
    public func orderBy<Value>(
        _ field: Field<Member, Value>,
        direction: SortDirection = .ascending,
        nulls: NullOrdering? = nil
    ) -> Self {
        var copy = self
        copy.orderBy.append(
            SortKey(
                .column(ColumnRef(column: field.name)),
                direction: direction,
                nulls: nulls
            )
        )
        return copy
    }

    /// Set the canonical read consistency.
    public func consistency(_ consistency: ReadConsistency?) -> Self {
        var copy = self
        copy.options = ReadExecutionOptions(
            consistency: consistency,
            pageSize: copy.options.pageSize,
            continuation: copy.options.continuation,
            budget: copy.options.budget,
            continuationScope: copy.options.continuationScope,
            continuationSnapshotIsStable:
                copy.options.continuationSnapshotIsStable
        )
        return copy
    }

    /// Set the canonical page size.
    public func pageSize(_ pageSize: Int?) -> Self {
        var copy = self
        copy.options = ReadExecutionOptions(
            consistency: copy.options.consistency,
            pageSize: pageSize,
            continuation: copy.options.continuation,
            budget: copy.options.budget,
            continuationScope: copy.options.continuationScope,
            continuationSnapshotIsStable:
                copy.options.continuationSnapshotIsStable
        )
        return copy
    }

    /// Continue from a previous canonical continuation token.
    public func continuing(from continuation: QueryContinuation?) -> Self {
        var copy = self
        copy.options = ReadExecutionOptions(
            consistency: copy.options.consistency,
            pageSize: copy.options.pageSize,
            continuation: continuation,
            budget: copy.options.budget,
            continuationScope: copy.options.continuationScope,
            continuationSnapshotIsStable:
                copy.options.continuationSnapshotIsStable
        )
        return copy
    }

    /// Build the canonical query for this polymorphic logical source.
    public func makeSelectQuery() -> SelectQuery {
        makeSelectQuery(accessPath: nil)
    }

    /// Build the canonical query for this polymorphic logical source.
    @_spi(PolymorphicRuntime)
    public func makeSelectQuery(accessPath: AccessPath?) -> SelectQuery {
        SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: LogicalSourceKind.polymorphic,
                    identifier: groupIdentifier
                )
            ),
            accessPath: accessPath,
            orderBy: orderBy.isEmpty ? nil : orderBy,
            limit: limit,
            offset: offset
        )
    }

    /// Execute the logical query and return canonical rows.
    public func query() async throws -> QueryResponse {
        try await query(accessPath: nil)
    }

    /// Execute the logical query and return canonical rows.
    @_spi(PolymorphicRuntime)
    public func query(accessPath: AccessPath?) async throws -> QueryResponse {
        try await context.query(
            makeSelectQuery(accessPath: accessPath),
            options: options
        )
    }

    /// Execute the logical query and decode polymorphic rows into concrete items.
    public func executePage() async throws -> PolymorphicQueryPage {
        try await executePage(resolvingAccessPath: { _ in nil })
    }

    /// Resolves an access path, executes it, and decodes its rows under one
    /// immutable schema generation.
    @_spi(PolymorphicRuntime)
    public func executePage(
        resolvingAccessPath: @Sendable (Schema) throws -> AccessPath?
    ) async throws -> PolymorphicQueryPage {
        try await context.withDataOperation { [self] in
            let schema = try context.readPolicy().schema
            let response = try await query(
                accessPath: try resolvingAccessPath(schema)
            )
            return try decodePage(
                from: response,
                schema: schema
            )
        }
    }

    /// Execute the logical query and return decoded results.
    public func execute() async throws -> [PolymorphicQueryResult] {
        try await executePage().results
    }

    /// Execute the logical query and return the first decoded result.
    public func first() async throws -> PolymorphicQueryResult? {
        try await executePage().results.first
    }

    /// Execute the logical query and return the first decoded result.
    @_spi(PolymorphicRuntime)
    public func first(accessPath: AccessPath?) async throws -> PolymorphicQueryResult? {
        try await executePage { _ in accessPath }.results.first
    }

    private func decodePage(
        from response: QueryResponse,
        schema: Schema
    ) throws -> PolymorphicQueryPage {
        PolymorphicQueryPage(
            results: try response.rows.map {
                try decodeResult(from: $0, schema: schema)
            },
            continuation: response.continuation,
            metadata: response.metadata
        )
    }

    private func decodeResult(
        from row: QueryRow,
        schema: Schema
    ) throws -> PolymorphicQueryResult {
        guard let typeName = row.annotations[PolymorphicRowAnnotation.typeName]?.stringValue else {
            throw PolymorphicQueryError.missingTypeName
        }

        guard let entity = schema.entity(named: typeName) else {
            throw PolymorphicQueryError.unknownType(typeName)
        }

        let model = try QueryRowCodec.persistedModel(
            from: row,
            entity: entity
        )
        let typeCode = row.annotations[PolymorphicRowAnnotation.typeCode]?.int64Value
            ?? Member.typeCode(for: typeName)

        return PolymorphicQueryResult(
            model: model,
            typeName: typeName,
            typeCode: typeCode,
            row: row
        )
    }

    /// Resolve a shared index name from polymorphic group metadata.
    @_spi(PolymorphicRuntime)
    public func resolveIndexName(
        indexType: IndexType,
        fieldName: String,
        in schema: Schema
    ) throws -> String? {
        guard let group = schema.polymorphicGroup(
            identifier: groupIdentifier
        ) else {
            throw PolymorphicRuntimeError.missingGroup(
                identifier: groupIdentifier
            )
        }
        return group.indexes.first { descriptor in
            descriptor.type == indexType
                && descriptorFields(descriptor.fieldNames, contain: fieldName)
        }?.name
    }

    private func descriptorFields(_ fieldNames: [String], contain fieldName: String) -> Bool {
        fieldNames.contains { descriptorFieldName in
            descriptorFieldName == fieldName
                || normalizedDescriptorFieldName(descriptorFieldName) == fieldName
        }
    }

    private func normalizedDescriptorFieldName(_ fieldName: String) -> String {
        guard fieldName.hasPrefix("\\"),
              let rootSeparatorIndex = fieldName.firstIndex(of: ".") else {
            return fieldName
        }

        let fieldStartIndex = fieldName.index(after: rootSeparatorIndex)
        return String(fieldName[fieldStartIndex...])
    }
}

extension DatabaseContext {
    /// Start a polymorphic logical query using any concrete conforming type.
    ///
    /// All conforming types share the same logical group, so `Article.self` and
    /// `Report.self` produce the same source when both conform to `Document`.
    public func findPolymorphic<Member: Persistable & Polymorphable>(
        _ memberType: Member.Type
    ) -> PolymorphicQuery<Member> {
        PolymorphicQuery(
            context: self,
            groupIdentifier: Member.polymorphableType
        )
    }
}
