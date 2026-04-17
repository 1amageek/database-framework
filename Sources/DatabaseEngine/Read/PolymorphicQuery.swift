import Foundation
import Core
import QueryIR
import DatabaseClientProtocol

/// A decoded polymorphic row returned from a logical polymorphic query.
public struct PolymorphicQueryResult: Sendable {
    public let item: any Persistable
    public let typeName: String
    public let typeCode: Int64
    public let row: QueryRow

    public init(
        item: any Persistable,
        typeName: String,
        typeCode: Int64,
        row: QueryRow
    ) {
        self.item = item
        self.typeName = typeName
        self.typeCode = typeCode
        self.row = row
    }

    public var fields: [String: FieldValue] { row.fields }
    public var annotations: [String: FieldValue] { row.annotations }

    public func item<Concrete: Persistable>(as type: Concrete.Type) -> Concrete? {
        item as? Concrete
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

    public var description: String {
        switch self {
        case .missingTypeName:
            return "Polymorphic query row is missing the _typeName annotation."
        case .unknownType(let typeName):
            return "Polymorphic query row references unknown schema type '\(typeName)'."
        }
    }
}

/// Developer-facing builder for querying a polymorphic logical source.
///
/// Use any concrete type that conforms to the polymorphic protocol. Swift cannot
/// pass existential protocol metatypes such as `Document.self` into this generic
/// API, but every conforming type maps to the same logical group.
public struct PolymorphicQuery<Member: Persistable & Polymorphable>: Sendable {
    private let context: FDBContext
    private let groupIdentifier: String
    private var limit: Int?
    private var offset: Int?
    private var orderBy: [SortKey] = []
    private var options: ReadExecutionOptions = .default

    internal init(context: FDBContext, groupIdentifier: String) {
        self.context = context
        self.groupIdentifier = groupIdentifier
    }

    /// The logical polymorphic group identifier.
    public var identifier: String { groupIdentifier }

    /// The current result limit, if configured.
    public var limitCount: Int? { limit }

    /// The current result offset, if configured.
    public var offsetCount: Int? { offset }

    /// The current ORDER BY keys, if configured.
    public var sortKeys: [SortKey]? { orderBy.isEmpty ? nil : orderBy }

    /// The current canonical read options.
    public var executionOptions: ReadExecutionOptions { options }

    /// Limit the number of results returned by the logical source.
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limit = count
        return copy
    }

    /// Skip the first N results from the logical source.
    public func offset(_ count: Int) -> Self {
        var copy = self
        copy.offset = count
        return copy
    }

    /// Append an ORDER BY clause using a shared field name.
    public func orderBy(
        _ fieldName: String,
        direction: SortDirection = .ascending,
        nulls: NullOrdering? = nil
    ) -> Self {
        var copy = self
        copy.orderBy.append(
            SortKey(
                .column(ColumnRef(column: fieldName)),
                direction: direction,
                nulls: nulls
            )
        )
        return copy
    }

    /// Append an ORDER BY clause using a key path on the concrete member type.
    ///
    /// The key path should refer to a field that is shared across the polymorphic
    /// group. The runtime still validates against actual row data.
    public func orderBy<Value>(
        _ keyPath: KeyPath<Member, Value>,
        direction: SortDirection = .ascending,
        nulls: NullOrdering? = nil
    ) -> Self {
        orderBy(Member.fieldName(for: keyPath), direction: direction, nulls: nulls)
    }

    /// Set the canonical read consistency.
    public func consistency(_ consistency: ReadConsistency?) -> Self {
        var copy = self
        copy.options = ReadExecutionOptions(
            consistency: consistency,
            pageSize: copy.options.pageSize,
            continuation: copy.options.continuation
        )
        return copy
    }

    /// Set the canonical page size.
    public func pageSize(_ pageSize: Int?) -> Self {
        var copy = self
        copy.options = ReadExecutionOptions(
            consistency: copy.options.consistency,
            pageSize: pageSize,
            continuation: copy.options.continuation
        )
        return copy
    }

    /// Continue from a previous canonical continuation token.
    public func continuing(from continuation: QueryContinuation?) -> Self {
        var copy = self
        copy.options = ReadExecutionOptions(
            consistency: copy.options.consistency,
            pageSize: copy.options.pageSize,
            continuation: continuation
        )
        return copy
    }

    /// Build the canonical query for this polymorphic logical source.
    public func makeSelectQuery(accessPath: AccessPath? = nil) -> SelectQuery {
        SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
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
    public func query(accessPath: AccessPath? = nil) async throws -> QueryResponse {
        try await context.query(
            makeSelectQuery(accessPath: accessPath),
            options: options
        )
    }

    /// Execute the logical query and decode polymorphic rows into concrete items.
    public func executePage(accessPath: AccessPath? = nil) async throws -> PolymorphicQueryPage {
        let response = try await query(accessPath: accessPath)
        return try decodePage(from: response)
    }

    /// Execute the logical query and return decoded results.
    public func execute() async throws -> [PolymorphicQueryResult] {
        try await executePage().results
    }

    /// Execute the logical query and return the first decoded result.
    public func first(accessPath: AccessPath? = nil) async throws -> PolymorphicQueryResult? {
        try await executePage(accessPath: accessPath).results.first
    }

    /// Decode an existing canonical response produced by this logical source.
    public func decodePage(from response: QueryResponse) throws -> PolymorphicQueryPage {
        PolymorphicQueryPage(
            results: try response.rows.map(decodeResult(from:)),
            continuation: response.continuation,
            metadata: response.metadata
        )
    }

    private func decodeResult(from row: QueryRow) throws -> PolymorphicQueryResult {
        guard let typeName = row.annotations[PolymorphicRowAnnotation.typeName]?.stringValue else {
            throw PolymorphicQueryError.missingTypeName
        }

        guard let runtimeType = context.container.schema.entity(named: typeName)?.persistableType else {
            throw PolymorphicQueryError.unknownType(typeName)
        }

        let item = try decodeItem(row, as: runtimeType)
        let typeCode = row.annotations[PolymorphicRowAnnotation.typeCode]?.int64Value
            ?? Member.typeCode(for: typeName)

        return PolymorphicQueryResult(
            item: item,
            typeName: typeName,
            typeCode: typeCode,
            row: row
        )
    }

    private func decodeItem(
        _ row: QueryRow,
        as type: any Persistable.Type
    ) throws -> any Persistable {
        func helper<Concrete: Persistable>(_ concreteType: Concrete.Type) throws -> any Persistable {
            try QueryRowCodec.decode(row, as: concreteType)
        }

        return try _openExistential(type, do: helper)
    }

    /// Resolve a shared index name from polymorphic group metadata.
    public func resolveIndexName(
        kindIdentifier: String,
        fieldName: String
    ) throws -> String? {
        let group = try context.container.polymorphicGroup(identifier: groupIdentifier)
        return group.indexes.first { descriptor in
            descriptor.kindIdentifier == kindIdentifier
                && descriptor.fieldNames.contains(fieldName)
        }?.name
    }
}

extension FDBContext {
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
