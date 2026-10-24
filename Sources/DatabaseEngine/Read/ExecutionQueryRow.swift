import DatabaseKit
import DatabaseTypes

/// A caller-owned positional row interpreted only with an explicit column schema.
public struct ExecutionQueryRow: Sendable, Hashable {
    public let values: [QueryValue]
    public let annotations: [String: FieldValue]
    public let version: PersistableVersionToken?

    public init(
        values: [QueryValue],
        columns: [QueryColumn],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) throws {
        self.init(validatedValues: values, annotations: annotations, version: version)
        try validate(columns: columns)
    }

    /// The canonical owner has already validated arity, type and nullability
    /// against its retained relation schema before admitted construction.
    package init(
        validatedValues: [QueryValue],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        self.values = validatedValues
        self.annotations = annotations
        self.version = version
    }

    public func validate(columns: [QueryColumn]) throws {
        try QueryColumn.validate(columns)
        guard values.count == columns.count else {
            throw QueryRowCodecError.arityMismatch(expected: columns.count, actual: values.count)
        }
        for (column, value) in zip(columns, values) {
            guard QueryValueValidator.accepts(value, type: column.type) else {
                throw QueryRowCodecError.incompatibleValue(column: column.number)
            }
            guard column.isNullable || !value.isNull else {
                throw QueryRowCodecError.nullInRequiredColumn(column: column.number)
            }
        }
    }

    public func namedValues(columns: [QueryColumn]) throws -> [String: QueryValue] {
        try validate(columns: columns)
        var result: [String: QueryValue] = [:]
        result.reserveCapacity(columns.count)
        for (column, value) in zip(columns, values) {
            guard result.updateValue(value, forKey: column.name) == nil else {
                throw QueryRowCodecError.duplicateColumn(column.name)
            }
        }
        return result
    }

    public func primitiveFields(columns: [QueryColumn]) throws -> [String: FieldValue] {
        let named = try namedValues(columns: columns)
        var result: [String: FieldValue] = [:]
        result.reserveCapacity(named.count)
        for (name, value) in named {
            guard case .primitive(let primitive) = value else {
                throw QueryRowCodecError.richValueRequiresDeclaredAdapter(field: name)
            }
            result[name] = primitive
        }
        return result
    }
}

