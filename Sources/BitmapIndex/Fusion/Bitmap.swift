import DatabaseKit
import DatabaseTypes

/// Immutable bitmap eligibility input for a canonical Fusion plan.
public struct Bitmap<Item: Persistable>: FusionQueryInput, Sendable {
    private enum Predicate: Sendable {
        case equals(FieldValue)
        case any([FieldValue])
    }

    private let field: FieldIdentity
    private let predicate: Predicate
    private var indexName: String?
    private var resultLimit: UInt64?

    public init<Value: FieldValueRepresentable>(
        _ field: Field<Item, Value>,
        equals value: Value
    ) throws {
        self.field = field.identity
        self.predicate = .equals(
            try Self.canonicalTupleValue(value.fieldValue)
        )
    }

    public init<Value: FieldValueRepresentable>(
        _ field: Field<Item, Value?>,
        equals value: Value
    ) throws {
        self.field = field.identity
        self.predicate = .equals(
            try Self.canonicalTupleValue(value.fieldValue)
        )
    }

    public init<Value: FieldValueRepresentable>(
        _ field: Field<Item, Value>,
        in values: [Value]
    ) throws {
        self.field = field.identity
        self.predicate = .any(
            try values.map {
                try Self.canonicalTupleValue($0.fieldValue)
            }
        )
    }

    public func index(named name: String) -> Self {
        var copy = self
        copy.indexName = name
        return copy
    }

    public func limit(_ count: UInt64) -> Self {
        var copy = self
        copy.resultLimit = count
        return copy
    }

    public var fusionInput: FusionInput {
        var parameters: [String: FieldValue] = [
            BitmapReadParameter.fieldName: .string(field.name),
        ]
        switch predicate {
        case .equals(let value):
            parameters[BitmapReadParameter.operation] = .string(
                BitmapReadParameter.equalsOperation
            )
            parameters[BitmapReadParameter.values] = .array([value])
        case .any(let values):
            parameters[BitmapReadParameter.operation] = .string(
                BitmapReadParameter.inOperation
            )
            parameters[BitmapReadParameter.values] = .array(values)
        }
        let selection: FusionIndexSelection = if let indexName {
            .named(name: indexName, type: .bitmap)
        } else {
            .matching(type: .bitmap, fields: [field], fieldMatch: .exact)
        }
        return FusionInput(
            operation: .index(
                FusionIndexSource(
                    selection: selection,
                    referencedFields: [field],
                    parameters: parameters
                )
            ),
            limit: resultLimit
        )
    }

    private static func canonicalTupleValue(
        _ value: FieldValue
    ) throws -> FieldValue {
        switch value {
        case .null, .bool, .int8, .int16, .int32, .int64,
                .uint8, .uint16, .uint32, .uint64, .float32, .float64,
                .string, .bytes, .uuid:
            return value
        default:
            throw BitmapFusionInputError.unsupportedValue(value)
        }
    }
}
