import DatabaseKit
import DatabaseTypes

/// Immutable nearest-neighbor input for a canonical Fusion plan.
public struct Similar<Item: Persistable>: FusionQueryInput, Sendable {
    private let field: FieldIdentity
    private let dimensions: Int
    private var indexName: String?
    private var queryVector: Vector?
    private var resultCount: Int = 10
    private var distanceMetric: VectorDistanceMetric = .cosine

    public init(_ field: Field<Item, Vector>, dimensions: Int) {
        self.field = field.identity
        self.dimensions = dimensions
    }

    public init(_ field: Field<Item, Vector?>, dimensions: Int) {
        self.field = field.identity
        self.dimensions = dimensions
    }

    public func index(named name: String) -> Self {
        var copy = self
        copy.indexName = name
        return copy
    }

    public func nearest(to elements: [Float], k: Int) throws -> Self {
        try nearest(to: Vector(float32: elements), k: k)
    }

    public func nearest(
        to vector: Vector,
        k: Int
    ) throws(VectorFusionInputError) -> Self {
        guard k >= 0 else { throw .invalidResultCount(k) }
        var copy = self
        copy.queryVector = vector
        copy.resultCount = k
        return copy
    }

    public func metric(_ metric: VectorDistanceMetric) -> Self {
        var copy = self
        copy.distanceMetric = metric
        return copy
    }

    public var fusionInput: FusionInput {
        var parameters: [String: FieldValue] = [
            VectorReadParameter.fieldName: .string(field.name),
            VectorReadParameter.dimensions: .int64(Int64(dimensions)),
            VectorReadParameter.metric: .string(distanceMetric.rawValue),
        ]
        if let queryVector {
            parameters[VectorReadParameter.queryVector] = .vector(queryVector)
        }
        let selection: FusionIndexSelection = if let indexName {
            .named(name: indexName, type: .vector)
        } else {
            .matching(type: .vector, fields: [field], fieldMatch: .exact)
        }
        return FusionInput(
            operation: .index(
                FusionIndexSource(
                    selection: selection,
                    referencedFields: [field],
                    parameters: parameters
                )
            ),
            scoring: .annotation(
                name: "distance",
                order: .lowerIsBetter
            ),
            limit: UInt64(resultCount)
        )
    }
}
