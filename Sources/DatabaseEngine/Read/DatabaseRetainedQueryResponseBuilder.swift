/// Builds an admitted owner collection without losing the reservations held
/// by individual retained query responses.
@_spi(DatabaseExecution)
public struct DatabaseRetainedQueryResponseBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<
        DatabaseRetainedQueryResponse
    >
    private let workMeter: DatabaseWorkMeter

    public init(workMeter: DatabaseWorkMeter) throws {
        self.storage = try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: DatabaseRetainedQueryResponse.self)
        )
        self.workMeter = workMeter
    }

    public mutating func append(
        _ response: consuming DatabaseRetainedQueryResponse
    ) throws {
        try response.validateWorkMeter(
            workMeter,
            sourceName: "retained query response collection"
        )
        let admission = try storage.prepareAppend(
            footprint: DatabaseIntermediateFootprint(),
            at: .resultMaterialization
        )
        storage.append(consume response, using: consume admission)
    }

    public consuming func finish()
        throws -> DatabaseRetainedQueryResponses {
        DatabaseRetainedQueryResponses(
            storage: try storage.finish().moveToSharedOwnership(
                at: .resultMaterialization
            )
        )
    }
}
