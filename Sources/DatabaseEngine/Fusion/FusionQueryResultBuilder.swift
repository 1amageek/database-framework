import DatabaseKit
import DatabaseTypes

/// Linear builder that admits source rows before retaining them.
public struct FusionQueryResultBuilder<Item: Persistable>: ~Copyable {
    private static var scoreAnnotationRetainedByteCount: UInt64 { 128 }
    private var storage: DatabaseRetainedArrayBuilder<ScoredResult<Item>>
    private let workMeter: DatabaseWorkMeter
    private var previousScore: Double?
    private var previousIdentity: ReferenceIdentifier?
    private var requiresCanonicalOrdering: Bool

    public init(
        execution: ReadExecutionContext,
        expectedCount: Int = 0
    ) throws {
        self.workMeter = execution.workMeter
        self.previousScore = nil
        self.previousIdentity = nil
        self.requiresCanonicalOrdering = false
        self.storage = try DatabaseRetainedArrayBuilder(
            workMeter: execution.workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: ScoredResult<Item>.self),
            expectedCount: expectedCount
        )
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    /// Admits the row footprint and Array growth before retaining the result.
    public mutating func append(
        _ result: consuming ScoredResult<Item>
    ) throws {
        let orderingRequiresSort = try validateOrdering(
            item: result.item,
            score: result.score
        )
        let admission = try storage.prepareAppend(
            footprint: try CanonicalRelationalFootprintMeter.footprint(
                of: result.item,
                annotations: ["score": .float64(result.score)],
                workMeter: workMeter
            ),
            at: .indexScan
        )
        recordOrdering(
            item: result.item,
            score: result.score,
            requiresSort: orderingRequiresSort
        )
        storage.append(result, using: admission)
    }

    /// Decodes and admits one retained canonical row without exposing the row's
    /// COW dictionaries across the package boundary.
    @discardableResult
    @_spi(DatabaseExecution)
    public mutating func appendDecodedRow(
        _ row: DatabaseRetainedQueryRow,
        score: Double,
        where isIncluded: (borrowing Item) throws -> Bool = { _ in true }
    ) throws -> Bool {
        guard score.isFinite else {
            throw FusionQueryError.invalidConfiguration(
                "Fusion sources must produce finite scores"
            )
        }
        let sourceRow = row.materializeForRetainedTransfer()
        let sourceFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: sourceRow,
            workMeter: workMeter
        )
        let admission = try storage.prepareAppend(
            footprint: try sourceFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.scoreAnnotationRetainedByteCount
                )
            ),
            at: .indexScan
        )
        let item = try QueryRowCodec.decode(sourceRow, as: Item.self)
        guard try isIncluded(item) else { return false }
        let orderingRequiresSort = try validateOrdering(
            item: item,
            score: score
        )
        recordOrdering(
            item: item,
            score: score,
            requiresSort: orderingRequiresSort
        )
        storage.append(
            ScoredResult(item: item, score: score),
            using: admission
        )
        return true
    }

    /// Admits a typed Fusion destination before decoding a retained compiled
    /// model. The caller must keep the source owner alive for this call.
    @discardableResult
    @_spi(DatabaseExecution)
    public mutating func appendDecodedModel(
        _ model: borrowing PersistedModel,
        score: Double,
        where isIncluded: (borrowing Item) throws -> Bool = { _ in true }
    ) throws -> Bool {
        guard score.isFinite else {
            throw FusionQueryError.invalidConfiguration(
                "Fusion sources must produce finite scores"
            )
        }
        let sourceFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: model,
            workMeter: workMeter
        )
        let admission = try storage.prepareAppend(
            footprint: try sourceFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.scoreAnnotationRetainedByteCount
                )
            ),
            at: .indexScan
        )
        let item = try model.decode(as: Item.self)
        guard try isIncluded(item) else { return false }
        let orderingRequiresSort = try validateOrdering(
            item: item,
            score: score
        )
        recordOrdering(
            item: item,
            score: score,
            requiresSort: orderingRequiresSort
        )
        storage.append(
            ScoredResult(item: item, score: score),
            using: admission
        )
        return true
    }

    /// Admits the typed destination before decoding, then derives its score
    /// from the scoped decoded value. Returning `nil` excludes the row.
    @discardableResult
    @_spi(DatabaseExecution)
    public mutating func appendDecodedModel(
        _ model: borrowing PersistedModel,
        score: (borrowing Item) throws -> Double?
    ) throws -> Bool {
        let sourceFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: model,
            workMeter: workMeter
        )
        let admission = try storage.prepareAppend(
            footprint: try sourceFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.scoreAnnotationRetainedByteCount
                )
            ),
            at: .indexScan
        )
        let item = try model.decode(as: Item.self)
        guard let value = try score(item) else { return false }
        let orderingRequiresSort = try validateOrdering(
            item: item,
            score: value
        )
        recordOrdering(
            item: item,
            score: value,
            requiresSort: orderingRequiresSort
        )
        storage.append(
            ScoredResult(item: item, score: value),
            using: admission
        )
        return true
    }

    private func validateOrdering(
        item: borrowing Item,
        score: Double
    ) throws -> Bool {
        guard score.isFinite else {
            throw FusionQueryError.invalidConfiguration(
                "Fusion sources must produce finite scores"
            )
        }
        let identity = item.id.persistableIdentifierValue
        guard let previousScore, let previousIdentity else { return false }
        return previousScore < score
            || (previousScore == score && previousIdentity > identity)
    }

    private mutating func recordOrdering(
        item: borrowing Item,
        score: Double,
        requiresSort: Bool
    ) {
        if requiresSort { requiresCanonicalOrdering = true }
        previousScore = score
        previousIdentity = item.id.persistableIdentifierValue
    }

    public consuming func finish() throws -> FusionQueryResult<Item> {
        let retained: DatabaseRetainedBuffer<ScoredResult<Item>>
        if requiresCanonicalOrdering {
            retained = storage.finish().sortingElements { lhs, rhs in
                if lhs.score != rhs.score { return lhs.score > rhs.score }
                return lhs.item.id.persistableIdentifierValue
                    < rhs.item.id.persistableIdentifierValue
            }
        } else {
            retained = storage.finish()
        }
        return FusionQueryResult(
            storage: try retained.moveToSharedOwnership(at: .indexScan),
            workMeter: workMeter,
            ordering: .scoreDescendingCanonicalIdentity
        )
    }
}
