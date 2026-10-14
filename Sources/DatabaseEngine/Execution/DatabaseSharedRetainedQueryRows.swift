import DatabaseKit

/// Shared caller-visible schema and request-accounted canonical rows.
@_spi(DatabaseExecution)
public struct DatabaseSharedRetainedQueryRows: Sendable {
    private let storage: DatabaseSharedRetainedArray<QueryRow>
    public let columns: [QueryColumn]
    package let payloadLeases: [DatabaseQueryPayloadLease]
    package let schemaLeases: [DatabaseIntermediateReservation]

    /// The producing owner has validated every row and admitted the complete
    /// schema and payload before transferring its immutable storage here.
    package init(
        storage: consuming DatabaseSharedRetainedArray<QueryRow>,
        columns: [QueryColumn],
        payloadLeases: [DatabaseQueryPayloadLease] = [],
        schemaLeases: [DatabaseIntermediateReservation] = []
    ) {
        self.storage = storage
        self.columns = columns
        self.payloadLeases = payloadLeases
        self.schemaLeases = schemaLeases
    }

    package init(
        rows: consuming DatabaseRetainedQueryRows,
        at stage: DatabaseWorkStage
    ) throws {
        self = try rows.moveToSharedOwnership(at: stage)
    }

    /// Producer-known origins include the aggregate's own retained claims.
    package var originPayloadLeases: [DatabaseQueryPayloadLease] {
        payloadLeases + [storage.payloadLease]
    }

    package var workMeter: DatabaseWorkMeter { storage.workMeter }
    public var count: Int { storage.count }
    package var isEmpty: Bool { storage.isEmpty }
    package var startIndex: Int { storage.startIndex }
    package var endIndex: Int { storage.endIndex }

    package func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing QueryRow) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        defer { withExtendedLifetime(self) {} }
        return try storage.withElement(at: index, body)
    }

    package func withElement<Result, Failure: Error>(
        at index: Int,
        isolation actor: isolated (any Actor)? = #isolation,
        _ body: (borrowing QueryRow) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        defer { withExtendedLifetime(self) {} }
        return try await storage.withElement(at: index, isolation: actor, body)
    }

    package func withSpan<Result, Failure: Error>(
        _ body: (Span<QueryRow>) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        defer { withExtendedLifetime(self) {} }
        return try storage.withSpan(body)
    }

    package func boundedView(_ range: Range<Int>) -> View {
        View(owner: self, range: range)
    }

    /// The caller consumes the result at its final public array boundary.
    package consuming func promoteToOutput() -> [QueryRow] {
        defer {
            withExtendedLifetime(payloadLeases) {}
            withExtendedLifetime(schemaLeases) {}
        }
        return storage.promoteToOutput()
    }

    /// Copies a final output page while this snapshot retains its complete
    /// schema, source payload and row claims for subsequent pages.
    public func materializePage(_ range: Range<Int>) -> [QueryRow] {
        precondition(range.lowerBound >= 0 && range.upperBound <= storage.count,
                     "Page range is outside the shared retained rows")
        defer { withExtendedLifetime(self) {} }
        var output: [QueryRow] = []
        output.reserveCapacity(range.count)
        for index in range {
            storage.withElement(at: index) { output.append(copy $0) }
        }
        return output
    }

    /// A bounded view retains the complete schema even for an empty range.
    package struct View: Sendable {
        package let owner: DatabaseSharedRetainedQueryRows
        package let range: Range<Int>

        package init(owner: DatabaseSharedRetainedQueryRows, range: Range<Int>) {
            precondition(range.lowerBound >= owner.startIndex && range.upperBound <= owner.endIndex,
                         "View range is outside the shared retained rows")
            self.owner = owner
            self.range = range
        }

        package var columns: [QueryColumn] { owner.columns }
        package var payloadLeases: [DatabaseQueryPayloadLease] { owner.payloadLeases }
        package var originPayloadLeases: [DatabaseQueryPayloadLease] { owner.originPayloadLeases }
        package var schemaLeases: [DatabaseIntermediateReservation] { owner.schemaLeases }
        package var count: Int { range.count }
        package var isEmpty: Bool { range.isEmpty }
        package var startIndex: Int { 0 }
        package var endIndex: Int { range.count }
        package var workMeter: DatabaseWorkMeter { owner.workMeter }

        package func withElement<Result, Failure: Error>(
            at index: Int,
            _ body: (borrowing QueryRow) throws(Failure) -> Result
        ) throws(Failure) -> Result {
            precondition(index >= 0 && index < count)
            return try owner.withElement(at: range.lowerBound + index, body)
        }

        package func withElement<Result, Failure: Error>(
            at index: Int,
            isolation actor: isolated (any Actor)? = #isolation,
            _ body: (borrowing QueryRow) async throws(Failure) -> Result
        ) async throws(Failure) -> Result {
            precondition(index >= 0 && index < count)
            return try await owner.withElement(at: range.lowerBound + index, isolation: actor, body)
        }

        package func withSpan<Result, Failure: Error>(
            _ body: (Span<QueryRow>) throws(Failure) -> Result
        ) throws(Failure) -> Result {
            try owner.withSpan { (span: Span<QueryRow>) throws(Failure) -> Result in
                try body(span.extracting(range))
            }
        }
    }
}
