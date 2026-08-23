import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Request-accounted SPARQL result for framework-internal operator hand-offs.
///
/// Public query results intentionally promote their binding buffer to an
/// ordinary Array. This SPI result instead retains immutable graph storage and
/// its work-meter reservation while another database operator consumes rows
/// through scoped borrows.
package struct SPARQLRetainedResult: Sendable {
    private let bindings: SPARQLSharedBindingSnapshot
    private let workMeter: DatabaseWorkMeter

    package let projectedVariables: [String]
    package let isComplete: Bool
    package let limitReason: SPARQLLimitReason?
    package let statistics: ExecutionStatistics

    init(
        bindings: SPARQLSharedBindingSnapshot,
        workMeter: DatabaseWorkMeter,
        projectedVariables: [String],
        isComplete: Bool,
        limitReason: SPARQLLimitReason?,
        statistics: ExecutionStatistics
    ) {
        self.bindings = bindings
        self.workMeter = workMeter
        self.projectedVariables = projectedVariables
        self.isComplete = isComplete
        self.limitReason = limitReason
        self.statistics = statistics
    }

    package var count: Int { bindings.count }
    package var isEmpty: Bool { count == 0 }

    package func withBinding<Result, Failure: Error>(
        at index: Int,
        workMeter: DatabaseWorkMeter,
        _ body: (borrowing VariableBinding) throws(Failure) -> Result
    ) throws -> Result {
        guard self.workMeter === workMeter else {
            throw SPARQLRetainedResultError.workMeterMismatch
        }
        return try bindings.withElement(at: index, body)
    }

    package func withBinding<Result, Failure: Error>(
        at index: Int,
        workMeter: DatabaseWorkMeter,
        _ body: (borrowing VariableBinding) async throws(Failure) -> Result
    ) async throws -> Result {
        guard self.workMeter === workMeter else {
            throw SPARQLRetainedResultError.workMeterMismatch
        }
        return try await bindings.withElement(at: index, body)
    }

    package func retainedValues(
        for variable: String,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedBuffer<FieldValue> {
        guard self.workMeter === workMeter else {
            throw SPARQLRetainedResultError.workMeterMismatch
        }
        var values = try DatabaseRetainedArrayBuilder<FieldValue>(
            workMeter: workMeter,
            stage: .expressionEvaluation,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: FieldValue.self),
            expectedCount: count
        )
        for index in 0..<count {
            try workMeter.consume(at: .expressionEvaluation)
            try bindings.withElement(at: index) { binding in
                guard let value = binding[variable] else {
                    throw SPARQLRetainedResultError.missingVariable(variable)
                }
                try values.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: QueryRow(fields: ["value": value]),
                        workMeter: workMeter
                    ),
                    make: { value }
                )
            }
        }
        return values.finish()
    }

    package func recordingDuration(
        nanoseconds: UInt64
    ) -> SPARQLRetainedResult {
        var statistics = statistics
        statistics.durationNs = nanoseconds
        return SPARQLRetainedResult(
            bindings: bindings,
            workMeter: workMeter,
            projectedVariables: projectedVariables,
            isComplete: isComplete,
            limitReason: limitReason,
            statistics: statistics
        )
    }
}
