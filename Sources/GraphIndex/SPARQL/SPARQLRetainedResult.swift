import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Request-accounted SPARQL result for framework-internal operator hand-offs.
///
/// Public query results intentionally promote their binding buffer to an
/// ordinary Array. This SPI result instead retains immutable graph storage and
/// its work-meter reservation while another database operator consumes rows
/// through scoped borrows.
package struct SPARQLRetainedResult: ~Copyable, Sendable {
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

    /// Ends retained intermediate ownership at a top-level SPARQL result
    /// boundary and moves the same COW binding buffer into public output.
    package consuming func promoteToResult() -> SPARQLResult {
        SPARQLResult(
            bindings: bindings.retainedBindings().promoteToOutput(),
            projectedVariables: projectedVariables,
            isComplete: isComplete,
            limitReason: limitReason,
            statistics: statistics
        )
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
            layout: try DatabaseRetainedArrayLayout.forElement(FieldValue.self),
            expectedCount: count
        )
        for index in 0..<count {
            try workMeter.consume(at: .expressionEvaluation)
            try bindings.withElement(at: index) { binding in
                guard let value = binding[variable] else {
                    throw SPARQLRetainedResultError.missingVariable(variable)
                }
                try values.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .valueFootprint(
                            of: value,
                            workMeter: workMeter,
                            stage: .expressionEvaluation
                        ),
                    make: {
                        value
                    }
                )
            }
        }
        return values.finish()
    }

    package func retainedQueryRows(
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedQueryRows {
        guard self.workMeter === workMeter else {
            throw SPARQLRetainedResultError.workMeterMismatch
        }
        let columns = try projectedVariables.enumerated().map { index, variable in
            try QueryColumn(number: UInt32(index + 1), name: outputName(variable),
                            type: .primitive(.rdfTerm), isNullable: true)
        }
        var rows = try DatabaseRetainedQueryRowsBuilder(
            columns: columns, workMeter: workMeter,
            stage: .resultMaterialization, expectedCount: count
        )
        for index in 0..<count {
            try bindings.withElement(at: index) { binding in
                let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                    count: projectedVariables.count,
                    valueAt: { .primitive(binding[projectedVariables[$0]] ?? .null) },
                    annotations: [:], version: nil,
                    workMeter: workMeter, stage: .resultMaterialization
                )
                try rows.append(footprint: footprint) {
                    var values: [QueryValue] = []
                    values.reserveCapacity(projectedVariables.count)
                    for variable in projectedVariables {
                        values.append(.primitive(binding[variable] ?? .null))
                    }
                    return try QueryRow(values: values, columns: columns)
                }
            }
        }
        return rows.finish()
    }

    package consuming func recordingDuration(
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

    private func outputName(_ variable: String) -> String {
        if variable.hasPrefix("?") || variable.hasPrefix("$") {
            return String(variable.dropFirst())
        }
        return variable
    }
}
