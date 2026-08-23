import DatabaseEngine
import DatabaseKit
import DatabaseTypes

enum SPARQLRetainedScanCacheError: Error, Sendable, Equatable {
    case capacityOverflow
}

/// Request-accounted local cache for substituted atomic RDF scans.
///
/// Cached relations retain their own reservations. This owner accounts only
/// for Dictionary capacity, signature payloads, and entry owner headers.
final class SPARQLRetainedScanCache {
    private struct Entry {
        let bindings: SPARQLSharedBindingSnapshot
        let reservation: DatabaseIntermediateReservation
    }

    private static let containerByteCount: UInt64 = 64
    private static let entryOwnerByteCount: UInt64 = 64
    private static let capacitySlotByteCount: UInt64 = 192
    private static let signatureHeaderByteCount: UInt64 = 256
    private static let termHeaderByteCount: UInt64 = 64
    private static let graphSetHeaderByteCount: UInt64 = 64
    private static let graphSetSlotByteCount: UInt64 = 64

    private let workMeter: DatabaseWorkMeter
    private let containerReservation: DatabaseIntermediateReservation
    private let fieldMeter: SPARQLBindingFootprintMeter
    private var values: [SPARQLQueryExecutor.ScanSignature: Entry] = [:]
    private var accountedCapacity = 0

    private init(
        workMeter: DatabaseWorkMeter,
        containerReservation: DatabaseIntermediateReservation,
        fieldMeter: SPARQLBindingFootprintMeter
    ) {
        self.workMeter = workMeter
        self.containerReservation = containerReservation
        self.fieldMeter = fieldMeter
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLRetainedScanCache {
        let fieldMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .subqueryCache
        )
        do {
            return SPARQLRetainedScanCache(
                workMeter: workMeter,
                containerReservation: try workMeter.reserveIntermediate(
                    bytes: containerByteCount,
                    at: .subqueryCache
                ),
                fieldMeter: fieldMeter
            )
        } catch {
            fieldMeter.shutdown()
            throw error
        }
    }

    func shutdown() {
        fieldMeter.shutdown()
    }

    func value(
        for pattern: borrowing ExecutionTriple,
        graphTarget: RDFGraphScanTarget
    ) throws -> SPARQLRetainedBindings? {
        let footprint = try signatureFootprint(
            pattern: pattern,
            graphTarget: graphTarget
        )
        let probeReservation = try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: .subqueryCache
        )
        defer { probeReservation.release() }
        let signature = SPARQLQueryExecutor.ScanSignature(
            subject: pattern.subject,
            predicate: pattern.predicate,
            object: pattern.object,
            graphTarget: graphTarget
        )
        return values[signature]?.bindings.retainedBindings()
    }

    func store(
        _ bindings: SPARQLSharedBindingSnapshot,
        for pattern: borrowing ExecutionTriple,
        graphTarget: RDFGraphScanTarget
    ) throws {
        let footprint = try signatureFootprint(
            pattern: pattern,
            graphTarget: graphTarget
        )
        let requiredCount = try Self.checkedIncrement(values.count)
        let targetCapacity = try Self.targetCapacity(
            current: accountedCapacity,
            requiredCount: requiredCount
        )
        let additionalCapacity = targetCapacity - accountedCapacity
        let capacityBytes = try Self.checkedMultiply(
            UInt64(additionalCapacity),
            Self.capacitySlotByteCount
        )
        let entryReservation = try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: try Self.checkedAdd(
                footprint.bytes,
                Self.entryOwnerByteCount
            ),
            at: .subqueryCache
        )
        let signature = SPARQLQueryExecutor.ScanSignature(
            subject: pattern.subject,
            predicate: pattern.predicate,
            object: pattern.object,
            graphTarget: graphTarget
        )
        guard values[signature] == nil else {
            entryReservation.release()
            return
        }
        do {
            try containerReservation.reserveAdditional(
                bytes: capacityBytes,
                at: .subqueryCache
            )
        } catch {
            entryReservation.release()
            throw error
        }
        if targetCapacity != accountedCapacity {
            values.reserveCapacity(targetCapacity)
            accountedCapacity = targetCapacity
        }
        values[signature] = Entry(
            bindings: bindings,
            reservation: entryReservation
        )
    }

    private func signatureFootprint(
        pattern: borrowing ExecutionTriple,
        graphTarget: RDFGraphScanTarget
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            bytes: Self.signatureHeaderByteCount
        ).adding(termFootprint(pattern.subject))
            .adding(termFootprint(pattern.predicate))
            .adding(termFootprint(pattern.object))
            .adding(graphFootprint(graphTarget))
    }

    private func termFootprint(
        _ term: borrowing ExecutionTerm
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = DatabaseIntermediateFootprint(
            bytes: Self.termHeaderByteCount
        )
        switch term {
        case .variable(let variable):
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(variable.utf8.count)
                )
            )
        case .value(let value):
            footprint = try footprint.adding(
                fieldMeter.footprint(of: value)
            )
        case .wildcard:
            break
        case .tripleTerm(let subject, let predicate, let object):
            footprint = try footprint.adding(termFootprint(subject))
                .adding(termFootprint(predicate))
                .adding(termFootprint(object))
        }
        return footprint
    }

    private func graphFootprint(
        _ graphTarget: RDFGraphScanTarget
    ) throws -> DatabaseIntermediateFootprint {
        switch graphTarget {
        case .empty, .defaultGraph, .allNamedGraphs:
            return DatabaseIntermediateFootprint(bytes: 32)
        case .named(let graph):
            return try fieldMeter.footprint(of: .rdfTerm(graph.term))
        case .namedGraphUnion(let graphs):
            var footprint = try DatabaseIntermediateFootprint(
                bytes: Self.graphSetHeaderByteCount
            ).adding(
                try DatabaseIntermediateFootprint(
                    bytes: Self.graphSetSlotByteCount
                ).multiplied(by: UInt64(graphs.count))
            )
            for graph in graphs {
                footprint = try footprint.adding(
                    fieldMeter.footprint(of: .rdfTerm(graph.term))
                )
            }
            return footprint
        }
    }

    private static func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        var capacity = max(1, current)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw SPARQLRetainedScanCacheError.capacityOverflow
            }
            capacity = next
        }
        return capacity
    }

    private static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLRetainedScanCacheError.capacityOverflow
        }
        return result
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw SPARQLRetainedScanCacheError.capacityOverflow
        }
        return result
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw SPARQLRetainedScanCacheError.capacityOverflow
        }
        return result
    }
}
