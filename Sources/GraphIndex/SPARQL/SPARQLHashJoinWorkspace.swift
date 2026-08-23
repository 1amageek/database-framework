import DatabaseEngine
import DatabaseTypes

enum SPARQLHashJoinWorkspaceError: Error, Sendable, Equatable {
    case capacityOverflow
}

/// Request-accounted hash workspace for one SPARQL join build side.
/// Join-key and bucket storage is admitted before either Array or Dictionary
/// materializes capacity.
final class SPARQLHashJoinWorkspace {
    private final class Bucket {
        private static let ownerByteCount: UInt64 = 64
        private static let indexSlotByteCount: UInt64 = 8

        private let reservation: DatabaseIntermediateReservation
        private var indices: [Int]
        private var accountedCapacity: Int

        init(
            reservation: DatabaseIntermediateReservation,
            firstIndex: Int
        ) {
            var indices: [Int] = []
            indices.reserveCapacity(1)
            indices.append(firstIndex)
            self.reservation = reservation
            self.indices = indices
            self.accountedCapacity = 1
        }

        static func initialFootprint(
            keyFootprint: DatabaseIntermediateFootprint
        ) throws -> DatabaseIntermediateFootprint {
            try keyFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: ownerByteCount + indexSlotByteCount
                )
            )
        }

        func append(_ index: Int) throws {
            let requiredCount = try SPARQLHashJoinWorkspace.checkedIncrement(
                indices.count
            )
            let targetCapacity = try SPARQLHashJoinWorkspace.targetCapacity(
                current: accountedCapacity,
                requiredCount: requiredCount
            )
            if targetCapacity != accountedCapacity {
                let additionalBytes = try SPARQLHashJoinWorkspace
                    .checkedMultiply(
                        UInt64(targetCapacity - accountedCapacity),
                        Self.indexSlotByteCount
                    )
                try reservation.reserveAdditional(
                    bytes: additionalBytes,
                    at: .joinCandidate
                )
                indices.reserveCapacity(targetCapacity)
                accountedCapacity = targetCapacity
            }
            indices.append(index)
        }

        func withIndices<Result: Sendable>(
            _ body: (borrowing [Int]) async throws -> Result
        ) async throws -> Result {
            try await body(indices)
        }
    }

    private static let containerByteCount: UInt64 = 64
    private static let dictionaryCapacitySlotByteCount: UInt64 = 192
    private static let keyArrayHeaderByteCount: UInt64 = 64
    private static let keyValueSlotByteCount: UInt64 = 64
    private static let variablesArrayHeaderByteCount: UInt64 = 64
    private static let variableSlotByteCount: UInt64 = 32

    private let workMeter: DatabaseWorkMeter
    private let containerReservation: DatabaseIntermediateReservation
    private let fieldMeter: SPARQLBindingFootprintMeter
    private let variables: [String]
    private var buckets: [SPARQLQueryExecutor.JoinKey: Bucket] = [:]
    private var accountedCapacity = 0

    private init(
        workMeter: DatabaseWorkMeter,
        containerReservation: DatabaseIntermediateReservation,
        fieldMeter: SPARQLBindingFootprintMeter,
        variables: consuming [String]
    ) {
        self.workMeter = workMeter
        self.containerReservation = containerReservation
        self.fieldMeter = fieldMeter
        self.variables = variables
    }

    static func make(
        joinVariables: Set<String>,
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLHashJoinWorkspace {
        var variablesFootprint = try DatabaseIntermediateFootprint(
            bytes: variablesArrayHeaderByteCount
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: variableSlotByteCount
            ).multiplied(by: UInt64(joinVariables.count))
        )
        for variable in joinVariables {
            variablesFootprint = try variablesFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(variable.utf8.count)
                )
            )
        }
        let fieldMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .joinCandidate
        )
        do {
            let reservation = try workMeter.reserveIntermediate(
                rows: variablesFootprint.rows,
                bytes: try Self.checkedAdd(
                    Self.containerByteCount,
                    variablesFootprint.bytes
                ),
                at: .joinCandidate
            )
            return SPARQLHashJoinWorkspace(
                workMeter: workMeter,
                containerReservation: reservation,
                fieldMeter: fieldMeter,
                variables: joinVariables.sorted()
            )
        } catch {
            fieldMeter.shutdown()
            throw error
        }
    }

    func shutdown() {
        fieldMeter.shutdown()
    }

    func insert(
        binding: borrowing VariableBinding,
        index: Int
    ) throws {
        let keyFootprint = try keyFootprint(binding: binding)
        let probeReservation = try workMeter.reserveIntermediate(
            rows: keyFootprint.rows,
            bytes: keyFootprint.bytes,
            at: .joinCandidate
        )
        defer { probeReservation.release() }
        let key = SPARQLQueryExecutor.JoinKey(
            binding: binding,
            variables: variables
        )

        if let bucket = buckets[key] {
            try bucket.append(index)
            return
        }

        let requiredCount = try Self.checkedIncrement(buckets.count)
        let targetCapacity = try Self.targetCapacity(
            current: accountedCapacity,
            requiredCount: requiredCount
        )
        let additionalCapacity = targetCapacity - accountedCapacity
        let capacityBytes = try Self.checkedMultiply(
            UInt64(additionalCapacity),
            Self.dictionaryCapacitySlotByteCount
        )
        let bucketReservation = try workMeter.reserveIntermediate(
            rows: keyFootprint.rows,
            bytes: try Bucket.initialFootprint(
                keyFootprint: keyFootprint
            ).bytes,
            at: .joinCandidate
        )
        do {
            try containerReservation.reserveAdditional(
                bytes: capacityBytes,
                at: .joinCandidate
            )
        } catch {
            bucketReservation.release()
            throw error
        }
        if targetCapacity != accountedCapacity {
            buckets.reserveCapacity(targetCapacity)
            accountedCapacity = targetCapacity
        }
        buckets[key] = Bucket(
            reservation: bucketReservation,
            firstIndex: index
        )
    }

    func withMatchIndices<Result: Sendable>(
        binding: borrowing VariableBinding,
        _ body: (borrowing [Int]) async throws -> Result
    ) async throws -> Result? {
        let footprint = try keyFootprint(binding: binding)
        let probeReservation = try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: .joinCandidate
        )
        defer { probeReservation.release() }
        let key = SPARQLQueryExecutor.JoinKey(
            binding: binding,
            variables: variables
        )
        guard let bucket = buckets[key] else { return nil }
        return try await bucket.withIndices(body)
    }

    private func keyFootprint(
        binding: borrowing VariableBinding
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = try DatabaseIntermediateFootprint(
            bytes: Self.keyArrayHeaderByteCount
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: Self.keyValueSlotByteCount
            ).multiplied(by: UInt64(variables.count))
        )
        for variable in variables {
            let value = binding[variable] ?? .null
            footprint = try footprint.adding(
                fieldMeter.footprint(of: value)
            )
        }
        return footprint
    }

    fileprivate static func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        var capacity = max(1, current)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw SPARQLHashJoinWorkspaceError.capacityOverflow
            }
            capacity = next
        }
        return capacity
    }

    fileprivate static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLHashJoinWorkspaceError.capacityOverflow
        }
        return result
    }

    fileprivate static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw SPARQLHashJoinWorkspaceError.capacityOverflow
        }
        return result
    }

    fileprivate static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw SPARQLHashJoinWorkspaceError.capacityOverflow
        }
        return result
    }
}
