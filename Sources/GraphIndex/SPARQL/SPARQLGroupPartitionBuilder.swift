import DatabaseTypes
import DatabaseKit
import DatabaseEngine

enum SPARQLGroupPartitionBuilder {
    private static let scratchArrayOwnerByteCount: UInt64 = 64
    private static let scratchArrayCount: UInt64 = 4

    static func build(
        source: consuming SPARQLRetainedBindings,
        grouping: SPARQLGroupingPlan,
        expressionContext: SPARQLQueryExpressionContext,
        workMeter: DatabaseWorkMeter,
        evaluateKey: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
    ) async throws -> SPARQLGroupPartition {
        let sourceCount = source.count
        let groupKeys = grouping.keys
        let expressionScopes = try expressionContext.reserveExpressionScopes(
            count: sourceCount
        )
        let keyScratchByteCount = try checkedAdd(
            scratchArrayOwnerByteCount,
            try checkedMultiply(UInt64(groupKeys.count), 64)
        )
        let keyScratchReservation = try workMeter.reserveIntermediate(
            bytes: keyScratchByteCount,
            at: .aggregateInput
        )
        defer { keyScratchReservation.release() }

        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .aggregateInput
        )
        defer { footprintMeter.shutdown() }

        var keyValues = try DatabaseRetainedArrayBuilder<GroupValue>(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try retainedArrayLayout()
        )
        var members = try DatabaseRetainedArrayBuilder<SPARQLGroupMember>(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try retainedArrayLayout(),
            expectedCount: sourceCount
        )
        var lookup = try SPARQLGroupLookup.make(workMeter: workMeter)
        var nextGroupIdentifier = 0

        for sourceIndex in 0..<sourceCount {
            try workMeter.consume(at: .aggregateInput)
            let reservedScopeIdentifier = expressionScopes.lowerBound
                + UInt64(sourceIndex)
            var key: [GroupValue] = []
            key.reserveCapacity(groupKeys.count)
            let expressionScopeIdentifier = try await source.withElement(
                at: sourceIndex
            ) { unscopedBinding in
                let binding = expressionContext.bindingWithExpressionScope(
                    unscopedBinding,
                    reservedIdentifier: reservedScopeIdentifier
                )
                for groupKey in groupKeys {
                    switch try await evaluateKey(
                            groupKey.expression,
                            binding
                        ) {
                    case .value(let value):
                        key.append(
                            GroupValue(from: value == .null ? nil : value)
                        )
                    case .expressionError(let evaluationError):
                        if evaluationError.isSPARQLEvaluationError {
                            key.append(.unbound)
                        } else {
                            throw evaluationError
                        }
                    }
                }
                guard let identifier = binding.expressionScopeIdentifier else {
                    throw SPARQLExpressionEvaluationError.runtimeInvariant(
                        "group member is missing its expression scope"
                    )
                }
                return identifier
            }

            let groupIdentifier: Int
            if groupKeys.isEmpty {
                groupIdentifier = 0
                nextGroupIdentifier = 1
            } else {
                let lookupResult = try lookup.identifier(
                    for: key,
                    inserting: nextGroupIdentifier
                )
                groupIdentifier = lookupResult.identifier
                if lookupResult.inserted {
                    for value in key {
                        let footprint: DatabaseIntermediateFootprint
                        switch value {
                        case .bound(let fieldValue):
                            footprint = try footprintMeter.footprint(
                                of: fieldValue
                            )
                        case .unbound:
                            footprint = DatabaseIntermediateFootprint()
                        }
                        try keyValues.append(
                            footprint: footprint,
                            at: .aggregateInput,
                            make: { value }
                        )
                    }
                    nextGroupIdentifier = try checkedIncrement(
                        nextGroupIdentifier
                    )
                }
            }

            try members.append(
                footprint: DatabaseIntermediateFootprint(),
                at: .aggregateInput,
                make: {
                    SPARQLGroupMember(
                        sourceIndex: sourceIndex,
                        groupIdentifier: groupIdentifier,
                        expressionScopeIdentifier: expressionScopeIdentifier
                    )
                }
            )
        }

        let retainedKeys = keyValues.finish()
        let retainedMembers = try sortedMembers(
            members.finish(),
            workMeter: workMeter
        )
        let groupCount: Int
        if groupKeys.isEmpty {
            groupCount = sourceCount > 0 || grouping.createsGroupForEmptyInput
                ? 1
                : 0
        } else {
            groupCount = nextGroupIdentifier
        }
        let descriptors = try makeDescriptors(
            members: retainedMembers,
            groupCount: groupCount,
            keyCount: groupKeys.count,
            workMeter: workMeter
        )
        return SPARQLGroupPartition(
            source: consume source,
            keyValues: consume retainedKeys,
            members: consume retainedMembers,
            groups: consume descriptors,
            keyCount: groupKeys.count
        )
    }

    private static func sortedMembers(
        _ members: consuming DatabaseRetainedBuffer<SPARQLGroupMember>,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedBuffer<SPARQLGroupMember> {
        guard members.count > 1 else { return consume members }
        let count = members.count
        let elementCount = try checkedMultiply(UInt64(count), 3)
        let retainedByteCount = try checkedAdd(
            try checkedMultiply(
                elementCount,
                UInt64(MemoryLayout<Int>.stride)
            ),
            try checkedMultiply(
                scratchArrayCount,
                scratchArrayOwnerByteCount
            )
        )
        let reservation = try workMeter.reserveIntermediate(
            bytes: retainedByteCount,
            at: .aggregateInput
        )
        defer { reservation.release() }

        var originalIndices = Array(0..<count)
        try originalIndices.sort { leftIndex, rightIndex in
            try workMeter.consume(2, at: .aggregateInput)
            let left = members.withElement(at: leftIndex) {
                ($0.groupIdentifier, $0.sourceIndex)
            }
            let right = members.withElement(at: rightIndex) {
                ($0.groupIdentifier, $0.sourceIndex)
            }
            if left.0 != right.0 { return left.0 < right.0 }
            return left.1 < right.1
        }
        return members.reorderingElements { destination in
            originalIndices[destination]
        }
    }

    private static func makeDescriptors(
        members: borrowing DatabaseRetainedBuffer<SPARQLGroupMember>,
        groupCount: Int,
        keyCount: Int,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedBuffer<SPARQLGroupDescriptor> {
        var descriptors = try DatabaseRetainedArrayBuilder<
            SPARQLGroupDescriptor
        >(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try retainedArrayLayout(),
            expectedCount: groupCount
        )

        var rangeStart = 0
        while rangeStart < members.count {
            let groupIdentifier = members.withElement(at: rangeStart) {
                $0.groupIdentifier
            }
            guard groupIdentifier == descriptors.count else {
                throw SPARQLGroupStorageError.invalidGroupIdentifier(
                    groupIdentifier
                )
            }
            var rangeEnd = rangeStart + 1
            while rangeEnd < members.count {
                let candidate = members.withElement(at: rangeEnd) {
                    $0.groupIdentifier
                }
                guard candidate == groupIdentifier else { break }
                rangeEnd += 1
            }
            let keyOffset: Int?
            if keyCount == 0 {
                keyOffset = nil
            } else {
                let (offset, overflow) = groupIdentifier
                    .multipliedReportingOverflow(by: keyCount)
                guard !overflow else {
                    throw SPARQLGroupStorageError.keyStorageOverflow
                }
                keyOffset = offset
            }
            try descriptors.append(
                footprint: DatabaseIntermediateFootprint(),
                at: .aggregateInput,
                make: {
                    SPARQLGroupDescriptor(
                        representativeKeyOffset: keyOffset,
                        memberRange: rangeStart..<rangeEnd
                    )
                }
            )
            rangeStart = rangeEnd
        }

        if members.isEmpty && groupCount == 1 {
            try descriptors.append(
                footprint: DatabaseIntermediateFootprint(),
                at: .aggregateInput,
                make: {
                    SPARQLGroupDescriptor(
                        representativeKeyOffset: nil,
                        memberRange: 0..<0
                    )
                }
            )
        }
        guard descriptors.count == groupCount else {
            throw SPARQLGroupStorageError.invalidGroupIdentifier(
                descriptors.count
            )
        }
        return descriptors.finish()
    }

    private static func retainedArrayLayout()
        throws -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 64,
            elementCapacitySlotByteCount: 64,
            sharedOwnerByteCount: 64,
            appendAdmissionByteCount: 64
        )
    }

    private static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLGroupStorageError.capacityOverflow
        }
        return result
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw SPARQLGroupStorageError.capacityOverflow
        }
        return result
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw SPARQLGroupStorageError.capacityOverflow
        }
        return result
    }
}
