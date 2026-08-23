import DatabaseKit
import DatabaseTypes
import DatabaseWire
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Database retained buffers")
struct DatabaseRetainedBufferTests {
    @Test("reservation rejection precedes element creation")
    func rejectionPrecedesCreation() throws {
        let creationCount = Mutex(0)
        let meter = makeMeter(rows: 1, bytes: 4)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .joinCandidate,
                layout: try testLayout()
            )

            #expect(throws: DatabaseWorkLimitError.self) {
                try builder.append(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: 2
                    )
                ) {
                    creationCount.withLock { $0 += 1 }
                    return 42
                }
            }

            let builderIsEmpty = builder.isEmpty
            #expect(builderIsEmpty)
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 2)
        }

        #expect(creationCount.withLock { $0 } == 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("failed creation rolls back only its claim")
    func failedCreationRollsBackClaim() throws {
        let meter = makeMeter(rows: 2, bytes: 13)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .projection,
                layout: try testLayout()
            )
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
            ) {
                1
            }

            #expect(throws: RetainedElementCreationFailure.creationFailed) {
                try builder.append(
                    footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
                ) {
                    throw RetainedElementCreationFailure.creationFailed
                }
            }

            let builderCount = builder.count
            #expect(builderCount == 1)
            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes == 8)
            let retained = builder.finish()
            let containsExpectedElement = retained.withSpan { span in
                span.count == 1 && span[0] == 1
            }
            #expect(containsExpectedElement)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("finished storage retains and shares one reservation")
    func finishedStorageOwnsReservation() throws {
        let meter = makeMeter(rows: 2, bytes: 15)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .resultMaterialization,
                layout: try testLayout(),
                expectedCount: 2
            )
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
            ) {
                1
            }
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
            ) {
                2
            }
            let retained = builder.finish()
            let admission = try retained.prepareToShare(at: .subqueryCache)
            let first = retained.share(using: admission)
            do {
                let second = first
                let firstAddress = first.withSpan { span in
                    span.withUnsafeBufferPointer { buffer in
                        UInt(bitPattern: buffer.baseAddress)
                    }
                }
                let secondAddress = second.withSpan { span in
                    span.withUnsafeBufferPointer { buffer in
                        UInt(bitPattern: buffer.baseAddress)
                    }
                }
                let containsExpectedElements = second.withSpan { span in
                    span.count == 2 && span[0] == 1 && span[1] == 2
                }
                #expect(containsExpectedElements)
                #expect(secondAddress == firstAddress)
                #expect(meter.retainedIntermediateRows == 2)
                #expect(meter.retainedIntermediateBytes == 15)
            }
            #expect(meter.retainedIntermediateRows == 2)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("sharing preserves contiguous element storage")
    func sharingReusesBackingStorage() throws {
        let meter = makeMeter(rows: 2, bytes: 15)
        var builder = try DatabaseRetainedArrayBuilder<Int>(
            workMeter: meter,
            stage: .resultMaterialization,
            layout: try testLayout(),
            expectedCount: 2
        )
        for value in 1...2 {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
            ) {
                value
            }
        }
        let retained = builder.finish()
        let retainedAddress = retained.withSpan { span in
            span.withUnsafeBufferPointer { buffer in
                UInt(bitPattern: buffer.baseAddress)
            }
        }
        let admission = try retained.prepareToShare(at: .subqueryCache)
        let shared = retained.share(using: admission)
        let sharedAddress = shared.withSpan { span in
            span.withUnsafeBufferPointer { buffer in
                UInt(bitPattern: buffer.baseAddress)
            }
        }

        #expect(sharedAddress == retainedAddress)
        #expect(meter.retainedIntermediateRows == 2)
        #expect(meter.retainedIntermediateBytes == 15)
    }

    @Test("operator hand-off retains reservation through every shared alias")
    func operatorHandOffRetainsReservation() throws {
        let meter = makeMeter(rows: 2, bytes: 15)
        do {
            let builder = try makeTwoElementBuilder(meter: meter)
            let shared = try builder.finish().moveToSharedOwnership(
                at: .joinCandidate
            )
            do {
                let downstream = shared
                #expect(Array(downstream) == [1, 2])
                #expect(meter.retainedIntermediateRows == 2)
                #expect(meter.retainedIntermediateBytes == 15)

                let output = shared.promoteToOutput()
                #expect(output == [1, 2])
                #expect(meter.retainedIntermediateRows == 2)
                #expect(meter.retainedIntermediateBytes == 15)
            }
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("failed operator hand-off releases the consumed unique owner")
    func failedOperatorHandOffReleasesReservation() throws {
        let meter = makeMeter(rows: 2, bytes: 14)
        let builder = try makeTwoElementBuilder(meter: meter)

        do {
            _ = try builder.finish().moveToSharedOwnership(at: .joinCandidate)
            Issue.record("Expected shared-owner admission to fail")
        } catch is DatabaseWorkLimitError {
            // Expected typed admission failure.
        } catch {
            Issue.record("Unexpected operator hand-off failure: \(error)")
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("failed sharing admission preserves the unique buffer")
    func failedSharingAdmissionPreservesUniqueBuffer() throws {
        let meter = makeMeter(rows: 2, bytes: 13)
        var builder = try DatabaseRetainedArrayBuilder<Int>(
            workMeter: meter,
            stage: .resultMaterialization,
            layout: try testLayout(),
            expectedCount: 2
        )
        for value in 1...2 {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
            ) {
                value
            }
        }
        let retained = builder.finish()

        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try retained.prepareToShare(at: .subqueryCache)
        }
        let output = retained.promoteToOutput()

        #expect(output == [1, 2])
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("abandoned sharing admission releases only owner storage")
    func abandonedSharingAdmissionReleasesOnlyOwnerStorage() throws {
        let meter = makeMeter(rows: 2, bytes: 15)
        do {
            let builder = try makeTwoElementBuilder(meter: meter)
            let retained = builder.finish()
            do {
                let admission = try retained.prepareToShare(
                    at: .subqueryCache
                )
                _ = consumeWithoutCommitting(admission)
                #expect(meter.retainedIntermediateRows == 2)
                #expect(meter.retainedIntermediateBytes == 12)
            }
            #expect(meter.retainedIntermediateRows == 2)
            #expect(meter.retainedIntermediateBytes == 12)
            let output = retained.promoteToOutput()
            #expect(output == [1, 2])
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("sharing admission lifetime is independent from promotion")
    func sharingAdmissionOutlivesPromotedSource() throws {
        let meter = makeMeter(rows: 2, bytes: 15)
        do {
            let builder = try makeTwoElementBuilder(meter: meter)
            let retained = builder.finish()
            let admission = try retained.prepareToShare(at: .subqueryCache)
            let output = retained.promoteToOutput()

            #expect(output == [1, 2])
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 3)
            _ = consumeWithoutCommitting(admission)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("append admission accounts for a candidate before construction")
    func appendAdmissionPrecedesCandidateConstruction() throws {
        let meter = makeMeter(rows: 1, bytes: 6)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .joinCandidate,
                layout: try testLayout()
            )
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 2)
            )

            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes == 6)

            let candidate = 42
            builder.append(candidate, using: admission)

            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes == 5)
            let retained = builder.finish()
            let retainedValue = retained.withSpan { $0[0] }
            #expect(retainedValue == 42)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("abandoned append admission retains only materialized capacity")
    func abandonedAppendAdmissionRollsBackPayload() throws {
        let meter = makeMeter(rows: 1, bytes: 6)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .joinCandidate,
                layout: try testLayout()
            )
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 2)
            )

            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes == 6)
            _ = consumeWithoutAppending(admission)

            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 3)
            let retained = builder.finish()
            let retainedIsEmpty = retained.isEmpty
            #expect(retainedIsEmpty)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("append admission lifetime is independent from source promotion")
    func appendAdmissionOutlivesPromotedSource() throws {
        let meter = makeMeter(rows: 1, bytes: 6)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .joinCandidate,
                layout: try testLayout()
            )
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 2)
            )
            let retained = builder.finish()
            let output = retained.promoteToOutput()

            #expect(output.isEmpty)
            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes == 3)
            _ = consumeWithoutAppending(admission)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("canonical capacity grows geometrically")
    func canonicalCapacityGrowthIsFullyAccounted() throws {
        let meter = makeMeter(rows: 1, bytes: 11)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .resultMaterialization,
                layout: try testLayout()
            )
            let retainedBytes: [UInt64] = [3, 4, 6, 6, 10]
            for (value, expectedBytes) in zip(0..<5, retainedBytes) {
                try builder.append(
                    footprint: DatabaseIntermediateFootprint()
                ) {
                    value
                }
                #expect(meter.retainedIntermediateBytes == expectedBytes)
            }
            let retained = builder.finish()
            let retainedCount = retained.count
            #expect(retainedCount == 5)
        }

        #expect(meter.retainedIntermediateBytes == 0)
        #expect(meter.peakIntermediateBytes == 11)
    }

    @Test("expected count reserves canonical spare capacity")
    func expectedCountReservesCanonicalCapacity() throws {
        let meter = makeMeter(rows: 1, bytes: 6)
        do {
            let builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .resultMaterialization,
                layout: try testLayout(),
                expectedCount: 3
            )
            #expect(meter.retainedIntermediateBytes == 6)
            let retained = builder.finish()
            let retainedIsEmpty = retained.isEmpty
            #expect(retainedIsEmpty)
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("layout rejects invalid capacity and zero admission units")
    func layoutValidationIsTyped() throws {
        let layout = try testLayout()
        #expect {
            try layout.growth(from: -1, toFit: 0)
        } throws: { error in
            error as? DatabaseRetainedArrayLayoutError
                == .invalidCurrentCapacity(-1)
        }
        #expect {
            try layout.growth(from: Int.min, toFit: 0)
        } throws: { error in
            error as? DatabaseRetainedArrayLayoutError
                == .invalidCurrentCapacity(Int.min)
        }
        #expect {
            try DatabaseRetainedArrayLayout.validated(
                containerByteCount: 0,
                elementCapacitySlotByteCount: 1,
                sharedOwnerByteCount: 1,
                appendAdmissionByteCount: 1
            )
        } throws: { error in
            error as? DatabaseRetainedArrayLayoutError
                == .zeroContainerByteCount
        }
        #expect {
            try DatabaseRetainedArrayLayout.validated(
                containerByteCount: 1,
                elementCapacitySlotByteCount: 0,
                sharedOwnerByteCount: 1,
                appendAdmissionByteCount: 1
            )
        } throws: { error in
            error as? DatabaseRetainedArrayLayoutError
                == .zeroElementCapacitySlotByteCount
        }
        #expect {
            try DatabaseRetainedArrayLayout.validated(
                containerByteCount: 1,
                elementCapacitySlotByteCount: 1,
                sharedOwnerByteCount: 0,
                appendAdmissionByteCount: 1
            )
        } throws: { error in
            error as? DatabaseRetainedArrayLayoutError
                == .zeroSharedOwnerByteCount
        }
        #expect {
            try DatabaseRetainedArrayLayout.validated(
                containerByteCount: 1,
                elementCapacitySlotByteCount: 1,
                sharedOwnerByteCount: 1,
                appendAdmissionByteCount: 0
            )
        } throws: { error in
            error as? DatabaseRetainedArrayLayoutError
                == .zeroAppendAdmissionByteCount
        }
    }

    @Test("top-level promotion preserves contiguous element storage")
    func promotionReusesBackingStorage() throws {
        let meter = makeMeter(rows: 2, bytes: 13)
        var builder = try DatabaseRetainedArrayBuilder<Int>(
            workMeter: meter,
            stage: .resultMaterialization,
            layout: try testLayout(),
            expectedCount: 2
        )
        for value in 1...2 {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
            ) {
                value
            }
        }
        let retained = builder.finish()
        let retainedAddress = retained.withSpan { span in
            span.withUnsafeBufferPointer { buffer in
                UInt(bitPattern: buffer.baseAddress)
            }
        }

        var output = retained.promoteToOutput()
        let outputAddress = output.withUnsafeBufferPointer { buffer in
            UInt(bitPattern: buffer.baseAddress)
        }
        output[0] = 7
        let mutatedAddress = output.withUnsafeBufferPointer { buffer in
            UInt(bitPattern: buffer.baseAddress)
        }

        #expect(output == [7, 2])
        #expect(outputAddress == retainedAddress)
        #expect(mutatedAddress == outputAddress)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("footprint arithmetic rejects overflow")
    func footprintArithmeticRejectsOverflow() {
        #expect {
            try DatabaseIntermediateFootprint(
                rows: UInt64.max,
                bytes: 0
            ).adding(DatabaseIntermediateFootprint(rows: 1, bytes: 0))
        } throws: { error in
            error as? DatabaseIntermediateFootprintError
                == .rowAdditionOverflow(left: UInt64.max, right: 1)
        }
        #expect {
            try DatabaseIntermediateFootprint(
                rows: 0,
                bytes: UInt64.max
            ).multiplied(by: 2)
        } throws: { error in
            error as? DatabaseIntermediateFootprintError
                == .byteMultiplicationOverflow(
                    value: UInt64.max,
                    multiplier: 2
                )
        }
    }

    @Test("retained RDF graph rejects a second row and releases every claim")
    func retainedRDFGraphLimitFailureReleasesClaims() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 10,
                maximumIntermediateRows: 5,
                maximumIntermediateBytes: 16_384,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )

        do {
            var builder = try DatabaseRetainedRDFGraphBuilder(
                workMeter: meter
            )
            try builder.append(try graphQuad(identifier: 1))
            #expect {
                try builder.append(try graphQuad(identifier: 2))
            } throws: { error in
                error as? DatabaseWorkLimitError
                    == .maximumIntermediateRows(
                        stage: .resultMaterialization,
                        consumed: 5,
                        requested: 1,
                        maximum: 5
                    )
            }
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("partial RDF graph promotion releases the hidden full graph")
    func partialRDFGraphPromotionReleasesFullGraph() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 10,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 16_384,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        var builder = try DatabaseRetainedRDFGraphBuilder(workMeter: meter)
        for identifier in 1...3 {
            try builder.append(try graphQuad(identifier: identifier))
        }
        let graph = builder.finish()
        #expect(meter.retainedIntermediateRows == 3)

        let page = graph.promotePage(1..<2)

        #expect(page == [try graphQuad(identifier: 2)])
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeMeter(
        rows: UInt32,
        bytes: UInt64
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                maximumIntermediateRows: rows,
                maximumIntermediateBytes: bytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func testLayout() throws -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 2,
            elementCapacitySlotByteCount: 1,
            sharedOwnerByteCount: 3,
            appendAdmissionByteCount: 1
        )
    }

    private func makeTwoElementBuilder(
        meter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedArrayBuilder<Int> {
        var builder = try DatabaseRetainedArrayBuilder<Int>(
            workMeter: meter,
            stage: .resultMaterialization,
            layout: try testLayout(),
            expectedCount: 2
        )
        for value in 1...2 {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 4)
            ) {
                value
            }
        }
        return builder
    }

    private func graphQuad(identifier: Int) throws -> RDFQuad {
        RDFQuad(
            subject: .iri(try RDFIRI("urn:subject:\(identifier)")),
            predicate: try RDFPredicateIRI("urn:predicate"),
            object: .iri(try RDFIRI("urn:object:\(identifier)"))
        )
    }

    private func consumeWithoutCommitting<Element>(
        _ admission: consuming DatabaseRetainedShareAdmission<Element>
    ) -> Bool {
        false
    }

    private func consumeWithoutAppending<Element>(
        _ admission: consuming DatabaseRetainedArrayAppendAdmission<Element>
    ) -> Bool {
        false
    }
}

private enum RetainedElementCreationFailure: Error {
    case creationFailed
}
