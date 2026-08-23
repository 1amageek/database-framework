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
        let meter = makeMeter(rows: 2, bytes: 1_024)
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
            let retainedBytes = meter.retainedIntermediateBytes
            let retained = builder.finish()
            let containsExpectedElement = retained.withSpan { span in
                span.count == 1 && span[0] == 1
            }
            #expect(containsExpectedElement)
            let output = retained.promoteToOutput()
            #expect(retainedBytes == 2 + UInt64(output.capacity) + 4)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("finished storage retains and shares one reservation")
    func finishedStorageOwnsReservation() throws {
        let meter = makeMeter(rows: 2, bytes: 1_024)
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
            let retainedBytes = meter.retainedIntermediateBytes
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
                #expect(meter.retainedIntermediateBytes == retainedBytes + 3)
            }
            #expect(meter.retainedIntermediateRows == 2)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("sharing preserves contiguous element storage")
    func sharingReusesBackingStorage() throws {
        let meter = makeMeter(rows: 2, bytes: 1_024)
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
        let retainedBytes = meter.retainedIntermediateBytes
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
        #expect(meter.retainedIntermediateBytes == retainedBytes + 3)
    }

    @Test("operator hand-off retains reservation through every shared alias")
    func operatorHandOffRetainsReservation() throws {
        let meter = makeMeter(rows: 2, bytes: 1_024)
        do {
            let builder = try makeTwoElementBuilder(meter: meter)
            let retained = builder.finish()
            let retainedBytes = meter.retainedIntermediateBytes
            let shared = try retained.moveToSharedOwnership(
                at: .joinCandidate
            )
            do {
                let downstream = shared
                #expect(Array(downstream) == [1, 2])
                #expect(meter.retainedIntermediateRows == 2)
                #expect(meter.retainedIntermediateBytes == retainedBytes + 3)

                let output = shared.promoteToOutput()
                #expect(output == [1, 2])
                #expect(meter.retainedIntermediateRows == 2)
                #expect(meter.retainedIntermediateBytes == retainedBytes + 3)
            }
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("failed operator hand-off releases the consumed unique owner")
    func failedOperatorHandOffReleasesReservation() throws {
        let meter = makeMeter(rows: 2, bytes: 512)
        let builder = try makeTwoElementBuilder(
            meter: meter,
            layout: try testLayout(sharedOwnerByteCount: 1_024)
        )

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
        let meter = makeMeter(rows: 2, bytes: 512)
        var builder = try DatabaseRetainedArrayBuilder<Int>(
            workMeter: meter,
            stage: .resultMaterialization,
            layout: try testLayout(sharedOwnerByteCount: 1_024),
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
        let meter = makeMeter(rows: 2, bytes: 1_024)
        do {
            let builder = try makeTwoElementBuilder(meter: meter)
            let retained = builder.finish()
            let retainedBytes = meter.retainedIntermediateBytes
            do {
                let admission = try retained.prepareToShare(
                    at: .subqueryCache
                )
                _ = consumeWithoutCommitting(admission)
                #expect(meter.retainedIntermediateRows == 2)
                #expect(meter.retainedIntermediateBytes == retainedBytes)
            }
            #expect(meter.retainedIntermediateRows == 2)
            #expect(meter.retainedIntermediateBytes == retainedBytes)
            let output = retained.promoteToOutput()
            #expect(output == [1, 2])
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("sharing admission lifetime is independent from promotion")
    func sharingAdmissionOutlivesPromotedSource() throws {
        let meter = makeMeter(rows: 2, bytes: 1_024)
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
        let meter = makeMeter(rows: 1, bytes: 1_024)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .joinCandidate,
                layout: try testLayout()
            )
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 2)
            )
            let admittedBytes = meter.retainedIntermediateBytes

            #expect(meter.retainedIntermediateRows == 1)
            #expect(admittedBytes >= 5)

            let candidate = 42
            builder.append(candidate, using: admission)

            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes == admittedBytes - 1)
            let retained = builder.finish()
            let retainedValue = retained.withSpan { $0[0] }
            #expect(retainedValue == 42)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("abandoned append admission retains only materialized capacity")
    func abandonedAppendAdmissionRollsBackPayload() throws {
        let meter = makeMeter(rows: 1, bytes: 1_024)
        do {
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .joinCandidate,
                layout: try testLayout()
            )
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 2)
            )
            let admittedBytes = meter.retainedIntermediateBytes

            #expect(meter.retainedIntermediateRows == 1)
            _ = consumeWithoutAppending(admission)

            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == admittedBytes - 3)
            let retained = builder.finish()
            let retainedIsEmpty = retained.isEmpty
            #expect(retainedIsEmpty)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("append admission lifetime is independent from source promotion")
    func appendAdmissionOutlivesPromotedSource() throws {
        let meter = makeMeter(rows: 1, bytes: 1_024)
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

    @Test("actual Array capacity is fully accounted after every growth")
    func actualCapacityGrowthIsFullyAccounted() throws {
        for elementCount in 1...5 {
            let meter = makeMeter(rows: 1, bytes: 1_024)
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .resultMaterialization,
                layout: try testLayout()
            )
            for value in 0..<elementCount {
                try builder.append(
                    footprint: DatabaseIntermediateFootprint()
                ) {
                    value
                }
            }
            let retainedBytes = meter.retainedIntermediateBytes
            let peakBytes = meter.peakIntermediateBytes
            let output = builder.finish().promoteToOutput()

            #expect(output.count == elementCount)
            #expect(retainedBytes == 2 + UInt64(output.capacity))
            #expect(peakBytes >= retainedBytes)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("expected count reserves canonical spare capacity")
    func expectedCountReservesCanonicalCapacity() throws {
        let meter = makeMeter(rows: 1, bytes: 1_024)
        do {
            let builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .resultMaterialization,
                layout: try testLayout(),
                expectedCount: 3
            )
            let accountedBytes = meter.retainedIntermediateBytes
            let retained = builder.finish()
            let retainedIsEmpty = retained.isEmpty
            #expect(retainedIsEmpty)
            let output = retained.promoteToOutput()
            #expect(accountedBytes == 2 + UInt64(output.capacity))
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("reservation follows the Array buffer's actual capacity")
    func actualArrayCapacityIsAccounted() throws {
        let containerBytes: UInt64 = 11
        let slotBytes: UInt64 = 17
        let layout = try DatabaseRetainedArrayLayout.validated(
            containerByteCount: containerBytes,
            elementCapacitySlotByteCount: slotBytes,
            sharedOwnerByteCount: 3,
            appendAdmissionByteCount: 1
        )
        let meter = makeMeter(rows: 1, bytes: 1_024)
        let builder = try DatabaseRetainedArrayBuilder<Int>(
            workMeter: meter,
            stage: .resultMaterialization,
            layout: layout,
            expectedCount: 3
        )
        let accountedBytes = meter.retainedIntermediateBytes
        let output = builder.finish().promoteToOutput()

        #expect(
            accountedBytes
                == containerBytes + UInt64(output.capacity) * slotBytes
        )
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
        let meter = makeMeter(rows: 2, bytes: 1_024)
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

    @Test("RDF graph promotion and shared continuation pages preserve accounting")
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

        var sharedBuilder = try DatabaseRetainedRDFGraphBuilder(
            workMeter: meter
        )
        for identifier in 1...3 {
            try sharedBuilder.append(try graphQuad(identifier: identifier))
        }
        let shared = try sharedBuilder.finish()
            .moveToSharedOwnership()
        let sourceRows = meter.retainedIntermediateRows
        let promotedPage: [RDFQuad]
        do {
            let retainedPage = try shared.retainedPage(
                1..<3,
                workMeter: meter
            )
            #expect(meter.retainedIntermediateRows == sourceRows + 2)
            promotedPage = retainedPage.promoteToOutput()
        }
        #expect(
            promotedPage == [
                try graphQuad(identifier: 2),
                try graphQuad(identifier: 3),
            ]
        )
        #expect(meter.retainedIntermediateRows == sourceRows)
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

    private func testLayout(
        sharedOwnerByteCount: UInt64 = 3
    ) throws -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 2,
            elementCapacitySlotByteCount: 1,
            sharedOwnerByteCount: sharedOwnerByteCount,
            appendAdmissionByteCount: 1
        )
    }

    private func makeTwoElementBuilder(
        meter: DatabaseWorkMeter,
        layout: DatabaseRetainedArrayLayout? = nil
    ) throws -> DatabaseRetainedArrayBuilder<Int> {
        let selectedLayout = try layout ?? testLayout()
        var builder = try DatabaseRetainedArrayBuilder<Int>(
            workMeter: meter,
            stage: .resultMaterialization,
            layout: selectedLayout,
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
