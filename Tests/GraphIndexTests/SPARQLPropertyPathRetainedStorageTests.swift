import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Testing
@testable import GraphIndex

@Suite("SPARQL property-path retained storage")
struct SPARQLPropertyPathRetainedStorageTests {
    @Test("Unique relation resumes without replacing its Array buffer")
    func uniqueRelationResumesWithoutBufferCopy() throws {
        let workMeter = makePropertyPathWorkMeter()

        do {
            var firstBuilder = try SPARQLPropertyPathMatchBuilder.make(
                workMeter: workMeter,
                maximumResults: 2,
                expectedCount: 2
            )
            try firstBuilder.append(
                start: .iri("urn:start:1"),
                end: .iri("urn:end:1")
            )
            let first = firstBuilder.finish()
            let firstAddress = propertyPathMatchBufferAddress(first)

            var resumed = try SPARQLPropertyPathMatchBuilder.resuming(
                consume first,
                workMeter: workMeter,
                maximumResults: 2
            )
            try resumed.append(
                start: .iri("urn:start:2"),
                end: .iri("urn:end:2")
            )
            let second = resumed.finish()
            let secondAddress = propertyPathMatchBufferAddress(second)
            let secondCount = second.count

            #expect(firstAddress != nil)
            #expect(secondAddress == firstAddress)
            #expect(secondCount == 2)
            _ = consume second
        }

        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Result limit rejects before footprint or capacity admission")
    func resultLimitPrecedesAdmission() throws {
        let workMeter = makePropertyPathWorkMeter()

        do {
            var builder = try SPARQLPropertyPathMatchBuilder.make(
                workMeter: workMeter,
                maximumResults: 0,
                expectedCount: 128
            )
            let retainedBytes = workMeter.retainedIntermediateBytes
            let peakBytes = workMeter.peakIntermediateBytes
            let deepTerm = makeDeepPropertyPathTerm(depth: 32)

            do {
                try builder.append(
                    start: deepTerm,
                    end: .iri("urn:end")
                )
                Issue.record("Expected the result limit to reject the match")
            } catch let error as SPARQLQueryError {
                guard case .propertyPathResultLimitExceeded(let maximum) = error else {
                    Issue.record("Unexpected SPARQL error: \(error)")
                    return
                }
                #expect(maximum == 0)
            }

            #expect(workMeter.retainedIntermediateBytes == retainedBytes)
            #expect(workMeter.peakIntermediateBytes == peakBytes)
            let builderIsEmpty = builder.isEmpty
            #expect(builderIsEmpty)
            let empty = builder.finish()
            let relationIsEmpty = empty.isEmpty
            #expect(relationIsEmpty)
            _ = consume empty
        }

        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Deep RDF-star scratch failure is bounded and reusable")
    func deepRDFStarScratchFailureIsBounded() throws {
        let workMeter = makePropertyPathWorkMeter(
            maximumIntermediateBytes: 500
        )
        let footprintMeter = try SPARQLPropertyPathMatchFootprintMeter.make(
            workMeter: workMeter,
            stage: .pathExpansion
        )
        defer { footprintMeter.shutdown() }

        #expect {
            try footprintMeter.footprint(
                start: makeDeepPropertyPathTerm(depth: 8),
                end: .iri("urn:end")
            )
        } throws: { error in
            error as? DatabaseWorkLimitError
                == .maximumIntermediateBytes(
                    stage: .pathExpansion,
                    consumed: 320,
                    requested: 256,
                    maximum: 500
                )
        }

        let shallow = try footprintMeter.footprint(
            start: .iri("urn:start"),
            end: .iri("urn:end")
        )
        #expect(shallow.rows == 1)
        #expect(shallow.bytes > 0)
        #expect(workMeter.retainedIntermediateBytes == 320)

        footprintMeter.shutdown()
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Set duplicate probe retains no additional memory")
    func setDuplicateProbeDoesNotGrow() throws {
        let workMeter = makePropertyPathWorkMeter()
        let match = SPARQLPropertyPathMatch(
            start: .iri("urn:start"),
            end: .iri("urn:end")
        )

        do {
            var matches = try SPARQLPropertyPathMatchSet.make(
                workMeter: workMeter
            )
            let inserted = try matches.insert(match)
            #expect(inserted)
            let retainedRows = workMeter.retainedIntermediateRows
            let retainedBytes = workMeter.retainedIntermediateBytes
            let peakBytes = workMeter.peakIntermediateBytes

            let containsMatch = matches.contains(match)
            #expect(containsMatch)
            let insertedDuplicate = try matches.insert(match)
            #expect(!insertedDuplicate)
            #expect(workMeter.retainedIntermediateRows == retainedRows)
            #expect(workMeter.retainedIntermediateBytes == retainedBytes)
            #expect(workMeter.peakIntermediateBytes == peakBytes)
            let matchCount = matches.count
            #expect(matchCount == 1)
        }

        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Array and Set owners are both retained and released")
    func simultaneousOwnersHaveExactLifetime() throws {
        let workMeter = makePropertyPathWorkMeter()
        let match = SPARQLPropertyPathMatch(
            start: .iri("urn:start"),
            end: .iri("urn:end")
        )

        do {
            var seen = try SPARQLPropertyPathMatchSet.make(
                workMeter: workMeter
            )
            var builder = try SPARQLPropertyPathMatchBuilder.make(
                workMeter: workMeter,
                maximumResults: 1,
                expectedCount: 1
            )

            let inserted = try seen.insert(match)
            #expect(inserted)
            try builder.appendBorrowed(match)
            let relation = builder.finish()

            let seenCount = seen.count
            let relationCount = relation.count
            #expect(seenCount == 1)
            #expect(relationCount == 1)
            #expect(workMeter.retainedIntermediateRows == 2)
            _ = consume relation
            #expect(workMeter.retainedIntermediateRows == 1)
            let retainedSeenCount = seen.count
            #expect(retainedSeenCount == 1)
        }

        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Set capacity overflow is typed and allocation-free")
    func setCapacityOverflowIsTyped() {
        let overflowingCapacity = (Int.max / 2) + 1

        #expect {
            try SPARQLPropertyPathMatchSet.capacityGrowth(
                from: overflowingCapacity,
                toFit: Int.max
            )
        } throws: { error in
            error as? SPARQLPropertyPathMatchStorageError
                == .setCapacityOverflow(
                    currentCapacity: overflowingCapacity
                )
        }
    }

    @Test("Set owner is admitted before construction")
    func setOwnerAdmissionPrecedesConstruction() {
        let workMeter = makePropertyPathWorkMeter(
            maximumIntermediateBytes: 127
        )

        #expect {
            let set = try SPARQLPropertyPathMatchSet.make(
                workMeter: workMeter
            )
            _ = consume set
        } throws: { error in
            error as? DatabaseWorkLimitError
                == .maximumIntermediateBytes(
                    stage: .deduplication,
                    consumed: 64,
                    requested: 64,
                    maximum: 127
                )
        }
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }
}

private func makePropertyPathWorkMeter(
    maximumIntermediateBytes: UInt64 = 1_000_000
) -> DatabaseWorkMeter {
    DatabaseWorkMeter(
        budget: DatabaseExecutionBudget(
            maximumRows: 100,
            maximumWorkUnits: 1_000,
            maximumIntermediateRows: 100,
            maximumIntermediateBytes: maximumIntermediateBytes,
            timeoutMilliseconds: 30_000
        )
    )
}

private func makeDeepPropertyPathTerm(
    depth: Int
) -> DatabaseRDFTerm {
    var term = DatabaseRDFTerm.iri("urn:leaf")
    for index in 0..<depth {
        term = .tripleTerm(
            subject: term,
            predicate: .iri("urn:predicate:\(index)"),
            object: .iri("urn:object:\(index)")
        )
    }
    return term
}

private func propertyPathMatchBufferAddress(
    _ matches: borrowing SPARQLPropertyPathMatches
) -> UInt? {
    matches.withSpan { span in
        span.withUnsafeBufferPointer { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        }
    }
}
