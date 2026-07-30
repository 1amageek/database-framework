import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import Testing
import TestSupport
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
                start: try .iri(validating: "urn:start:1"),
                end: try .iri(validating: "urn:end:1")
            )
            let first = firstBuilder.finish()
            let firstAddress = propertyPathMatchBufferAddress(first)

            var resumed = try SPARQLPropertyPathMatchBuilder.resuming(
                consume first,
                workMeter: workMeter,
                maximumResults: 2
            )
            try resumed.append(
                start: try .iri(validating: "urn:start:2"),
                end: try .iri(validating: "urn:end:2")
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
            let deepTerm = try makeDeepPropertyPathTerm(depth: 32)

            do {
                try builder.append(
                    start: deepTerm,
                    end: try .iri(validating: "urn:end")
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

    @Test("Deep RDF-star footprint uses constant traversal storage")
    func deepRDFStarFootprintUsesConstantTraversalStorage() throws {
        let workMeter = makePropertyPathWorkMeter(
            maximumIntermediateBytes: 1
        )
        let footprint = try SPARQLPropertyPathMatchRetainedFootprint.measure(
            start: try makeDeepPropertyPathTerm(depth: 512),
            end: try .iri(validating: "urn:end")
        )
        #expect(footprint.rows == 1)
        #expect(footprint.bytes > 0)
        #expect(workMeter.peakIntermediateBytes == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Set duplicate probe retains no additional memory")
    func setDuplicateProbeDoesNotGrow() throws {
        let workMeter = makePropertyPathWorkMeter()
        let match = SPARQLPropertyPathMatch(
            start: try .iri(validating: "urn:start"),
            end: try .iri(validating: "urn:end")
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
            start: try .iri(validating: "urn:start"),
            end: try .iri(validating: "urn:end")
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
            maximumIntermediateBytes: 63
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
                    consumed: 0,
                    requested: 64,
                    maximum: 63
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
        budget: ExecutionBudget(
            maximumRows: 100,
            maximumWorkUnits: 1_000,
            maximumIntermediateRows: 100,
            maximumIntermediateBytes: maximumIntermediateBytes,
            timeoutMilliseconds: 30_000
        ),
        monotonicClock: TestProcessMonotonicClock()
    )
}

private func makeDeepPropertyPathTerm(
    depth: Int
) throws -> RDFTerm {
    var term = try RDFTerm.iri(validating: "urn:leaf")
    for index in 0..<depth {
        term = .tripleTerm(
            subject: .iri(try RDFIRI("urn:subject:\(index)")),
            predicate: try RDFPredicateIRI("urn:predicate:\(index)"),
            object: term
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
