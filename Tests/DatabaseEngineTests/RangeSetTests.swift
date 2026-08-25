#if !os(WASI)
import DatabaseTypes
import Testing
import TestHeartbeat
@testable import DatabaseEngine

@Suite("RangeSet Tests", .heartbeat)
struct RangeSetTests {
    @Test("RangeSet creation with initial range")
    func rangeSetCreation() {
        let begin: ByteString = [0x00]
        let end: ByteString = [0xFF]

        let rangeSet = RangeSet(initialRange: (begin: begin, end: end))

        #expect(!rangeSet.isEmpty)
    }

    @Test("RangeSet batch extraction")
    func rangeSetBatchExtraction() {
        let begin: ByteString = [0x00]
        let end: ByteString = [0xFF]

        let rangeSet = RangeSet(initialRange: (begin: begin, end: end))
        let bounds = rangeSet.nextBatchBounds()

        #expect(bounds != nil)
        #expect(bounds?.begin == begin)
    }

    @Test("RangeSet marks completed ranges")
    func rangeSetMarksCompletedRanges() throws {
        let begin: ByteString = [0x00]
        let end: ByteString = [0xFF]
        var rangeSet = RangeSet(initialRange: (begin: begin, end: end))

        if let bounds = rangeSet.nextBatchBounds() {
            try rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
        }

        #expect(rangeSet.nextBatchBounds() == nil)
    }

    @Test("RangeSet storage encoding round-trips")
    func rangeSetStorageEncodingRoundTrips() throws {
        let begin: ByteString = [0x00]
        let end: ByteString = [0xFF]
        let rangeSet = RangeSet(
            initialRange: (begin: begin, end: end)
        )

        let encoded = try RangeSetCodec.encode(rangeSet)
        let decoded = try RangeSetCodec.decode(encoded)

        #expect(!decoded.isEmpty)
    }

    @Test("Empty RangeSet is complete")
    func emptyRangeSetIsComplete() throws {
        let begin: ByteString = [0x00]
        let end: ByteString = [0x01]
        var rangeSet = RangeSet(
            initialRange: (begin: begin, end: end)
        )

        while let bounds = rangeSet.nextBatchBounds() {
            try rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
        }

        #expect(rangeSet.isEmpty)
    }

    @Test("RangeSet rejects invalid progress state")
    func rangeSetRejectsInvalidProgressState() throws {
        let begin: ByteString = [0x10]
        let end: ByteString = [0x20]
        var rangeSet = RangeSet(
            initialRange: (begin: begin, end: end)
        )

        #expect(throws: RangeSetError.invalidRangeIndex(index: -1, count: 1)) {
            try rangeSet.markRangeComplete(rangeIndex: -1)
        }
        #expect(
            throws: RangeSetError.progressKeyOutsideRange(index: 0, key: [0x20])
        ) {
            try rangeSet.recordProgress(
                rangeIndex: 0,
                lastProcessedKey: [0x20],
                isComplete: false
            )
        }

        try rangeSet.recordProgress(
            rangeIndex: 0,
            lastProcessedKey: [0x18],
            isComplete: false
        )
        #expect(
            throws: RangeSetError.progressRegression(
                index: 0,
                previous: [0x18],
                next: [0x17]
            )
        ) {
            try rangeSet.recordProgress(
                rangeIndex: 0,
                lastProcessedKey: [0x17],
                isComplete: false
            )
        }
    }
}
#endif
