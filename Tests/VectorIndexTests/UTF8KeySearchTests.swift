import DatabaseTypes
import TestHeartbeat
import Testing
@testable import VectorIndex

@Suite("UTF-8 key search", .heartbeat)
struct UTF8KeySearchTests {
    @Test("Search accepts a retained non-zero-index slice")
    func retainedSlice() {
        let framed = ByteString([0xFF])
            .appending(
                contentsOf: ByteString([
                    112, 114, 101, 102, 105, 120, 45,
                    104, 110, 115, 119,
                    45, 115, 117, 102, 102, 105, 120
                ])
            )
            .appending(0xEE)
        let slice = framed[1..<(framed.count - 1)]

        #expect(slice.startIndex == 1)
        #expect(containsHNSWMarker(in: slice))
    }

    @Test("Finds the HNSW marker without materializing text")
    func findsMarkerInValidUTF8() {
        #expect(containsHNSWMarker(in: [0xE9, 0x9B, 0xAA, 104, 110, 115, 119]))
        #expect(!containsHNSWMarker(in: [0xE9, 0x9B, 0xAA, 110, 115, 119]))
    }

    @Test("Rejects malformed UTF-8 like the former strict String decoder")
    func rejectsMalformedUTF8() {
        #expect(!containsHNSWMarker(in: [104, 110, 115, 119, 0xFF]))
        #expect(!containsHNSWMarker(in: [104, 110, 115, 119, 0xE9, 0x9B]))
    }
}
