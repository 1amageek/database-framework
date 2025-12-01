import Testing
import Foundation
import DatabaseEngine
@testable import DatabaseEngine

@Suite("IndexState Tests")
struct IndexStateTests {

    @Test("IndexState rawValue")
    func testRawValue() {
        #expect(IndexState.readable.rawValue == 0)
        #expect(IndexState.disabled.rawValue == 1)
        #expect(IndexState.writeOnly.rawValue == 2)
    }

    @Test("IndexState isReadable")
    func testIsReadable() {
        #expect(IndexState.readable.isReadable == true)
        #expect(IndexState.disabled.isReadable == false)
        #expect(IndexState.writeOnly.isReadable == false)
    }

    @Test("IndexState shouldMaintain")
    func testShouldMaintain() {
        #expect(IndexState.readable.shouldMaintain == true)
        #expect(IndexState.disabled.shouldMaintain == false)
        #expect(IndexState.writeOnly.shouldMaintain == true)
    }

    @Test("IndexState description")
    func testDescription() {
        #expect(IndexState.readable.description == "readable")
        #expect(IndexState.disabled.description == "disabled")
        #expect(IndexState.writeOnly.description == "writeOnly")
    }

    @Test("IndexState Sendable conformance")
    func testSendable() {
        let state = IndexState.readable

        Task {
            let _ = state  // Can be captured in async context
        }

        #expect(state.isReadable == true)
    }

    @Test("IndexState roundtrip from rawValue")
    func testRoundtrip() {
        let states: [IndexState] = [.readable, .disabled, .writeOnly]

        for state in states {
            let rawValue = state.rawValue
            let reconstructed = IndexState(rawValue: rawValue)
            #expect(reconstructed == state)
        }
    }
}
