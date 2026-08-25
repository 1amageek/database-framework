#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import Foundation
import Testing
import TestHeartbeat
@testable import DatabaseEngine

@Suite("ContinuationToken Tests", .serialized, .heartbeat)
struct ContinuationTokenTests {
    @Test("Empty raw token is rejected")
    func emptyTokenIsRejected() {
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationToken(data: [])
        }
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationToken(base64URLString: "")
        }
    }

    @Test("Raw token storage is bounded")
    func rawTokenStorageIsBounded() throws {
        let token = try ContinuationToken(data: [1, 2, 3])
        #expect(token.byteCount == 3)

        let oversized = ByteString(
            [UInt8](
                repeating: 0,
                count: ContinuationStateFormat.maximumByteCount + 1
            )
        )
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationToken(data: oversized)
        }
    }

    @Test("Base64url round trip preserves bytes without padding")
    func base64URLRoundTrip() throws {
        let original: ByteString = [0xfb, 0xff, 0xef, 0x01]
        let token = try ContinuationToken(data: original)

        let encoded = token.base64URLString
        #expect(!encoded.contains("+"))
        #expect(!encoded.contains("/"))
        #expect(!encoded.contains("="))
        #expect(
            try ContinuationToken(base64URLString: encoded).data == original
        )
    }

    @Test("Invalid base64url is rejected")
    func invalidBase64URLIsRejected() {
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationToken(base64URLString: "not+base64")
        }
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationToken(base64URLString: "A")
        }
    }

    @Test("Continuation state round trip preserves canonical position")
    func continuationStateRoundTrip() throws {
        let fingerprint = ByteString(
            [UInt8](
                repeating: 0x5a,
                count: SHA256Accumulator.digestByteCount
            )
        )
        let state = ContinuationState(
            nextOffset: 42,
            remainingLimit: 58,
            queryFingerprint: fingerprint
        )

        let decoded = try ContinuationState.decode(state.token())
        #expect(decoded.version == ContinuationToken.currentVersion)
        #expect(decoded.nextOffset == 42)
        #expect(decoded.remainingLimit == 58)
        #expect(decoded.queryFingerprint == fingerprint)
    }

    @Test("Continuation without a query limit round trips")
    func unlimitedContinuationRoundTrip() throws {
        let state = ContinuationState(
            nextOffset: 9,
            remainingLimit: nil,
            queryFingerprint: ByteString(
                [UInt8](
                    repeating: 1,
                    count: SHA256Accumulator.digestByteCount
                )
            )
        )

        let decoded = try ContinuationState.decode(state.token())
        #expect(decoded.nextOffset == 9)
        #expect(decoded.remainingLimit == nil)
    }

    @Test("Malformed continuation state is rejected")
    func malformedContinuationIsRejected() throws {
        let truncated = try ContinuationToken(data: [0x01, 0x02])
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationState.decode(truncated)
        }
    }

    @Test("Canonical query fingerprint covers predicates and ordering")
    func queryFingerprintCoversQuerySemantics() throws {
        let ascending = Query<ContinuationCursorUser>()
            .where(ContinuationCursorUser.fields.age > 20)
            .orderBy(ContinuationCursorUser.fields.name, .ascending)
        let same = Query<ContinuationCursorUser>()
            .where(ContinuationCursorUser.fields.age > 20)
            .orderBy(ContinuationCursorUser.fields.name, .ascending)
        let differentValue = Query<ContinuationCursorUser>()
            .where(ContinuationCursorUser.fields.age > 21)
            .orderBy(ContinuationCursorUser.fields.name, .ascending)
        let descending = Query<ContinuationCursorUser>()
            .where(ContinuationCursorUser.fields.age > 20)
            .orderBy(ContinuationCursorUser.fields.name, .descending)

        let fingerprint = try QueryFingerprint.compute(for: ascending)
        #expect(try QueryFingerprint.compute(for: same) == fingerprint)
        #expect(
            try QueryFingerprint.compute(for: differentValue) != fingerprint
        )
        #expect(try QueryFingerprint.compute(for: descending) != fingerprint)
    }

    @Test("NoNextReason descriptions are meaningful")
    func noNextReasonDescriptions() {
        #expect(NoNextReason.sourceExhausted.description == "Source exhausted")
        #expect(NoNextReason.returnLimitReached.description == "Return limit reached")
    }

    @Test("Cursor result reports continuation and completion")
    func cursorResultReportsState() throws {
        let token = try ContinuationToken(data: [1, 2, 3])
        let more: CursorResult<ContinuationCursorUser> = .more(
            items: [],
            continuation: token
        )
        #expect(more.hasMore)
        #expect(more.noNextReason == nil)

        let done: CursorResult<ContinuationCursorUser> = .done(
            items: [],
            reason: .sourceExhausted
        )
        #expect(!done.hasMore)
        #expect(done.continuation == nil)
        #expect(done.noNextReason == .sourceExhausted)
    }
}

@Persistable
private struct ContinuationCursorUser {
    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int64 = 0
}
#endif
