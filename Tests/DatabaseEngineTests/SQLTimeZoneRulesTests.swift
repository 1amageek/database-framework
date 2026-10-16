import DatabaseKit
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Synchronization
import Testing
@testable import DatabaseEngine

@Suite("Explicit immutable SQL time zone rules")
struct SQLTimeZoneRulesTests {
    private var limits: SQLTimeZoneDatabase.Limits {
        .init(maximumInputBytes: 100_000, maximumRetainedBytes: 1_000_000, maximumTransitions: 10_000)
    }
    private func meter(work: UInt64 = 100_000, bytes: UInt64 = 0) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: work, maximumIntermediateBytes: bytes),
            monotonicClock: SystemStorageClock())
    }
    private func database(_ bytes: ByteString, limits: SQLTimeZoneDatabase.Limits? = nil) throws -> SQLTimeZoneDatabase {
        try SQLTimeZoneDatabase(definitions: [.init(name: "Zone", tzif: bytes)],
            provenance: ByteString(utf8: "IANA 2026e; pinned fixture manifest"), limits: limits ?? self.limits)
    }
    private func rules(_ bytes: ByteString, meter: DatabaseWorkMeter? = nil) throws -> SQLTimeZoneRules {
        try database(bytes).rules(named: "Zone", workMeter: meter ?? self.meter())
    }
    private func offset(_ rules: SQLTimeZoneRules, _ second: Int64) throws -> Int32 {
        try rules.offset(at: Timestamp(secondsSinceUnixEpoch: second), workMeter: meter())
    }

    @Test("Supplied names and explicit aliases use ASCII case lookup and reject conflicts/cycles")
    func namesAndAliases() throws {
        let db = try SQLTimeZoneDatabase(definitions: [.init(name: "America/New_York", tzif: SQLTimeZoneFixtures.newYork())],
            aliases: [.init(name: "Alias", target: "US/Eastern"), .init(name: "US/Eastern", target: "america/new_york")],
            provenance: ByteString(utf8: "2026e"), limits: limits)
        let work = meter()
        let provider: any SQLTimeZoneRulesProvider = db
        for name in ["America/New_York", "AMERICA/new_York", "alias", "us/EASTERN"] {
            let rule = try provider.rules(named: name, workMeter: work)
            let actual = try offset(rule, 1_704_067_200)
            #expect(actual == -18_000)
        }
        #expect(work.retainedIntermediateBytes == 0)
        #expect(throws: SQLTimeZoneError.unknownZone) { try provider.rules(named: "EST5EDT", workMeter: work) }
        #expect(throws: SQLTimeZoneError.malformedData) {
            try SQLTimeZoneDatabase(definitions: [.init(name: "Zone", tzif: SQLTimeZoneFixtures.utc())],
                aliases: [.init(name: "ZONE", target: "Zone")], provenance: ByteString(utf8: "v"), limits: limits)
        }
        #expect(throws: SQLTimeZoneError.malformedData) {
            try SQLTimeZoneDatabase(definitions: [], aliases: [.init(name: "A", target: "B"), .init(name: "B", target: "A")],
                provenance: ByteString(utf8: "v"), limits: limits)
        }
        #expect(throws: SQLTimeZoneError.malformedData) {
            try SQLTimeZoneDatabase(definitions: [.init(name: "Zöne", tzif: SQLTimeZoneFixtures.utc())],
                provenance: ByteString(utf8: "v"), limits: limits)
        }
    }

    @Test("TZif versions 2/3/4 with zero leap records preserve full timestamp width")
    func supportedDataVersions() throws {
        for version: UInt8 in [50, 51, 52] {
            let rule = try rules(synthetic(footer: "STD-1", version: version))
            for second in [Int64.min, -1, 0, 4_102_444_800, Int64.max] {
                let actual = try offset(rule, second)
                #expect(actual == 3600)
            }
        }
        #expect(throws: SQLTimeZoneError.unsupportedData) { try database(synthetic(footer: "STD0", version: 0)) }
        var leap = Array(SQLTimeZoneFixtures.utc())
        leap[31] = 1
        #expect(throws: SQLTimeZoneError.unsupportedData) { try database(exact(leap)) }
    }

    @Test("Malformed headers, indices, ordering, designations and footer remain typed failures")
    func invalidData() throws {
        let source = Array(SQLTimeZoneFixtures.newYork())
        for count in [0, 4, 43, source.count - 1] {
            #expect(throws: SQLTimeZoneError.malformedData) { try database(exact(Array(source.prefix(count)))) }
        }
        var flags = Array(synthetic(footer: "STD-1"))
        flags[5] = 1
        #expect(throws: SQLTimeZoneError.malformedData) { try database(exact(flags)) }
        var enormous = Array(synthetic(footer: "STD-1"))
        for index in 32..<36 { enormous[index] = 255 }
        #expect(throws: SQLTimeZoneError.malformedData) { try database(exact(enormous)) }
        var index = Array(synthetic(footer: "", transitions: [100]))
        // Legacy block is 44 + 6 + 4 bytes; second header is another 44 bytes.
        index[98 + 8] = 2
        #expect(throws: SQLTimeZoneError.malformedData) { try database(exact(index)) }
        for (position, value): (Int, UInt8) in [(102, 2), (103, 255), (117, 65)] {
            var invalidType = Array(synthetic(footer: "STD-1"))
            invalidType[position] = value
            #expect(throws: SQLTimeZoneError.malformedData) { try database(exact(invalidType)) }
        }
        #expect(throws: SQLTimeZoneError.malformedData) { try database(synthetic(footer: "", transitions: [100, 100])) }
        for footer in ["STD0DST", "STD0DST,J0,J300", "STD0DST,M0.1.0,M11.1.0", "STD0DST,M3.2.0/-1,M11.1.0", "STD0\n", "STD0DST,M3.2.0,M11.1.0garbage"] {
            #expect(throws: SQLTimeZoneError.malformedData) { try database(synthetic(footer: footer)) }
        }
    }

    @Test("Type zero and empty-footer exact-last coverage follow RFC 9636")
    func coverageBoundaries() throws {
        let bounded = try rules(synthetic(footer: "", transitions: [100]))
        let before = try offset(bounded, -1_000)
        let inside = try offset(bounded, 99)
        #expect(before == 3600)
        #expect(inside == 3600)
        for second: Int64 in [100, 101, .max] {
            #expect(throws: SQLTimeZoneError.rangeUnavailable) { try offset(bounded, second) }
            #expect(throws: SQLTimeZoneError.rangeUnavailable) {
                try bounded.transition(beforeOrAt: Timestamp(secondsSinceUnixEpoch: second), workMeter: meter())
            }
        }
        let fixed = try rules(synthetic(footer: ""))
        let actual = try offset(fixed, .max)
        let next = try fixed.transition(after: Timestamp(secondsSinceUnixEpoch: 0), workMeter: meter())
        #expect(actual == 3600)
        #expect(next == nil)
        let unknown = try rules(synthetic(footer: "<-00>0"))
        #expect(throws: SQLTimeZoneError.rangeUnavailable) { try offset(unknown, 0) }
    }

    @Test("Real New York rules expose exact DST boundaries and future recurring transitions")
    func newYorkTransitions() throws {
        let rule = try rules(SQLTimeZoneFixtures.newYork())
        for (second, expected): (Int64, Int32) in [(1_710_053_999, -18_000), (1_710_054_000, -14_400),
            (1_730_613_599, -14_400), (1_730_613_600, -18_000), (4_102_444_800, -18_000), (4_118_083_200, -14_400)] {
            let actual = try offset(rule, second)
            #expect(actual == expected)
        }
        let next = try rule.transition(after: Timestamp(secondsSinceUnixEpoch: 1_710_053_999), workMeter: meter())
        let previous = try rule.transition(beforeOrAt: Timestamp(secondsSinceUnixEpoch: 1_710_054_000), workMeter: meter())
        let expected = SQLTimeZoneRules.Transition(instant: Timestamp(secondsSinceUnixEpoch: 1_710_054_000),
            offsetBefore: -18_000, offsetAfter: -14_400)
        #expect(next == expected)
        #expect(previous == expected)
        let future = try rule.transition(after: Timestamp(secondsSinceUnixEpoch: 4_102_444_800), workMeter: meter())
        #expect(future != nil)
        #expect(future?.offsetBefore == -18_000)
        #expect(future?.offsetAfter == -14_400)
    }

    @Test("Lord Howe keeps half-hour DST and southern-hemisphere future designations")
    func lordHoweTransitions() throws {
        let rule = try rules(SQLTimeZoneFixtures.lordHowe())
        for (second, expected): (Int64, Int32) in [(1_712_415_599, 39_600), (1_712_415_600, 37_800),
            (1_728_142_199, 37_800), (1_728_142_200, 39_600), (4_102_444_800, 39_600), (4_118_083_200, 37_800)] {
            let actual = try offset(rule, second)
            #expect(actual == expected)
        }
        let previous = try rule.transition(beforeOrAt: Timestamp(secondsSinceUnixEpoch: 1_712_415_600), workMeter: meter())
        #expect(previous?.offsetBefore == 39_600)
        #expect(previous?.offsetAfter == 37_800)
        let summer = try rule.designation(at: Timestamp(secondsSinceUnixEpoch: 4_102_444_800), workMeter: meter())
        let winter = try rule.designation(at: Timestamp(secondsSinceUnixEpoch: 4_118_083_200), workMeter: meter())
        #expect(summer.abbreviation == ByteString(utf8: "+11"))
        #expect(winter.abbreviation == ByteString(utf8: "+1030"))
    }

    @Test("Historical second-based offsets and bounded designation views preserve the raw owner")
    func historicalDesignations() throws {
        let paris = try rules(SQLTimeZoneFixtures.paris())
        let historical = try paris.designation(at: Timestamp(secondsSinceUnixEpoch: -2_208_988_800), workMeter: meter())
        #expect(historical.offsetSeconds == 561)
        #expect(historical.abbreviation == ByteString(utf8: "PMT"))
        #expect(historical.abbreviation.retainedByteCount == SQLTimeZoneFixtures.paris().count)
        let newYork = try rules(SQLTimeZoneFixtures.newYork())
        let first = try newYork.designation(at: Timestamp(secondsSinceUnixEpoch: -3_786_825_600), workMeter: meter())
        #expect(first.offsetSeconds == -17_762)
        #expect(first.abbreviation == ByteString(utf8: "LMT"))
    }

    @Test("Future footer handles Julian/ordinal leap rules, signed times and perpetual DST")
    func futureRules() throws {
        let julian = try rules(synthetic(footer: "STD0DST-1,J60/0,J300/0"))
        let ordinal = try rules(synthetic(footer: "STD0DST-1,59/0,300/0"))
        let julianLeap = try offset(julian, 1_709_208_000)
        let ordinalLeap = try offset(ordinal, 1_709_208_000)
        let julianMarch = try offset(julian, 1_709_294_400)
        #expect(julianLeap == 0)
        #expect(ordinalLeap == 3600)
        #expect(julianMarch == 3600)
        let signed = try rules(synthetic(footer: "STD0DST-1,M3.2.0/-1,M11.1.0/26", version: 51))
        let before = try offset(signed, 1_710_025_199)
        let after = try offset(signed, 1_710_025_200)
        #expect(before == 0)
        #expect(after == 3600)
        let perpetual = try rules(synthetic(footer: "XXX3EDT4,0/0,J365/23", version: 51))
        for second: Int64 in [0, 1_704_067_200, 1_719_792_000] {
            let actual = try offset(perpetual, second)
            #expect(actual == -14_400)
        }
        let transition = try perpetual.transition(after: Timestamp(secondsSinceUnixEpoch: 0), workMeter: meter())
        #expect(transition == nil)
    }

    @Test("Bootstrap input/retained/transition limits admit exact boundaries and reject one past")
    func bootstrapAdmission() throws {
        let payload = SQLTimeZoneFixtures.newYork()
        let probe = try database(payload)
        let exactLimits = SQLTimeZoneDatabase.Limits(maximumInputBytes: probe.inputByteCount,
            maximumRetainedBytes: probe.retainedByteCount, maximumTransitions: probe.transitionCount)
        let accepted = try database(payload, limits: exactLimits)
        #expect(accepted.transitionCount == 175)
        for limits in [
            SQLTimeZoneDatabase.Limits(maximumInputBytes: probe.inputByteCount - 1, maximumRetainedBytes: probe.retainedByteCount, maximumTransitions: probe.transitionCount),
            .init(maximumInputBytes: probe.inputByteCount, maximumRetainedBytes: probe.retainedByteCount - 1, maximumTransitions: probe.transitionCount),
            .init(maximumInputBytes: probe.inputByteCount, maximumRetainedBytes: probe.retainedByteCount, maximumTransitions: probe.transitionCount - 1)] {
            #expect(throws: SQLTimeZoneError.admissionExceeded) { try database(payload, limits: limits) }
        }
        let padded = ByteString(repeating: 0, count: 4096)
        let view = padded[0..<payload.count]
        // Known enclosing owners are accounted in full even when visible data is shorter.
        let originalOwner = SQLTimeZoneTestOwner(payload: payload, reportedExtent: 4096)
        let ownerBytes = ByteString(retaining: originalOwner)
        let enclosing = try database(ownerBytes)
        #expect(enclosing.retainedByteCount - probe.retainedByteCount == 4096 - UInt64(payload.count))
        #expect(view.retainedByteCount == padded.retainedByteCount)
    }

    @Test("Query work, cancellation, deadline and immutable snapshot lifetime preserve resource causes")
    func queryResourcesAndSnapshotLifetime() async throws {
        let db = try database(SQLTimeZoneFixtures.newYork())
        let probe = meter()
        let rule = try db.rules(named: "Zone", workMeter: probe)
        _ = try rule.designation(at: Timestamp(secondsSinceUnixEpoch: 4_118_083_200), workMeter: probe)
        let exactMeter = meter(work: probe.consumedWorkUnits)
        let exactRule = try db.rules(named: "Zone", workMeter: exactMeter)
        _ = try exactRule.designation(at: Timestamp(secondsSinceUnixEpoch: 4_118_083_200), workMeter: exactMeter)
        let short = meter(work: probe.consumedWorkUnits - 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            let selected = try db.rules(named: "Zone", workMeter: short)
            _ = try selected.designation(at: Timestamp(secondsSinceUnixEpoch: 4_118_083_200), workMeter: short)
        }
        #expect(short.retainedIntermediateBytes == 0)
        #expect(exactMeter.retainedIntermediateBytes == 0)
        let cancelled = meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try db.rules(named: "Zone", workMeter: cancelled)
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        let deadline = DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: 100_000, timeoutMilliseconds: 0),
            monotonicClock: SQLTimeZoneDeadlineClock())
        #expect(throws: DatabaseWorkLimitError.deadline(stage: .expressionEvaluation)) {
            try db.rules(named: "Zone", workMeter: deadline)
        }
        weak var retainedOwner: SQLTimeZoneTestOwner?
        var retainedRule: SQLTimeZoneRules?
        do {
            let owner = SQLTimeZoneTestOwner(payload: SQLTimeZoneFixtures.utc(), reportedExtent: 111)
            retainedOwner = owner
            retainedRule = try rules(ByteString(retaining: owner))
        }
        #expect(retainedOwner != nil)
        let retainedOffset = try offset(retainedRule!, 0)
        #expect(retainedOffset == 0)
        retainedRule = nil
        #expect(retainedOwner == nil)
        weak var copiedOwner: SQLTimeZoneTestOwner?
        var copiedRule: SQLTimeZoneRules?
        do {
            let owner = SQLTimeZoneTestOwner(payload: SQLTimeZoneFixtures.utc(), reportedExtent: nil)
            copiedOwner = owner
            copiedRule = try rules(ByteString(retaining: owner))
        }
        #expect(copiedOwner == nil)
        let copiedOffset = try offset(copiedRule!, 0)
        #expect(copiedOffset == 0)
        copiedRule = nil
        #expect(cancelled.retainedIntermediateBytes == 0)
        #expect(deadline.retainedIntermediateBytes == 0)
    }

    private func exact(_ bytes: [UInt8]) -> ByteString {
        bytes.withUnsafeBytes { source in ByteString.copying(count: source.count) { $0.copyMemory(from: source) } }
    }

    // Controlled RFC fixtures, not official IANA zones. They isolate version,
    // undefined coverage and POSIX grammar behavior without host timezone APIs.
    private func synthetic(footer: String, version: UInt8 = 50, transitions: [Int64] = []) -> ByteString {
        var bytes: [UInt8] = []
        func u32(_ value: UInt32) { for shift in stride(from: 24, through: 0, by: -8) { bytes.append(UInt8(truncatingIfNeeded: value >> shift)) } }
        func header(times: Int, types: UInt32, chars: UInt32) {
            bytes += [84,90,105,102,version]
            bytes += [UInt8](repeating: 0, count: 15)
            for value: UInt32 in [0,0,0,UInt32(times),types,chars] { u32(value) }
        }
        header(times: 0, types: 1, chars: 4)
        u32(0); bytes += [0,0,85,84,67,0]
        header(times: transitions.count, types: 2, chars: 8)
        for second in transitions {
            for shift in stride(from: 56, through: 0, by: -8) { bytes.append(UInt8(truncatingIfNeeded: UInt64(bitPattern: second) >> shift)) }
        }
        bytes += [UInt8](repeating: 1, count: transitions.count)
        u32(3600); bytes += [0,0]
        u32(7200); bytes += [1,4]
        bytes += [83,84,68,0,68,83,84,0,10]
        bytes += footer.utf8
        bytes.append(10)
        return exact(bytes)
    }
}

private final class SQLTimeZoneTestOwner: ByteStringOwner, Sendable {
    let payload: ByteString
    let reportedExtent: Int?
    init(payload: ByteString, reportedExtent: Int?) { self.payload = payload; self.reportedExtent = reportedExtent }
    var count: Int { payload.count }
    var retainedByteCount: Int? { reportedExtent }
    func borrowBytes(_ body: (UnsafeRawBufferPointer) throws -> Void) rethrows { try payload.withUnsafeBytes(body) }
}

private final class SQLTimeZoneDeadlineClock: StorageMonotonicClock, Sendable {
    private let ticks = Mutex(Int64(0))
    var now: StorageInstant {
        ticks.withLock { value in
            defer { value += 1 }
            return StorageInstant(durationSinceReference: .milliseconds(value))
        }
    }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
}
