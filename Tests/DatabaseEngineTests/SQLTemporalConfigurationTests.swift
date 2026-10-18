import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("SQL temporal supplied configuration", .timeLimit(.minutes(1)))
struct SQLTemporalConfigurationTests {
    @Test("Fixed abbreviation admission rejects invalid names, builtin overrides and case duplicates")
    func abbreviationAdmissionBoundaries() throws {
        typealias Abbreviation = SQLTemporalConfiguration.Abbreviation
        for name in ["", "ABCDEFGHIJK", "A1", "A-B", "A_B", "é", "A B"] {
            #expect(throws: SQLTimeZoneError.malformedData) { try Abbreviation(name: name, offsetSeconds: 0) }
        }
        let valid = try Abbreviation(name: "AbCdEfGhIj", offsetSeconds: 1)
        #expect(valid.name == "AbCdEfGhIj")
        for name in ["utc", "GMT", "z"] {
            #expect(throws: SQLTimeZoneError.malformedData) {
                try SQLTemporalConfiguration(abbreviations: [Abbreviation(name: name, offsetSeconds: 1)],
                    limits: .init(maximumAbbreviations: 1, maximumRetainedBytes: 15))
            }
        }
        #expect(throws: SQLTimeZoneError.malformedData) {
            try SQLTemporalConfiguration(abbreviations: [
                Abbreviation(name: "Custom", offsetSeconds: 1), Abbreviation(name: "cUSTOM", offsetSeconds: 2),
            ], limits: .init(maximumAbbreviations: 2, maximumRetainedBytes: 30))
        }
        for limits in [
            SQLTemporalConfiguration.Limits(maximumAbbreviations: 0, maximumRetainedBytes: 15),
            .init(maximumAbbreviations: 1, maximumRetainedBytes: 14),
        ] {
            #expect(throws: SQLTimeZoneError.admissionExceeded) {
                try SQLTemporalConfiguration(abbreviations: [valid], limits: limits)
            }
        }
        let admitted = try SQLTemporalConfiguration(abbreviations: [valid],
            limits: .init(maximumAbbreviations: 1, maximumRetainedBytes: 15))
        #expect(admitted.retainedByteCount == 15)
        let empty = try SQLTemporalConfiguration(limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 0))
        #expect(empty.retainedByteCount == 0)
    }

    @Test("Abbreviation lookup preserves full offset width, builtin names and explicit work limits")
    func fixedOffsetLookupAndWork() throws {
        let configuration = try SQLTemporalConfiguration(abbreviations: [
            .init(name: "Low", offsetSeconds: .min), .init(name: "High", offsetSeconds: .max),
        ], limits: .init(maximumAbbreviations: 2, maximumRetainedBytes: 30))
        let meter = workMeter()
        #expect(try configuration.fixedOffset(forAbbreviation: "lOW", workMeter: meter) == .min)
        #expect(try configuration.fixedOffset(forAbbreviation: "hIGH", workMeter: meter) == .max)
        for name in ["UTC", "gmt", "Z"] {
            #expect(try configuration.fixedOffset(forAbbreviation: name, workMeter: meter) == 0)
        }
        #expect(try configuration.fixedOffset(forAbbreviation: "Missing", workMeter: meter) == nil)
        #expect(try configuration.fixedOffset(forAbbreviation: "ABCDEFGHIJK", workMeter: meter) == nil)
        #expect(configuration.retainedByteCount == 30)
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(throws: DatabaseWorkLimitError.maximumWorkUnits(
            stage: .expressionEvaluation, consumed: 0, requested: 3, maximum: 2
        )) { try configuration.fixedOffset(forAbbreviation: "Low", workMeter: workMeter(work: 2)) }
    }

    @Test("UTC, fixed and named zones use only supplied immutable configuration")
    func explicitZonePolicyAndProvider() throws {
        let utc = SQLTemporalConfiguration.utc
        guard case .utc = utc.timeZone else {
            Issue.record("Default SQL configuration must be UTC")
            return
        }
        #expect(utc.rulesProvider == nil)
        #expect(utc.retainedByteCount == 0)
        for offset: Int32 in [.min, 0, .max] {
            let configuration = try SQLTemporalConfiguration(timeZone: .fixed(offsetSeconds: offset),
                limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 0))
            let instant = Timestamp(secondsSinceUnixEpoch: 1_704_067_200)
            #expect(try CanonicalSQLTemporalEvaluator.offset(at: instant, zone: configuration.timeZone,
                workMeter: workMeter()) == offset)
        }
        let provider = try SQLTimeZoneDatabase(definitions: [.init(name: "Supplied/UTC", tzif: SQLTimeZoneFixtures.utc())],
            provenance: ByteString(utf8: "IANA 2026e test UTC"),
            limits: .init(maximumInputBytes: 1_000, maximumRetainedBytes: 4_000, maximumTransitions: 10))
        let rule = try provider.rules(named: "Supplied/UTC", workMeter: workMeter())
        let configuration = try SQLTemporalConfiguration(timeZone: .named(name: "Supplied/UTC", rules: rule), rulesProvider: provider,
            limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 12))
        let retainedProvider = try #require(configuration.rulesProvider)
        let retainedRule = try retainedProvider.rules(named: "supplied/utc", workMeter: workMeter())
        for second: Int64 in [-2_208_988_800, 1_704_067_200, 4_102_444_800] {
            let instant = Timestamp(secondsSinceUnixEpoch: second)
            #expect(try retainedRule.offset(at: instant, workMeter: workMeter()) == 0)
            #expect(try CanonicalSQLTemporalEvaluator.offset(at: instant, zone: configuration.timeZone,
                workMeter: workMeter()) == 0)
        }
        #expect(throws: SQLTimeZoneError.unknownZone) {
            try retainedProvider.rules(named: "Host/Timezone", workMeter: workMeter())
        }
    }

    @Test("Named zone identity rejects malformed names and admits exact retained bytes")
    func namedZoneIdentityAdmission() throws {
        let provider = try SQLTimeZoneDatabase(definitions: [.init(name: "Supplied/UTC", tzif: SQLTimeZoneFixtures.utc())],
            provenance: ByteString(utf8: "UTC fixture"),
            limits: .init(maximumInputBytes: 1_000, maximumRetainedBytes: 4_000, maximumTransitions: 10))
        let rules = try provider.rules(named: "Supplied/UTC", workMeter: workMeter())
        for name in ["", "Supplied/\0UTC"] {
            #expect(throws: SQLTimeZoneError.malformedData) {
                try SQLTemporalConfiguration(timeZone: .named(name: name, rules: rules),
                    limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 12))
            }
        }
        #expect(throws: SQLTimeZoneError.admissionExceeded) {
            try SQLTemporalConfiguration(timeZone: .named(name: "Supplied/UTC", rules: rules),
                limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 11))
        }
        let admitted = try SQLTemporalConfiguration(timeZone: .named(name: "Supplied/UTC", rules: rules),
            limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 12))
        #expect(admitted.retainedByteCount == 12)
        let copy = admitted.replacing(timeZone: admitted.timeZone)
        guard case .named(let name, _) = copy.timeZone else { Issue.record("Expected retained named zone"); return }
        #expect(name == "Supplied/UTC")
        #expect(copy.retainedByteCount == 12)
    }

    private func workMeter(work: UInt64 = 1_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: work), monotonicClock: TestProcessMonotonicClock())
    }
}
