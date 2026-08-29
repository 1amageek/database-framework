import DatabaseEngine
import DatabaseTypes
import Testing

/// Golden vectors for the canonical Directory component conversion.
///
/// Every expected component in this suite is written by hand from the textual
/// grammar owned by `Sources/DatabaseEngine/Directory/DESIGN.md`. None of them
/// is produced by calling the converter under test, so a converter change that
/// silently redefines the canonical image fails here instead of migrating the
/// expectation with itself.
@Suite("Canonical Directory component codec")
struct DirectoryComponentCodecTests {
    /// Declared field kind and canonical value paired with its exact component.
    private static func canonicalVectors() throws -> [(value: FieldValue, component: String)] {
        [
            (.bool(true), "b-1"),
            (.bool(false), "b-0"),
            (.int8(-128), "i8-n128"),
            (.int8(127), "i8-127"),
            (.int64(Int64.min), "i64-n9223372036854775808"),
            (.int64(Int64.max), "i64-9223372036854775807"),
            (.int64(0), "i64-0"),
            (.uint8(0), "u8-0"),
            (.uint64(UInt64.max), "u64-18446744073709551615"),
            (.float32(1.0), "f32-3F800000"),
            (.float64(0.0), "f64-0000000000000000"),
            (.float64(-0.0), "f64-8000000000000000"),
            (.float64(1.0), "f64-3FF0000000000000"),
            (.float64(Double.nan), "f64-7FF8000000000000"),
            (.decimal(ExactDecimal(coefficient: 12345, scale: 2)), "dec-12345.2"),
            (.decimal(ExactDecimal(coefficient: 0, scale: 5)), "dec-0.0"),
            (
                .decimal(ExactDecimal(coefficient: Int128.min, scale: -1)),
                "dec-n170141183460469231731687303715884105728.n1"
            ),
            (.string(""), "s-"),
            (.string("abc"), "s-abc"),
            (.string("A_~9"), "s-A_~9"),
            (.string("a-b.c"), "s-a%2Db%2Ec"),
            (.string("\u{65E5}"), "s-%E6%97%A5"),
            (.bytes(ByteString()), "y-"),
            (.bytes(ByteString([0x00, 0xFF])), "y-%00%FF"),
            (.bytes(ByteString(utf8: "x")), "y-x"),
            (.string("x"), "s-x"),
            (try .date(CivilDate(year: 2026, month: 8, day: 29)), "d-2026.8.29"),
            (try .date(CivilDate(year: -1, month: 1, day: 1)), "d-n1.1.1"),
            (
                try .time(CivilTime(hour: 0, minute: 0, second: 0, nanoseconds: 0)),
                "t-0.0.0.0"
            ),
            (
                try .time(
                    CivilTime(hour: 23, minute: 59, second: 59, nanoseconds: 999_999_999)
                ),
                "t-23.59.59.999999999"
            ),
            (
                .dateTime(
                    CivilDateTime(
                        date: try CivilDate(year: 2026, month: 8, day: 29),
                        time: try CivilTime(hour: 1, minute: 2, second: 3, nanoseconds: 4)
                    )
                ),
                "dt-2026.8.29.1.2.3.4"
            ),
            (try .timestamp(Timestamp(secondsSinceUnixEpoch: -1, nanoseconds: 1)), "ts-n1.1"),
            (try .timeSpan(TimeSpan(seconds: 0, nanoseconds: 0)), "sp-0.0"),
            (.calendarPeriod(CalendarPeriod(months: -13, days: 0)), "cp-n13.0"),
            (
                try .geographicPoint(GeographicPoint(latitude: 0, longitude: 0)),
                "gp-0000000000000000.0000000000000000"
            ),
            (
                try .geographicPosition(
                    GeographicPosition(
                        point: try GeographicPoint(latitude: 0, longitude: 0),
                        ellipsoidalHeightInMeters: 0
                    )
                ),
                "gq-0000000000000000.0000000000000000.0000000000000000"
            ),
            (
                .uuid(UUID(high: 0x0123_4567_89AB_CDEF, low: 0xFEDC_BA98_7654_3210)),
                "uu-0123456789ABCDEFFEDCBA9876543210"
            ),
            (.uuid(UUID(high: 0, low: 0)), "uu-00000000000000000000000000000000"),
        ]
    }

    @Test("A canonical value converts to exactly its declared component")
    func encodesCanonicalComponents() throws {
        for (value, component) in try Self.canonicalVectors() {
            let encoded = try DirectoryComponentCodec.encode(value)
            #expect(encoded == component, "encoded \(encoded) for \(component)")
        }
    }

    @Test("A canonical component converts back to exactly its declared value")
    func decodesCanonicalComponents() throws {
        for (value, component) in try Self.canonicalVectors() {
            let decoded = try DirectoryComponentCodec.decode(component)
            #expect(decoded == value, "decoded \(decoded) for \(component)")
        }
    }

    @Test("Distinct canonical values never share a component")
    func componentsAreInjectiveAcrossKinds() throws {
        var seen: Set<String> = []
        for (_, component) in try Self.canonicalVectors() {
            #expect(seen.insert(component).inserted, "duplicate component \(component)")
        }
    }

    @Test("A non-canonical or malformed component is rejected with its reason")
    func rejectsNonCanonicalComponents() throws {
        let vectors: [(component: String, error: DirectoryComponentCodecError)] = [
            ("i64-n0", .nonCanonicalComponent(canonical: "i64-0")),
            ("i64-007", .nonCanonicalComponent(canonical: "i64-7")),
            ("s-%41", .nonCanonicalComponent(canonical: "s-A")),
            ("dec-100.2", .nonCanonicalComponent(canonical: "dec-1.0")),
            (
                "gp-8000000000000000.0000000000000000",
                .nonCanonicalComponent(canonical: "gp-0000000000000000.0000000000000000")
            ),
            ("f64-3ff0000000000000", .malformedComponent(.invalidHexadecimal)),
            ("s-%2d", .malformedComponent(.invalidEscape)),
            ("s-%FF", .malformedComponent(.invalidUTF8)),
            ("zz-1", .unknownTag("zz")),
            ("b1", .malformedComponent(.missingTagSeparator)),
            ("-1", .malformedComponent(.emptyTag)),
            ("d-2026.8", .malformedComponent(.tokenCount(expected: 3, actual: 2))),
            ("d-2026.13.1", .malformedComponent(.invalidValue)),
            ("i8-128", .malformedComponent(.numberOutOfRange)),
            ("s-a-b", .malformedComponent(.invalidCharacter)),
            ("u8-n1", .malformedComponent(.invalidNumber)),
            ("uu-0123456789ABCDEF", .malformedComponent(.invalidHexadecimal)),
            ("b-2", .malformedComponent(.invalidValue)),
            ("s-%2", .malformedComponent(.invalidEscape)),
        ]
        for (component, error) in vectors {
            #expect(throws: error) {
                try DirectoryComponentCodec.decode(component)
            }
        }
    }

    @Test("A field kind without a canonical component is rejected, never encoded")
    func rejectsFieldKindsWithoutCanonicalComponents() throws {
        let vectors: [(value: FieldValue, kind: String)] = [
            (.null, "null"),
            (.array([.int64(1)]), "array"),
            (.object(FieldObject()), "object"),
        ]
        for (value, kind) in vectors {
            #expect(throws: DirectoryComponentCodecError.unsupportedFieldKind(kind)) {
                try DirectoryComponentCodec.encode(value)
            }
        }
    }
}
