#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import Testing
@testable import DatabaseEngine

@Suite("Field schema value validation")
struct TypeValidationTests {
    @Test("Every canonical schema type accepts its corresponding value")
    func canonicalValuesAreAccepted() throws {
        let date = try CivilDate(year: 2026, month: 7, day: 26)
        let time = try CivilTime(
            hour: 12,
            minute: 34,
            second: 56,
            nanoseconds: 789
        )
        let point = try GeographicPoint(latitude: 35.6812, longitude: 139.7671)
        let reference = try EntityReference(
            entity: "Event",
            id: .string("event-1")
        )
        let values: [(FieldSchemaType, FieldValue)] = [
            (.bool, .bool(true)),
            (.int8, .int8(-8)),
            (.int16, .int16(-16)),
            (.int32, .int32(-32)),
            (.int64, .int64(-64)),
            (.uint8, .uint8(8)),
            (.uint16, .uint16(16)),
            (.uint32, .uint32(32)),
            (.uint64, .uint64(64)),
            (.float32, .float32(1.25)),
            (.float64, .float64(2.5)),
            (
                .decimal,
                .decimal(ExactDecimal(coefficient: 125, scale: 2))
            ),
            (.string, .string("value")),
            (.bytes, .bytes(ByteString([0, 1, 2]))),
            (.date, .date(date)),
            (.time, .time(time)),
            (.dateTime, .dateTime(CivilDateTime(date: date, time: time))),
            (
                .timestamp,
                .timestamp(
                    try Timestamp(
                        secondsSinceUnixEpoch: 1_774_310_400,
                        nanoseconds: 123
                    )
                )
            ),
            (.timeSpan, .timeSpan(try TimeSpan(seconds: 5, nanoseconds: 10))),
            (.calendarPeriod, .calendarPeriod(CalendarPeriod(months: 2, days: 3))),
            (.geographicPoint, .geographicPoint(point)),
            (
                .geographicPosition,
                .geographicPosition(
                    try GeographicPosition(
                        point: point,
                        ellipsoidalHeightInMeters: 12.5
                    )
                )
            ),
            (.vector, .vector(try Vector(float32: [1, 0, 0]))),
            (.uuid, .uuid(DatabaseTypes.UUID(high: 1, low: 2))),
            (.object, .object(FieldObject())),
            (.nested, .object(FieldObject())),
            (.reference, .reference(reference)),
            (.rdfTerm, .rdfTerm(.iri(try RDFIRI("urn:event")))),
            (.enum, .string("active")),
            (.enum, .int64(1)),
        ]

        for (type, value) in values {
            #expect(FieldSchemaValueValidator.accepts(value, as: type))
        }
    }

    @Test("Width, structure, and semantic mismatches are rejected")
    func mismatchedValuesAreRejected() {
        let mismatches: [(FieldSchemaType, FieldValue)] = [
            (.int8, .int16(1)),
            (.int64, .uint64(1)),
            (.float32, .float64(1)),
            (.string, .bytes(ByteString())),
            (.object, .array([])),
            (.nested, .string("object")),
            (.reference, .string("entity")),
            (.rdfTerm, .string("urn:event")),
            (.enum, .bool(true)),
        ]

        for (type, value) in mismatches {
            #expect(!FieldSchemaValueValidator.accepts(value, as: type))
        }
    }
}
#endif
