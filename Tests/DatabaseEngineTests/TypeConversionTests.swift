#if !os(WASI)
#if FOUNDATION_DB
// TypeConversionTests.swift
// DatabaseEngine - Tests for unified type conversion

import Testing
import TestHeartbeat
import Foundation
import DatabaseTypes
@testable import DatabaseEngine
import DatabaseKit
import DatabaseKitFoundation
import StorageKit

@Suite("TypeConversion Tests", .heartbeat)
struct TypeConversionTests {

    // MARK: - asInt64 Tests

    @Test("asInt64 converts Int types")
    func testAsInt64IntTypes() {
        #expect(TypeConversion.asInt64(42 as Int) == 42)
        #expect(TypeConversion.asInt64(42 as Int64) == 42)
        #expect(TypeConversion.asInt64(42 as Int32) == 42)
        #expect(TypeConversion.asInt64(42 as Int16) == 42)
        #expect(TypeConversion.asInt64(42 as Int8) == 42)
    }

    @Test("asInt64 converts UInt types within range")
    func testAsInt64UIntTypes() {
        #expect(TypeConversion.asInt64(42 as UInt) == 42)
        #expect(TypeConversion.asInt64(42 as UInt64) == 42)
        #expect(TypeConversion.asInt64(42 as UInt32) == 42)
        #expect(TypeConversion.asInt64(42 as UInt16) == 42)
        #expect(TypeConversion.asInt64(42 as UInt8) == 42)
    }

    @Test("asInt64 handles UInt64 overflow")
    func testAsInt64Overflow() {
        let safe: UInt64 = UInt64(Int64.max)
        let overflow: UInt64 = UInt64(Int64.max) + 1

        #expect(TypeConversion.asInt64(safe) == Int64.max)
        #expect(TypeConversion.asInt64(overflow) == nil)
    }

    @Test("asInt64 converts Bool")
    func testAsInt64Bool() {
        #expect(TypeConversion.asInt64(true) == 1)
        #expect(TypeConversion.asInt64(false) == 0)
    }

    @Test("asInt64 returns nil for non-integer types")
    func testAsInt64NonInteger() {
        #expect(TypeConversion.asInt64(3.14) == nil)
        #expect(TypeConversion.asInt64("42") == nil)
        #expect(TypeConversion.asInt64(Date()) == nil)
    }

    // MARK: - asDouble Tests

    @Test("asDouble converts floating-point types")
    func testAsDoubleFloatingPoint() {
        #expect(TypeConversion.asDouble(3.14 as Double) == 3.14)
        #expect(TypeConversion.asDouble(3.14 as Float) != nil)
    }

    @Test("asDouble converts integer types")
    func testAsDoubleIntTypes() {
        #expect(TypeConversion.asDouble(42 as Int) == 42.0)
        #expect(TypeConversion.asDouble(42 as Int64) == 42.0)
        #expect(TypeConversion.asDouble(42 as Int32) == 42.0)
        #expect(TypeConversion.asDouble(42 as UInt64) == 42.0)
    }

    @Test("asDouble converts Date")
    func testAsDoubleDate() {
        let date = Date(timeIntervalSince1970: 1000.0)
        #expect(TypeConversion.asDouble(date) == 1000.0)
    }

    @Test("asDouble returns nil for String")
    func testAsDoubleString() {
        #expect(TypeConversion.asDouble("42") == nil)
        #expect(TypeConversion.asDouble("3.14") == nil)
    }

    // MARK: - asString Tests

    @Test("asString converts String")
    func testAsStringString() {
        #expect(TypeConversion.asString("hello") == "hello")
        #expect(TypeConversion.asString("") == "")
    }

    @Test("asString converts UUID")
    func testAsStringUUID() {
        let uuid = UUID()
        #expect(TypeConversion.asString(uuid) == uuid.uuidString)
    }

    @Test("asString returns nil for numbers")
    func testAsStringNumber() {
        #expect(TypeConversion.asString(42) == nil)
        #expect(TypeConversion.asString(3.14) == nil)
    }

    // MARK: - toFieldValue Tests

    @Test("toFieldValue converts Bool")
    func testToFieldValueBool() throws {
        #expect(try TypeConversion.toFieldValue(true) == .bool(true))
        #expect(try TypeConversion.toFieldValue(false) == .bool(false))
    }

    @Test("toFieldValue preserves signed and unsigned integer domains")
    func testToFieldValueIntegers() throws {
        #expect(try TypeConversion.toFieldValue(42 as Int) == .int64(42))
        #expect(try TypeConversion.toFieldValue(42 as Int64) == .int64(42))
        #expect(try TypeConversion.toFieldValue(42 as Int32) == .int32(42))
        #expect(try TypeConversion.toFieldValue(42 as UInt32) == .uint32(42))
        #expect(
            try TypeConversion.toFieldValue(UInt64.max)
                == .uint64(UInt64.max)
        )
    }

    @Test("toFieldValue converts floating-point to double")
    func testToFieldValueFloatingPoint() throws {
        #expect(
            try TypeConversion.toFieldValue(3.14 as Double)
                == .float64(3.14)
        )
        if case .float32 = try TypeConversion.toFieldValue(3.14 as Float) {
        } else {
            Issue.record("Expected .float32 for Float")
        }
    }

    @Test("toFieldValue converts String")
    func testToFieldValueString() throws {
        #expect(
            try TypeConversion.toFieldValue("hello") == .string("hello")
        )
    }

    @Test("toFieldValue preserves UUID identity")
    func testToFieldValueUUID() throws {
        let uuid = UUID()
        #expect(
            try TypeConversion.toFieldValue(uuid)
                == .uuid(DatabaseTypes.UUID(uuid))
        )
    }

    @Test("toFieldValue preserves Date as a timestamp")
    func testToFieldValueDate() throws {
        let date = Date(timeIntervalSince1970: 1000.0)
        guard case .timestamp(let timestamp) =
            try TypeConversion.toFieldValue(date)
        else {
            Issue.record("Expected .timestamp for Date")
            return
        }
        #expect(timestamp.secondsSinceUnixEpoch == 1_000)
        #expect(timestamp.nanoseconds == 0)
    }

    @Test("toFieldValue handles Data")
    func testToFieldValueData() throws {
        let data = Data([1, 2, 3])
        #expect(
            try TypeConversion.toFieldValue(data)
                == .bytes(ByteString(retaining: data))
        )
    }

    @Test("toFieldValue rejects unsupported values with a typed error")
    func testToFieldValueUnsupportedType() {
        let value = UnsupportedValue()
        let expected = TypeConversionError.unsupportedType(
            String(reflecting: type(of: value))
        )

        #expect(throws: expected) {
            try TypeConversion.toFieldValue(value)
        }
    }

    @Test("toFieldValue reports the failing collection element")
    func testToFieldValueInvalidCollectionElement() {
        let unsupported = UnsupportedValue()
        let values: [Any] = [Int64(1), unsupported]
        let expected = TypeConversionError.invalidCollectionElement(
            index: 1,
            reason: .unsupportedType(
                String(reflecting: type(of: unsupported))
            )
        )

        #expect(throws: expected) {
            try TypeConversion.toFieldValue(values)
        }
    }

    @Test("toTupleElement preserves the full UInt64 domain")
    func testToTupleElementFullWidthUInt64() throws {
        let element = try TypeConversion.toTupleElement(UInt64.max)
        #expect(element as? UInt64 == UInt64.max)
    }

    // MARK: - TupleElement Extraction Tests

    @Test("int64(from:) extracts Int64")
    func testInt64From() throws {
        let element: any TupleElement = Int64(42)
        #expect(try TypeConversion.int64(from: element) == 42)
    }

    @Test("double(from:) extracts Double")
    func testDoubleFrom() throws {
        let element: any TupleElement = Double(3.14)
        #expect(try TypeConversion.double(from: element) == 3.14)
    }

    @Test("string(from:) extracts String")
    func testStringFrom() throws {
        let element: any TupleElement = "hello"
        #expect(try TypeConversion.string(from: element) == "hello")
    }

    // MARK: - toTupleElement Tests

    @Test("toTupleElement converts basic types")
    func testToTupleElement() throws {
        // Int → Int64
        let intElement = try TypeConversion.toTupleElement(42 as Int)
        #expect(intElement as? Int64 == 42)

        // String → String
        let strElement = try TypeConversion.toTupleElement("hello")
        #expect(strElement as? String == "hello")

        // Double → Double
        let doubleElement = try TypeConversion.toTupleElement(3.14)
        #expect(doubleElement as? Double == 3.14)

        // Bool → Bool
        let boolElement = try TypeConversion.toTupleElement(true)
        #expect(boolElement as? Bool == true)
    }

    // MARK: - Edge Cases

    @Test("handles negative integers")
    func testNegativeIntegers() throws {
        #expect(TypeConversion.asInt64(-42) == -42)
        #expect(TypeConversion.asDouble(-3.14) == -3.14)
        #expect(try TypeConversion.toFieldValue(-42) == .int64(-42))
    }

    @Test("handles zero values")
    func testZeroValues() {
        #expect(TypeConversion.asInt64(0) == 0)
        #expect(TypeConversion.asDouble(0.0) == 0.0)
        #expect(TypeConversion.asString("") == "")
    }

    @Test("handles Int64 boundary values")
    func testInt64Boundaries() {
        #expect(TypeConversion.asInt64(Int64.max) == Int64.max)
        #expect(TypeConversion.asInt64(Int64.min) == Int64.min)
    }

    private struct UnsupportedValue: Sendable {}
}
#endif

#endif
