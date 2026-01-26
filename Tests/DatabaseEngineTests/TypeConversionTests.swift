// TypeConversionTests.swift
// DatabaseEngine - Tests for unified type conversion

import Testing
import Foundation
@testable import DatabaseEngine
import Core
import FoundationDB

@Suite("TypeConversion Tests")
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
    func testToFieldValueBool() {
        #expect(TypeConversion.toFieldValue(true) == .bool(true))
        #expect(TypeConversion.toFieldValue(false) == .bool(false))
    }

    @Test("toFieldValue converts integers to int64")
    func testToFieldValueIntegers() {
        #expect(TypeConversion.toFieldValue(42 as Int) == .int64(42))
        #expect(TypeConversion.toFieldValue(42 as Int64) == .int64(42))
        #expect(TypeConversion.toFieldValue(42 as Int32) == .int64(42))
        #expect(TypeConversion.toFieldValue(42 as UInt32) == .int64(42))
    }

    @Test("toFieldValue converts floating-point to double")
    func testToFieldValueFloatingPoint() {
        #expect(TypeConversion.toFieldValue(3.14 as Double) == .double(3.14))
        // Float loses precision, so just check it's a double
        if case .double = TypeConversion.toFieldValue(3.14 as Float) {
            // OK
        } else {
            Issue.record("Expected .double for Float")
        }
    }

    @Test("toFieldValue converts String")
    func testToFieldValueString() {
        #expect(TypeConversion.toFieldValue("hello") == .string("hello"))
    }

    @Test("toFieldValue handles UUID as string")
    func testToFieldValueUUID() {
        let uuid = UUID()
        #expect(TypeConversion.toFieldValue(uuid) == .string(uuid.uuidString))
    }

    @Test("toFieldValue handles Date as double")
    func testToFieldValueDate() {
        let date = Date(timeIntervalSince1970: 1000.0)
        #expect(TypeConversion.toFieldValue(date) == .double(1000.0))
    }

    @Test("toFieldValue handles Data")
    func testToFieldValueData() {
        let data = Data([1, 2, 3])
        #expect(TypeConversion.toFieldValue(data) == .data(data))
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

    @Test("valueOrNil returns nil on type mismatch")
    func testValueOrNil() {
        let stringElement: any TupleElement = "hello"
        #expect(TypeConversion.valueOrNil(from: stringElement, as: Int64.self) == nil)
        #expect(TypeConversion.valueOrNil(from: stringElement, as: String.self) == "hello")

        let intElement: any TupleElement = Int64(42)
        #expect(TypeConversion.valueOrNil(from: intElement, as: Int64.self) == 42)
        #expect(TypeConversion.valueOrNil(from: intElement, as: String.self) == nil)
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

    @Test("toTupleElementOrNil returns nil for unsupported types")
    func testToTupleElementOrNil() {
        // Basic types should work
        #expect(TypeConversion.toTupleElementOrNil(42 as Int) != nil)
        #expect(TypeConversion.toTupleElementOrNil("hello") != nil)

        // Custom struct should fail
        struct CustomType {}
        #expect(TypeConversion.toTupleElementOrNil(CustomType()) == nil)
    }

    // MARK: - Edge Cases

    @Test("handles negative integers")
    func testNegativeIntegers() {
        #expect(TypeConversion.asInt64(-42) == -42)
        #expect(TypeConversion.asDouble(-3.14) == -3.14)
        #expect(TypeConversion.toFieldValue(-42) == .int64(-42))
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
}
