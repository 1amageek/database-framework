// TypeValidationTests.swift
// FDBIndexing Tests - TypeValidation helper function tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine

@Suite("TypeValidation Tests")
struct TypeValidationTests {

    // MARK: - isNumeric Tests

    @Test("TypeValidation.isNumeric detects integer types")
    func testIsNumericIntegers() {
        #expect(TypeValidation.isNumeric(Int.self))
        #expect(TypeValidation.isNumeric(Int8.self))
        #expect(TypeValidation.isNumeric(Int16.self))
        #expect(TypeValidation.isNumeric(Int32.self))
        #expect(TypeValidation.isNumeric(Int64.self))
        #expect(TypeValidation.isNumeric(UInt.self))
        #expect(TypeValidation.isNumeric(UInt8.self))
        #expect(TypeValidation.isNumeric(UInt16.self))
        #expect(TypeValidation.isNumeric(UInt32.self))
        #expect(TypeValidation.isNumeric(UInt64.self))
    }

    @Test("TypeValidation.isNumeric detects floating point types")
    func testIsNumericFloatingPoint() {
        #expect(TypeValidation.isNumeric(Float.self))
        #expect(TypeValidation.isNumeric(Float32.self))
        #expect(TypeValidation.isNumeric(Double.self))
    }

    @Test("TypeValidation.isNumeric rejects non-numeric types")
    func testIsNumericRejectsNonNumeric() {
        #expect(!TypeValidation.isNumeric(String.self))
        #expect(!TypeValidation.isNumeric(Bool.self))
        #expect(!TypeValidation.isNumeric(Date.self))
        #expect(!TypeValidation.isNumeric(UUID.self))
        #expect(!TypeValidation.isNumeric([Int].self))
    }

    // MARK: - isFloatingPoint Tests

    @Test("TypeValidation.isFloatingPoint detects floating point types")
    func testIsFloatingPoint() {
        #expect(TypeValidation.isFloatingPoint(Float.self))
        #expect(TypeValidation.isFloatingPoint(Float32.self))
        #expect(TypeValidation.isFloatingPoint(Double.self))
    }

    @Test("TypeValidation.isFloatingPoint rejects non-floating point types")
    func testIsFloatingPointRejects() {
        #expect(!TypeValidation.isFloatingPoint(Int.self))
        #expect(!TypeValidation.isFloatingPoint(Int64.self))
        #expect(!TypeValidation.isFloatingPoint(String.self))
        #expect(!TypeValidation.isFloatingPoint(Bool.self))
    }

    // MARK: - isInteger Tests

    @Test("TypeValidation.isInteger detects integer types")
    func testIsInteger() {
        #expect(TypeValidation.isInteger(Int.self))
        #expect(TypeValidation.isInteger(Int8.self))
        #expect(TypeValidation.isInteger(Int16.self))
        #expect(TypeValidation.isInteger(Int32.self))
        #expect(TypeValidation.isInteger(Int64.self))
        #expect(TypeValidation.isInteger(UInt.self))
        #expect(TypeValidation.isInteger(UInt8.self))
        #expect(TypeValidation.isInteger(UInt16.self))
        #expect(TypeValidation.isInteger(UInt32.self))
        #expect(TypeValidation.isInteger(UInt64.self))
    }

    @Test("TypeValidation.isInteger rejects non-integer types")
    func testIsIntegerRejects() {
        #expect(!TypeValidation.isInteger(Float.self))
        #expect(!TypeValidation.isInteger(Double.self))
        #expect(!TypeValidation.isInteger(String.self))
        #expect(!TypeValidation.isInteger(Bool.self))
    }

    // MARK: - isComparable Tests

    @Test("TypeValidation.isComparable detects Comparable types")
    func testIsComparable() {
        // Numeric types
        #expect(TypeValidation.isComparable(Int.self))
        #expect(TypeValidation.isComparable(Int64.self))
        #expect(TypeValidation.isComparable(Double.self))

        // String
        #expect(TypeValidation.isComparable(String.self))

        // Date, UUID
        #expect(TypeValidation.isComparable(Date.self))
        #expect(TypeValidation.isComparable(UUID.self))
    }

    @Test("TypeValidation.isComparable rejects non-Comparable types")
    func testIsComparableRejects() {
        // Array types (even if elements are Comparable, the array itself is not Comparable)
        #expect(!TypeValidation.isComparable([Int].self))
        #expect(!TypeValidation.isComparable([String].self))

        // Optional types (some Optionals are not Comparable)
        #expect(!TypeValidation.isComparable(Int?.self))
    }

    // MARK: - isArrayType Tests

    @Test("TypeValidation.isArrayType detects array types")
    func testIsArrayType() {
        #expect(TypeValidation.isArrayType([Int].self))
        #expect(TypeValidation.isArrayType([String].self))
        #expect(TypeValidation.isArrayType([Double].self))
        #expect(TypeValidation.isArrayType([Float32].self))
        #expect(TypeValidation.isArrayType([[Int]].self))  // Nested array
    }

    @Test("TypeValidation.isArrayType rejects non-array types")
    func testIsArrayTypeRejects() {
        #expect(!TypeValidation.isArrayType(Int.self))
        #expect(!TypeValidation.isArrayType(String.self))
        #expect(!TypeValidation.isArrayType(Double.self))
        #expect(!TypeValidation.isArrayType(Date.self))
    }

    // MARK: - Combined Usage Tests

    @Test("TypeValidation methods can be combined for complex checks")
    func testCombinedUsage() {
        // Int64: numeric && integer && Comparable
        #expect(TypeValidation.isNumeric(Int64.self))
        #expect(TypeValidation.isInteger(Int64.self))
        #expect(TypeValidation.isComparable(Int64.self))
        #expect(!TypeValidation.isFloatingPoint(Int64.self))
        #expect(!TypeValidation.isArrayType(Int64.self))

        // Double: numeric && floating point && Comparable
        #expect(TypeValidation.isNumeric(Double.self))
        #expect(TypeValidation.isFloatingPoint(Double.self))
        #expect(TypeValidation.isComparable(Double.self))
        #expect(!TypeValidation.isInteger(Double.self))
        #expect(!TypeValidation.isArrayType(Double.self))

        // String: Comparable only
        #expect(TypeValidation.isComparable(String.self))
        #expect(!TypeValidation.isNumeric(String.self))
        #expect(!TypeValidation.isInteger(String.self))
        #expect(!TypeValidation.isFloatingPoint(String.self))
        #expect(!TypeValidation.isArrayType(String.self))

        // [Float32]: array only
        #expect(TypeValidation.isArrayType([Float32].self))
        #expect(!TypeValidation.isNumeric([Float32].self))
        #expect(!TypeValidation.isComparable([Float32].self))
    }
}
