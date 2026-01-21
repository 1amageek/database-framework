// TupleElementConverterTests.swift
// DatabaseEngine Tests - TupleElementConverter tests for type conversion

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine

// MARK: - TupleElementConverter Tests

@Suite("TupleElementConverter Tests")
struct TupleElementConverterTests {

    // MARK: - String Conversion

    @Test("converts String to TupleElement")
    func testStringConversion() throws {
        let result = try TupleElementConverter.convert("hello")

        #expect(result.count == 1)
        #expect((result[0] as? String) == "hello")
    }

    @Test("converts empty String to TupleElement")
    func testEmptyStringConversion() throws {
        let result = try TupleElementConverter.convert("")

        #expect(result.count == 1)
        #expect((result[0] as? String) == "")
    }

    // MARK: - Integer Conversion

    @Test("converts Int to Int64")
    func testIntConversion() throws {
        let result = try TupleElementConverter.convert(42)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 42)
    }

    @Test("converts negative Int to Int64")
    func testNegativeIntConversion() throws {
        let result = try TupleElementConverter.convert(-100)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == -100)
    }

    @Test("converts Int64 directly")
    func testInt64Conversion() throws {
        let value: Int64 = 9_223_372_036_854_775_807  // Int64.max
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == Int64.max)
    }

    @Test("converts Int32 to Int64")
    func testInt32Conversion() throws {
        let value: Int32 = 2_147_483_647  // Int32.max
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 2_147_483_647)
    }

    @Test("converts Int16 to Int64")
    func testInt16Conversion() throws {
        let value: Int16 = 32_767  // Int16.max
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 32_767)
    }

    @Test("converts Int8 to Int64")
    func testInt8Conversion() throws {
        let value: Int8 = 127  // Int8.max
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 127)
    }

    // MARK: - Unsigned Integer Conversion

    @Test("converts UInt within Int64 range to Int64")
    func testUIntConversion() throws {
        let value: UInt = 1_000_000
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 1_000_000)
    }

    @Test("converts UInt64 within Int64 range to Int64")
    func testUInt64WithinRangeConversion() throws {
        let value: UInt64 = UInt64(Int64.max)
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == Int64.max)
    }

    @Test("throws on UInt64 overflow")
    func testUInt64OverflowThrows() throws {
        let value: UInt64 = UInt64(Int64.max) + 1

        #expect(throws: TupleConversionError.self) {
            _ = try TupleElementConverter.convert(value)
        }
    }

    @Test("converts UInt32 to Int64")
    func testUInt32Conversion() throws {
        let value: UInt32 = 4_294_967_295  // UInt32.max
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 4_294_967_295)
    }

    @Test("converts UInt16 to Int64")
    func testUInt16Conversion() throws {
        let value: UInt16 = 65_535  // UInt16.max
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 65_535)
    }

    @Test("converts UInt8 to Int64")
    func testUInt8Conversion() throws {
        let value: UInt8 = 255  // UInt8.max
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 255)
    }

    // MARK: - Floating Point Conversion

    @Test("converts Double directly")
    func testDoubleConversion() throws {
        let result = try TupleElementConverter.convert(3.14159)

        #expect(result.count == 1)
        #expect((result[0] as? Double) == 3.14159)
    }

    @Test("converts Float to Double")
    func testFloatConversion() throws {
        let value: Float = 2.5
        let result = try TupleElementConverter.convert(value)

        #expect(result.count == 1)
        #expect((result[0] as? Double) == 2.5)
    }

    @Test("converts negative Double")
    func testNegativeDoubleConversion() throws {
        let result = try TupleElementConverter.convert(-273.15)

        #expect(result.count == 1)
        #expect((result[0] as? Double) == -273.15)
    }

    // MARK: - Bool Conversion

    @Test("converts true Bool")
    func testTrueBoolConversion() throws {
        let result = try TupleElementConverter.convert(true)

        #expect(result.count == 1)
        #expect((result[0] as? Bool) == true)
    }

    @Test("converts false Bool")
    func testFalseBoolConversion() throws {
        let result = try TupleElementConverter.convert(false)

        #expect(result.count == 1)
        #expect((result[0] as? Bool) == false)
    }

    // MARK: - Date Conversion (fdb-swift-bindings compatibility)

    @Test("converts Date directly (not to Double)")
    func testDateConversion() throws {
        let date = Date(timeIntervalSince1970: 1_000_000)
        let result = try TupleElementConverter.convert(date)

        #expect(result.count == 1)
        // Date should be returned as Date, not Double
        #expect(result[0] is Date)
        #expect((result[0] as? Date) == date)
    }

    @Test("Date encoding matches fdb-swift-bindings")
    func testDateEncodingConsistency() throws {
        let date = Date(timeIntervalSince1970: 1_700_000_000)  // 2023-11-14
        let result = try TupleElementConverter.convert(date)

        // Verify it's a Date (fdb-swift-bindings will encode using timeIntervalSince1970)
        guard result[0] is Date else {
            Issue.record("Expected Date but got \(type(of: result[0]))")
            return
        }

        // Create a Tuple directly with the date
        let directTuple = Tuple([date])
        let converterTuple = Tuple(result)

        // Both should produce identical packed bytes
        #expect(directTuple.pack() == converterTuple.pack())
    }

    @Test("Date array conversion returns Date elements")
    func testDateArrayConversion() throws {
        let dates = [
            Date(timeIntervalSince1970: 1_000_000),
            Date(timeIntervalSince1970: 2_000_000)
        ]
        let result = try TupleElementConverter.convert(dates)

        #expect(result.count == 2)
        #expect(result[0] is Date)
        #expect(result[1] is Date)
    }

    // MARK: - UUID Conversion

    @Test("converts UUID directly")
    func testUUIDConversion() throws {
        let uuid = UUID()
        let result = try TupleElementConverter.convert(uuid)

        #expect(result.count == 1)
        #expect((result[0] as? UUID) == uuid)
    }

    // MARK: - Data/Bytes Conversion

    @Test("converts Data to [UInt8]")
    func testDataConversion() throws {
        let data = Data([0x01, 0x02, 0x03])
        let result = try TupleElementConverter.convert(data)

        #expect(result.count == 1)
        #expect((result[0] as? [UInt8]) == [0x01, 0x02, 0x03])
    }

    @Test("converts [UInt8] directly")
    func testBytesConversion() throws {
        let bytes: [UInt8] = [0xFF, 0x00, 0xAB]
        let result = try TupleElementConverter.convert(bytes)

        #expect(result.count == 1)
        #expect((result[0] as? [UInt8]) == bytes)
    }

    // MARK: - Tuple Conversion

    @Test("converts Tuple directly")
    func testTupleConversion() throws {
        let tuple = Tuple(["nested", Int64(42)])
        let result = try TupleElementConverter.convert(tuple)

        #expect(result.count == 1)
        #expect((result[0] as? Tuple) == tuple)
    }

    // MARK: - Array Conversion

    @Test("converts [Float] to [Double]")
    func testFloatArrayConversion() throws {
        let floats: [Float] = [1.0, 2.5, 3.14]
        let result = try TupleElementConverter.convert(floats)

        #expect(result.count == 3)
        #expect((result[0] as? Double) == 1.0)
        #expect((result[1] as? Double) == 2.5)
        // Float precision
        #expect(abs((result[2] as! Double) - 3.14) < 0.001)
    }

    @Test("converts [Double] directly")
    func testDoubleArrayConversion() throws {
        let doubles: [Double] = [1.0, 2.0, 3.0]
        let result = try TupleElementConverter.convert(doubles)

        #expect(result.count == 3)
        #expect((result[0] as? Double) == 1.0)
        #expect((result[1] as? Double) == 2.0)
        #expect((result[2] as? Double) == 3.0)
    }

    @Test("converts [Int] to [Int64]")
    func testIntArrayConversion() throws {
        let ints: [Int] = [1, 2, 3]
        let result = try TupleElementConverter.convert(ints)

        #expect(result.count == 3)
        #expect((result[0] as? Int64) == 1)
        #expect((result[1] as? Int64) == 2)
        #expect((result[2] as? Int64) == 3)
    }

    @Test("converts [Int64] directly")
    func testInt64ArrayConversion() throws {
        let int64s: [Int64] = [Int64.min, 0, Int64.max]
        let result = try TupleElementConverter.convert(int64s)

        #expect(result.count == 3)
        #expect((result[0] as? Int64) == Int64.min)
        #expect((result[1] as? Int64) == 0)
        #expect((result[2] as? Int64) == Int64.max)
    }

    @Test("converts [String] directly")
    func testStringArrayConversion() throws {
        let strings = ["a", "b", "c"]
        let result = try TupleElementConverter.convert(strings)

        #expect(result.count == 3)
        #expect((result[0] as? String) == "a")
        #expect((result[1] as? String) == "b")
        #expect((result[2] as? String) == "c")
    }

    @Test("converts [any TupleElement] directly")
    func testTupleElementArrayConversion() throws {
        let elements: [any TupleElement] = ["string", Int64(42), true]
        let result = try TupleElementConverter.convert(elements)

        #expect(result.count == 3)
        #expect((result[0] as? String) == "string")
        #expect((result[1] as? Int64) == 42)
        #expect((result[2] as? Bool) == true)
    }

    // MARK: - Optional Handling

    @Test("throws on nil Optional")
    func testNilOptionalThrows() throws {
        let nilString: String? = nil

        #expect(throws: TupleConversionError.self) {
            _ = try TupleElementConverter.convert(nilString as Any)
        }
    }

    @Test("unwraps non-nil Optional")
    func testNonNilOptionalUnwraps() throws {
        let optionalString: String? = "hello"
        let result = try TupleElementConverter.convert(optionalString as Any)

        #expect(result.count == 1)
        #expect((result[0] as? String) == "hello")
    }

    @Test("unwraps nested Optional")
    func testNestedOptionalUnwraps() throws {
        let nestedOptional: Int?? = 42
        let result = try TupleElementConverter.convert(nestedOptional as Any)

        #expect(result.count == 1)
        #expect((result[0] as? Int64) == 42)
    }

    // MARK: - Error Cases

    @Test("throws on unsupported type")
    func testUnsupportedTypeThrows() throws {
        struct CustomType {}
        let custom = CustomType()

        #expect(throws: TupleConversionError.self) {
            _ = try TupleElementConverter.convert(custom)
        }
    }

    // MARK: - convertSingle Tests

    @Test("convertSingle returns single element")
    func testConvertSingle() throws {
        let result = try TupleElementConverter.convertSingle("hello")

        #expect((result as? String) == "hello")
    }

    @Test("convertSingle throws on array input")
    func testConvertSingleThrowsOnArray() throws {
        let array = ["a", "b", "c"]

        #expect(throws: TupleConversionError.self) {
            _ = try TupleElementConverter.convertSingle(array)
        }
    }

    // MARK: - convertFirst Tests

    @Test("convertFirst returns first element of array")
    func testConvertFirstReturnsFirst() throws {
        let array = ["first", "second", "third"]
        let result = try TupleElementConverter.convertFirst(array)

        #expect((result as? String) == "first")
    }

    @Test("convertFirst works with single value")
    func testConvertFirstWithSingleValue() throws {
        let result = try TupleElementConverter.convertFirst(42)

        #expect((result as? Int64) == 42)
    }

    // MARK: - Tuple Encoding Verification

    @Test("converted values pack correctly into Tuple")
    func testTuplePackingConsistency() throws {
        // Various types
        let stringResult = try TupleElementConverter.convert("test")
        let intResult = try TupleElementConverter.convert(42)
        let doubleResult = try TupleElementConverter.convert(3.14)
        let boolResult = try TupleElementConverter.convert(true)
        let uuidValue = UUID()
        let uuidResult = try TupleElementConverter.convert(uuidValue)

        // Create Tuple from converted values
        let tuple = Tuple(stringResult + intResult + doubleResult + boolResult + uuidResult)

        // Pack should succeed without errors
        let packed = tuple.pack()
        #expect(packed.count > 0)

        // Unpack should match
        let unpackedElements = try Tuple.unpack(from: packed)
        #expect(unpackedElements.count == 5)
        #expect((unpackedElements[0] as? String) == "test")
        #expect((unpackedElements[1] as? Int64) == 42)
        #expect((unpackedElements[2] as? Double) == 3.14)
        #expect((unpackedElements[3] as? Bool) == true)
        #expect((unpackedElements[4] as? UUID) == uuidValue)
    }

    // MARK: - Numeric Range Tests (Edge Cases)

    @Test("converts zero values correctly")
    func testZeroConversion() throws {
        let intZero = try TupleElementConverter.convert(0)
        let doubleZero = try TupleElementConverter.convert(0.0)

        #expect((intZero[0] as? Int64) == 0)
        #expect((doubleZero[0] as? Double) == 0.0)
    }

    @Test("converts Int64.min correctly")
    func testInt64MinConversion() throws {
        let result = try TupleElementConverter.convert(Int64.min)

        #expect((result[0] as? Int64) == Int64.min)
    }
}
