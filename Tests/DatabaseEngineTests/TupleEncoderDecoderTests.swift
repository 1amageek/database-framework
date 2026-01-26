// TupleEncoderDecoderTests.swift
// DatabaseEngine Tests - TupleEncoder and TupleDecoder tests

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine

// MARK: - TupleEncoder Tests

@Suite("TupleEncoder Tests")
struct TupleEncoderTests {

    // MARK: - String Encoding

    @Test("encodes String directly")
    func testStringEncoding() throws {
        let result = try TupleEncoder.encode("hello")
        #expect((result as? String) == "hello")
    }

    @Test("encodes empty String")
    func testEmptyStringEncoding() throws {
        let result = try TupleEncoder.encode("")
        #expect((result as? String) == "")
    }

    // MARK: - Integer Encoding

    @Test("encodes Int to Int64")
    func testIntEncoding() throws {
        let result = try TupleEncoder.encode(42)
        #expect((result as? Int64) == 42)
    }

    @Test("encodes negative Int to Int64")
    func testNegativeIntEncoding() throws {
        let result = try TupleEncoder.encode(-100)
        #expect((result as? Int64) == -100)
    }

    @Test("encodes Int64 directly")
    func testInt64Encoding() throws {
        let value: Int64 = 9_223_372_036_854_775_807  // Int64.max
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == Int64.max)
    }

    @Test("encodes Int32 to Int64")
    func testInt32Encoding() throws {
        let value: Int32 = 2_147_483_647  // Int32.max
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == 2_147_483_647)
    }

    @Test("encodes Int16 to Int64")
    func testInt16Encoding() throws {
        let value: Int16 = 32_767  // Int16.max
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == 32_767)
    }

    @Test("encodes Int8 to Int64")
    func testInt8Encoding() throws {
        let value: Int8 = 127  // Int8.max
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == 127)
    }

    // MARK: - Unsigned Integer Encoding

    @Test("encodes UInt within Int64 range to Int64")
    func testUIntEncoding() throws {
        let value: UInt = 1_000_000
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == 1_000_000)
    }

    @Test("encodes UInt64 within Int64 range to Int64")
    func testUInt64WithinRangeEncoding() throws {
        let value: UInt64 = UInt64(Int64.max)
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == Int64.max)
    }

    @Test("throws on UInt64 overflow")
    func testUInt64OverflowThrows() throws {
        let value: UInt64 = UInt64(Int64.max) + 1

        #expect(throws: TupleEncodingError.self) {
            _ = try TupleEncoder.encode(value)
        }
    }

    @Test("encodes UInt32 to Int64")
    func testUInt32Encoding() throws {
        let value: UInt32 = 4_294_967_295  // UInt32.max
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == 4_294_967_295)
    }

    @Test("encodes UInt16 to Int64")
    func testUInt16Encoding() throws {
        let value: UInt16 = 65_535  // UInt16.max
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == 65_535)
    }

    @Test("encodes UInt8 to Int64")
    func testUInt8Encoding() throws {
        let value: UInt8 = 255  // UInt8.max
        let result = try TupleEncoder.encode(value)
        #expect((result as? Int64) == 255)
    }

    // MARK: - Floating Point Encoding

    @Test("encodes Double directly")
    func testDoubleEncoding() throws {
        let result = try TupleEncoder.encode(3.14159)
        #expect((result as? Double) == 3.14159)
    }

    @Test("encodes Float to Double")
    func testFloatEncoding() throws {
        let value: Float = 2.5
        let result = try TupleEncoder.encode(value)
        #expect((result as? Double) == 2.5)
    }

    @Test("encodes negative Double")
    func testNegativeDoubleEncoding() throws {
        let result = try TupleEncoder.encode(-273.15)
        #expect((result as? Double) == -273.15)
    }

    // MARK: - Bool Encoding

    @Test("encodes true Bool")
    func testTrueBoolEncoding() throws {
        let result = try TupleEncoder.encode(true)
        #expect((result as? Bool) == true)
    }

    @Test("encodes false Bool")
    func testFalseBoolEncoding() throws {
        let result = try TupleEncoder.encode(false)
        #expect((result as? Bool) == false)
    }

    // MARK: - Date Encoding

    @Test("encodes Date directly")
    func testDateEncoding() throws {
        let date = Date(timeIntervalSince1970: 1_000_000)
        let result = try TupleEncoder.encode(date)
        #expect(result is Date)
        #expect((result as? Date) == date)
    }

    // MARK: - UUID Encoding

    @Test("encodes UUID directly")
    func testUUIDEncoding() throws {
        let uuid = UUID()
        let result = try TupleEncoder.encode(uuid)
        #expect((result as? UUID) == uuid)
    }

    // MARK: - Data/Bytes Encoding

    @Test("encodes Data to [UInt8]")
    func testDataEncoding() throws {
        let data = Data([0x01, 0x02, 0x03])
        let result = try TupleEncoder.encode(data)
        #expect((result as? [UInt8]) == [0x01, 0x02, 0x03])
    }

    @Test("encodes [UInt8] directly")
    func testBytesEncoding() throws {
        let bytes: [UInt8] = [0xFF, 0x00, 0xAB]
        let result = try TupleEncoder.encode(bytes)
        #expect((result as? [UInt8]) == bytes)
    }

    // MARK: - Tuple Encoding

    @Test("encodes Tuple directly")
    func testTupleEncoding() throws {
        let tuple = Tuple(["nested", Int64(42)])
        let result = try TupleEncoder.encode(tuple)
        #expect((result as? Tuple) == tuple)
    }

    // MARK: - Array Encoding (encodeToArray)

    @Test("encodes [Float] to [Double]")
    func testFloatArrayEncoding() throws {
        let floats: [Float] = [1.0, 2.5, 3.14]
        let result = try TupleEncoder.encodeToArray(floats)

        #expect(result.count == 3)
        #expect((result[0] as? Double) == 1.0)
        #expect((result[1] as? Double) == 2.5)
        #expect(abs((result[2] as! Double) - 3.14) < 0.001)
    }

    @Test("encodes [Double] directly")
    func testDoubleArrayEncoding() throws {
        let doubles: [Double] = [1.0, 2.0, 3.0]
        let result = try TupleEncoder.encodeToArray(doubles)

        #expect(result.count == 3)
        #expect((result[0] as? Double) == 1.0)
        #expect((result[1] as? Double) == 2.0)
        #expect((result[2] as? Double) == 3.0)
    }

    @Test("encodes [Int] to [Int64]")
    func testIntArrayEncoding() throws {
        let ints: [Int] = [1, 2, 3]
        let result = try TupleEncoder.encodeToArray(ints)

        #expect(result.count == 3)
        #expect((result[0] as? Int64) == 1)
        #expect((result[1] as? Int64) == 2)
        #expect((result[2] as? Int64) == 3)
    }

    @Test("encodes [String] directly")
    func testStringArrayEncoding() throws {
        let strings = ["a", "b", "c"]
        let result = try TupleEncoder.encodeToArray(strings)

        #expect(result.count == 3)
        #expect((result[0] as? String) == "a")
        #expect((result[1] as? String) == "b")
        #expect((result[2] as? String) == "c")
    }

    @Test("encodes [Date] directly")
    func testDateArrayEncoding() throws {
        let dates = [
            Date(timeIntervalSince1970: 1_000_000),
            Date(timeIntervalSince1970: 2_000_000)
        ]
        let result = try TupleEncoder.encodeToArray(dates)

        #expect(result.count == 2)
        #expect(result[0] is Date)
        #expect(result[1] is Date)
    }

    // MARK: - Optional Handling

    @Test("throws on nil Optional")
    func testNilOptionalThrows() throws {
        let nilString: String? = nil

        #expect(throws: TupleEncodingError.self) {
            _ = try TupleEncoder.encode(nilString as Any)
        }
    }

    @Test("unwraps non-nil Optional")
    func testNonNilOptionalUnwraps() throws {
        let optionalString: String? = "hello"
        let result = try TupleEncoder.encode(optionalString as Any)
        #expect((result as? String) == "hello")
    }

    @Test("unwraps nested Optional")
    func testNestedOptionalUnwraps() throws {
        let nestedOptional: Int?? = 42
        let result = try TupleEncoder.encode(nestedOptional as Any)
        #expect((result as? Int64) == 42)
    }

    // MARK: - Error Cases

    @Test("throws on unsupported type")
    func testUnsupportedTypeThrows() throws {
        struct CustomType {}
        let custom = CustomType()

        #expect(throws: TupleEncodingError.self) {
            _ = try TupleEncoder.encode(custom)
        }
    }

    // MARK: - encodeAll Tests

    @Test("encodeAll converts multiple values")
    func testEncodeAll() throws {
        let result = try TupleEncoder.encodeAll(["hello", 42, 3.14])

        #expect(result.count == 3)
        #expect((result[0] as? String) == "hello")
        #expect((result[1] as? Int64) == 42)
        #expect((result[2] as? Double) == 3.14)
    }

    // MARK: - encodeOrNil Tests

    @Test("encodeOrNil returns nil on unsupported type")
    func testEncodeOrNilReturnsNil() throws {
        struct CustomType {}
        let result = TupleEncoder.encodeOrNil(CustomType())
        #expect(result == nil)
    }

    @Test("encodeOrNil returns element on supported type")
    func testEncodeOrNilReturnsElement() throws {
        let result = TupleEncoder.encodeOrNil(42)
        #expect((result as? Int64) == 42)
    }

    // MARK: - Tuple Packing Consistency

    @Test("encoded values pack correctly into Tuple")
    func testTuplePackingConsistency() throws {
        let stringResult = try TupleEncoder.encode("test")
        let intResult = try TupleEncoder.encode(42)
        let doubleResult = try TupleEncoder.encode(3.14)
        let boolResult = try TupleEncoder.encode(true)
        let uuidValue = UUID()
        let uuidResult = try TupleEncoder.encode(uuidValue)

        let tuple = Tuple([stringResult, intResult, doubleResult, boolResult, uuidResult])
        let packed = tuple.pack()
        #expect(packed.count > 0)

        let unpackedElements = try Tuple.unpack(from: packed)
        #expect(unpackedElements.count == 5)
        #expect((unpackedElements[0] as? String) == "test")
        #expect((unpackedElements[1] as? Int64) == 42)
        #expect((unpackedElements[2] as? Double) == 3.14)
        #expect((unpackedElements[3] as? Bool) == true)
        #expect((unpackedElements[4] as? UUID) == uuidValue)
    }

    // MARK: - Edge Cases

    @Test("encodes zero values correctly")
    func testZeroEncoding() throws {
        let intZero = try TupleEncoder.encode(0)
        let doubleZero = try TupleEncoder.encode(0.0)

        #expect((intZero as? Int64) == 0)
        #expect((doubleZero as? Double) == 0.0)
    }

    @Test("encodes Int64.min correctly")
    func testInt64MinEncoding() throws {
        let result = try TupleEncoder.encode(Int64.min)
        #expect((result as? Int64) == Int64.min)
    }
}

// MARK: - TupleDecoder Tests

@Suite("TupleDecoder Tests")
struct TupleDecoderTests {

    // MARK: - Integer Decoding

    @Test("decodes Int64 directly")
    func testDecodeInt64() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decodeInt64(element)
        #expect(result == 42)
    }

    @Test("decodes Int64 to Int")
    func testDecodeInt() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decodeInt(element)
        #expect(result == 42)
    }

    @Test("decodes Int64 to Int32")
    func testDecodeInt32() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decodeInt32(element)
        #expect(result == 42)
    }

    @Test("decodes Int64 to Int16")
    func testDecodeInt16() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decodeInt16(element)
        #expect(result == 42)
    }

    @Test("decodes Int64 to Int8")
    func testDecodeInt8() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decodeInt8(element)
        #expect(result == 42)
    }

    @Test("throws on Int32 overflow")
    func testDecodeInt32Overflow() throws {
        let element: any TupleElement = Int64(Int32.max) + 1

        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeInt32(element)
        }
    }

    @Test("throws on Int16 overflow")
    func testDecodeInt16Overflow() throws {
        let element: any TupleElement = Int64(Int16.max) + 1

        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeInt16(element)
        }
    }

    @Test("throws on Int8 overflow")
    func testDecodeInt8Overflow() throws {
        let element: any TupleElement = Int64(Int8.max) + 1

        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeInt8(element)
        }
    }

    // MARK: - Floating Point Decoding

    @Test("decodes Double directly")
    func testDecodeDouble() throws {
        let element: any TupleElement = Double(3.14)
        let result = try TupleDecoder.decodeDouble(element)
        #expect(result == 3.14)
    }

    @Test("decodes Double from Int64")
    func testDecodeDoubleFromInt() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decodeDouble(element)
        #expect(result == 42.0)
    }

    @Test("decodes Float from Double")
    func testDecodeFloat() throws {
        let element: any TupleElement = Double(3.14)
        let result = try TupleDecoder.decodeFloat(element)
        #expect(abs(result - 3.14) < 0.001)
    }

    // MARK: - String Decoding

    @Test("decodes String directly")
    func testDecodeString() throws {
        let element: any TupleElement = "hello"
        let result = try TupleDecoder.decodeString(element)
        #expect(result == "hello")
    }

    @Test("throws on String type mismatch")
    func testDecodeStringTypeMismatch() throws {
        let element: any TupleElement = Int64(42)

        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeString(element)
        }
    }

    // MARK: - Bool Decoding

    @Test("decodes Bool directly")
    func testDecodeBool() throws {
        let element: any TupleElement = true
        let result = try TupleDecoder.decodeBool(element)
        #expect(result == true)
    }

    @Test("decodes Bool from Int64 (0 = false)")
    func testDecodeBoolFromInt0() throws {
        let element: any TupleElement = Int64(0)
        let result = try TupleDecoder.decodeBool(element)
        #expect(result == false)
    }

    @Test("decodes Bool from Int64 (1 = true)")
    func testDecodeBoolFromInt1() throws {
        let element: any TupleElement = Int64(1)
        let result = try TupleDecoder.decodeBool(element)
        #expect(result == true)
    }

    @Test("decodes Bool from Int64 (non-zero = true)")
    func testDecodeBoolFromIntNonZero() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decodeBool(element)
        #expect(result == true)
    }

    // MARK: - Data Decoding

    @Test("decodes Data from [UInt8]")
    func testDecodeData() throws {
        let element: any TupleElement = [UInt8]([0x01, 0x02, 0x03])
        let result = try TupleDecoder.decodeData(element)
        #expect(result == Data([0x01, 0x02, 0x03]))
    }

    @Test("decodes [UInt8] from [UInt8]")
    func testDecodeBytes() throws {
        let element: any TupleElement = [UInt8]([0xFF, 0x00, 0xAB])
        let result = try TupleDecoder.decodeBytes(element)
        #expect(result == [0xFF, 0x00, 0xAB])
    }

    // MARK: - UUID Decoding

    @Test("decodes UUID directly")
    func testDecodeUUID() throws {
        let uuid = UUID()
        let element: any TupleElement = uuid
        let result = try TupleDecoder.decodeUUID(element)
        #expect(result == uuid)
    }

    @Test("throws on UUID type mismatch")
    func testDecodeUUIDTypeMismatch() throws {
        let element: any TupleElement = "not-a-uuid"

        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decodeUUID(element)
        }
    }

    // MARK: - Date Decoding

    @Test("decodes Date directly")
    func testDecodeDate() throws {
        let date = Date(timeIntervalSince1970: 1_000_000)
        let element: any TupleElement = date
        let result = try TupleDecoder.decodeDate(element)
        #expect(result == date)
    }

    // MARK: - Generic decode Tests

    @Test("generic decode works for Int64")
    func testGenericDecodeInt64() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decode(element, as: Int64.self)
        #expect(result == 42)
    }

    @Test("generic decode works for Int")
    func testGenericDecodeInt() throws {
        let element: any TupleElement = Int64(42)
        let result = try TupleDecoder.decode(element, as: Int.self)
        #expect(result == 42)
    }

    @Test("generic decode works for Double")
    func testGenericDecodeDouble() throws {
        let element: any TupleElement = Double(3.14)
        let result = try TupleDecoder.decode(element, as: Double.self)
        #expect(result == 3.14)
    }

    @Test("generic decode works for String")
    func testGenericDecodeString() throws {
        let element: any TupleElement = "hello"
        let result = try TupleDecoder.decode(element, as: String.self)
        #expect(result == "hello")
    }

    @Test("generic decode works for Bool")
    func testGenericDecodeBool() throws {
        let element: any TupleElement = true
        let result = try TupleDecoder.decode(element, as: Bool.self)
        #expect(result == true)
    }

    @Test("generic decode throws on unsupported type")
    func testGenericDecodeUnsupportedType() throws {
        struct CustomType {}
        let element: any TupleElement = "hello"

        #expect(throws: TupleDecodingError.self) {
            _ = try TupleDecoder.decode(element, as: CustomType.self)
        }
    }

    // MARK: - decodeOrNil Tests

    @Test("decodeOrNil returns nil on type mismatch")
    func testDecodeOrNilReturnsNil() throws {
        let element: any TupleElement = "hello"
        let result = TupleDecoder.decodeOrNil(element, as: Int64.self)
        #expect(result == nil)
    }

    @Test("decodeOrNil returns value on match")
    func testDecodeOrNilReturnsValue() throws {
        let element: any TupleElement = Int64(42)
        let result = TupleDecoder.decodeOrNil(element, as: Int64.self)
        #expect(result == 42)
    }
}

// MARK: - Round-Trip Tests

@Suite("TupleEncoder/Decoder Round-Trip Tests")
struct TupleEncoderDecoderRoundTripTests {

    @Test("round-trip Int")
    func testRoundTripInt() throws {
        let original: Int = 12345
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeInt(encoded)
        #expect(decoded == original)
    }

    @Test("round-trip Int64")
    func testRoundTripInt64() throws {
        let original: Int64 = Int64.max
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeInt64(encoded)
        #expect(decoded == original)
    }

    @Test("round-trip Double")
    func testRoundTripDouble() throws {
        let original: Double = -273.15
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeDouble(encoded)
        #expect(decoded == original)
    }

    @Test("round-trip String")
    func testRoundTripString() throws {
        let original: String = "Hello, World!"
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeString(encoded)
        #expect(decoded == original)
    }

    @Test("round-trip Bool")
    func testRoundTripBool() throws {
        let original: Bool = true
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeBool(encoded)
        #expect(decoded == original)
    }

    @Test("round-trip UUID")
    func testRoundTripUUID() throws {
        let original = UUID()
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeUUID(encoded)
        #expect(decoded == original)
    }

    @Test("round-trip Data")
    func testRoundTripData() throws {
        let original = Data([0x01, 0x02, 0x03, 0x04])
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeData(encoded)
        #expect(decoded == original)
    }

    @Test("round-trip Date")
    func testRoundTripDate() throws {
        let original = Date(timeIntervalSince1970: 1_700_000_000)
        let encoded = try TupleEncoder.encode(original)
        let decoded = try TupleDecoder.decodeDate(encoded)
        #expect(decoded == original)
    }
}
