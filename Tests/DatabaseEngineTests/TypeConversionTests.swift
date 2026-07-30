#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import StorageKit
import TestHeartbeat
import Testing
@testable import DatabaseEngine

@Suite("TypeConversion Tests", .heartbeat)
struct TypeConversionTests {
    @Test("Signed and unsigned FieldValue integers convert to Int64")
    func integerFieldValuesConvertToInt64() {
        #expect(TypeConversion.asInt64(.int8(8)) == 8)
        #expect(TypeConversion.asInt64(.int16(16)) == 16)
        #expect(TypeConversion.asInt64(.int32(32)) == 32)
        #expect(TypeConversion.asInt64(.int64(64)) == 64)
        #expect(TypeConversion.asInt64(.uint8(8)) == 8)
        #expect(TypeConversion.asInt64(.uint16(16)) == 16)
        #expect(TypeConversion.asInt64(.uint32(32)) == 32)
        #expect(TypeConversion.asInt64(.uint64(64)) == 64)
        #expect(TypeConversion.asInt64(.bool(true)) == 1)
        #expect(TypeConversion.asInt64(.bool(false)) == 0)
    }

    @Test("Int64 conversion rejects overflow and non-integer values")
    func int64ConversionRejectsInvalidDomains() {
        #expect(TypeConversion.asInt64(.uint64(UInt64(Int64.max))) == Int64.max)
        #expect(TypeConversion.asInt64(.uint64(UInt64(Int64.max) + 1)) == nil)
        #expect(TypeConversion.asInt64(.float64(3.14)) == nil)
        #expect(TypeConversion.asInt64(.string("42")) == nil)
    }

    @Test("Numeric FieldValue domains convert to Double")
    func numericFieldValuesConvertToDouble() {
        #expect(TypeConversion.asDouble(.int8(8)) == 8)
        #expect(TypeConversion.asDouble(.int64(42)) == 42)
        #expect(TypeConversion.asDouble(.uint32(32)) == 32)
        #expect(TypeConversion.asDouble(.uint64(64)) == 64)
        #expect(TypeConversion.asDouble(.float32(3.5)) == 3.5)
        #expect(TypeConversion.asDouble(.float64(3.14)) == 3.14)
        #expect(TypeConversion.asDouble(.string("3.14")) == nil)
    }

    @Test("String conversion accepts canonical string and UUID values")
    func canonicalStringDomainsConvertToString() {
        let uuid = DatabaseTypes.UUID(high: 1, low: 2)
        #expect(TypeConversion.asString(.string("hello")) == "hello")
        #expect(TypeConversion.asString(.uuid(uuid)) == uuid.description)
        #expect(TypeConversion.asString(.int64(42)) == nil)
    }

    @Test("FieldValueRepresentable values preserve their exact domains")
    func representableValuesPreserveExactDomains() {
        #expect(TypeConversion.toFieldValue(true) == .bool(true))
        #expect(TypeConversion.toFieldValue(42 as Int) == .int64(42))
        #expect(TypeConversion.toFieldValue(42 as Int32) == .int32(42))
        #expect(TypeConversion.toFieldValue(42 as UInt32) == .uint32(42))
        #expect(TypeConversion.toFieldValue(UInt64.max) == .uint64(UInt64.max))
        #expect(TypeConversion.toFieldValue(3.5 as Float) == .float32(3.5))
        #expect(TypeConversion.toFieldValue(3.14) == .float64(3.14))
        #expect(TypeConversion.toFieldValue("hello") == .string("hello"))
    }

    @Test("Tuple conversion preserves the full UInt64 domain")
    func tupleConversionPreservesFullWidthUInt64() throws {
        let element = try TypeConversion.toTupleElement(UInt64.max)
        #expect(try TupleDecoder.decodeUInt64(element) == UInt64.max)
    }

    @Test("Tuple scalar extraction preserves canonical values")
    func tupleScalarExtractionPreservesCanonicalValues() throws {
        let integer: any TupleElement = Int64(42)
        let floatingPoint: any TupleElement = Double(3.14)
        let string: any TupleElement = "hello"

        #expect(try TypeConversion.int64(from: integer) == 42)
        #expect(try TypeConversion.double(from: floatingPoint) == 3.14)
        #expect(try TypeConversion.string(from: string) == "hello")
    }

    @Test("Negative, zero, and boundary values remain exact")
    func numericBoundariesRemainExact() {
        #expect(TypeConversion.asInt64(.int64(-42)) == -42)
        #expect(TypeConversion.asDouble(.float64(-3.14)) == -3.14)
        #expect(TypeConversion.asInt64(.int64(0)) == 0)
        #expect(TypeConversion.asDouble(.float64(0)) == 0)
        #expect(TypeConversion.asInt64(.int64(Int64.max)) == Int64.max)
        #expect(TypeConversion.asInt64(.int64(Int64.min)) == Int64.min)
    }
}
#endif
#endif
