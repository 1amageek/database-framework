import Testing
@testable import DatabaseEngine

@Suite("Database Text Formatting Tests")
struct DatabaseTextFormattingTests {
    @Test("Fixed decimal formatting is locale independent and exact width")
    func fixedDecimalWidth() {
        #expect(DatabaseTextFormatting.fixedDecimal(0, fractionDigits: 0) == "0")
        #expect(DatabaseTextFormatting.fixedDecimal(0, fractionDigits: 3) == "0.000")
        #expect(DatabaseTextFormatting.fixedDecimal(12.25, fractionDigits: 3) == "12.250")
        #expect(DatabaseTextFormatting.fixedDecimal(0.0000001, fractionDigits: 8) == "0.00000010")
        #expect(
            DatabaseTextFormatting.fixedDecimal(1e20, fractionDigits: 1)
                == "100000000000000000000.0"
        )
    }

    @Test("Fixed decimal formatting uses decimal ties-to-even")
    func fixedDecimalRounding() {
        #expect(DatabaseTextFormatting.fixedDecimal(2.5, fractionDigits: 0) == "2")
        #expect(DatabaseTextFormatting.fixedDecimal(3.5, fractionDigits: 0) == "4")
        #expect(DatabaseTextFormatting.fixedDecimal(-2.5, fractionDigits: 0) == "-2")
        #expect(DatabaseTextFormatting.fixedDecimal(-3.5, fractionDigits: 0) == "-4")
        #expect(DatabaseTextFormatting.fixedDecimal(12.345, fractionDigits: 2) == "12.34")
        #expect(DatabaseTextFormatting.fixedDecimal(12.355, fractionDigits: 2) == "12.36")
        #expect(DatabaseTextFormatting.fixedDecimal(999.995, fractionDigits: 2) == "1000.00")
    }

    @Test("Fixed decimal formatting handles non-finite and negative zero")
    func fixedDecimalSpecialValues() {
        #expect(DatabaseTextFormatting.fixedDecimal(.nan, fractionDigits: 2) == "nan")
        #expect(DatabaseTextFormatting.fixedDecimal(.infinity, fractionDigits: 2) == "inf")
        #expect(DatabaseTextFormatting.fixedDecimal(-.infinity, fractionDigits: 2) == "-inf")
        #expect(DatabaseTextFormatting.fixedDecimal(-0.0, fractionDigits: 3) == "-0.000")
        #expect(DatabaseTextFormatting.fixedDecimal(-0.004, fractionDigits: 2) == "-0.00")
    }

    @Test("Lowercase hex reads array slices without an intermediate mapping")
    func lowercaseHex() {
        let bytes: [UInt8] = [0xFF, 0x00, 0x01, 0x0A, 0x10, 0xA5]

        #expect(DatabaseTextFormatting.lowercaseHex(bytes) == "ff00010a10a5")
        #expect(DatabaseTextFormatting.lowercaseHex(bytes[1..<5]) == "00010a10")
        #expect(DatabaseTextFormatting.lowercaseHex(bytes[0..<0]).isEmpty)
    }
}
