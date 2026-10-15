import DecimalFormatting

extension DatabaseMath {
    /// Lends initialized decimal UTF-8 bytes for one synchronous conversion.
    /// The nonescapable span cannot outlive the stack buffer. Callback errors propagate.
    public static func withDecimalFormatting<Result>(
        of value: Double,
        significantDigits: Int,
        _ body: (borrowing Span<UInt8>) throws -> Result
    ) throws -> Result {
        guard significantDigits == 6 || significantDigits == 15 else { throw DatabaseMathFormattingError.invalidPrecision }
        guard value.isFinite else { throw DatabaseMathFormattingError.nonFiniteValue }
        // %g uses fixed notation only for decimal exponents -4..<precision. That
        // needs at most sign+"0."+3 zeros+15 digits+NUL = 22 bytes. Scientific
        // notation needs sign+15 digits+dot+e+exponent sign+3 exponent digits+NUL = 23.
        return try withUnsafeTemporaryAllocation(of: UInt8.self, capacity: Int(DECIMAL_FORMATTING_CAPACITY)) { buffer in
            var count = 0
            let status = decimal_format_finite(value, Int32(significantDigits), buffer.baseAddress, buffer.count, &count)
            switch status {
            case Int32(DECIMAL_FORMATTING_SUCCESS): break
            case Int32(DECIMAL_FORMATTING_INVALID_ARGUMENT): throw DatabaseMathFormattingError.invalidPrecision
            case Int32(DECIMAL_FORMATTING_NONFINITE): throw DatabaseMathFormattingError.nonFiniteValue
            case Int32(DECIMAL_FORMATTING_INSUFFICIENT_CAPACITY): throw DatabaseMathFormattingError.insufficientCapacity
            case Int32(DECIMAL_FORMATTING_LOCALE_UNAVAILABLE): throw DatabaseMathFormattingError.localeUnavailable
            case Int32(DECIMAL_FORMATTING_LOCALE_RESTORATION_FAILED): throw DatabaseMathFormattingError.localeRestorationFailed
            default: throw DatabaseMathFormattingError.formattingFailed
            }
            guard count > 0, count < buffer.count, buffer[count] == 0 else { throw DatabaseMathFormattingError.invalidResult }
            // C initialized exactly count payload bytes and the terminator; all other
            // bytes stay uninitialized and are excluded from the borrowed span.
            let initialized = UnsafeBufferPointer(start: buffer.baseAddress, count: count)
            return try body(Span(_unsafeElements: initialized))
        }
    }
}
