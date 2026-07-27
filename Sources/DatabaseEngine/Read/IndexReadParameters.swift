import DatabaseTypes

/// Typed access to the canonical parameters of an index-native read.
///
/// The wrapper retains the dictionary's copy-on-write storage. It does not
/// materialize parameter values while validating their declared shape.
public struct IndexReadParameters: Sendable {
    private let values: [String: FieldValue]

    public init(_ values: [String: FieldValue]) {
        self.values = values
    }

    public subscript(name: String) -> FieldValue? {
        values[name]
    }

    public func requireString(named name: String) throws -> String {
        guard let value = values[name] else {
            throw IndexReadParameterError.missing(name: name)
        }
        guard case .string(let string) = value else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "string"
            )
        }
        return string
    }

    public func requireArray(named name: String) throws -> [FieldValue] {
        guard let value = values[name] else {
            throw IndexReadParameterError.missing(name: name)
        }
        guard case .array(let elements) = value else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "array"
            )
        }
        return elements
    }

    public func optionalArray(named name: String) throws -> [FieldValue]? {
        guard let value = values[name] else {
            return nil
        }
        guard case .array(let elements) = value else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "array"
            )
        }
        return elements
    }

    public func requireStringArray(named name: String) throws -> [String] {
        let elements = try requireArray(named: name)
        var result: [String] = []
        result.reserveCapacity(elements.count)
        for element in elements {
            guard case .string(let string) = element else {
                throw IndexReadParameterError.invalid(
                    name: name,
                    expected: "string array"
                )
            }
            result.append(string)
        }
        return result
    }

    public func requireInteger(named name: String) throws -> Int {
        guard let value = values[name] else {
            throw IndexReadParameterError.missing(name: name)
        }
        guard let integer = Self.integer(exactly: value) else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "runtime-sized integer"
            )
        }
        return integer
    }

    public func optionalInteger(named name: String) throws -> Int? {
        guard let value = values[name] else {
            return nil
        }
        guard let integer = Self.integer(exactly: value) else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "runtime-sized integer"
            )
        }
        return integer
    }

    public func requireFloatingPoint(named name: String) throws -> Double {
        guard let value = values[name] else {
            throw IndexReadParameterError.missing(name: name)
        }
        guard let number = Self.floatingPoint(exactly: value) else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "finite binary floating-point value"
            )
        }
        return number
    }

    public func optionalFloatingPoint(named name: String) throws -> Double? {
        guard let value = values[name] else {
            return nil
        }
        guard let number = Self.floatingPoint(exactly: value) else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "finite binary floating-point value"
            )
        }
        return number
    }

    public func optionalBoolean(named name: String) throws -> Bool? {
        guard let value = values[name] else {
            return nil
        }
        guard case .bool(let boolean) = value else {
            throw IndexReadParameterError.invalid(
                name: name,
                expected: "boolean"
            )
        }
        return boolean
    }

    private static func integer(exactly value: FieldValue) -> Int? {
        switch value {
        case .int8(let value):
            return Int(exactly: value)
        case .int16(let value):
            return Int(exactly: value)
        case .int32(let value):
            return Int(exactly: value)
        case .int64(let value):
            return Int(exactly: value)
        case .uint8(let value):
            return Int(exactly: value)
        case .uint16(let value):
            return Int(exactly: value)
        case .uint32(let value):
            return Int(exactly: value)
        case .uint64(let value):
            return Int(exactly: value)
        case .null, .bool, .float32, .float64, .decimal, .string, .bytes,
             .date, .time, .dateTime, .timestamp, .timeSpan,
             .calendarPeriod, .geographicPoint, .geographicPosition, .vector,
             .uuid, .array, .object, .reference, .rdfTerm:
            return nil
        }
    }

    private static func floatingPoint(exactly value: FieldValue) -> Double? {
        let result: Double?
        switch value {
        case .int8(let value):
            result = Double(exactly: value)
        case .int16(let value):
            result = Double(exactly: value)
        case .int32(let value):
            result = Double(exactly: value)
        case .int64(let value):
            result = Double(exactly: value)
        case .uint8(let value):
            result = Double(exactly: value)
        case .uint16(let value):
            result = Double(exactly: value)
        case .uint32(let value):
            result = Double(exactly: value)
        case .uint64(let value):
            result = Double(exactly: value)
        case .float32(let value):
            result = Double(value)
        case .float64(let value):
            result = value
        case .null, .bool, .decimal, .string, .bytes, .date, .time,
             .dateTime, .timestamp, .timeSpan, .calendarPeriod,
             .geographicPoint, .geographicPosition, .vector, .uuid,
             .array, .object, .reference, .rdfTerm:
            result = nil
        }
        guard let result, result.isFinite else {
            return nil
        }
        return result
    }
}
