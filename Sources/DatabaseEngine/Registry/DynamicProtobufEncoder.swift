/// DynamicProtobufEncoder - Encodes [String: Any] to Protobuf wire format using TypeCatalog
///
/// Converts JSON-like dictionary → Protobuf bytes using field name → (fieldNumber, type) mapping.
/// Does not require compiled @Persistable types.
///
/// **Wire Format**:
/// - Tag: `(fieldNumber << 3) | wireType`
/// - Wire types: 0=Varint, 1=64-bit, 2=Length-delimited, 5=32-bit

import Foundation
import Core

public struct DynamicProtobufEncoder: Sendable {

    /// Encode a dictionary to Protobuf bytes using TypeCatalog field metadata
    ///
    /// - Parameters:
    ///   - dict: Dictionary of field name → value (from JSON parsing)
    ///   - catalog: TypeCatalog providing field name → (fieldNumber, type) mapping
    /// - Returns: Protobuf wire format bytes
    public static func encode(_ dict: [String: Any], catalog: TypeCatalog) throws -> [UInt8] {
        // Build field name → FieldSchema lookup
        var fieldMap: [String: FieldSchema] = [:]
        for field in catalog.fields {
            fieldMap[field.name] = field
        }

        var data = Data()

        for (name, value) in dict {
            guard let schema = fieldMap[name] else { continue }

            if schema.isArray {
                // Encode repeated field
                guard let array = value as? [Any] else { continue }
                for element in array {
                    try encodeField(fieldNumber: schema.fieldNumber, value: element, schema: schema, to: &data)
                }
            } else {
                try encodeField(fieldNumber: schema.fieldNumber, value: value, schema: schema, to: &data)
            }
        }

        return Array(data)
    }

    // MARK: - Field Encoding

    private static func encodeField(fieldNumber: Int, value: Any, schema: FieldSchema, to data: inout Data) throws {
        switch schema.type {
        case .bool:
            let boolValue: Bool
            if let b = value as? Bool {
                boolValue = b
            } else if let n = value as? Int {
                boolValue = n != 0
            } else if let n = value as? NSNumber {
                boolValue = n.boolValue
            } else {
                throw DynamicCodecError.encodingFailed("Cannot convert \(value) to Bool")
            }
            let tag = (fieldNumber << 3) | 0
            data.append(contentsOf: encodeVarint(UInt64(tag)))
            data.append(contentsOf: encodeVarint(boolValue ? 1 : 0))

        case .int, .int8, .int16, .int32, .int64:
            let intValue: Int64
            if let i = value as? Int64 {
                intValue = i
            } else if let i = value as? Int {
                intValue = Int64(i)
            } else if let n = value as? NSNumber {
                intValue = n.int64Value
            } else {
                throw DynamicCodecError.encodingFailed("Cannot convert \(value) to Int64")
            }
            let tag = (fieldNumber << 3) | 0
            data.append(contentsOf: encodeVarint(UInt64(tag)))
            data.append(contentsOf: encodeVarint(UInt64(bitPattern: intValue)))

        case .uint, .uint8, .uint16, .uint32, .uint64:
            let uintValue: UInt64
            if let u = value as? UInt64 {
                uintValue = u
            } else if let u = value as? UInt {
                uintValue = UInt64(u)
            } else if let i = value as? Int {
                uintValue = UInt64(i)
            } else if let n = value as? NSNumber {
                uintValue = n.uint64Value
            } else {
                throw DynamicCodecError.encodingFailed("Cannot convert \(value) to UInt64")
            }
            let tag = (fieldNumber << 3) | 0
            data.append(contentsOf: encodeVarint(UInt64(tag)))
            data.append(contentsOf: encodeVarint(uintValue))

        case .double, .date:
            let doubleValue: Double
            if let d = value as? Double {
                doubleValue = d
            } else if let n = value as? NSNumber {
                doubleValue = n.doubleValue
            } else {
                throw DynamicCodecError.encodingFailed("Cannot convert \(value) to Double")
            }
            let tag = (fieldNumber << 3) | 1
            data.append(contentsOf: encodeVarint(UInt64(tag)))
            let bits = doubleValue.bitPattern
            for shift in stride(from: 0, to: 64, by: 8) {
                data.append(UInt8(truncatingIfNeeded: bits >> shift))
            }

        case .float:
            let floatValue: Float
            if let f = value as? Float {
                floatValue = f
            } else if let d = value as? Double {
                floatValue = Float(d)
            } else if let n = value as? NSNumber {
                floatValue = n.floatValue
            } else {
                throw DynamicCodecError.encodingFailed("Cannot convert \(value) to Float")
            }
            let tag = (fieldNumber << 3) | 5
            data.append(contentsOf: encodeVarint(UInt64(tag)))
            let bits = floatValue.bitPattern
            for shift in stride(from: 0, to: 32, by: 8) {
                data.append(UInt8(truncatingIfNeeded: bits >> shift))
            }

        case .string, .uuid:
            let stringValue: String
            if let s = value as? String {
                stringValue = s
            } else {
                stringValue = "\(value)"
            }
            let tag = (fieldNumber << 3) | 2
            data.append(contentsOf: encodeVarint(UInt64(tag)))
            let stringData = stringValue.data(using: .utf8) ?? Data()
            data.append(contentsOf: encodeVarint(UInt64(stringData.count)))
            data.append(stringData)

        case .data:
            let tag = (fieldNumber << 3) | 2
            data.append(contentsOf: encodeVarint(UInt64(tag)))
            // Accept base64 string or raw Data
            let rawData: Data
            if let d = value as? Data {
                rawData = d
            } else if let s = value as? String, let decoded = Data(base64Encoded: s) {
                rawData = decoded
            } else {
                throw DynamicCodecError.encodingFailed("Cannot convert \(value) to Data")
            }
            data.append(contentsOf: encodeVarint(UInt64(rawData.count)))
            data.append(rawData)

        case .nested:
            throw DynamicCodecError.encodingFailed("Nested type encoding not supported via dynamic encoder")

        case .enum:
            // Try as integer first, then as string
            if let i = value as? Int {
                let tag = (fieldNumber << 3) | 0
                data.append(contentsOf: encodeVarint(UInt64(tag)))
                data.append(contentsOf: encodeVarint(UInt64(bitPattern: Int64(i))))
            } else if let s = value as? String {
                let tag = (fieldNumber << 3) | 2
                data.append(contentsOf: encodeVarint(UInt64(tag)))
                let stringData = s.data(using: .utf8) ?? Data()
                data.append(contentsOf: encodeVarint(UInt64(stringData.count)))
                data.append(stringData)
            } else {
                throw DynamicCodecError.encodingFailed("Cannot encode enum value: \(value)")
            }
        }
    }

    // MARK: - Varint

    private static func encodeVarint(_ value: UInt64) -> [UInt8] {
        var result: [UInt8] = []
        var n = value
        while n >= 0x80 {
            result.append(UInt8(n & 0x7F) | 0x80)
            n >>= 7
        }
        result.append(UInt8(n))
        return result
    }
}
