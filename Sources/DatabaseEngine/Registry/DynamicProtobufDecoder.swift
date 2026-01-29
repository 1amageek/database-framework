/// DynamicProtobufDecoder - Decodes Protobuf wire format using TypeCatalog metadata
///
/// Converts raw Protobuf bytes → [String: Any] using field number → (name, type) mapping
/// from TypeCatalog. Does not require compiled @Persistable types.
///
/// **Wire Format**:
/// - Tag: `(fieldNumber << 3) | wireType`
/// - Wire types: 0=Varint, 1=64-bit, 2=Length-delimited, 5=32-bit

import Foundation
import Core

public struct DynamicProtobufDecoder: Sendable {

    /// Decode Protobuf bytes to a dictionary using TypeCatalog field metadata
    ///
    /// - Parameters:
    ///   - data: Raw Protobuf bytes (payload after ItemEnvelope header)
    ///   - catalog: TypeCatalog providing field number → (name, type) mapping
    /// - Returns: Dictionary of field name → decoded value
    public static func decode(_ data: [UInt8], catalog: TypeCatalog) throws -> [String: Any] {
        // Build field number → FieldSchema lookup
        var fieldMap: [Int: FieldSchema] = [:]
        for field in catalog.fields {
            fieldMap[field.fieldNumber] = field
        }

        // Parse wire format
        let parsedFields = try parseFields(Data(data))

        // Convert parsed fields to named dictionary
        var result: [String: Any] = [:]
        for (fieldNumber, entries) in parsedFields {
            guard let schema = fieldMap[fieldNumber] else { continue }

            if schema.isArray {
                // Collect all values for repeated fields
                var values: [Any] = []
                for entry in entries {
                    if let value = try decodeValue(wireType: entry.wireType, data: entry.data, schema: schema) {
                        values.append(value)
                    }
                }
                // For length-delimited packed arrays (varint, 32-bit, 64-bit in a single entry)
                if entries.count == 1 && entries[0].wireType == 2 && isPackableType(schema.type) {
                    let packed = try decodePackedArray(data: entries[0].data, schema: schema)
                    if !packed.isEmpty {
                        values = packed
                    }
                }
                result[schema.name] = values
            } else if let entry = entries.last {
                // Non-array: use last value (Protobuf semantics)
                if let value = try decodeValue(wireType: entry.wireType, data: entry.data, schema: schema) {
                    result[schema.name] = value
                }
            }
        }

        return result
    }

    // MARK: - Wire Format Parsing

    /// Parse raw Protobuf bytes into field number → [(wireType, data)]
    private static func parseFields(_ data: Data) throws -> [Int: [(wireType: Int, data: Data)]] {
        var offset = 0
        var fields: [Int: [(wireType: Int, data: Data)]] = [:]

        while offset < data.count {
            let tag = try decodeVarint(from: data, offset: &offset)
            let fieldNumber = Int(tag >> 3)
            let wireType = Int(tag & 0x7)

            switch wireType {
            case 0:  // Varint
                let value = try decodeVarint(from: data, offset: &offset)
                // Re-encode the varint value for later interpretation
                var valueData = Data()
                var n = value
                while n >= 0x80 {
                    valueData.append(UInt8(n & 0x7F) | 0x80)
                    n >>= 7
                }
                valueData.append(UInt8(n))
                fields[fieldNumber, default: []].append((wireType, valueData))

            case 1:  // 64-bit
                let endOffset = offset + 8
                guard endOffset <= data.count else {
                    throw DynamicCodecError.truncatedData
                }
                fields[fieldNumber, default: []].append((wireType, Data(data[offset..<endOffset])))
                offset = endOffset

            case 2:  // Length-delimited
                let length = try decodeVarint(from: data, offset: &offset)
                let endOffset = offset + Int(length)
                guard endOffset <= data.count else {
                    throw DynamicCodecError.truncatedData
                }
                fields[fieldNumber, default: []].append((wireType, Data(data[offset..<endOffset])))
                offset = endOffset

            case 5:  // 32-bit
                let endOffset = offset + 4
                guard endOffset <= data.count else {
                    throw DynamicCodecError.truncatedData
                }
                fields[fieldNumber, default: []].append((wireType, Data(data[offset..<endOffset])))
                offset = endOffset

            default:
                throw DynamicCodecError.unknownWireType(wireType)
            }
        }

        return fields
    }

    // MARK: - Value Decoding

    /// Decode a single field value based on its wire type and schema type
    private static func decodeValue(wireType: Int, data: Data, schema: FieldSchema) throws -> Any? {
        switch schema.type {
        case .bool:
            guard wireType == 0 else { return nil }
            var offset = 0
            let raw = try decodeVarint(from: data, offset: &offset)
            return raw != 0

        case .int, .int8, .int16, .int32, .int64:
            guard wireType == 0 else { return nil }
            var offset = 0
            let raw = try decodeVarint(from: data, offset: &offset)
            return Int64(bitPattern: raw)

        case .uint, .uint8, .uint16, .uint32, .uint64:
            guard wireType == 0 else { return nil }
            var offset = 0
            let raw = try decodeVarint(from: data, offset: &offset)
            return UInt64(raw)

        case .double:
            guard wireType == 1, data.count == 8 else { return nil }
            let bits = data.withUnsafeBytes { $0.load(as: UInt64.self) }
            return Double(bitPattern: bits)

        case .float:
            guard wireType == 5, data.count == 4 else { return nil }
            let bits = data.withUnsafeBytes { $0.load(as: UInt32.self) }
            return Float(bitPattern: bits)

        case .date:
            // Date is encoded as Double (timeIntervalSince1970)
            guard wireType == 1, data.count == 8 else { return nil }
            let bits = data.withUnsafeBytes { $0.load(as: UInt64.self) }
            let interval = Double(bitPattern: bits)
            return interval  // Return as Double; CLI can format as date string

        case .string, .uuid:
            guard wireType == 2 else { return nil }
            return String(data: data, encoding: .utf8) ?? "<invalid utf8>"

        case .data:
            guard wireType == 2 else { return nil }
            // Return as base64 string for JSON display
            return data.base64EncodedString()

        case .nested:
            guard wireType == 2 else { return nil }
            // Nested messages are opaque without sub-catalog; return as base64
            return "<nested:\(data.count)bytes>"

        case .enum:
            // Enums may be varint (raw int) or string
            if wireType == 0 {
                var offset = 0
                let raw = try decodeVarint(from: data, offset: &offset)
                return Int64(bitPattern: raw)
            } else if wireType == 2 {
                return String(data: data, encoding: .utf8) ?? "<invalid utf8>"
            }
            return nil
        }
    }

    /// Check if a field type can be packed (varint/32-bit/64-bit)
    private static func isPackableType(_ type: FieldSchemaType) -> Bool {
        switch type {
        case .bool, .int, .int8, .int16, .int32, .int64,
             .uint, .uint8, .uint16, .uint32, .uint64:
            return true  // varint-encoded
        case .double, .date:
            return true  // 64-bit fixed
        case .float:
            return true  // 32-bit fixed
        default:
            return false
        }
    }

    /// Decode a packed repeated field
    private static func decodePackedArray(data: Data, schema: FieldSchema) throws -> [Any] {
        var result: [Any] = []
        var offset = 0

        while offset < data.count {
            switch schema.type {
            case .bool:
                let raw = try decodeVarint(from: data, offset: &offset)
                result.append(raw != 0)
            case .int, .int8, .int16, .int32, .int64:
                let raw = try decodeVarint(from: data, offset: &offset)
                result.append(Int64(bitPattern: raw))
            case .uint, .uint8, .uint16, .uint32, .uint64:
                let raw = try decodeVarint(from: data, offset: &offset)
                result.append(UInt64(raw))
            case .double, .date:
                guard offset + 8 <= data.count else { break }
                let bits = data[offset..<offset+8].withUnsafeBytes { $0.load(as: UInt64.self) }
                result.append(Double(bitPattern: bits))
                offset += 8
            case .float:
                guard offset + 4 <= data.count else { break }
                let bits = data[offset..<offset+4].withUnsafeBytes { $0.load(as: UInt32.self) }
                result.append(Float(bitPattern: bits))
                offset += 4
            default:
                break
            }
        }

        return result
    }

    // MARK: - Varint

    private static func decodeVarint(from data: Data, offset: inout Int) throws -> UInt64 {
        var result: UInt64 = 0
        var shift: UInt64 = 0

        while offset < data.count {
            let byte = data[offset]
            offset += 1

            result |= UInt64(byte & 0x7F) << shift
            if byte & 0x80 == 0 {
                return result
            }
            shift += 7
            if shift > 63 {
                throw DynamicCodecError.varintOverflow
            }
        }

        throw DynamicCodecError.truncatedData
    }
}

// MARK: - Errors

public enum DynamicCodecError: Error, CustomStringConvertible {
    case truncatedData
    case unknownWireType(Int)
    case varintOverflow
    case invalidFieldType(String)
    case encodingFailed(String)

    public var description: String {
        switch self {
        case .truncatedData:
            return "Protobuf data is truncated"
        case .unknownWireType(let wt):
            return "Unknown wire type: \(wt)"
        case .varintOverflow:
            return "Varint exceeds 64-bit limit"
        case .invalidFieldType(let msg):
            return "Invalid field type: \(msg)"
        case .encodingFailed(let msg):
            return "Encoding failed: \(msg)"
        }
    }
}
