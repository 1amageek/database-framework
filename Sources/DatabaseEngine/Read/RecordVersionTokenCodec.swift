import Core
import Crypto
import DatabaseClientProtocol
import Foundation

public enum RecordVersionTokenCodec {
    public static func token(for fields: [String: FieldValue]) -> RecordVersionToken {
        RecordVersionToken(digest(fields: fields).base64EncodedString())
    }

    public static func digest(fields: [String: FieldValue]) -> Data {
        var payload = Data()
        for key in fields.keys.sorted() {
            appendString(key, to: &payload)
            appendValue(fields[key] ?? .null, to: &payload)
        }
        let digest = SHA256.hash(data: payload)
        return Data(digest)
    }

    public static func digest(from token: RecordVersionToken) throws -> [UInt8] {
        guard let data = Data(base64Encoded: token.value) else {
            throw RecordVersionTokenCodecError.invalidToken
        }
        return Array(data)
    }
}

public enum RecordVersionTokenCodecError: Error, Sendable {
    case invalidToken
}

private func appendString(_ value: String, to data: inout Data) {
    let bytes = Array(value.utf8)
    appendLength(bytes.count, to: &data)
    data.append(contentsOf: bytes)
}

private func appendLength(_ value: Int, to data: inout Data) {
    var bigEndian = UInt64(value).bigEndian
    withUnsafeBytes(of: &bigEndian) { data.append(contentsOf: $0) }
}

private func appendValue(_ value: FieldValue, to data: inout Data) {
    switch value {
    case .int64(let value):
        data.append(0x01)
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { data.append(contentsOf: $0) }
    case .double(let value):
        data.append(0x02)
        var bigEndian = value.bitPattern.bigEndian
        withUnsafeBytes(of: &bigEndian) { data.append(contentsOf: $0) }
    case .string(let value):
        data.append(0x03)
        appendString(value, to: &data)
    case .bool(let value):
        data.append(0x04)
        data.append(value ? 0x01 : 0x00)
    case .data(let value):
        data.append(0x05)
        appendLength(value.count, to: &data)
        data.append(value)
    case .null:
        data.append(0x06)
    case .array(let values):
        data.append(0x07)
        appendLength(values.count, to: &data)
        for value in values {
            appendValue(value, to: &data)
        }
    }
}
