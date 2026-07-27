import DatabaseTypes
import DatabaseEngine
import StorageKit

enum RankCounterError: Error, Sendable, Equatable {
    case invalidEncoding(ByteConversionError)
    case negativeValue(Int64)
    case exceedsPlatformInt(Int64)
}

enum RankCounterCodec {
    static func decode(_ bytes: ByteString) throws(RankCounterError) -> Int64 {
        let value: Int64
        do {
            value = try ByteConversion.bytesToInt64(bytes)
        } catch let error {
            throw .invalidEncoding(error)
        }
        guard value >= 0 else { throw .negativeValue(value) }
        return value
    }

    static func decodeInt(_ bytes: ByteString) throws(RankCounterError) -> Int {
        let value = try decode(bytes)
        guard let result = Int(exactly: value) else {
            throw .exceedsPlatformInt(value)
        }
        return result
    }
}
