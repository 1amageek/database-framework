import DatabaseTypes
import StorageKit

/// Encodes HNSW's native UInt64 labels through the FoundationDB Tuple Layer.
///
/// Tuple integer decoding is canonical by value rather than by the source
/// Swift type: nonnegative values through `Int64.max` decode as signed values,
/// while larger values decode as unsigned values. Both representations are
/// therefore valid for one HNSW UInt64 label.
enum HNSWLabelCodec {
    static func tuple(_ label: UInt64) -> Tuple {
        Tuple(label)
    }

    static func decode(_ tuple: Tuple) throws(VectorIndexError) -> UInt64 {
        guard tuple.count == 1 else {
            throw .invalidStructure("Invalid HNSW label tuple")
        }

        let value: TupleValue
        do {
            value = try tuple.value(at: 0)
        } catch {
            throw .invalidStructure("Invalid HNSW label tuple")
        }

        switch value {
        case .signedInteger(let label) where label >= 0:
            return UInt64(label)
        case .unsignedInteger(let label):
            return label
        default:
            throw .invalidStructure("Invalid HNSW label value")
        }
    }

    static func decodePacked(
        _ payload: ByteString
    ) throws(VectorIndexError) -> UInt64 {
        let tuple: Tuple
        do {
            tuple = try Tuple(packed: payload)
        } catch {
            throw .invalidStructure("Invalid HNSW label payload")
        }
        return try decode(tuple)
    }
}
