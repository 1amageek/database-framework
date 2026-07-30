import DatabaseTypes

/// Builds a validated Swift string from one explicitly owned UTF-8 buffer.
///
/// Swift 6.4's generic `String(decoding:as:)` implementation dynamically casts
/// arbitrary collections to its contiguous-bytes fast path. Embedded Swift does
/// not permit that runtime cast. The database runtime therefore materializes one
/// contiguous UTF-8 buffer only at the required `String` output boundary, then
/// passes its concrete buffer pointer to `String`.
@usableFromInline
enum UTF8StringBuilder {
    @usableFromInline
    static func make(
        byteCount: Int,
        initializing initialize: (UnsafeMutableRawBufferPointer) -> Void
    ) -> String {
        let bytes = ByteString.copying(
            count: byteCount,
            initialize
        )
        return bytes.withUnsafeBytes { source in
            String(decoding: source, as: UTF8.self)
        }
    }
}
