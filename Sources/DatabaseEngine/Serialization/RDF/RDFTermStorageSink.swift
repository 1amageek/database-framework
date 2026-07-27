/// A synchronous destination for canonical RDF binary bytes.
///
/// Implementations must consume `bytes` before `write(_:)` returns. The
/// borrowed buffer is not valid outside that call. This protocol lets a
/// storage frame or tuple key consume the representation without allocating
/// an intermediate payload.
package protocol RDFTermStorageSink {
    mutating func write(_ byte: UInt8)

    mutating func write(_ bytes: UnsafeRawBufferPointer)
}
