import DatabaseTypes

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit

final class GraphSnapshotByteOwner: ByteStringOwner, Sendable {
    private let data: Data

    init(data: Data) {
        self.data = data
    }

    var count: Int { data.count }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try data.withUnsafeBytes(body)
    }
}
