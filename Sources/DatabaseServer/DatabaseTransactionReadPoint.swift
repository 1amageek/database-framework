import DatabaseKit
import DatabaseTypes
import StorageKit

/// Captures the strongest read point exposed by one concrete transaction.
/// Backends without versioned reads receive a transaction-lifetime opaque ID;
/// callers must not attempt to restore that ID in a later transaction.
enum DatabaseTransactionReadPoint {
    enum Error: Swift.Error, Sendable {
        case invalidVersion(Int64)
    }

    static func capture(
        domainID: String,
        transaction: any TransactionAccess
    ) async throws -> DomainReadPoint {
        if transaction.capabilities.readVersion {
            let signedVersion = try await transaction.getReadVersion()
            guard let version = UInt64(exactly: signedVersion) else {
                throw Error.invalidVersion(signedVersion)
            }
            return try DomainReadPoint(
                domainID: domainID,
                position: .version(version)
            )
        }
        return try DomainReadPoint(
            domainID: domainID,
            position: .opaque(makeOpaqueIdentifier())
        )
    }

    static func restore(
        _ position: DomainReadPoint.Position,
        transaction: any TransactionAccess
    ) throws -> Bool {
        switch position {
        case .version(let version):
            guard transaction.capabilities.historicalReadVersion,
                  let signedVersion = Int64(exactly: version) else {
                return false
            }
            try transaction.setReadVersion(signedVersion)
            return true
        case .opaque:
            return false
        }
    }

    private static func makeOpaqueIdentifier() -> ByteString {
        var generator = SystemRandomNumberGenerator()
        return ByteString((0..<32).map { _ in
            UInt8.random(in: .min ... .max, using: &generator)
        })
    }
}
