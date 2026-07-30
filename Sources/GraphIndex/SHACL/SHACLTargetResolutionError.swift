import DatabaseKit
import DatabaseTypes

public enum SHACLTargetResolutionError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case implicitClassRequiresIRI(RDFTerm?)

    public var description: String {
        switch self {
        case .implicitClassRequiresIRI(let identifier):
            return "An implicit-class SHACL shape requires an IRI identifier, got \(identifier?.description ?? "nil")"
        }
    }
}
