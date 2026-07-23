/// Immutable, validated OWL data range ready for repeated membership checks.
public struct CompiledOWLDataRange: Sendable {
    let root: Node

    indirect enum Node: Sendable {
        case datatype(XSDDatatypeKind)
        case intersection([Node])
        case union([Node])
        case complement(Node)
        case oneOf([XSDParsedValue])
        case restriction(XSDDatatypeKind, [CompiledFacet])
    }
}
