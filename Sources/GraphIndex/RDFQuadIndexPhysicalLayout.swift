import DatabaseValue

/// Stable bytes reserved by the canonical RDF index key layout.
public enum RDFQuadIndexPhysicalLayout {
    /// RDF term tags occupy 1...4, leaving 0xFF as the default graph marker.
    public static let defaultGraphDiscriminator: DatabaseBytes = [0xff]
}
