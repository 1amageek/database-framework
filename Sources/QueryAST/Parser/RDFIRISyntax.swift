import DatabaseTypes

enum RDFIRISyntax {
    static func isAbsolute(_ value: String) -> Bool {
        do {
            _ = try RDFIRI(value)
            return true
        } catch {
            return false
        }
    }
}
