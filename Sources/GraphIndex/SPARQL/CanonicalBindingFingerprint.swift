import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Produces the canonical row fingerprint directly from a variable binding.
///
/// RDF bindings preserve their named-value fingerprint domain independently
/// of the positional query row fingerprint contract.
enum CanonicalBindingFingerprint {
    static func compute(
        _ fields: [String: FieldValue],
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        try CanonicalRowFingerprint.compute(
            fields: fields,
            workMeter: workMeter
        )
    }
}

extension VariableBinding {
    /// Computes the canonical fingerprint without copying binding storage.
    func canonicalFingerprint(
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        try withBindings { fields in
            try CanonicalBindingFingerprint.compute(
                fields,
                workMeter: workMeter
            )
        }
    }
}
