import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Produces the canonical row fingerprint directly from a variable binding.
///
/// A binding uses the same canonical fingerprint contract as an unannotated
/// query row. Constructing the row preserves the dictionary's copy-on-write
/// storage and does not materialize its fields.
enum CanonicalBindingFingerprint {
    static func compute(
        _ fields: [String: FieldValue],
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        try CanonicalRowFingerprint.compute(
            DatabaseEngine.QueryRow(fields: fields),
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
