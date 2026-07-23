import DatabaseEngine

/// A prospective SPARQL row append decision.
///
/// The admitted case owns both the retained-row reservation and any capacity
/// growth reservation. Dropping it before append rolls both reservations back.
enum SPARQLBindingAppendPreparation: ~Copyable {
    case incompatible
    case admitted(
        DatabaseRetainedArrayAppendAdmission<VariableBinding>
    )
}
