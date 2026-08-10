/// Request-local field projection used while trusted query execution holds a
/// complete persisted model but exposes only an authorized subset.
package enum RequestFieldAuthorization {
    @TaskLocal package static var fieldsByEntity: [String: Set<String>]?
}
