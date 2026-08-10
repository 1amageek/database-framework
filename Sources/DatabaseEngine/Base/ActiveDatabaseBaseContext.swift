/// Request-local immutable Base placement used by all relative data paths.
package enum ActiveDatabaseBaseContext {
    @TaskLocal package static var lease: DatabaseBaseLease?
}
