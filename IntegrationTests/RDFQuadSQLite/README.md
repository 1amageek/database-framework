# RDF quad SQLite integration harness

This package isolates the canonical RDF quad SQLite integration test from the
main package's full test graph. The framework dependency has no backend trait,
while the direct storage dependency explicitly enables only `SQLite`. The test
constructs `SQLiteStorageEngine` directly and has no fallback backend.

Run the package with Xcode's generated package scheme:

```sh
cd IntegrationTests/RDFQuadSQLite
xcodebuild test \
  -scheme RDFQuadSQLiteIntegration-Package \
  -destination 'platform=macOS' \
  -only-testing:RDFQuadSQLiteIntegrationTests
```

The integration boundary is:

```text
Persistable RDF quad
        |
        v
RDFQuadIndexKind
        |
        v
SQLite DBContainer
        |
        v
Default graph query + named graph query
```
