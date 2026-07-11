# Security

Security is evaluated at the database execution boundary, not only in a web
route or client.

## Policy Responsibilities

database-kit defines the client-visible security contracts. A model can conform
to SecurityPolicy and decide whether a resource may be read, listed, created,
updated, or deleted.

~~~swift
extension Post: SecurityPolicy {
    static func allowGet(
        resource: Post,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.isPublic || resource.authorID == auth?.userID
    }

    static func allowDelete(
        resource: Post,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.authorID == auth?.userID
    }
}
~~~

A list policy authorizes the query as a whole. It is not a post-query filter.
This avoids returning unauthorized rows and makes authorization behavior
independent of index selection.

## Request Context

Set authentication information around each request:

~~~swift
try await AuthContextKey.$current.withValue(requestAuth) {
    let context = container.newContext()
    let posts = try await context.fetch(Post.self).execute()
    _ = posts
}
~~~

DBContainer installs the security delegate when security is enabled:

~~~swift
let container = try await DBContainer(
    for: schema,
    configuration: configuration,
    security: .enabled()
)
~~~

The delegate evaluates reads and writes immediately before the operation is
accepted. Failed checks throw a typed security error.

## Tenant Isolation

Tenant isolation has two independent parts:

| Concern | Mechanism |
|---|---|
| Physical/logical partition | dynamic directory and partition binding |
| Authorization | SecurityPolicy and request AuthContext |

Every dynamic-directory read, delete, or enumeration must provide all required
partition fields. Authorization still applies after the partition is resolved.
A partition value is not an authorization credential.

## Production Rules

- Establish AuthContext at the request boundary.
- Keep the security configuration enabled in production.
- Never use client-side filtering as the authorization mechanism.
- Validate tenant and workspace identifiers before binding them to a directory.
- Test create, read, update, delete, list, and cross-tenant move behavior.
- Keep credentials and encryption keys outside source control.

Security policy definitions live with model code; execution and delegate
implementation lives in DatabaseEngine.
