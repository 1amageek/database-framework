# Security

Security is evaluated at the database execution boundary, not only in a web
route or client.

## Policy Responsibilities

database-kit defines the client-visible security contracts. A model can conform
to SecurityPolicy and decide whether a resource may be read, listed, created,
updated, or deleted.

~~~swift
extension Post: SecurityPolicy {
    static func permitsRead(
        of resource: borrowing Post,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.isPublic
            || resource.authorID == context.principal?.identifier
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.isAuthenticated && (query.limit ?? 0) <= 100
    }

    static func permitsDelete(
        _ resource: borrowing Post,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.authorID == context.principal?.identifier
    }
}
~~~

A list policy authorizes the query as a whole. It is not a post-query filter.
This avoids returning unauthorized rows and makes authorization behavior
independent of index selection.

## Request Context

Register every policy explicitly in the container-scoped runtime. Conformance
alone does not mutate a global registry and is not discovered through
reflection.

~~~swift
let runtime = try DatabaseFrameworkRuntime.configuration(
    entityRuntimes: [try DatabaseFrameworkRuntime.entity(Post.self)],
    authorizationPolicies: [AuthorizationPolicyHandler(Post.self)]
)
~~~

Set authorization information around each request:

~~~swift
let authorization = AuthorizationContext.authenticated(
    Principal(identifier: authenticatedUserID, roles: authenticatedRoles)
)
let context = container.newContext(authorization: authorization)
let posts = try await context.fetch(Post.self).execute()
_ = posts
~~~

DBContainer installs the security delegate when security is enabled:

~~~swift
let container = try await DBContainer.open(
    for: schema,
    configuration: configuration,
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime,
    security: .enabled()
)
~~~

The delegate evaluates reads and writes immediately before the operation is
accepted. Failed checks throw a typed security error.

## Database Root and Optional Base Isolation

Tenant isolation has two independent parts:

| Concern | Mechanism |
|---|---|
| Physical/logical boundary | database root, plus explicit Base roots with `MultipleBases` |
| Resource authorization | application boundary by default; persisted direct and role Grants with `MultipleBases` |
| Entity and field authorization | SecurityPolicy and `@Restricted` |

The standard runtime binds data operations to its single data root and applies
registered entity and field policy. It has no persisted Grant store. The
`MultipleBases` trait additionally accepts one exact Base or a read-only
Composition and enables persisted Grants. `#Directory` and dynamic partitions
are relative paths inside the selected root and are not credentials. The
trait-specific runtime opens the selected transaction, unions matching direct
and role Grants, requires the exact access bits, and only then executes entity
and field policy. A Composition read requires `.read` on every retained member
and fails as a whole when any member is unavailable or unauthorized.

## Production Rules

- Establish `AuthorizationContext` at the request boundary and bind it to the
  database root or a `DatabaseSession` selector.
- Use `.database` by default; when `MultipleBases` is enabled, select a Base or
  Composition explicitly when crossing that boundary.
- With `MultipleBases`, manage Base access through persisted Grants; role names
  are claims, not bypasses.
- Register each AuthorizationPolicyHandler in DatabaseRuntimeConfiguration.
- Keep the security configuration enabled in production.
- Never use client-side filtering as the authorization mechanism.
- Validate tenant and workspace identifiers before binding them to a directory.
- Test create, read, update, delete, list, and cross-tenant move behavior.
- Keep credentials and encryption keys outside source control.

Security policy definitions live with model code; execution and delegate
implementation lives in DatabaseEngine.
