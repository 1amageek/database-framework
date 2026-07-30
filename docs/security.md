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
try await RequestAuthorization.$context.withValue(authorization) {
    let context = container.newContext()
    let posts = try await context.fetch(Post.self).execute()
    _ = posts
}
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

## Tenant Isolation

Tenant isolation has two independent parts:

| Concern | Mechanism |
|---|---|
| Physical/logical partition | dynamic directory and partition binding |
| Authorization | SecurityPolicy and request AuthorizationContext |

Every dynamic-directory read, delete, or enumeration must provide all required
partition fields. Authorization still applies after the partition is resolved.
A partition value is not an authorization credential.

## Production Rules

- Establish AuthorizationContext at the request boundary.
- Register each AuthorizationPolicyHandler in DatabaseRuntimeConfiguration.
- Keep the security configuration enabled in production.
- Never use client-side filtering as the authorization mechanism.
- Validate tenant and workspace identifiers before binding them to a directory.
- Test create, read, update, delete, list, and cross-tenant move behavior.
- Keep credentials and encryption keys outside source control.

Security policy definitions live with model code; execution and delegate
implementation lives in DatabaseEngine.
