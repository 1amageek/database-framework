import DatabaseKit
import Testing
@testable import DatabaseEngine

@Suite("Canonical query field authorization scope")
struct DatabaseFieldReadAuthorizationPlanTests {
    @Test("Correlated subqueries authorize inner and outer fields in their own scopes")
    func correlatedSubqueryScopes() throws {
        let schema = try makeSchema()
        let childQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.col("child", "id")),
            ]),
            source: .table(TableRef(table: "Child", alias: "child")),
            filter: .and(
                .equal(
                    .col("child", "groupID"),
                    .col("parent", "groupID")
                ),
                .greaterThan(
                    .col("child", "secret"),
                    .string("minimum")
                )
            )
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.col("parent", "id")),
            ]),
            source: .table(TableRef(table: "Parent", alias: "parent")),
            filter: .exists(childQuery)
        )

        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: query,
            schema: schema
        )

        #expect(plan.fieldsByEntity["Parent"] == ["id", "groupID"])
        #expect(
            plan.fieldsByEntity["Child"]
                == ["id", "groupID", "secret"]
        )
    }

    @Test("Unqualified inner columns shadow outer columns")
    func innerColumnsShadowOuterColumns() throws {
        let schema = try makeSchema()
        let childQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.col("child", "id")),
            ]),
            source: .table(TableRef(table: "Child", alias: "child")),
            filter: .equal(.col("token"), .string("child-token"))
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.col("parent", "id")),
            ]),
            source: .table(TableRef(table: "Parent", alias: "parent")),
            filter: .exists(childQuery)
        )

        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: query,
            schema: schema
        )

        #expect(plan.fieldsByEntity["Parent"] == ["id"])
        #expect(plan.fieldsByEntity["Child"] == ["id", "token"])
    }

    @Test("Lateral sources inherit only the preceding relation scope")
    func lateralSourceScope() throws {
        let schema = try makeSchema()
        let lateralQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.col("child", "id")),
            ]),
            source: .table(TableRef(table: "Child", alias: "child")),
            filter: .equal(
                .col("child", "groupID"),
                .col("parent", "groupID")
            )
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.col("parent", "id")),
            ]),
            source: .join(
                JoinClause(
                    type: .lateral,
                    left: .table(
                        TableRef(table: "Parent", alias: "parent")
                    ),
                    right: .subquery(lateralQuery, alias: "matching_child"),
                    condition: .on(.bool(true))
                )
            )
        )

        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: query,
            schema: schema
        )

        #expect(plan.fieldsByEntity["Parent"] == ["id", "groupID"])
        #expect(plan.fieldsByEntity["Child"] == ["id", "groupID"])
    }

    @Test("Set operation authorization follows positional column lineage")
    func setOperationColumnLineage() throws {
        let schema = try makeSchema()
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.col("leftValue")),
            ]),
            source: .union([
                .table(TableRef("SetLeft")),
                .table(TableRef("SetRight")),
            ])
        )

        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: query,
            schema: schema
        )

        #expect(plan.fieldsByEntity["SetLeft"] == ["leftValue"])
        #expect(plan.fieldsByEntity["SetRight"] == ["rightValue"])
    }

    private func makeSchema() throws -> Schema {
        let parent = try Schema.Entity(
            name: "Parent",
            identifierType: .string,
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(
                    name: "groupID",
                    fieldNumber: 2,
                    type: .string
                ),
                FieldSchema(name: "token", fieldNumber: 3, type: .string),
            ]
        )
        let child = try Schema.Entity(
            name: "Child",
            identifierType: .string,
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(
                    name: "groupID",
                    fieldNumber: 2,
                    type: .string
                ),
                FieldSchema(name: "secret", fieldNumber: 3, type: .string),
                FieldSchema(name: "token", fieldNumber: 4, type: .string),
            ]
        )
        let setLeft = try Schema.Entity(
            name: "SetLeft",
            identifierType: .string,
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(
                    name: "leftValue",
                    fieldNumber: 2,
                    type: .string
                ),
            ]
        )
        let setRight = try Schema.Entity(
            name: "SetRight",
            identifierType: .string,
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(
                    name: "rightValue",
                    fieldNumber: 2,
                    type: .string
                ),
            ]
        )
        return try Schema(
            entities: [parent, child, setLeft, setRight]
        )
    }
}
