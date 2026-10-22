import DatabaseKit
import Testing
@testable import DatabaseEngine

@Suite("Canonical query field authorization scope")
struct DatabaseFieldReadAuthorizationPlanTests {

    @Test("Whole-row ORDER dependencies resolve physical fields before list admission")
    func wholeRowOrderingRequiresPhysicalFields() throws {
        let schema = try makeSchema()
        let entity = try #require(schema.entity(named: "Parent"))
        let source = DataSource.aliased(.table(TableRef("Parent")), name: "p")
        let selected = Expression.field(.wholeRow("p"), name: "token")
        func fields(_ query: SelectQuery) throws -> Set<String> {
            Set(try DatabaseReadPolicy.listRequirement(entityName: "Parent",
                selectQuery: query, schema: schema).orderBy ?? [])
        }
        let query = SelectQuery(projection: [.expression(.col("p", "id"))], source: source)
        #expect(try fields(query.replacing(orderBy: [SortKey(selected)])) == ["token"])
        #expect(try fields(query.replacing(orderBy: [SortKey(.field(.col("p"), name: "token"))])) == ["token"])
        #expect(try fields(query.replacing(orderBy: [SortKey(.wholeRow("p"))])) == Set(entity.allFields))
        #expect(try fields(query.replacing(orderBy: [SortKey(.col("p"))])) == Set(entity.allFields))
        let projected = query.replacing(projection: [.expression(selected, alias: "sort_value")])
        #expect(try fields(projected.replacing(orderBy: [SortKey(.col("sort_value"))])) == ["token"])
        #expect(try fields(projected.replacing(orderBy: [SortKey(.literal(.int(1)))])) == ["token"])
        let aliases = entity.allFields.map { $0 == "id" ? "p" : $0 }
        let priority = query.replacing(source: .aliased(.table(TableRef("Parent")), name: "p", columns: aliases))
        #expect(try fields(priority.replacing(orderBy: [SortKey(.col("p"))])) == ["id"])
        #expect(try fields(priority.replacing(orderBy: [SortKey(.field(.col("p"), name: "token"))])) == ["id"])
        let derived = DataSource.aliased(.subquery(SelectQuery(projection: [.column("token")],
            source: .table(TableRef("Parent")))), name: "d")
        let mixed = query.replacing(source: .join(JoinClause(type: .cross, left: source,
            right: derived)))
        for key: Expression in [.field(.wholeRow("d"), name: "token"),
            .field(.col("d"), name: "token"), .wholeRow("d"), .col("d", "token")] {
            #expect(try fields(mixed.replacing(orderBy: [SortKey(key)])) == ["token"])
        }
        #expect(try fields(mixed.replacing(orderBy: [SortKey(.col("p", "id"))])) == ["id"])
        let derivedProjection = mixed.replacing(projection: [
            .expression(.field(.wholeRow("d"), name: "token"), alias: "sort_value")])
        for key: Expression in [.col("sort_value"), .literal(.int(1))] {
            #expect(try fields(derivedProjection.replacing(orderBy: [SortKey(key)])) == ["token"])
        }
    }

    @Test("Fields inside SQL constructor children remain in the authorization plan")
    func constructorFieldsAreAuthorized() throws {
        let query = SelectQuery(projection: [ProjectionItem(.array([
            .row([.col("parent", "token"), .col("parent", "groupID")])
        ]))], source: .aliased(.table(TableRef("Parent")), name: "parent"))
        let plan = DatabaseFieldReadAuthorizationPlan.make(query: query, schema: try makeSchema())
        #expect(plan.fieldsByEntity["Parent"] == ["token", "groupID"])
        let schema = try makeSchema()
        let fixed = try DatabaseReadPolicy.listRequirement(entityName: "Parent",
            selectQuery: query.replacing(pagination: QueryPagination(limit: .count(.fixed(1), withTies: false))),
            schema: schema)
        #expect(fixed.window == .fixed(limit: 1, offset: nil))
        let declaration = QueryPagination(limit: .count(.expression(.literal(.int(1))), withTies: false))
        let deferred = try DatabaseReadPolicy.listRequirement(entityName: "Parent",
            selectQuery: query.replacing(pagination: declaration), schema: schema)
        #expect(deferred.window == .deferred(declaration))
        #expect(deferred != fixed)
        #expect(deferred.window.admission.limit == nil)
        let access = Expression.arrayAccess(.col("parent", "token"), subscripts: [
            .index(.col("parent", "id")),
            .slice(lower: nil, upper: .col("parent", "groupID")),
            .slice(lower: .col("parent", "id"), upper: nil),
        ])
        for expression in [access, .collectionOperation(.jsonPath, left: access, right: .col("parent", "groupID"))] {
            let query = SelectQuery(projection: [ProjectionItem(expression)],
                source: .aliased(.table(TableRef("Parent")), name: "parent"))
            let plan = DatabaseFieldReadAuthorizationPlan.make(query: query, schema: try makeSchema())
            #expect(plan.fieldsByEntity["Parent"] == ["id", "token", "groupID"])
            #expect(!plan.isCovered(by: ["Parent": ["token", "groupID"]]))
        }
    }

    @Test("Authorization coverage requires every entity and field")
    func authorizationCoverageRequiresEveryEntityAndField() {
        let plan = DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [
                "Parent": ["id", "groupID"],
                "Child": ["secret"],
            ]
        )

        #expect(!plan.isCovered(by: nil))
        #expect(
            !plan.isCovered(
                by: ["Parent": ["id", "groupID"]]
            )
        )
        #expect(
            !plan.isCovered(
                by: [
                    "Parent": ["id"],
                    "Child": ["secret"],
                ]
            )
        )
        #expect(
            plan.isCovered(
                by: [
                    "Parent": ["id", "groupID", "token"],
                    "Child": ["id", "secret"],
                ]
            )
        )
    }

    @Test("Correlated subqueries authorize inner and outer fields in their own scopes")
    func correlatedSubqueryScopes() throws {
        let schema = try makeSchema()
        let childQuery = SelectQuery(
            projection: [
                ProjectionItem(.col("child", "id")),
            ],
            source: .aliased(.table(TableRef("Child")), name: "child"),
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
            projection: [
                ProjectionItem(.col("parent", "id")),
            ],
            source: .aliased(.table(TableRef("Parent")), name: "parent"),
            filter: .exists(childQuery)
        )

        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: query,
            schema: schema
        )

        let orderedChild = childQuery.replacing(orderBy: [SortKey(.col("parent", "token"))])
        let scalar = query.replacing(projection: [.expression(.subquery(orderedChild))])
        #expect(try DatabaseFieldReadAuthorizationPlan.orderingDependencies(query: scalar,
            schema: schema, sqlResolution: nil) == ["Parent": ["token"]])
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
            projection: [
                ProjectionItem(.col("child", "id")),
            ],
            source: .aliased(.table(TableRef("Child")), name: "child"),
            filter: .equal(.col("token"), .string("child-token"))
        )
        let query = SelectQuery(
            projection: [
                ProjectionItem(.col("parent", "id")),
            ],
            source: .aliased(.table(TableRef("Parent")), name: "parent"),
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
            projection: [
                ProjectionItem(.col("child", "id")),
            ],
            source: .aliased(.table(TableRef("Child")), name: "child"),
            filter: .equal(
                .col("child", "groupID"),
                .col("parent", "groupID")
            )
        )
        let query = SelectQuery(
            projection: [
                ProjectionItem(.col("parent", "id")),
            ],
            source: .join(
                JoinClause(
                    type: .lateral,
                    left: .aliased(.table(TableRef("Parent")), name: "parent"),
                    right: .aliased(.subquery(lateralQuery), name: "matching_child"),
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
            projection: [
                ProjectionItem(.col("leftValue")),
            ],
            source: .setOperation(operator: .union, quantifier: .distinct,
                left: SelectQuery(projection: [.expression(.col("leftValue"))], source: .table(TableRef("SetLeft"))),
                right: SelectQuery(projection: [.expression(.col("rightValue"))], source: .table(TableRef("SetRight"))))
        )

        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: query,
            schema: schema
        )

        #expect(plan.fieldsByEntity["SetLeft"] == ["leftValue"])
        #expect(plan.fieldsByEntity["SetRight"] == ["rightValue"])
    }

    @Test("Join aliases map duplicate column names by output ordinal")
    func joinedAliasOrdinals() throws {
        let join = DataSource.join(JoinClause(type: .cross,
            left: .table(TableRef("Parent")), right: .table(TableRef("Child"))))
        let query = SelectQuery(projection: [.expression(.col("joined", "childID"))],
            source: .aliased(join, name: "joined", columns: ["parentID", "groupID", "token", "childID"]))
        let plan = DatabaseFieldReadAuthorizationPlan.make(query: query, schema: try makeSchema())
        #expect(plan.fieldsByEntity["Parent"] == [])
        #expect(plan.fieldsByEntity["Child"] == ["id"])
        #expect(plan.isCovered(by: ["Parent": [], "Child": ["id"]]))
        #expect(!plan.fieldsByEntity["Child", default: []].contains("secret"))

        let seed = NamedSubquery(name: "Seed", query: SelectQuery(
            projection: [.expression(.literal(.int(1)), alias: "seed")], source: .values([[]])))
        let shadow = NamedSubquery(name: "Parent", columns: ["x"], query: SelectQuery(
            projection: [.wildcard()], source: .table(TableRef("Seed"))))
        let cteJoin = DataSource.join(JoinClause(type: .cross,
            left: .table(TableRef("Parent")), right: .table(TableRef("Child"))))
        let shadowed = SelectQuery(projection: [.expression(.col("joined", "rightID"))],
            source: .aliased(cteJoin, name: "joined", columns: ["leftX", "rightID"]),
            subqueries: [seed, shadow])
        let shadowPlan = DatabaseFieldReadAuthorizationPlan.make(query: shadowed, schema: try makeSchema())
        #expect(shadowPlan.fieldsByEntity["Parent"] == nil)
        #expect(shadowPlan.fieldsByEntity["Child"] == ["id"])
        let inherited = SelectQuery(projection: [.wildcard()], source: .subquery(shadowed),
            subqueries: [seed])
        #expect(DatabaseFieldReadAuthorizationPlan.make(query: inherited, schema: try makeSchema())
            .fieldsByEntity == shadowPlan.fieldsByEntity)
    }

    @Test("USING and nested aliases retain both merged key origins without secret fields")
    func mergedJoinAliasOrigins() throws {
        let join = DataSource.join(JoinClause(type: .left,
            left: .table(TableRef("Parent")), right: .table(TableRef("Child")),
            condition: .using(["id"])))
        let source = DataSource.aliased(.aliased(join, name: "inner", columns: ["mergedID"]),
            name: "outer", columns: ["identity"])
        let query = SelectQuery(projection: [.expression(.col("outer", "identity"))], source: source)
        let plan = DatabaseFieldReadAuthorizationPlan.make(query: query, schema: try makeSchema())
        #expect(plan.fieldsByEntity["Parent"] == ["id"])
        #expect(plan.fieldsByEntity["Child"] == ["id"])
        #expect(plan.isCovered(by: ["Parent": ["id"], "Child": ["id"]]))
        #expect(!plan.fieldsByEntity["Child", default: []].contains("secret"))
    }

    @Test("Recursive self and forward references are logical while physical dependencies remain authorized")
    func recursiveLogicalAuthority() throws {
        let schema = try makeSchema()
        let seed = SelectQuery(projection: [.expression(.literal(.int(1)), alias: "n")], source: .values([[]]))
        let term = SelectQuery(projection: [.column("n")], source: .table(TableRef("Parent")))
        let recursive = NamedSubquery(name: "Parent", columns: ["n"],
            query: SelectQuery(projection: [.wildcard()], source: .setOperation(operator: .union,
                quantifier: .all, left: seed, right: term)),
            search: CTESearchClause(order: .depthFirst, columns: ["n"], sequenceColumn: "seq"),
            cycle: CTECycleClause(columns: ["n"], markColumn: "cyclic", pathColumn: "path"))
        let forward = NamedSubquery(name: "first", query: SelectQuery(projection: [.wildcard()], source: .table(TableRef("Parent"))))
        let query = SelectQuery(projection: [.wildcard()], source: .table(TableRef("first")),
            subqueries: [forward, recursive], withRecursive: true)
        #expect(DatabaseFieldReadAuthorizationPlan.make(query: query, schema: schema).fieldsByEntity.isEmpty)
        let physical = NamedSubquery(name: "physical", query: SelectQuery(projection: [.column("secret")],
            source: .table(TableRef("Child"))))
        let retained = query.replacing(subqueries: [forward, recursive, physical])
        #expect(DatabaseFieldReadAuthorizationPlan.make(query: retained, schema: schema).fieldsByEntity == ["Child": ["secret"]])
        let ordinary = query.replacing(withRecursive: false)
        #expect(DatabaseFieldReadAuthorizationPlan.make(query: ordinary, schema: schema).fieldsByEntity["Parent"] != nil)
    }

    @Test("Implicit lateral function arguments authorize preceding physical fields and nested reads")
    func tableFunctionArgumentAuthority() throws {
        let schema = try makeSchema()
        let inner = SelectQuery(projection: [.column("secret")], source: .table(TableRef("Child")))
        let function = DataSource.aliased(.tableFunction(TableFunctionSource(function: FunctionCall(name: "unnest",
            arguments: [.array([.col("p", "token"), .subquery(inner)])]), withOrdinality: true)), name: "f")
        let query = SelectQuery(projection: [.expression(.literal(.int(1)))], source: .join(JoinClause(type: .cross,
            left: .aliased(.table(TableRef("Parent")), name: "p"), right: function)))
        let plan = DatabaseFieldReadAuthorizationPlan.make(query: query, schema: schema)
        #expect(plan.fieldsByEntity["Parent"] == ["token"])
        #expect(plan.fieldsByEntity["Child"] == ["secret"])
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
