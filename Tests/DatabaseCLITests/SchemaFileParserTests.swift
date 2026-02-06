// SchemaFileParserTests.swift
// Tests for YAML schema parsing

import Testing
import Foundation
@testable import DatabaseCLICore
@testable import DatabaseEngine
import Core

@Suite("Schema File Parser Tests")
struct SchemaFileParserTests {

    @Test("Parse simple schema with scalar index")
    func testSimpleSchema() throws {
        let yaml = """
        User:
          "#Directory": [app, users]

          id: string
          name: string
          email: string#scalar(unique:true)
          age: int#scalar
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.name == "User")
        #expect(catalog.fields.count == 4)
        #expect(catalog.directoryComponents.count == 2)
        #expect(catalog.indexes.count == 2)

        // Check fields
        #expect(catalog.fields[0].name == "id")
        #expect(catalog.fields[0].type == .string)

        #expect(catalog.fields[1].name == "name")
        #expect(catalog.fields[2].name == "email")
        #expect(catalog.fields[3].name == "age")

        // Check directory
        if case .staticPath(let path) = catalog.directoryComponents[0] {
            #expect(path == "app")
        } else {
            Issue.record("Expected static path")
        }

        // Check indexes
        let emailIndex = catalog.indexes.first { $0.fieldNames == ["email"] }
        #expect(emailIndex != nil)
        #expect(emailIndex?.unique == true)

        let ageIndex = catalog.indexes.first { $0.fieldNames == ["age"] }
        #expect(ageIndex != nil)
        #expect(ageIndex?.unique == false)
    }

    @Test("Parse vector index")
    func testVectorIndex() throws {
        let yaml = """
        Product:
          "#Directory": [catalog, products]

          id: string
          embedding: array<float>#vector(dimensions:384, metric:cosine, algorithm:hnsw)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.fields.count == 2)
        #expect(catalog.indexes.count == 1)

        let vectorIndex = catalog.indexes[0]
        #expect(vectorIndex.kindIdentifier == "vector")
        #expect(vectorIndex.fieldNames == ["embedding"])
        #expect(vectorIndex.kind.metadata["dimensions"]?.intValue == 384)
        #expect(vectorIndex.kind.metadata["metric"]?.stringValue == "cosine")
        #expect(vectorIndex.kind.metadata["algorithm"]?.stringValue == "hnsw")
    }

    @Test("Parse graph index")
    func testGraphIndex() throws {
        let yaml = """
        Follow:
          "#Directory": [social, follows]

          id: string
          follower: string
          following: string

          "#Index":
            - kind: graph
              name: social_graph
              from: follower
              edge: follows
              to: following
              strategy: tripleStore
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)

        let graphIndex = catalog.indexes[0]
        #expect(graphIndex.kindIdentifier == "graph")
        #expect(graphIndex.kind.metadata["fromField"]?.stringValue == "follower")
        #expect(graphIndex.kind.metadata["edgeField"]?.stringValue == "follows")
        #expect(graphIndex.kind.metadata["toField"]?.stringValue == "following")
        #expect(graphIndex.kind.metadata["strategy"]?.stringValue == "tripleStore")
    }

    @Test("Parse dynamic directory")
    func testDynamicDirectory() throws {
        let yaml = """
        Order:
          "#Directory":
            - orders
            - field: tenantId

          id: string
          tenantId: string
          amount: double
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.directoryComponents.count == 2)

        if case .staticPath(let path) = catalog.directoryComponents[0] {
            #expect(path == "orders")
        } else {
            Issue.record("Expected static path")
        }

        if case .dynamicField(let fieldName) = catalog.directoryComponents[1] {
            #expect(fieldName == "tenantId")
        } else {
            Issue.record("Expected dynamic field")
        }

        #expect(catalog.hasDynamicDirectory == true)
        #expect(catalog.dynamicFieldNames == ["tenantId"])
    }

    @Test("Parse composite scalar index")
    func testCompositeIndex() throws {
        let yaml = """
        User:
          "#Directory": [app, users]

          id: string
          name: string
          age: int
          city: string

          "#Index":
            - kind: scalar
              name: name_age_idx
              fields: [name, age]
              unique: false
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let compositeIndex = catalog.indexes.first { $0.name == "name_age_idx" }
        #expect(compositeIndex != nil)
        #expect(compositeIndex?.fieldNames == ["name", "age"])
        #expect(compositeIndex?.unique == false)
    }

    @Test("Parse permuted index")
    func testPermutedIndex() throws {
        let yaml = """
        Product:
          "#Directory": [catalog, products]

          id: string
          category: string
          brand: string
          price: double

          "#Index":
            - kind: permuted
              name: multi_field_idx
              fields: [category, brand, price]
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let permutedIndex = catalog.indexes.first { $0.kindIdentifier == "permuted" }
        #expect(permutedIndex != nil)
        #expect(permutedIndex?.fieldNames == ["category", "brand", "price"])
    }

    @Test("Parse optional and array types")
    func testComplexTypes() throws {
        let yaml = """
        User:
          "#Directory": [app, users]

          id: string
          nickname: optional<string>
          tags: array<string>
          scores: array<double>
          optionalTags: optional<array<string>>
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.fields.count == 5)

        // Check optional
        #expect(catalog.fields[1].isOptional == true)
        #expect(catalog.fields[1].type == .string)

        // Check array
        #expect(catalog.fields[2].isArray == true)
        #expect(catalog.fields[2].type == .string)

        // Check nested optional<array<string>>
        #expect(catalog.fields[4].isOptional == true)
        #expect(catalog.fields[4].isArray == true)
        #expect(catalog.fields[4].type == .string)
    }

    @Test("Parse fulltext index")
    func testFullTextIndex() throws {
        let yaml = """
        Article:
          "#Directory": [content, articles]

          id: string
          title: string#fulltext(language:english, tokenizer:standard)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let fulltextIndex = catalog.indexes.first { $0.kindIdentifier == "fulltext" }
        #expect(fulltextIndex != nil)
        #expect(fulltextIndex?.kind.metadata["language"]?.stringValue == "english")
        #expect(fulltextIndex?.kind.metadata["tokenizer"]?.stringValue == "standard")
    }

    @Test("Parse spatial index")
    func testSpatialIndex() throws {
        let yaml = """
        Store:
          "#Directory": [locations, stores]

          id: string
          location: string#spatial(strategy:geohash)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let spatialIndex = catalog.indexes.first { $0.kindIdentifier == "spatial" }
        #expect(spatialIndex != nil)
        #expect(spatialIndex?.kind.metadata["strategy"]?.stringValue == "geohash")
    }

    @Test("Parse leaderboard index")
    func testLeaderboardIndex() throws {
        let yaml = """
        Player:
          "#Directory": [game, players]

          id: string
          score: int#leaderboard(name:global_ranking)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let leaderboardIndex = catalog.indexes.first { $0.kindIdentifier == "leaderboard" }
        #expect(leaderboardIndex != nil)
        #expect(leaderboardIndex?.kind.metadata["leaderboardName"]?.stringValue == "global_ranking")
    }

    @Test("Parse aggregation index")
    func testAggregationIndex() throws {
        let yaml = """
        Order:
          "#Directory": [ecommerce, orders]

          id: string
          amount: double#aggregation(functions:sum,avg,min,max)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let aggregationIndex = catalog.indexes.first { $0.kindIdentifier == "aggregation" }
        #expect(aggregationIndex != nil)
        #expect(aggregationIndex?.kind.metadata["functions"]?.stringValue == "sum,avg,min,max")
    }

    @Test("Parse relationship index")
    func testRelationshipIndex() throws {
        let yaml = """
        UserGroup:
          "#Directory": [app, user_groups]

          id: string
          userId: string
          groupId: string

          "#Index":
            - kind: relationship
              name: user_group_rel
              from: userId
              to: groupId
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let relationshipIndex = catalog.indexes.first { $0.kindIdentifier == "relationship" }
        #expect(relationshipIndex != nil)
        #expect(relationshipIndex?.kind.metadata["from"]?.stringValue == "userId")
        #expect(relationshipIndex?.kind.metadata["to"]?.stringValue == "groupId")
    }

    @Test("Error on invalid type")
    func testInvalidType() throws {
        let yaml = """
        User:
          "#Directory": [app, users]

          id: invalidtype
        """

        #expect(throws: SchemaFileError.self) {
            try SchemaFileParser.parseYAML(yaml)
        }
    }

    @Test("Error on invalid index kind")
    func testInvalidIndexKind() throws {
        let yaml = """
        User:
          "#Directory": [app, users]

          id: string#nonexistent
        """

        #expect(throws: SchemaFileError.self) {
            try SchemaFileParser.parseYAML(yaml)
        }
    }

    @Test("Error on missing vector dimensions")
    func testMissingVectorDimensions() throws {
        let yaml = """
        Product:
          "#Directory": [catalog, products]

          id: string
          embedding: array<float>#vector(metric:cosine)
        """

        #expect(throws: SchemaFileError.self) {
            try SchemaFileParser.parseYAML(yaml)
        }
    }

    @Test("Error on incomplete graph index")
    func testIncompleteGraphIndex() throws {
        let yaml = """
        Follow:
          "#Directory": [social, follows]

          id: string
          follower: string

          "#Index":
            - kind: graph
              from: follower
        """

        #expect(throws: SchemaFileError.self) {
            try SchemaFileParser.parseYAML(yaml)
        }
    }
}
