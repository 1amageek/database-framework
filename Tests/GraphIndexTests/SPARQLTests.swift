// SPARQLTests.swift
// Unit and integration tests for SPARQL-like query functionality

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import QueryIR
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - ExecutionTerm Tests

@Suite("ExecutionTerm Tests")
struct ExecutionTermTests {

    @Test("String literal to value term")
    func testValueTerm() {
        let term: ExecutionTerm = "Alice"
        #expect(term == .value("Alice"))
        #expect(!term.isVariable)
        #expect(!term.isWildcard)
    }

    @Test("String starting with ? becomes variable")
    func testVariableTerm() {
        let term: ExecutionTerm = "?person"
        #expect(term == .variable("?person"))
        #expect(term.isVariable)
        #expect(!term.isWildcard)
    }

    @Test("Wildcard term")
    func testWildcardTerm() {
        let term: ExecutionTerm = .wildcard
        #expect(term.isWildcard)
        #expect(!term.isVariable)
    }

    @Test("Variable name extraction")
    func testVariableName() {
        let variable: ExecutionTerm = "?name"
        let value: ExecutionTerm = "Alice"
        let wildcard: ExecutionTerm = .wildcard

        #expect(variable.variableName == "?name")
        #expect(value.variableName == nil)
        #expect(wildcard.variableName == nil)
    }

    @Test("Substitute variable with value")
    func testSubstitute() {
        let variable: ExecutionTerm = "?person"
        let value: ExecutionTerm = "Alice"

        var binding = VariableBinding()
        binding = binding.binding("?person", to: "Bob")

        let substitutedVar = variable.substitute(binding)
        let substitutedVal = value.substitute(binding)

        #expect(substitutedVar == .value("Bob"))
        #expect(substitutedVal == .value("Alice"))
    }
}

// MARK: - ExecutionTriple Tests

@Suite("ExecutionTriple Tests")
struct ExecutionTripleTests {

    @Test("Create triple pattern from strings")
    func testCreateFromStrings() {
        let pattern = ExecutionTriple("?person", "knows", "Alice")

        #expect(pattern.subject == .variable("?person"))
        #expect(pattern.predicate == .value("knows"))
        #expect(pattern.object == .value("Alice"))
    }

    @Test("Variables extraction")
    func testVariablesExtraction() {
        let pattern = ExecutionTriple("?person", "?relation", "Alice")

        let variables = pattern.variables
        #expect(variables.count == 2)
        #expect(variables.contains("?person"))
        #expect(variables.contains("?relation"))
    }

    @Test("Selectivity score calculation")
    func testSelectivityScore() {
        let allBound = ExecutionTriple("Alice", "knows", "Bob")
        let twoBound = ExecutionTriple("Alice", "knows", "?friend")
        let oneBound = ExecutionTriple("?person", "knows", "?friend")
        let noneBound = ExecutionTriple("?s", "?p", "?o")

        #expect(allBound.selectivityScore > twoBound.selectivityScore)
        #expect(twoBound.selectivityScore > oneBound.selectivityScore)
        #expect(oneBound.selectivityScore > noneBound.selectivityScore)
    }

    @Test("Substitute variables in pattern")
    func testSubstitute() {
        let pattern = ExecutionTriple("?person", "knows", "?friend")

        var binding = VariableBinding()
        binding = binding.binding("?person", to: "Alice")

        let substituted = pattern.substitute(binding)

        #expect(substituted.subject == .value("Alice"))
        #expect(substituted.predicate == .value("knows"))
        #expect(substituted.object == .variable("?friend"))
    }
}

// MARK: - VariableBinding Tests

@Suite("VariableBinding Tests")
struct VariableBindingTests {

    @Test("Empty binding")
    func testEmptyBinding() {
        let binding = VariableBinding()
        #expect(binding.isEmpty)
        #expect(binding.count == 0)
        #expect(binding["?x"] == nil)
    }

    @Test("Single binding")
    func testSingleBinding() {
        var binding = VariableBinding()
        binding = binding.binding("?name", to: "Alice")

        #expect(!binding.isEmpty)
        #expect(binding.count == 1)
        #expect(binding["?name"] == "Alice")
        #expect(binding.isBound("?name"))
    }

    @Test("Multiple bindings")
    func testMultipleBindings() {
        var binding = VariableBinding()
        binding = binding.binding("?name", to: "Alice")
        binding = binding.binding("?age", to: "30")

        #expect(binding.count == 2)
        #expect(binding["?name"] == "Alice")
        #expect(binding["?age"] == "30")
    }

    @Test("Merge compatible bindings")
    func testMergeCompatible() {
        var b1 = VariableBinding()
        b1 = b1.binding("?name", to: "Alice")

        var b2 = VariableBinding()
        b2 = b2.binding("?age", to: "30")

        let merged = b1.merged(with: b2)
        #expect(merged != nil)
        #expect(merged?["?name"] == "Alice")
        #expect(merged?["?age"] == "30")
    }

    @Test("Merge overlapping compatible bindings")
    func testMergeOverlappingCompatible() {
        var b1 = VariableBinding()
        b1 = b1.binding("?name", to: "Alice")
        b1 = b1.binding("?age", to: "30")

        var b2 = VariableBinding()
        b2 = b2.binding("?name", to: "Alice")
        b2 = b2.binding("?city", to: "NYC")

        let merged = b1.merged(with: b2)
        #expect(merged != nil)
        #expect(merged?["?name"] == "Alice")
        #expect(merged?["?age"] == "30")
        #expect(merged?["?city"] == "NYC")
    }

    @Test("Merge conflicting bindings returns nil")
    func testMergeConflicting() {
        var b1 = VariableBinding()
        b1 = b1.binding("?name", to: "Alice")

        var b2 = VariableBinding()
        b2 = b2.binding("?name", to: "Bob")

        let merged = b1.merged(with: b2)
        #expect(merged == nil)
    }

    @Test("Project binding to subset of variables")
    func testProject() {
        var binding = VariableBinding()
        binding = binding.binding("?name", to: "Alice")
        binding = binding.binding("?age", to: "30")
        binding = binding.binding("?city", to: "NYC")

        let projected = binding.project(Set(["?name", "?city"]))

        #expect(projected.count == 2)
        #expect(projected["?name"] == "Alice")
        #expect(projected["?city"] == "NYC")
        #expect(projected["?age"] == nil)
    }

    @Test("Is compatible check")
    func testIsCompatible() {
        var b1 = VariableBinding()
        b1 = b1.binding("?name", to: "Alice")

        var compatible = VariableBinding()
        compatible = compatible.binding("?name", to: "Alice")

        var incompatible = VariableBinding()
        incompatible = incompatible.binding("?name", to: "Bob")

        var disjoint = VariableBinding()
        disjoint = disjoint.binding("?other", to: "xyz")

        #expect(b1.isCompatible(with: compatible))
        #expect(!b1.isCompatible(with: incompatible))
        #expect(b1.isCompatible(with: disjoint))
    }

    @Test("Int accessor")
    func testIntAccessor() {
        var binding = VariableBinding()
        binding = binding.binding("?age", to: "30")
        binding = binding.binding("?name", to: "Alice")

        #expect(binding.int("?age") == 30)
        #expect(binding.int("?name") == nil)
        #expect(binding.int("?missing") == nil)
    }

    @Test("Double accessor")
    func testDoubleAccessor() {
        var binding = VariableBinding()
        binding = binding.binding("?score", to: "3.14")
        binding = binding.binding("?name", to: "Alice")

        #expect(binding.double("?score") == 3.14)
        #expect(binding.double("?name") == nil)
    }
}

// MARK: - ExecutionPattern Tests

@Suite("ExecutionPattern Tests")
struct ExecutionPatternTests {

    @Test("Basic pattern variables")
    func testBasicPatternVariables() {
        let pattern: ExecutionPattern = .basic([
            ExecutionTriple("?person", "knows", "Alice"),
            ExecutionTriple("?person", "name", "?name")
        ])

        let variables = pattern.variables
        #expect(variables.count == 2)
        #expect(variables.contains("?person"))
        #expect(variables.contains("?name"))
    }

    @Test("Join pattern variables")
    func testJoinPatternVariables() {
        let left: ExecutionPattern = .basic([ExecutionTriple("?x", "knows", "Alice")])
        let right: ExecutionPattern = .basic([ExecutionTriple("?x", "age", "?age")])
        let join: ExecutionPattern = .join(left, right)

        let variables = join.variables
        #expect(variables.count == 2)
        #expect(variables.contains("?x"))
        #expect(variables.contains("?age"))
    }

    @Test("Optional pattern required variables")
    func testOptionalRequiredVariables() {
        let left: ExecutionPattern = .basic([ExecutionTriple("?person", "type", "User")])
        let right: ExecutionPattern = .basic([ExecutionTriple("?person", "email", "?email")])
        let optional: ExecutionPattern = .optional(left, right)

        let required = optional.requiredVariables
        let all = optional.variables
        let optionalVars = optional.optionalVariables

        #expect(required.contains("?person"))
        #expect(!required.contains("?email"))
        #expect(all.contains("?email"))
        #expect(optionalVars.contains("?email"))
    }

    @Test("Union pattern required variables")
    func testUnionRequiredVariables() {
        let left: ExecutionPattern = .basic([ExecutionTriple("?x", "type", "A"), ExecutionTriple("?x", "name", "?name")])
        let right: ExecutionPattern = .basic([ExecutionTriple("?x", "type", "B"), ExecutionTriple("?x", "age", "?age")])
        let union: ExecutionPattern = .union(left, right)

        let required = union.requiredVariables

        // Only ?x is required in both branches
        #expect(required.contains("?x"))
        #expect(!required.contains("?name"))
        #expect(!required.contains("?age"))
    }

    @Test("Empty pattern check")
    func testEmptyPattern() {
        let empty: ExecutionPattern = .basic([])
        let nonEmpty: ExecutionPattern = .basic([ExecutionTriple("?x", "y", "z")])

        #expect(empty.isEmpty)
        #expect(!nonEmpty.isEmpty)
    }

    @Test("Pattern count")
    func testPatternCount() {
        let single: ExecutionPattern = .basic([ExecutionTriple("a", "b", "c")])
        let double: ExecutionPattern = .basic([
            ExecutionTriple("a", "b", "c"),
            ExecutionTriple("d", "e", "f")
        ])

        #expect(single.patternCount == 1)
        #expect(double.patternCount == 2)
    }
}

// MARK: - FilterExpression Tests

@Suite("FilterExpression Tests")
struct FilterExpressionTests {

    @Test("Equals filter")
    func testEquals() {
        let filter = FilterExpression.equals("?name", "Alice")

        var binding = VariableBinding()
        binding = binding.binding("?name", to: "Alice")

        var otherBinding = VariableBinding()
        otherBinding = otherBinding.binding("?name", to: "Bob")

        #expect(filter.evaluate(binding))
        #expect(!filter.evaluate(otherBinding))
    }

    @Test("Not equals filter")
    func testNotEquals() {
        let filter = FilterExpression.notEquals("?name", "Alice")

        var aliceBinding = VariableBinding()
        aliceBinding = aliceBinding.binding("?name", to: "Alice")

        var bobBinding = VariableBinding()
        bobBinding = bobBinding.binding("?name", to: "Bob")

        // Unbound variable should return false (SPARQL semantics)
        let unboundBinding = VariableBinding()

        #expect(!filter.evaluate(aliceBinding))
        #expect(filter.evaluate(bobBinding))
        #expect(!filter.evaluate(unboundBinding))  // SPARQL: unbound → false
    }

    @Test("Variable equals filter")
    func testVariableEquals() {
        let filter = FilterExpression.variableEquals("?x", "?y")

        var sameBinding = VariableBinding()
        sameBinding = sameBinding.binding("?x", to: "Alice")
        sameBinding = sameBinding.binding("?y", to: "Alice")

        var diffBinding = VariableBinding()
        diffBinding = diffBinding.binding("?x", to: "Alice")
        diffBinding = diffBinding.binding("?y", to: "Bob")

        #expect(filter.evaluate(sameBinding))
        #expect(!filter.evaluate(diffBinding))
    }

    @Test("Variable not equals filter")
    func testVariableNotEquals() {
        let filter = FilterExpression.variableNotEquals("?x", "?y")

        var sameBinding = VariableBinding()
        sameBinding = sameBinding.binding("?x", to: "Alice")
        sameBinding = sameBinding.binding("?y", to: "Alice")

        var diffBinding = VariableBinding()
        diffBinding = diffBinding.binding("?x", to: "Alice")
        diffBinding = diffBinding.binding("?y", to: "Bob")

        #expect(!filter.evaluate(sameBinding))
        #expect(filter.evaluate(diffBinding))
    }

    @Test("Bound filter")
    func testBound() {
        let filter = FilterExpression.bound("?email")

        var withEmail = VariableBinding()
        withEmail = withEmail.binding("?email", to: "alice@example.com")

        let withoutEmail = VariableBinding()

        #expect(filter.evaluate(withEmail))
        #expect(!filter.evaluate(withoutEmail))
    }

    @Test("Not bound filter")
    func testNotBound() {
        let filter = FilterExpression.notBound("?email")

        var withEmail = VariableBinding()
        withEmail = withEmail.binding("?email", to: "alice@example.com")

        let withoutEmail = VariableBinding()

        #expect(!filter.evaluate(withEmail))
        #expect(filter.evaluate(withoutEmail))
    }

    @Test("Contains filter")
    func testContains() {
        let filter = FilterExpression.contains("?name", "li")

        var aliceBinding = VariableBinding()
        aliceBinding = aliceBinding.binding("?name", to: "Alice")

        var bobBinding = VariableBinding()
        bobBinding = bobBinding.binding("?name", to: "Bob")

        #expect(filter.evaluate(aliceBinding))
        #expect(!filter.evaluate(bobBinding))
    }

    @Test("Starts with filter")
    func testStartsWith() {
        let filter = FilterExpression.startsWith("?name", "Al")

        var aliceBinding = VariableBinding()
        aliceBinding = aliceBinding.binding("?name", to: "Alice")

        var bobBinding = VariableBinding()
        bobBinding = bobBinding.binding("?name", to: "Bob")

        #expect(filter.evaluate(aliceBinding))
        #expect(!filter.evaluate(bobBinding))
    }

    @Test("Ends with filter")
    func testEndsWith() {
        let filter = FilterExpression.endsWith("?name", "ce")

        var aliceBinding = VariableBinding()
        aliceBinding = aliceBinding.binding("?name", to: "Alice")

        var bobBinding = VariableBinding()
        bobBinding = bobBinding.binding("?name", to: "Bob")

        #expect(filter.evaluate(aliceBinding))
        #expect(!filter.evaluate(bobBinding))
    }

    @Test("Regex filter")
    func testRegex() {
        let filter = FilterExpression.regex("?name", "^[A-Z]")

        var capitalBinding = VariableBinding()
        capitalBinding = capitalBinding.binding("?name", to: "Alice")

        var lowerBinding = VariableBinding()
        lowerBinding = lowerBinding.binding("?name", to: "alice")

        #expect(filter.evaluate(capitalBinding))
        #expect(!filter.evaluate(lowerBinding))
    }

    @Test("Case insensitive regex filter")
    func testRegexCaseInsensitive() {
        let filter = FilterExpression.regexWithFlags("?name", "alice", "i")

        var upperBinding = VariableBinding()
        upperBinding = upperBinding.binding("?name", to: "Alice")

        var lowerBinding = VariableBinding()
        lowerBinding = lowerBinding.binding("?name", to: "alice")

        #expect(filter.evaluate(upperBinding))
        #expect(filter.evaluate(lowerBinding))
    }

    @Test("AND filter")
    func testAnd() {
        let filter = FilterExpression.and(
            .bound("?name"),
            .equals("?status", "active")
        )

        var validBinding = VariableBinding()
        validBinding = validBinding.binding("?name", to: "Alice")
        validBinding = validBinding.binding("?status", to: "active")

        var invalidBinding = VariableBinding()
        invalidBinding = invalidBinding.binding("?name", to: "Alice")
        invalidBinding = invalidBinding.binding("?status", to: "inactive")

        #expect(filter.evaluate(validBinding))
        #expect(!filter.evaluate(invalidBinding))
    }

    @Test("OR filter")
    func testOr() {
        let filter = FilterExpression.or(
            .equals("?status", "active"),
            .equals("?status", "pending")
        )

        var activeBinding = VariableBinding()
        activeBinding = activeBinding.binding("?status", to: "active")

        var pendingBinding = VariableBinding()
        pendingBinding = pendingBinding.binding("?status", to: "pending")

        var inactiveBinding = VariableBinding()
        inactiveBinding = inactiveBinding.binding("?status", to: "inactive")

        #expect(filter.evaluate(activeBinding))
        #expect(filter.evaluate(pendingBinding))
        #expect(!filter.evaluate(inactiveBinding))
    }

    @Test("NOT filter")
    func testNot() {
        let filter = FilterExpression.not(.equals("?status", "deleted"))

        var activeBinding = VariableBinding()
        activeBinding = activeBinding.binding("?status", to: "active")

        var deletedBinding = VariableBinding()
        deletedBinding = deletedBinding.binding("?status", to: "deleted")

        #expect(filter.evaluate(activeBinding))
        #expect(!filter.evaluate(deletedBinding))
    }

    @Test("Numeric filter")
    func testNumeric() {
        let ageFilter = FilterExpression.numeric("?age", ">=", 18)

        var adultBinding = VariableBinding()
        adultBinding = adultBinding.binding("?age", to: .int64(25))

        var minorBinding = VariableBinding()
        minorBinding = minorBinding.binding("?age", to: .int64(15))

        #expect(ageFilter.evaluate(adultBinding))
        #expect(!ageFilter.evaluate(minorBinding))
    }

    @Test("allOf convenience constructor")
    func testAllOf() {
        let filter = FilterExpression.allOf([
            .bound("?name"),
            .bound("?age"),
            .equals("?status", "active")
        ])

        var validBinding = VariableBinding()
        validBinding = validBinding.binding("?name", to: "Alice")
        validBinding = validBinding.binding("?age", to: "30")
        validBinding = validBinding.binding("?status", to: "active")

        var invalidBinding = VariableBinding()
        invalidBinding = invalidBinding.binding("?name", to: "Alice")
        invalidBinding = invalidBinding.binding("?status", to: "active")
        // missing ?age

        #expect(filter.evaluate(validBinding))
        #expect(!filter.evaluate(invalidBinding))
    }

    @Test("anyOf convenience constructor")
    func testAnyOf() {
        let filter = FilterExpression.anyOf([
            .equals("?type", "admin"),
            .equals("?type", "moderator")
        ])

        var adminBinding = VariableBinding()
        adminBinding = adminBinding.binding("?type", to: "admin")

        var userBinding = VariableBinding()
        userBinding = userBinding.binding("?type", to: "user")

        #expect(filter.evaluate(adminBinding))
        #expect(!filter.evaluate(userBinding))
    }

    @Test("Comparison filters")
    func testComparisons() {
        // Note: These are STRING comparisons (lexicographical), not numeric
        var binding = VariableBinding()
        binding = binding.binding("?score", to: "50")

        // String comparison: "50" < "60" (true), "50" < "100" (false - lexicographical)
        #expect(FilterExpression.lessThan("?score", "60").evaluate(binding))
        #expect(FilterExpression.lessThanOrEqual("?score", "50").evaluate(binding))
        #expect(FilterExpression.greaterThan("?score", "40").evaluate(binding))
        #expect(FilterExpression.greaterThanOrEqual("?score", "50").evaluate(binding))
    }

    @Test("Variables property")
    func testVariables() {
        let simpleFilter = FilterExpression.equals("?name", "Alice")
        #expect(simpleFilter.variables == Set(["?name"]))

        let varFilter = FilterExpression.variableEquals("?x", "?y")
        #expect(varFilter.variables == Set(["?x", "?y"]))

        let compoundFilter = FilterExpression.and(
            .equals("?a", "1"),
            .or(.equals("?b", "2"), .equals("?c", "3"))
        )
        #expect(compoundFilter.variables == Set(["?a", "?b", "?c"]))
    }

    @Test("Always true/false")
    func testAlwaysTrueFalse() {
        let binding = VariableBinding()

        #expect(FilterExpression.alwaysTrue.evaluate(binding))
        #expect(!FilterExpression.alwaysFalse.evaluate(binding))
    }

    @Test("Custom filter")
    func testCustomFilter() {
        let filter = FilterExpression.custom { binding in
            guard let age = binding.int("?age") else { return false }
            return age >= 21 && age <= 65
        }

        var validBinding = VariableBinding()
        validBinding = validBinding.binding("?age", to: "30")

        var tooYoung = VariableBinding()
        tooYoung = tooYoung.binding("?age", to: "18")

        var tooOld = VariableBinding()
        tooOld = tooOld.binding("?age", to: "70")

        #expect(filter.evaluate(validBinding))
        #expect(!filter.evaluate(tooYoung))
        #expect(!filter.evaluate(tooOld))
    }
}

// MARK: - SPARQLResult Tests

@Suite("SPARQLResult Tests")
struct SPARQLResultTests {

    @Test("Empty result")
    func testEmptyResult() {
        let result = SPARQLResult(
            bindings: [],
            projectedVariables: ["?x"]
        )

        #expect(result.isEmpty)
        #expect(result.count == 0)
        #expect(result.first == nil)
    }

    @Test("Result with bindings")
    func testResultWithBindings() {
        var b1 = VariableBinding()
        b1 = b1.binding("?name", to: "Alice")

        var b2 = VariableBinding()
        b2 = b2.binding("?name", to: "Bob")

        let result = SPARQLResult(
            bindings: [b1, b2],
            projectedVariables: ["?name"]
        )

        #expect(!result.isEmpty)
        #expect(result.count == 2)
        #expect(result.first?["?name"] == "Alice")
    }

    @Test("Values for variable")
    func testValuesForVariable() {
        var b1 = VariableBinding()
        b1 = b1.binding("?name", to: "Alice")
        b1 = b1.binding("?age", to: "30")

        var b2 = VariableBinding()
        b2 = b2.binding("?name", to: "Bob")
        // no age

        let result = SPARQLResult(
            bindings: [b1, b2],
            projectedVariables: ["?name", "?age"]
        )

        let names = result.nonNilValues(for: "?name")
        let ages = result.values(for: "?age")

        #expect(names == [FieldValue.string("Alice"), FieldValue.string("Bob")])
        #expect(ages.count == 2)
        #expect(ages[0] == FieldValue.string("30"))
        #expect(ages[1] == nil)
    }

    @Test("Distinct values")
    func testDistinctValues() {
        var b1 = VariableBinding()
        b1 = b1.binding("?city", to: "NYC")

        var b2 = VariableBinding()
        b2 = b2.binding("?city", to: "LA")

        var b3 = VariableBinding()
        b3 = b3.binding("?city", to: "NYC")

        let result = SPARQLResult(
            bindings: [b1, b2, b3],
            projectedVariables: ["?city"]
        )

        let distinct = result.distinctValues(for: "?city")
        #expect(distinct.count == 2)
        #expect(distinct.contains("NYC"))
        #expect(distinct.contains("LA"))
    }

    @Test("Filter result")
    func testFilterResult() {
        var b1 = VariableBinding()
        b1 = b1.binding("?age", to: "25")

        var b2 = VariableBinding()
        b2 = b2.binding("?age", to: "15")

        var b3 = VariableBinding()
        b3 = b3.binding("?age", to: "30")

        let result = SPARQLResult(
            bindings: [b1, b2, b3],
            projectedVariables: ["?age"]
        )

        let filtered = result.filter { binding in
            guard let age = binding.int("?age") else { return false }
            return age >= 18
        }

        #expect(filtered.count == 2)
    }

    @Test("Prefix result")
    func testPrefixResult() {
        var bindings: [VariableBinding] = []
        for i in 1...10 {
            var b = VariableBinding()
            b = b.binding("?i", toString: "\(i)")
            bindings.append(b)
        }

        let result = SPARQLResult(
            bindings: bindings,
            projectedVariables: ["?i"]
        )

        let prefixed = result.prefix(3)
        #expect(prefixed.count == 3)
        #expect(!prefixed.isComplete)
    }

    @Test("Has values for variable")
    func testHasValues() {
        var b1 = VariableBinding()
        b1 = b1.binding("?name", to: "Alice")

        let result = SPARQLResult(
            bindings: [b1],
            projectedVariables: ["?name", "?age"]
        )

        #expect(result.hasValues(for: "?name"))
        #expect(!result.hasValues(for: "?age"))
    }

    @Test("Sequence conformance")
    func testSequence() {
        var b1 = VariableBinding()
        b1 = b1.binding("?x", to: "1")

        var b2 = VariableBinding()
        b2 = b2.binding("?x", to: "2")

        let result = SPARQLResult(
            bindings: [b1, b2],
            projectedVariables: ["?x"]
        )

        var values: [String] = []
        for binding in result {
            if let v = binding.string("?x") {
                values.append(v)
            }
        }

        #expect(values == ["1", "2"])
    }
}

// MARK: - SPARQLQueryBuilder Unit Tests

@Suite("SPARQLQueryBuilder Unit Tests")
struct SPARQLQueryBuilderUnitTests {

    @Test("Query description")
    func testQueryDescription() {
        // This test verifies the description property without FDB
        let pattern: ExecutionPattern = .basic([
            ExecutionTriple("Alice", "knows", "?friend")
        ])

        #expect(!pattern.isEmpty)
        #expect(pattern.variables.contains("?friend"))
    }

    @Test("Graph pattern construction")
    func testExecutionPatternConstruction() {
        let p1 = ExecutionTriple("Alice", "knows", "?friend")
        let p2 = ExecutionTriple("?friend", "name", "?name")

        let basic: ExecutionPattern = .basic([p1, p2])
        #expect(basic.patternCount == 2)

        let variables = basic.variables
        #expect(variables.contains("?friend"))
        #expect(variables.contains("?name"))
    }

    @Test("Filter pattern construction")
    func testFilterPatternConstruction() {
        let basic: ExecutionPattern = .basic([ExecutionTriple("?x", "age", "?age")])
        let filtered: ExecutionPattern = .filter(basic, .numeric("?age", ">=", 18))

        #expect(!filtered.isEmpty)
        #expect(filtered.variables.contains("?x"))
        #expect(filtered.variables.contains("?age"))
    }
}

// MARK: - ExpressionEvaluator Variable Resolution Tests

@Suite("ExpressionEvaluator Variable Key Resolution")
struct ExpressionEvaluatorVariableTests {

    /// VariableBinding keys use "?"-prefixed names.
    /// QueryIR.Variable.name strips the "?" prefix.
    /// ExpressionEvaluator must bridge this gap.
    private func makeBinding(_ pairs: [(String, String)]) -> VariableBinding {
        var binding = VariableBinding()
        for (key, value) in pairs {
            binding = binding.binding(key, to: .string(value))
        }
        return binding
    }

    @Test("Variable lookup resolves with ?-prefixed binding key")
    func variableLookup() {
        let binding = makeBinding([("?name", "Alice")])
        // QueryIR.Variable("name") has .name == "name" (no ?)
        let expr = QueryIR.Expression.variable(QueryIR.Variable("name"))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .string("Alice"))
    }

    @Test("Variable lookup when Variable already has ? prefix")
    func variableLookupWithPrefix() {
        let binding = makeBinding([("?age", "30")])
        let expr = QueryIR.Expression.variable(QueryIR.Variable("?age"))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .string("30"))
    }

    @Test("CONTAINS function with variable resolves correctly")
    func containsFilter() {
        let binding = makeBinding([("?name", "Google")])
        let expr = QueryIR.Expression.function(QueryIR.FunctionCall(
            name: "CONTAINS",
            arguments: [
                .variable(QueryIR.Variable("name")),
                .literal(.string("oo"))
            ],
            distinct: false
        ))
        let result = ExpressionEvaluator.evaluateAsBoolean(expr, binding: binding)
        #expect(result == true)
    }

    @Test("CONTAINS function returns false when no match")
    func containsFilterNoMatch() {
        let binding = makeBinding([("?name", "Apple")])
        let expr = QueryIR.Expression.function(QueryIR.FunctionCall(
            name: "CONTAINS",
            arguments: [
                .variable(QueryIR.Variable("name")),
                .literal(.string("oo"))
            ],
            distinct: false
        ))
        let result = ExpressionEvaluator.evaluateAsBoolean(expr, binding: binding)
        #expect(result == false)
    }

    @Test("LCASE nested in CONTAINS resolves variable")
    func lcaseContainsNested() {
        let binding = makeBinding([("?name", "OpenAI")])
        // CONTAINS(LCASE(?name), "ai")
        let expr = QueryIR.Expression.function(QueryIR.FunctionCall(
            name: "CONTAINS",
            arguments: [
                .function(QueryIR.FunctionCall(
                    name: "LCASE",
                    arguments: [.variable(QueryIR.Variable("name"))],
                    distinct: false
                )),
                .literal(.string("ai"))
            ],
            distinct: false
        ))
        let result = ExpressionEvaluator.evaluateAsBoolean(expr, binding: binding)
        #expect(result == true)
    }

    @Test("Equality comparison resolves variable")
    func equalityFilter() {
        let binding = makeBinding([("?name", "Toyota")])
        let expr = QueryIR.Expression.equal(
            .variable(QueryIR.Variable("name")),
            .literal(.string("Toyota"))
        )
        let result = ExpressionEvaluator.evaluateAsBoolean(expr, binding: binding)
        #expect(result == true)
    }

    @Test("BOUND check resolves variable")
    func boundCheck() {
        let binding = makeBinding([("?name", "Alice")])
        let expr = QueryIR.Expression.bound(QueryIR.Variable("name"))
        let result = ExpressionEvaluator.evaluateAsBoolean(expr, binding: binding)
        #expect(result == true)
    }

    @Test("BOUND check returns false for unbound variable")
    func boundCheckUnbound() {
        let binding = makeBinding([("?name", "Alice")])
        let expr = QueryIR.Expression.bound(QueryIR.Variable("email"))
        let result = ExpressionEvaluator.evaluateAsBoolean(expr, binding: binding)
        #expect(result == false)
    }

    @Test("BOUND function form resolves variable")
    func boundFunctionForm() {
        let binding = makeBinding([("?x", "value")])
        let expr = QueryIR.Expression.function(QueryIR.FunctionCall(
            name: "BOUND",
            arguments: [.variable(QueryIR.Variable("x"))],
            distinct: false
        ))
        let result = ExpressionEvaluator.evaluateAsBoolean(expr, binding: binding)
        #expect(result == true)
    }
}
