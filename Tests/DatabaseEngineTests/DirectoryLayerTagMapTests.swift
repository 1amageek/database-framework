import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import Testing

/// Derivation of the layer tag of every Directory node position from the
/// complete set of `#Directory` declarations.
@Suite("Directory layer tag derivation")
struct DirectoryLayerTagMapTests {

    // MARK: - Fixtures

    private static func field(
        _ name: String,
        _ number: Int,
        _ type: FieldSchemaType,
        referenceTargetEntity: String? = nil
    ) -> FieldSchema {
        FieldSchema(
            name: name,
            fieldNumber: number,
            type: type,
            referenceTargetEntity: referenceTargetEntity
        )
    }

    private static func entity(
        _ name: String,
        fields: [FieldSchema] = [],
        components: [DirectoryPathComponent],
        layer: DirectoryLayer = .default,
        enumMetadata: [String: [String]] = [:]
    ) throws -> Schema.Entity {
        try Schema.Entity(
            name: name,
            identifierType: .string,
            fields: fields,
            directoryComponents: components,
            directoryLayer: layer,
            enumMetadata: enumMetadata
        )
    }

    /// `users/{tenantID}` declared as a Partition leaf.
    private static func tenant() throws -> Schema.Entity {
        try entity(
            "Tenant",
            fields: [field("tenantID", 1, .string)],
            components: [.staticPath("users"), .dynamicField(fieldName: "tenantID")],
            layer: .partition
        )
    }

    /// `users/{tenantID}/orders` passing through the position above.
    private static func order() throws -> Schema.Entity {
        try entity(
            "Order",
            fields: [field("tenantID", 1, .string)],
            components: [
                .staticPath("users"),
                .dynamicField(fieldName: "tenantID"),
                .staticPath("orders"),
            ]
        )
    }

    // MARK: - Derivation

    @Test("A position resolved as a Partition leaf is a Partition for every declaration through it")
    func derivesPartitionForEveryDeclarationPassingThrough() throws {
        let map = try DirectoryLayerTagMap(entities: [try Self.tenant(), try Self.order()], polymorphicGroups: [])
        #expect(map.layers(forEntityNamed: "Tenant") == [.default, .partition])
        #expect(map.layers(forEntityNamed: "Order") == [.default, .partition, .default])
    }

    @Test("The derivation does not depend on the order declarations are supplied in")
    func derivationIsIndependentOfDeclarationOrder() throws {
        let forward = try DirectoryLayerTagMap(entities: [try Self.tenant(), try Self.order()], polymorphicGroups: [])
        let reverse = try DirectoryLayerTagMap(entities: [try Self.order(), try Self.tenant()], polymorphicGroups: [])
        #expect(forward.layers(forEntityNamed: "Order") == reverse.layers(forEntityNamed: "Order"))
        #expect(forward.layers(forEntityNamed: "Tenant") == reverse.layers(forEntityNamed: "Tenant"))
    }

    @Test("A position no declaration resolves as a leaf is a plain Directory")
    func positionWithoutALeafIsAPlainDirectory() throws {
        let map = try DirectoryLayerTagMap(entities: [try Self.order()], polymorphicGroups: [])
        #expect(map.layers(forEntityNamed: "Order") == [.default, .default, .default])
    }

    @Test("An entity outside the derived schema has no layers")
    func unknownEntityHasNoLayers() throws {
        let map = try DirectoryLayerTagMap(entities: [try Self.order()], polymorphicGroups: [])
        #expect(map.layers(forEntityNamed: "Tenant") == nil)
    }

    @Test("A declaration without components occupies no node position")
    func declarationWithoutComponentsOccupiesNoPosition() throws {
        let bare = try Self.entity("Bare", components: [])
        let map = try DirectoryLayerTagMap(entities: [bare], polymorphicGroups: [])
        #expect(map.layers(forEntityNamed: "Bare") == [])
    }

    // MARK: - Typed schema errors

    @Test("Two declarations that resolve one position as a leaf must assign it one layer")
    func rejectsLeafLayerDisagreement() throws {
        let alpha = try Self.entity(
            "Alpha",
            fields: [Self.field("id", 1, .string)],
            components: [.staticPath("t"), .dynamicField(fieldName: "id")]
        )
        let beta = try Self.entity(
            "Beta",
            fields: [Self.field("id", 1, .string)],
            components: [.staticPath("t"), .dynamicField(fieldName: "id")],
            layer: .partition
        )
        let expected = DirectoryLayerTagError.inconsistentLayer(
            position: "t/{string}",
            declaration: .entity("Beta"),
            layer: .partition,
            conflictingDeclaration: .entity("Alpha"),
            conflictingLayer: .default
        )
        #expect(throws: expected) { try DirectoryLayerTagMap(entities: [alpha, beta], polymorphicGroups: []) }
        #expect(throws: expected) { try DirectoryLayerTagMap(entities: [beta, alpha], polymorphicGroups: []) }
    }

    @Test("Two declarations that share a dynamic position must declare one field kind")
    func rejectsDynamicFieldKindDisagreement() throws {
        let alpha = try Self.entity(
            "Alpha",
            fields: [Self.field("id", 1, .string)],
            components: [.staticPath("t"), .dynamicField(fieldName: "id")]
        )
        let beta = try Self.entity(
            "Beta",
            fields: [Self.field("id", 1, .int64)],
            components: [.staticPath("t"), .dynamicField(fieldName: "id")]
        )
        let expected = DirectoryLayerTagError.inconsistentDynamicFieldKind(
            position: "t",
            entity: "Beta",
            kind: .int64,
            conflictingEntity: "Alpha",
            conflictingKind: .string
        )
        #expect(throws: expected) { try DirectoryLayerTagMap(entities: [alpha, beta], polymorphicGroups: []) }
        #expect(throws: expected) { try DirectoryLayerTagMap(entities: [beta, alpha], polymorphicGroups: []) }
    }

    /// A kind without a canonical component is refused where the declaration is
    /// written, so no such entity can be constructed and handed to derivation.
    /// The map keeps its own guard as a precondition on a `package` input it
    /// does not construct; a validated entity cannot trip it, so the contract
    /// under test here is the refusal itself.
    @Test("A field kind without a canonical component cannot name a node")
    func rejectsFieldKindWithoutACanonicalComponent() throws {
        #expect(
            throws: SchemaEntityError.unsupportedDirectoryFieldKind(
                fieldName: "point", type: .vector
            )
        ) {
            try Self.entity(
                "Embedding",
                fields: [Self.field("point", 1, .vector)],
                components: [.staticPath("v"), .dynamicField(fieldName: "point")]
            )
        }

        #expect(
            throws: SchemaEntityError.unsupportedDirectoryFieldKind(
                fieldName: "subject", type: .rdfTerm
            )
        ) {
            try Self.entity(
                "Statement",
                fields: [Self.field("subject", 1, .rdfTerm)],
                components: [.staticPath("g"), .dynamicField(fieldName: "subject")]
            )
        }

        #expect(
            throws: SchemaEntityError.unsupportedDirectoryFieldKind(
                fieldName: "owner", type: .reference
            )
        ) {
            try Self.entity(
                "Membership",
                fields: [Self.field("owner", 1, .reference, referenceTargetEntity: "Tenant")],
                components: [.staticPath("m"), .dynamicField(fieldName: "owner")]
            )
        }
    }

    @Test("A static component that is a canonical component image is rejected")
    func rejectsStaticComponentInTheCanonicalImage() throws {
        let shadowing = try Self.entity("Shadow", components: [.staticPath("s-abc")])
        #expect(
            throws: DirectoryLayerTagError.staticComponentInCanonicalImage(
                declaration: .entity("Shadow"), component: "s-abc"
            )
        ) { try DirectoryLayerTagMap(entities: [shadowing], polymorphicGroups: []) }

        // A literal that merely contains a hyphen is not a canonical component,
        // because its leading token is not a canonical tag.
        let ordinary = try Self.entity(
            "Ordinary",
            components: [.staticPath("users"), .staticPath("database-framework"), .staticPath("bases")]
        )
        let map = try DirectoryLayerTagMap(entities: [ordinary], polymorphicGroups: [])
        #expect(map.layers(forEntityNamed: "Ordinary") == [.default, .default, .default])
    }

    // MARK: - Polymorphic group declarations

    private static func group(
        _ identifier: String,
        components: [DirectoryPathComponent],
        layer: DirectoryLayer = .default
    ) -> PolymorphicGroup {
        PolymorphicGroup(
            identifier: identifier,
            directoryComponents: components,
            directoryLayer: layer
        )
    }

    @Test("A polymorphic group resolves its own leaf position")
    func derivesLayerForAGroupLeaf() throws {
        let shapes = Self.group(
            "Shape",
            components: [.staticPath("shapes")],
            layer: .partition
        )
        let map = try DirectoryLayerTagMap(entities: [], polymorphicGroups: [shapes])
        #expect(map.layers(forPath: ["shapes"]) == [.partition])
    }

    @Test("A group Partition leaf is a Partition for an entity declaration passing through it")
    func derivesGroupPartitionForEntityPassingThrough() throws {
        let shapes = Self.group(
            "Shape",
            components: [.staticPath("shapes")],
            layer: .partition
        )
        // The entity resolves a position below the group leaf, so without the
        // group declaration its first component would derive as a plain
        // Directory and the node would be created with the wrong layer.
        let circle = try Self.entity(
            "Circle",
            components: [.staticPath("shapes"), .staticPath("circles")]
        )
        let map = try DirectoryLayerTagMap(
            entities: [circle],
            polymorphicGroups: [shapes]
        )
        #expect(map.layers(forEntityNamed: "Circle") == [.partition, .default])
        #expect(map.layers(forPath: ["shapes", "circles"]) == [.partition, .default])
    }

    @Test("A group and an entity that resolve one position must assign it one layer")
    func rejectsGroupAndEntityLayerDisagreement() throws {
        let shapes = Self.group(
            "Shape",
            components: [.staticPath("shapes")],
            layer: .partition
        )
        let legacy = try Self.entity("Legacy", components: [.staticPath("shapes")])
        let expected = DirectoryLayerTagError.inconsistentLayer(
            position: "shapes",
            declaration: .polymorphicGroup("Shape"),
            layer: .partition,
            conflictingDeclaration: .entity("Legacy"),
            conflictingLayer: .default
        )
        #expect(throws: expected) {
            try DirectoryLayerTagMap(entities: [legacy], polymorphicGroups: [shapes])
        }
    }

    @Test("Two groups that resolve one position must assign it one layer")
    func rejectsGroupLayerDisagreement() throws {
        let alpha = Self.group("Alpha", components: [.staticPath("shared")], layer: .partition)
        let beta = Self.group("Beta", components: [.staticPath("shared")])
        let expected = DirectoryLayerTagError.inconsistentLayer(
            position: "shared",
            declaration: .polymorphicGroup("Beta"),
            layer: .default,
            conflictingDeclaration: .polymorphicGroup("Alpha"),
            conflictingLayer: .partition
        )
        #expect(throws: expected) {
            try DirectoryLayerTagMap(entities: [], polymorphicGroups: [alpha, beta])
        }
        #expect(throws: expected) {
            try DirectoryLayerTagMap(entities: [], polymorphicGroups: [beta, alpha])
        }
    }

    @Test("A group declaration carries no dynamic component")
    func rejectsDynamicComponentInAGroup() throws {
        let dynamic = Self.group(
            "Shape",
            components: [.staticPath("shapes"), .dynamicField(fieldName: "tenantID")]
        )
        #expect(
            throws: DirectoryLayerTagError.dynamicComponentInPolymorphicGroup(
                group: "Shape", fieldName: "tenantID"
            )
        ) {
            try DirectoryLayerTagMap(entities: [], polymorphicGroups: [dynamic])
        }
    }

    @Test("A group static component that is a canonical component image is rejected")
    func rejectsGroupStaticComponentInTheCanonicalImage() throws {
        let shadowing = Self.group("Shape", components: [.staticPath("s-abc")])
        #expect(
            throws: DirectoryLayerTagError.staticComponentInCanonicalImage(
                declaration: .polymorphicGroup("Shape"), component: "s-abc"
            )
        ) {
            try DirectoryLayerTagMap(entities: [], polymorphicGroups: [shadowing])
        }
    }

    // MARK: - Admitted kinds

    @Test("Two declarations may share an enum position across raw representations")
    func admitsSharedEnumPositionAcrossRawRepresentations() throws {
        let alpha = try Self.entity(
            "Alpha",
            fields: [Self.field("role", 1, .enum)],
            components: [.staticPath("t"), .dynamicField(fieldName: "role")],
            enumMetadata: ["role": ["admin", "member"]]
        )
        let beta = try Self.entity(
            "Beta",
            fields: [Self.field("level", 1, .enum)],
            components: [.staticPath("t"), .dynamicField(fieldName: "level")],
            enumMetadata: ["level": ["low", "high"]]
        )
        let map = try DirectoryLayerTagMap(entities: [alpha, beta], polymorphicGroups: [])
        #expect(map.layers(forEntityNamed: "Alpha") == [.default, .default])
        #expect(map.layers(forEntityNamed: "Beta") == [.default, .default])

        // The two raw representations an enum field can materialize produce
        // components under distinct tags, so sharing the position is safe.
        #expect(try DirectoryComponentCodec.encode(.string("admin")) == "s-admin")
        #expect(try DirectoryComponentCodec.encode(.int64(3)) == "i64-3")
    }

    @Test("A field kind is admitted exactly when it has a canonical component")
    func admittedKindsMatchTheComponentCodec() throws {
        let table = try Self.fieldKindRepresentatives()
        // Every case of FieldSchemaType is represented; the type is not
        // CaseIterable, so the count is asserted rather than derived.
        #expect(table.count == 29)
        for (kind, values) in table {
            let admitted = DirectoryLayerTagMap.admitsDynamicComponent(kind)
            for value in values {
                var encodes = true
                do {
                    _ = try DirectoryComponentCodec.encode(value)
                } catch {
                    encodes = false
                }
                #expect(
                    encodes == admitted,
                    "\(kind.rawValue): admitted=\(admitted) but encodable=\(encodes)"
                )
            }
        }
    }

    /// One materializable value per declared field kind. An enum field carries
    /// both representations it can materialize.
    private static func fieldKindRepresentatives() throws -> [(FieldSchemaType, [FieldValue])] {
        let date = try CivilDate(year: 2026, month: 8, day: 29)
        let time = try CivilTime(hour: 1, minute: 2, second: 3, nanoseconds: 4)
        let point = try GeographicPoint(latitude: 1, longitude: 2)
        return [
            (.bool, [.bool(true)]),
            (.int8, [.int8(1)]),
            (.int16, [.int16(1)]),
            (.int32, [.int32(1)]),
            (.int64, [.int64(1)]),
            (.uint8, [.uint8(1)]),
            (.uint16, [.uint16(1)]),
            (.uint32, [.uint32(1)]),
            (.uint64, [.uint64(1)]),
            (.float32, [.float32(1)]),
            (.float64, [.float64(1)]),
            (.decimal, [.decimal(ExactDecimal(coefficient: 1, scale: 0))]),
            (.string, [.string("a")]),
            (.bytes, [.bytes(ByteString(utf8: "a"))]),
            (.date, [.date(date)]),
            (.time, [.time(time)]),
            (.dateTime, [.dateTime(CivilDateTime(date: date, time: time))]),
            (.timestamp, [.timestamp(try Timestamp(secondsSinceUnixEpoch: 1, nanoseconds: 2))]),
            (.timeSpan, [.timeSpan(try TimeSpan(seconds: 1, nanoseconds: 2))]),
            (.calendarPeriod, [.calendarPeriod(CalendarPeriod(months: 1, days: 2))]),
            (.geographicPoint, [.geographicPoint(point)]),
            (
                .geographicPosition,
                [
                    .geographicPosition(
                        try GeographicPosition(point: point, ellipsoidalHeightInMeters: 0)
                    )
                ]
            ),
            (.vector, [.vector(Vector(int8: [1]))]),
            (.uuid, [.uuid(UUID(high: 1, low: 2))]),
            (.object, [.object(FieldObject())]),
            (.rdfTerm, [.rdfTerm(.iri(try RDFIRI("http://example.com/a")))]),
            (.reference, [.reference(try EntityReference(entity: "Tenant", id: .string("1")))]),
            (.nested, [.object(FieldObject())]),
            (.enum, [.string("admin"), .int64(3)]),
        ]
    }
}
