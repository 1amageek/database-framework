import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Request-owned RDFS closure storage.
///
/// Every retained collection owns the reservation that admitted its container,
/// capacity, and semantic payload before allocation. The public entailment is
/// a Copyable facade over this owner, so copies cannot outlive the work claim.
final class RDFSGraphEntailmentStorage: Sendable {
    let ontologyContext: OntologyContext
    let classSuperClosure: RetainedSetMap<String, String>
    let classSubClosure: RetainedSetMap<String, String>
    let propertySubClosure: RetainedSetMap<String, String>
    let instancesByClass: RetainedSetMap<String, RDFTerm>
    let typesByNode: RetainedSetMap<RDFTerm, String>

    private let ownerReservation: DatabaseIntermediateReservation

    private init(
        ontologyContext: OntologyContext,
        classSuperClosure: consuming RetainedSetMap<String, String>,
        classSubClosure: consuming RetainedSetMap<String, String>,
        propertySubClosure: consuming RetainedSetMap<String, String>,
        instancesByClass: consuming RetainedSetMap<String, RDFTerm>,
        typesByNode: consuming RetainedSetMap<RDFTerm, String>,
        ownerReservation: DatabaseIntermediateReservation
    ) {
        let workMeter = ownerReservation.workMeter
        precondition(classSuperClosure.workMeter === workMeter)
        precondition(classSubClosure.workMeter === workMeter)
        precondition(propertySubClosure.workMeter === workMeter)
        precondition(instancesByClass.workMeter === workMeter)
        precondition(typesByNode.workMeter === workMeter)
        self.ontologyContext = ontologyContext
        self.classSuperClosure = classSuperClosure
        self.classSubClosure = classSubClosure
        self.propertySubClosure = propertySubClosure
        self.instancesByClass = instancesByClass
        self.typesByNode = typesByNode
        self.ownerReservation = ownerReservation
    }

    struct Builder {
        private var directClassSupers: RetainedSetMap<String, String>
        private var directPropertySupers: RetainedSetMap<String, String>
        private var domains: RetainedSetMap<String, String>
        private var ranges: RetainedSetMap<String, String>
        private var directTypes: RetainedSetMap<RDFTerm, String>
        private var assertions: RetainedArray<Assertion>
        private let workMeter: DatabaseWorkMeter

        init(workMeter: DatabaseWorkMeter) throws {
            self.workMeter = workMeter
            self.directClassSupers = try RetainedSetMap.make(
                workMeter: workMeter
            )
            self.directPropertySupers = try RetainedSetMap.make(
                workMeter: workMeter
            )
            self.domains = try RetainedSetMap.make(workMeter: workMeter)
            self.ranges = try RetainedSetMap.make(workMeter: workMeter)
            self.directTypes = try RetainedSetMap.make(
                workMeter: workMeter
            )
            self.assertions = try RetainedArray.make(workMeter: workMeter)
        }

        mutating func ingest(
            _ quad: borrowing RDFQuad,
            budget: SHACLValidationWorkBudget
        ) throws {
            precondition(
                budget.workMeter === workMeter,
                "RDFS input and destination must use the same work meter"
            )
            try budget.consume(at: .validation)
            let subject = quad.subject.term
            let predicate = quad.predicate.rawValue
            switch predicate {
            case Vocabulary.rdfType:
                if case .iri(let classIRI) = quad.object {
                    try directTypes.insert(
                        classIRI.rawValue,
                        for: subject,
                        keyFootprint: try rdfTermFootprint(subject),
                        valueFootprint: try stringFootprint(
                            classIRI.rawValue
                        )
                    )
                }
            case Vocabulary.subClassOf:
                if case .iri(let subClass) = subject,
                   case .iri(let superClass) = quad.object {
                    try directClassSupers.insert(
                        superClass.rawValue,
                        for: subClass.rawValue,
                        keyFootprint: try stringFootprint(
                            subClass.rawValue
                        ),
                        valueFootprint: try stringFootprint(
                            superClass.rawValue
                        )
                    )
                }
            case Vocabulary.subPropertyOf:
                if case .iri(let subProperty) = subject,
                   case .iri(let superProperty) = quad.object {
                    try directPropertySupers.insert(
                        superProperty.rawValue,
                        for: subProperty.rawValue,
                        keyFootprint: try stringFootprint(
                            subProperty.rawValue
                        ),
                        valueFootprint: try stringFootprint(
                            superProperty.rawValue
                        )
                    )
                }
            case Vocabulary.domain:
                if case .iri(let property) = subject,
                   case .iri(let domain) = quad.object {
                    try domains.insert(
                        domain.rawValue,
                        for: property.rawValue,
                        keyFootprint: try stringFootprint(
                            property.rawValue
                        ),
                        valueFootprint: try stringFootprint(domain.rawValue)
                    )
                }
            case Vocabulary.range:
                if case .iri(let property) = subject,
                   case .iri(let range) = quad.object {
                    try ranges.insert(
                        range.rawValue,
                        for: property.rawValue,
                        keyFootprint: try stringFootprint(
                            property.rawValue
                        ),
                        valueFootprint: try stringFootprint(range.rawValue)
                    )
                }
            default:
                let assertion = Assertion(
                    subject: subject,
                    predicate: predicate,
                    object: quad.object
                )
                try assertions.append(
                    assertion,
                    footprint: try assertionFootprint(assertion)
                )
            }
        }

        consuming func finish(
            budget: SHACLValidationWorkBudget
        ) throws -> RDFSGraphEntailmentStorage {
            precondition(
                budget.workMeter === workMeter,
                "RDFS closure and destination must use the same work meter"
            )
            let classSuperClosure = try transitiveClosure(
                directClassSupers,
                budget: budget
            )
            let propertySuperClosure = try transitiveClosure(
                directPropertySupers,
                budget: budget
            )
            let classSubClosure = try inverted(
                classSuperClosure,
                workMeter: workMeter
            )
            let propertySubClosure = try inverted(
                propertySuperClosure,
                workMeter: workMeter
            )

            var entailedTypes = try RetainedSetMap<RDFTerm, String>.make(
                workMeter: workMeter
            )
            try directTypes.forEach { node, types in
                for type in types {
                    try entailedTypes.insert(
                        type,
                        for: node,
                        keyFootprint: try rdfTermFootprint(node),
                        valueFootprint: try stringFootprint(type)
                    )
                }
            }
            try assertions.forEach { assertion in
                try budget.consume(at: .validation)
                var entailedProperties = try RetainedSet<String>.make(
                    workMeter: workMeter
                )
                _ = try entailedProperties.insert(
                    assertion.predicate,
                    footprint: try stringFootprint(assertion.predicate)
                )
                for property in propertySuperClosure.values(
                    for: assertion.predicate
                ) {
                    _ = try entailedProperties.insert(
                        property,
                        footprint: try stringFootprint(property)
                    )
                }
                try entailedProperties.forEach { property in
                    for domain in domains.values(for: property) {
                        try entailedTypes.insert(
                            domain,
                            for: assertion.subject,
                            keyFootprint: try rdfTermFootprint(
                                assertion.subject
                            ),
                            valueFootprint: try stringFootprint(domain)
                        )
                    }
                    if assertion.object.isRDFSubject {
                        for range in ranges.values(for: property) {
                            try entailedTypes.insert(
                                range,
                                for: assertion.object,
                                keyFootprint: try rdfTermFootprint(
                                    assertion.object
                                ),
                                valueFootprint: try stringFootprint(range)
                            )
                        }
                    }
                }
            }

            var closedTypes = try RetainedSetMap<RDFTerm, String>.make(
                workMeter: workMeter
            )
            try entailedTypes.forEach { node, direct in
                try budget.consume(at: .validation)
                for type in direct {
                    try closedTypes.insert(
                        type,
                        for: node,
                        keyFootprint: try rdfTermFootprint(node),
                        valueFootprint: try stringFootprint(type)
                    )
                    for superClass in classSuperClosure.values(for: type) {
                        try closedTypes.insert(
                            superClass,
                            for: node,
                            keyFootprint: try rdfTermFootprint(node),
                            valueFootprint: try stringFootprint(superClass)
                        )
                    }
                }
            }

            var instancesByClass = try RetainedSetMap<String, RDFTerm>.make(
                workMeter: workMeter
            )
            try closedTypes.forEach { node, types in
                for type in types {
                    try budget.consume(at: .deduplication)
                    try instancesByClass.insert(
                        node,
                        for: type,
                        keyFootprint: try stringFootprint(type),
                        valueFootprint: try rdfTermFootprint(node)
                    )
                }
            }

            var propertyScans = try RetainedArrayMap<
                String,
                OntologyEntailedPropertyScan
            >.make(workMeter: workMeter)
            try propertySuperClosure.forEachKey { property in
                try budget.consume(at: .validation)
                let direct = OntologyEntailedPropertyScan(
                    predicateIRI: property,
                    isInverse: false
                )
                try propertyScans.append(
                    direct,
                    for: property,
                    keyFootprint: try stringFootprint(property),
                    valueFootprint: try propertyScanFootprint(direct)
                )
                for subProperty in propertySubClosure.values(for: property) {
                    let scan = OntologyEntailedPropertyScan(
                        predicateIRI: subProperty,
                        isInverse: false
                    )
                    try propertyScans.append(
                        scan,
                        for: property,
                        keyFootprint: try stringFootprint(property),
                        valueFootprint: try propertyScanFootprint(scan)
                    )
                }
                try propertyScans.sortValues(for: property) { lhs, rhs in
                    try budget.consume(2, at: .sortComparison)
                    return lhs.predicateIRI < rhs.predicateIRI
                }
            }

            let ontologyContext = OntologyContext(
                rdfsPropertySubClosure: propertySubClosure.rawStorage,
                entailedPropertyScans: propertyScans.rawStorage,
                propertySubClosureReservation:
                    propertySubClosure.reservation,
                entailedPropertyScansReservation: propertyScans.reservation
            )
            let ownerReservation = try workMeter.reserveIntermediate(
                bytes: 64,
                at: .validation
            )
            return RDFSGraphEntailmentStorage(
                ontologyContext: ontologyContext,
                classSuperClosure: classSuperClosure,
                classSubClosure: classSubClosure,
                propertySubClosure: propertySubClosure,
                instancesByClass: instancesByClass,
                typesByNode: closedTypes,
                ownerReservation: ownerReservation
            )
        }
    }
}

private extension RDFSGraphEntailmentStorage {
    struct Assertion: Sendable {
        let subject: RDFTerm
        let predicate: String
        let object: RDFTerm
    }

    enum Vocabulary {
        static let rdfType =
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        static let subClassOf =
            "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        static let subPropertyOf =
            "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
        static let domain =
            "http://www.w3.org/2000/01/rdf-schema#domain"
        static let range =
            "http://www.w3.org/2000/01/rdf-schema#range"
    }

    static func transitiveClosure(
        _ direct: RetainedSetMap<String, String>,
        budget: SHACLValidationWorkBudget
    ) throws -> RetainedSetMap<String, String> {
        let workMeter = direct.workMeter
        precondition(workMeter === budget.workMeter)
        var universe = try RetainedSet<String>.make(workMeter: workMeter)
        try direct.forEach { source, targets in
            _ = try universe.insert(
                source,
                footprint: try stringFootprint(source)
            )
            for target in targets {
                _ = try universe.insert(
                    target,
                    footprint: try stringFootprint(target)
                )
            }
        }

        var result = try RetainedSetMap<String, String>.make(
            workMeter: workMeter
        )
        try universe.forEach { origin in
            try result.ensureKey(
                origin,
                keyFootprint: try stringFootprint(origin)
            )
            var visited = try RetainedSet<String>.make(workMeter: workMeter)
            var queue = try RetainedArray<String>.make(workMeter: workMeter)
            for target in direct.values(for: origin) {
                try queue.append(
                    target,
                    footprint: try stringFootprint(target)
                )
            }
            var cursor = 0
            while cursor < queue.count {
                try budget.consume(at: .validation)
                let current = queue[cursor]
                cursor += 1
                guard try visited.insert(
                    current,
                    footprint: try stringFootprint(current)
                ) else { continue }
                for target in direct.values(for: current) {
                    try queue.append(
                        target,
                        footprint: try stringFootprint(target)
                    )
                }
            }
            try visited.forEach { target in
                guard target != origin else { return }
                try result.insert(
                    target,
                    for: origin,
                    keyFootprint: try stringFootprint(origin),
                    valueFootprint: try stringFootprint(target)
                )
            }
        }
        return result
    }

    static func inverted(
        _ closure: RetainedSetMap<String, String>,
        workMeter: DatabaseWorkMeter
    ) throws -> RetainedSetMap<String, String> {
        precondition(closure.workMeter === workMeter)
        var result = try RetainedSetMap<String, String>.make(
            workMeter: workMeter
        )
        try closure.forEach { source, targets in
            for target in targets {
                try result.insert(
                    source,
                    for: target,
                    keyFootprint: try stringFootprint(target),
                    valueFootprint: try stringFootprint(source)
                )
            }
        }
        return result
    }

    static func stringFootprint(
        _ value: String
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(bytes: 16).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(value.utf8.count)
            )
        )
    }

    static func rdfTermFootprint(
        _ value: borrowing RDFTerm
    ) throws -> DatabaseIntermediateFootprint {
        try RDFTermRetainedFootprint.measure(value)
    }

    static func assertionFootprint(
        _ assertion: borrowing Assertion
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(bytes: 32)
            .adding(rdfTermFootprint(assertion.subject))
            .adding(stringFootprint(assertion.predicate))
            .adding(rdfTermFootprint(assertion.object))
    }

    static func propertyScanFootprint(
        _ scan: OntologyEntailedPropertyScan
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(bytes: 16).adding(
            stringFootprint(scan.predicateIRI)
        )
    }
}

extension RDFSGraphEntailmentStorage {
    struct RetainedSetMap<
        Key: Hashable & Sendable,
        Element: Hashable & Sendable
    >: Sendable {
        private var storage: [Key: Set<Element>]
        private var innerCapacities: [Key: Int]
        private var accountedMapCapacity: Int
        private let storageLayout: DatabaseRetainedHashTableLayout
        private let capacityLayout: DatabaseRetainedHashTableLayout
        private let setLayout: DatabaseRetainedHashTableLayout
        let reservation: DatabaseIntermediateReservation
        let workMeter: DatabaseWorkMeter
        private let stage: DatabaseWorkStage

        static func make(
            workMeter: DatabaseWorkMeter,
            stage: DatabaseWorkStage = .validation
        ) throws -> Self {
            let storageLayout = try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: UInt64(
                    max(1, MemoryLayout<[Key: Set<Element>]>.stride)
                ) + 32,
                elementCapacitySlotByteCount: UInt64(
                    max(
                        1,
                        MemoryLayout<Key>.stride
                            + MemoryLayout<Set<Element>>.stride
                    )
                )
            )
            let capacityLayout = try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: UInt64(
                    max(1, MemoryLayout<[Key: Int]>.stride)
                ) + 32,
                elementCapacitySlotByteCount: UInt64(
                    max(
                        1,
                        MemoryLayout<Key>.stride + MemoryLayout<Int>.stride
                    )
                )
            )
            let setLayout = try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: UInt64(
                    max(1, MemoryLayout<Set<Element>>.stride)
                ) + 32,
                elementCapacitySlotByteCount: UInt64(
                    max(1, MemoryLayout<Element>.stride)
                )
            )
            let reservation = try workMeter.reserveIntermediate(
                bytes: try checkedAdd(
                    storageLayout.containerByteCount,
                    capacityLayout.containerByteCount
                ),
                at: stage
            )
            return Self(
                storage: [:],
                innerCapacities: [:],
                accountedMapCapacity: 0,
                storageLayout: storageLayout,
                capacityLayout: capacityLayout,
                setLayout: setLayout,
                reservation: reservation,
                workMeter: workMeter,
                stage: stage
            )
        }

        mutating func insert(
            _ element: Element,
            for key: Key,
            keyFootprint: DatabaseIntermediateFootprint,
            valueFootprint: DatabaseIntermediateFootprint
        ) throws {
            guard storage[key]?.contains(element) != true else { return }
            if storage[key] == nil {
                let requiredCount = try checkedIncrement(storage.count)
                let storageGrowth = try storageLayout.growth(
                    from: accountedMapCapacity,
                    toFit: requiredCount
                )
                let capacityGrowth = try capacityLayout.growth(
                    from: accountedMapCapacity,
                    toFit: requiredCount
                )
                let setGrowth = try setLayout.growth(
                    from: 0,
                    toFit: 1
                )
                let admitted = try keyFootprint
                    .adding(keyFootprint)
                    .adding(valueFootprint)
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: storageGrowth.additionalByteCount
                        )
                    )
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: capacityGrowth.additionalByteCount
                        )
                    )
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: setLayout.containerByteCount
                        )
                    )
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: setGrowth.additionalByteCount
                        )
                    )
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: admitted.bytes,
                    at: stage
                )
                if storageGrowth.capacity != accountedMapCapacity {
                    storage.reserveCapacity(storageGrowth.capacity)
                    innerCapacities.reserveCapacity(capacityGrowth.capacity)
                    accountedMapCapacity = storageGrowth.capacity
                }
                var values = Set<Element>()
                values.reserveCapacity(setGrowth.capacity)
                let insertion = values.insert(element)
                precondition(insertion.inserted)
                storage[key] = values
                innerCapacities[key] = setGrowth.capacity
                return
            }

            let currentCapacity = innerCapacities[key]!
            let requiredCount = try checkedIncrement(storage[key]!.count)
            let growth = try setLayout.growth(
                from: currentCapacity,
                toFit: requiredCount
            )
            let admitted = try valueFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                )
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: admitted.bytes,
                at: stage
            )
            if growth.capacity != currentCapacity {
                storage[key]!.reserveCapacity(growth.capacity)
                innerCapacities[key] = growth.capacity
            }
            let insertion = storage[key]!.insert(element)
            precondition(insertion.inserted)
        }

        mutating func ensureKey(
            _ key: Key,
            keyFootprint: DatabaseIntermediateFootprint
        ) throws {
            guard storage[key] == nil else { return }
            let requiredCount = try checkedIncrement(storage.count)
            let storageGrowth = try storageLayout.growth(
                from: accountedMapCapacity,
                toFit: requiredCount
            )
            let capacityGrowth = try capacityLayout.growth(
                from: accountedMapCapacity,
                toFit: requiredCount
            )
            let admitted = try keyFootprint
                .adding(keyFootprint)
                .adding(
                    DatabaseIntermediateFootprint(
                        bytes: storageGrowth.additionalByteCount
                    )
                )
                .adding(
                    DatabaseIntermediateFootprint(
                        bytes: capacityGrowth.additionalByteCount
                    )
                )
                .adding(
                    DatabaseIntermediateFootprint(
                        bytes: setLayout.containerByteCount
                    )
                )
            try reservation.reserveAdditional(
                bytes: admitted.bytes,
                at: stage
            )
            if storageGrowth.capacity != accountedMapCapacity {
                storage.reserveCapacity(storageGrowth.capacity)
                innerCapacities.reserveCapacity(capacityGrowth.capacity)
                accountedMapCapacity = storageGrowth.capacity
            }
            storage[key] = []
            innerCapacities[key] = 0
        }

        func values(for key: Key) -> Set<Element> {
            storage[key] ?? []
        }

        func contains(_ element: Element, for key: Key) -> Bool {
            storage[key]?.contains(element) == true
        }

        func forEach(
            _ body: (Key, Set<Element>) throws -> Void
        ) rethrows {
            for (key, values) in storage {
                try body(key, values)
            }
        }

        func forEachKey(_ body: (Key) throws -> Void) rethrows {
            for key in storage.keys {
                try body(key)
            }
        }

        var rawStorage: [Key: Set<Element>] { storage }
    }

    struct RetainedSet<Element: Hashable & Sendable>: Sendable {
        private var storage: Set<Element>
        private var accountedCapacity: Int
        private let layout: DatabaseRetainedHashTableLayout
        private let reservation: DatabaseIntermediateReservation
        private let stage: DatabaseWorkStage

        static func make(
            workMeter: DatabaseWorkMeter,
            stage: DatabaseWorkStage = .validation
        ) throws -> Self {
            let layout = try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: UInt64(
                    max(1, MemoryLayout<Set<Element>>.stride)
                ) + 32,
                elementCapacitySlotByteCount: UInt64(
                    max(1, MemoryLayout<Element>.stride)
                )
            )
            return Self(
                storage: [],
                accountedCapacity: 0,
                layout: layout,
                reservation: try workMeter.reserveIntermediate(
                    bytes: layout.containerByteCount,
                    at: stage
                ),
                stage: stage
            )
        }

        mutating func insert(
            _ element: Element,
            footprint: DatabaseIntermediateFootprint
        ) throws -> Bool {
            guard !storage.contains(element) else { return false }
            let requiredCount = try checkedIncrement(storage.count)
            let growth = try layout.growth(
                from: accountedCapacity,
                toFit: requiredCount
            )
            let admitted = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                )
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: admitted.bytes,
                at: stage
            )
            if growth.capacity != accountedCapacity {
                storage.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
            }
            let insertion = storage.insert(element)
            precondition(insertion.inserted)
            return true
        }

        func forEach(_ body: (Element) throws -> Void) rethrows {
            for element in storage {
                try body(element)
            }
        }
    }

    struct RetainedArray<Element: Sendable>: Sendable {
        private var storage: [Element]
        private var accountedCapacity: Int
        private let layout: DatabaseRetainedArrayLayout
        private let reservation: DatabaseIntermediateReservation
        private let stage: DatabaseWorkStage

        static func make(
            workMeter: DatabaseWorkMeter,
            stage: DatabaseWorkStage = .validation
        ) throws -> Self {
            let layout = try DatabaseRetainedArrayLayout.forElement(
                Element.self
            )
            let baseBytes = try checkedAdd(
                layout.containerByteCount,
                layout.sharedOwnerByteCount
            )
            return Self(
                storage: [],
                accountedCapacity: 0,
                layout: layout,
                reservation: try workMeter.reserveIntermediate(
                    bytes: baseBytes,
                    at: stage
                ),
                stage: stage
            )
        }

        mutating func append(
            _ element: Element,
            footprint: DatabaseIntermediateFootprint
        ) throws {
            let requiredCount = try checkedIncrement(storage.count)
            let growth = try layout.growth(
                from: accountedCapacity,
                toFit: requiredCount
            )
            let admitted = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                )
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: admitted.bytes,
                at: stage
            )
            if growth.capacity != accountedCapacity {
                storage.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
            }
            storage.append(element)
        }

        var count: Int { storage.count }

        subscript(index: Int) -> Element { storage[index] }

        func forEach(_ body: (Element) throws -> Void) rethrows {
            for element in storage {
                try body(element)
            }
        }
    }

    struct RetainedArrayMap<
        Key: Hashable & Sendable,
        Element: Sendable
    >: Sendable {
        private var storage: [Key: [Element]]
        private var innerCapacities: [Key: Int]
        private var accountedMapCapacity: Int
        private let storageLayout: DatabaseRetainedHashTableLayout
        private let capacityLayout: DatabaseRetainedHashTableLayout
        private let arrayLayout: DatabaseRetainedArrayLayout
        let reservation: DatabaseIntermediateReservation
        private let stage: DatabaseWorkStage

        static func make(
            workMeter: DatabaseWorkMeter,
            stage: DatabaseWorkStage = .validation
        ) throws -> Self {
            let storageLayout = try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: UInt64(
                    max(1, MemoryLayout<[Key: [Element]]>.stride)
                ) + 32,
                elementCapacitySlotByteCount: UInt64(
                    max(
                        1,
                        MemoryLayout<Key>.stride
                            + MemoryLayout<[Element]>.stride
                    )
                )
            )
            let capacityLayout = try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: UInt64(
                    max(1, MemoryLayout<[Key: Int]>.stride)
                ) + 32,
                elementCapacitySlotByteCount: UInt64(
                    max(
                        1,
                        MemoryLayout<Key>.stride + MemoryLayout<Int>.stride
                    )
                )
            )
            let arrayLayout = try DatabaseRetainedArrayLayout.forElement(
                Element.self
            )
            let reservation = try workMeter.reserveIntermediate(
                bytes: try checkedAdd(
                    storageLayout.containerByteCount,
                    capacityLayout.containerByteCount
                ),
                at: stage
            )
            return Self(
                storage: [:],
                innerCapacities: [:],
                accountedMapCapacity: 0,
                storageLayout: storageLayout,
                capacityLayout: capacityLayout,
                arrayLayout: arrayLayout,
                reservation: reservation,
                stage: stage
            )
        }

        mutating func append(
            _ element: Element,
            for key: Key,
            keyFootprint: DatabaseIntermediateFootprint,
            valueFootprint: DatabaseIntermediateFootprint
        ) throws {
            if storage[key] == nil {
                let requiredCount = try checkedIncrement(storage.count)
                let storageGrowth = try storageLayout.growth(
                    from: accountedMapCapacity,
                    toFit: requiredCount
                )
                let capacityGrowth = try capacityLayout.growth(
                    from: accountedMapCapacity,
                    toFit: requiredCount
                )
                let arrayGrowth = try arrayLayout.growth(
                    from: 0,
                    toFit: 1
                )
                let admitted = try keyFootprint
                    .adding(keyFootprint)
                    .adding(valueFootprint)
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: storageGrowth.additionalByteCount
                        )
                    )
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: capacityGrowth.additionalByteCount
                        )
                    )
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: try checkedAdd(
                                arrayLayout.containerByteCount,
                                arrayLayout.sharedOwnerByteCount
                            )
                        )
                    )
                    .adding(
                        DatabaseIntermediateFootprint(
                            bytes: arrayGrowth.additionalByteCount
                        )
                    )
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: admitted.bytes,
                    at: stage
                )
                if storageGrowth.capacity != accountedMapCapacity {
                    storage.reserveCapacity(storageGrowth.capacity)
                    innerCapacities.reserveCapacity(capacityGrowth.capacity)
                    accountedMapCapacity = storageGrowth.capacity
                }
                var values: [Element] = []
                values.reserveCapacity(arrayGrowth.capacity)
                values.append(element)
                storage[key] = values
                innerCapacities[key] = arrayGrowth.capacity
                return
            }

            let currentCapacity = innerCapacities[key]!
            let requiredCount = try checkedIncrement(storage[key]!.count)
            let growth = try arrayLayout.growth(
                from: currentCapacity,
                toFit: requiredCount
            )
            let admitted = try valueFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                )
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: admitted.bytes,
                at: stage
            )
            if growth.capacity != currentCapacity {
                storage[key]!.reserveCapacity(growth.capacity)
                innerCapacities[key] = growth.capacity
            }
            storage[key]!.append(element)
        }

        mutating func sortValues(
            for key: Key,
            by areInIncreasingOrder: (Element, Element) throws -> Bool
        ) throws {
            guard storage[key]!.count > 1 else { return }
            try Self.heapSort(
                &storage[key]!,
                by: areInIncreasingOrder
            )
        }

        var rawStorage: [Key: [Element]] { storage }

        private static func heapSort(
            _ values: inout [Element],
            by areInIncreasingOrder: (Element, Element) throws -> Bool
        ) throws {
            var start = values.count / 2
            while start > 0 {
                start -= 1
                try siftDown(
                    &values,
                    from: start,
                    through: values.count - 1,
                    by: areInIncreasingOrder
                )
            }
            var end = values.count - 1
            while end > 0 {
                values.swapAt(0, end)
                end -= 1
                try siftDown(
                    &values,
                    from: 0,
                    through: end,
                    by: areInIncreasingOrder
                )
            }
        }

        private static func siftDown(
            _ values: inout [Element],
            from start: Int,
            through end: Int,
            by areInIncreasingOrder: (Element, Element) throws -> Bool
        ) throws {
            var root = start
            while true {
                let left = root * 2 + 1
                guard left <= end else { return }
                var selected = left
                let right = left + 1
                if right <= end,
                   try areInIncreasingOrder(
                       values[selected],
                       values[right]
                   ) {
                    selected = right
                }
                guard try areInIncreasingOrder(
                    values[root],
                    values[selected]
                ) else { return }
                values.swapAt(root, selected)
                root = selected
            }
        }
    }

    static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseRetainedHashTableLayoutError.capacityOverflow(
                currentCapacity: value
            )
        }
        return result
    }

    static func checkedAdd(_ left: UInt64, _ right: UInt64) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: left).adding(
            DatabaseIntermediateFootprint(bytes: right)
        ).bytes
    }
}

private extension RDFTerm {
    var isRDFSubject: Bool {
        switch self {
        case .iri, .blankNode:
            return true
        case .literal, .tripleTerm:
            return false
        }
    }
}
