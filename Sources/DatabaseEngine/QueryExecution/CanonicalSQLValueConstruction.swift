import DatabaseKit

/// A task-local postorder plan followed by one flat selected destination.
enum CanonicalSQLValueConstruction {
    private enum Shape {
        case owned([QueryArrayDimension])
        case view(QueryArrayDimensions)
        var count: Int { switch self { case .owned(let x): x.count; case .view(let x): x.count } }
        subscript(_ index: Int) -> QueryArrayDimension {
            switch self { case .owned(let x): x[index]; case .view(let x): x[index] }
        }
    }
    private enum Node {
        case leaf(QueryValue)
        case array(DataType, Shape, Range<Int>)
        case composite(QueryCompositeType, Range<Int>)
    }
    private enum Visit {
        case value(QueryValue, QueryColumnType?, QueryColumnType)
        case array(DataType, Shape, Int)
        case composite(QueryCompositeType, Int)
    }
    /// Confined to this synchronous construction; no shared mutable state escapes.
    private final class Memory {
        let meter: DatabaseWorkMeter
        let reservation: DatabaseIntermediateReservation
        init(_ meter: DatabaseWorkMeter) throws {
            self.meter = meter
            reservation = try meter.reserveIntermediate(at: .expressionEvaluation)
        }
        deinit { reservation.release() }
        func bytes(_ bytes: UInt64) throws {
            try reservation.reserveAdditional(bytes: bytes, at: .expressionEvaluation)
        }
        func prepare<T>(_ values: inout [T], adding count: Int = 1) throws {
            let next = values.count.addingReportingOverflow(count)
            guard count >= 0, !next.overflow else { throw QueryValueError.arithmeticOverflow }
            guard next.partialValue > values.capacity else { return }
            let layout = try DatabaseRetainedArrayLayout.forElement(T.self)
            let growth = try layout.growth(from: values.capacity, toFit: next.partialValue)
            let header = values.capacity == 0 ? layout.containerByteCount : 0
            try bytes(try DatabaseIntermediateFootprint(bytes: header).adding(.init(bytes: growth.additionalByteCount)).bytes)
            values.reserveCapacity(growth.capacity)
            if values.capacity > growth.capacity {
                try bytes(try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                    .multiplied(by: UInt64(values.capacity - growth.capacity)).bytes)
            }
        }
        func append<T>(_ value: T, to values: inout [T]) throws {
            try meter.consume(at: .expressionEvaluation)
            try prepare(&values)
            values.append(value)
        }
    }

    static func build(type: QueryColumnType, values: [QueryValue], dimensions: [QueryArrayDimension] = [],
        sourceTypes: [QueryColumnType]? = nil, sourceElementType: QueryColumnType? = nil,
        context: CanonicalQueryTypeBinder.CastContext = .implicit,
        temporalEnvironment: SQLTemporalEnvironment? = nil, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let memory = try Memory(workMeter)
        var visits: [Visit] = [], nodes: [Node] = [], completed: [Int] = [], edges: [Int] = []
        switch type {
        case .sql(.array(let element)):
            try memory.append(.array(element, .owned(dimensions), values.count), to: &visits)
            for index in values.indices.reversed() {
                let source = sourceElementType ?? sourceTypes.flatMap { $0.indices.contains(index) ? $0[index] : nil }
                try memory.append(.value(values[index], source, .sql(element)), to: &visits)
            }
        case .sql(.composite(let descriptor)):
            guard descriptor.columns.count == values.count else {
                throw QueryValueError.compositeFieldCountMismatch(expected: UInt64(descriptor.columns.count), actual: UInt64(values.count))
            }
            try memory.append(.composite(descriptor, values.count), to: &visits)
            for index in values.indices.reversed() {
                let source = sourceTypes.flatMap { $0.indices.contains(index) ? $0[index] : nil }
                try memory.append(.value(values[index], source, descriptor.columns[index].type), to: &visits)
            }
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "flat selected destination")
        }
        if let sourceTypes, sourceTypes.count != values.count { throw QueryValueError.invalidValueConstruction }
        func finish(_ count: Int) throws -> Range<Int> {
            guard count >= 0, completed.count >= count else { throw QueryValueError.invalidValueConstruction }
            try memory.prepare(&edges, adding: count)
            let start = edges.count
            edges.append(contentsOf: completed.suffix(count))
            completed.removeLast(count)
            return start..<edges.count
        }
        while let visit = visits.popLast() {
            try workMeter.consume(at: .expressionEvaluation)
            let node: Node
            switch visit {
            case .value(let value, let source, let target):
                if let source, source != target, !value.isNull {
                    if case .composite(let record) = value, case .sql(.composite(let destination)) = target {
                        guard case .sql(.composite(let origin)) = source,
                              record.fields.count == destination.columns.count else { throw QueryValueError.invalidValueConstruction }
                        try memory.append(.composite(destination, record.fields.count), to: &visits)
                        for index in record.fields.indices.reversed() {
                            try memory.append(.value(record.fields[index], origin.columns[index].type,
                                destination.columns[index].type), to: &visits)
                        }
                        continue
                    }
                    if case .array(let array) = value, case .sql(.array(let element)) = target {
                        guard case .sql(.array(let origin)) = source else { throw QueryValueError.invalidValueConstruction }
                        try memory.append(.array(element, .view(array.dimensions), array.elements.count), to: &visits)
                        for index in array.elements.indices.reversed() {
                            try memory.append(.value(array.elements[index], .sql(origin), .sql(element)), to: &visits)
                        }
                        continue
                    }
                    // Lexical collection conversion is completed before final builder
                    // ceilings are selected; its result is imported once as a leaf subtree.
                    node = .leaf(try converted(value, from: source, to: target, context: context,
                        environment: temporalEnvironment, memory: memory, limits: limits))
                } else { node = .leaf(value) }
            case .array(let element, let shape, let count): node = .array(element, shape, try finish(count))
            case .composite(let descriptor, let count): node = .composite(descriptor, try finish(count))
            }
            let index = nodes.count
            try memory.append(node, to: &nodes)
            try memory.append(index, to: &completed)
        }
        guard completed.count == 1, let root = completed.first else { throw QueryValueError.invalidValueConstruction }
        var valueCount = 0, childCount = edges.count, dimensionCount = 0, typeCount = 0, columnCount = 0
        var leafBytes: UInt64 = 0, names: UInt64 = 0
        var types: [QueryColumnType] = [], leafVisits: [QueryValue] = []
        func add(_ count: Int, to total: inout Int) throws {
            let sum = total.addingReportingOverflow(count)
            guard !sum.overflow else { throw QueryValueError.arithmeticOverflow }
            total = sum.partialValue
        }
        for node in nodes {
            try workMeter.consume(at: .expressionEvaluation)
            switch node {
            case .leaf(let value):
                let logical = try QueryValueValidator.measure(value)
                guard let count = Int(exactly: logical.nodeCount), let children = Int(exactly: logical.collectionElementCount) else {
                    throw QueryValueError.arithmeticOverflow
                }
                try add(count, to: &valueCount); try add(children, to: &childCount)
                leafBytes = try DatabaseIntermediateFootprint(bytes: leafBytes).adding(.init(bytes: logical.retainedByteCount)).bytes
                try memory.append(value, to: &leafVisits)
                while let source = leafVisits.popLast() {
                    try workMeter.consume(at: .expressionEvaluation)
                    switch source {
                    case .array(let array):
                        try add(array.dimensions.count, to: &dimensionCount)
                        try memory.append(.sql(array.elementType), to: &types)
                        for value in array.elements { try memory.append(value, to: &leafVisits) }
                    case .composite(let record):
                        try memory.append(.sql(.composite(record.type)), to: &types)
                        for value in record.fields { try memory.append(value, to: &leafVisits) }
                    default: break
                    }
                }
            case .array(let element, let shape, _):
                try add(1, to: &valueCount); try add(shape.count, to: &dimensionCount)
                try memory.append(.sql(element), to: &types)
            case .composite(let descriptor, _):
                try add(1, to: &valueCount)
                try memory.append(.sql(.composite(descriptor)), to: &types)
            }
        }
        while let type = types.popLast() {
            try workMeter.consume(at: .expressionEvaluation)
            try add(1, to: &typeCount)
            switch type {
            case .sql(.array(let child)): try memory.append(.sql(child), to: &types)
            case .sql(.composite(let descriptor)):
                try add(descriptor.columns.count, to: &columnCount)
                for column in descriptor.columns {
                    names = try DatabaseIntermediateFootprint(bytes: names).adding(.init(bytes: UInt64(column.name.utf8.count))).bytes
                    try memory.append(column.type, to: &types)
                }
            default: break
            }
        }
        let plannedBytes = try QueryValueBuilder.constructionByteCount(nodeCapacity: valueCount,
            childIndexCapacity: childCount, dimensionCapacity: dimensionCount, typeNodeCapacity: typeCount, columnCapacity: columnCount)
        var admitted = try DatabaseIntermediateFootprint(bytes: plannedBytes).adding(.init(bytes: leafBytes)).adding(.init(bytes: names)).bytes
        let reservation = try workMeter.reserveIntermediate(bytes: admitted, at: .expressionEvaluation)
        var successful = false
        defer { if !successful { reservation.release() } }
        var builder = try QueryValueBuilder(limits: limits, nodeCapacity: valueCount, childIndexCapacity: childCount,
            dimensionCapacity: dimensionCount, typeNodeCapacity: typeCount, columnCapacity: columnCount)
        if builder.constructionRetainedByteCount > plannedBytes {
            let excess = builder.constructionRetainedByteCount - plannedBytes
            try reservation.reserveAdditional(bytes: excess, at: .expressionEvaluation)
            admitted = try DatabaseIntermediateFootprint(bytes: admitted).adding(.init(bytes: excess)).bytes
        }
        var ids: [Int] = [], children: [Int] = [], shape: [QueryArrayDimension] = []
        try memory.prepare(&ids, adding: nodes.count)
        for node in nodes {
            try workMeter.consume(at: .expressionEvaluation)
            let id: Int
            switch node {
            case .leaf(let value): id = try builder.append(value)
            case .composite(let descriptor, let range):
                children.removeAll(keepingCapacity: true)
                try memory.prepare(&children, adding: range.count)
                for edge in range { children.append(ids[edges[edge]]) }
                id = try builder.appendComposite(type: descriptor, fields: children)
            case .array(let element, let dimensions, let range):
                children.removeAll(keepingCapacity: true); shape.removeAll(keepingCapacity: true)
                try memory.prepare(&children, adding: range.count); try memory.prepare(&shape, adding: dimensions.count)
                for edge in range { children.append(ids[edges[edge]]) }
                for index in 0..<dimensions.count { shape.append(dimensions[index]) }
                id = try builder.appendArray(elementType: element, dimensions: shape, elements: children)
            }
            ids.append(id)
        }
        let value = try builder.finish(root: ids[root])
        let actual = try CanonicalRelationalFootprintMeter.valueFootprint(of: value, workMeter: workMeter,
            stage: .expressionEvaluation).bytes
        if actual > admitted { try reservation.reserveAdditional(bytes: actual - admitted, at: .expressionEvaluation) }
        else { try reservation.releasePartial(bytes: admitted - actual) }
        successful = true
        return (value, DatabaseQueryPayloadLease(retaining: reservation))
    }

    private static func converted(_ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext, environment: SQLTemporalEnvironment?, memory: Memory,
        limits: QueryValueLimits) throws -> QueryValue {
        func retain(_ value: QueryValue) throws -> QueryValue {
            let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
                workMeter: memory.meter, stage: .expressionEvaluation)
            try memory.bytes(footprint.bytes)
            return value
        }
        if CanonicalSQLScalarCast.usesPayload(target) {
            let result = try CanonicalSQLScalarCast.evaluatePayload(value, from: source, to: target,
                context: context, temporalEnvironment: environment, workMeter: memory.meter)
            return try result.withValue { try retain(.primitive(copy $0)) }
        }
        if CanonicalQueryTypeBinder.isCollection(target) || CanonicalQueryTypeBinder.compositeType(target) != nil {
            let result = try CanonicalSQLScalarCast.evaluateCollection(value, from: source, to: target,
                context: context, temporalEnvironment: environment, workMeter: memory.meter, limits: limits)
            return try withExtendedLifetime(result) { try retain(result.value) }
        }
        return try CanonicalSQLScalarCast.evaluateValue(value, from: source, to: target, context: context,
            temporalEnvironment: environment, workMeter: memory.meter)
    }
}
