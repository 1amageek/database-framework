extension CostModel {
    public func indexOnlySavings(entities: Double) -> Double {
        entities * entityFetchWeight
    }
}
