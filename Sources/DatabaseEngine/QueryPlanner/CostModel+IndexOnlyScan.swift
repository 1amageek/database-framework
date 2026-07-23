extension CostModel {
    public func indexOnlySavings(records: Double) -> Double {
        records * recordFetchWeight
    }
}
