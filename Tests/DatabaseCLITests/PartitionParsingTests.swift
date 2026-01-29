import Testing
@testable import DatabaseCLICore

@Suite("DataCommands.parsePartitionValues / removePartitionArgs")
struct PartitionParsingTests {

    // MARK: - parsePartitionValues

    @Test func noPartition() {
        let result = DataCommands.parsePartitionValues(from: ["get", "User", "id-1"])
        #expect(result.isEmpty)
    }

    @Test func singlePartition() {
        let result = DataCommands.parsePartitionValues(from: ["get", "User", "id-1", "--partition", "tenantId=t_123"])
        #expect(result == ["tenantId": "t_123"])
    }

    @Test func multiplePartitions() {
        let tokens = ["find", "Order", "--partition", "tenantId=t_1", "--partition", "channelId=ch_2"]
        let result = DataCommands.parsePartitionValues(from: tokens)
        #expect(result == ["tenantId": "t_1", "channelId": "ch_2"])
    }

    @Test func valueContainsEquals() {
        // "key=val=ue" should split only on first "="
        let result = DataCommands.parsePartitionValues(from: ["--partition", "data=a=b=c"])
        #expect(result == ["data": "a=b=c"])
    }

    @Test func partitionWithoutValue() {
        // "--partition" at end of tokens (no following value) is skipped
        let result = DataCommands.parsePartitionValues(from: ["get", "User", "--partition"])
        #expect(result.isEmpty)
    }

    @Test func malformedPartitionNoEquals() {
        // "tenantId" without "=" → split produces 1 part, not added
        let result = DataCommands.parsePartitionValues(from: ["--partition", "tenantId"])
        #expect(result.isEmpty)
    }

    // MARK: - removePartitionArgs

    @Test func removeNone() {
        let result = DataCommands.removePartitionArgs(from: ["find", "User", "--limit", "10"])
        #expect(result == ["find", "User", "--limit", "10"])
    }

    @Test func removeSingle() {
        let result = DataCommands.removePartitionArgs(from: ["find", "User", "--partition", "t=1", "--limit", "10"])
        #expect(result == ["find", "User", "--limit", "10"])
    }

    @Test func removeMultiple() {
        let tokens = ["--partition", "a=1", "find", "--partition", "b=2", "User"]
        let result = DataCommands.removePartitionArgs(from: tokens)
        #expect(result == ["find", "User"])
    }

    @Test func removeTrailingPartition() {
        // "--partition" at end without value: treated as standalone token, not removed as pair
        let result = DataCommands.removePartitionArgs(from: ["find", "--partition"])
        #expect(result == ["find", "--partition"])
    }
}
