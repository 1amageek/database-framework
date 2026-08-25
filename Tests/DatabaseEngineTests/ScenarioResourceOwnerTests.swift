#if !os(WASI)
import TestSupport
import Testing

@Suite("Scenario Resource Owner")
struct ScenarioResourceOwnerTests {
    @Test("Shutdown drains every registered resource exactly once")
    func shutdownDrainsEveryResourceExactlyOnce() async {
        let owner = ScenarioResourceOwner()
        let first = ScenarioResourceProbe()
        let second = ScenarioResourceProbe()

        #expect(await owner.register(first, shutdown: { await $0.shutdown() }))
        #expect(await owner.register(first, shutdown: { await $0.shutdown() }))
        #expect(await owner.register(second, shutdown: { await $0.shutdown() }))
        #expect(await owner.resourceCount == 2)

        await owner.shutdownAll()
        await owner.shutdownAll()

        #expect(await first.shutdownCount == 1)
        #expect(await second.shutdownCount == 1)
        #expect(await owner.resourceCount == 0)
    }

    @Test("Registration is rejected after shutdown starts")
    func registrationIsRejectedAfterShutdownStarts() async {
        let owner = ScenarioResourceOwner()
        let resource = ScenarioResourceProbe()

        await owner.shutdownAll()

        #expect(
            await !owner.register(
                resource,
                shutdown: { await $0.shutdown() }
            )
        )
        #expect(await resource.shutdownCount == 0)
    }
}

private actor ScenarioResourceProbe {
    private(set) var shutdownCount = 0

    func shutdown() {
        shutdownCount += 1
    }
}
#endif
