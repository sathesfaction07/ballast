import pytest

from adaptive_fetch.core.governor import PayloadMode, Pressure, ResourceGovernor


class TestPressureClassification:
    @pytest.mark.parametrize(
        "memory_percent,expected",
        [
            (95.0, Pressure.CRITICAL),
            (90.0, Pressure.CRITICAL),
            (85.0, Pressure.HIGH),
            (80.0, Pressure.HIGH),
            (70.0, Pressure.NORMAL),
            (61.0, Pressure.NORMAL),
            (60.0, Pressure.LOW),
            (20.0, Pressure.LOW),
        ],
    )
    def test_bands(self, limits, snapshot_factory, memory_percent, expected):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        decision = governor.evaluate(snapshot_factory(memory_percent=memory_percent))
        assert decision.pressure is expected


class TestShrinkFastGrowSlow:
    def test_high_pressure_halves_immediately(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        decision = governor.evaluate(snapshot_factory(memory_percent=85.0))
        assert decision.batch_size == 32

    def test_critical_drops_straight_to_floor(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=128, total_items=100)
        decision = governor.evaluate(snapshot_factory(memory_percent=95.0))
        assert decision.batch_size == limits.min_batch_size

    def test_low_pressure_does_not_grow_before_patience(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        for _ in range(limits.growth_patience - 1):
            decision = governor.evaluate(snapshot_factory(memory_percent=40.0))
        assert decision.batch_size == 64

    def test_low_pressure_grows_after_patience(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        for _ in range(limits.growth_patience):
            decision = governor.evaluate(snapshot_factory(memory_percent=40.0))
        assert decision.batch_size == 80

    def test_healthy_streak_resets_on_pressure(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        governor.evaluate(snapshot_factory(memory_percent=40.0))
        governor.evaluate(snapshot_factory(memory_percent=40.0))
        governor.evaluate(snapshot_factory(memory_percent=70.0))
        decision = governor.evaluate(snapshot_factory(memory_percent=40.0))
        assert decision.batch_size == 64

    def test_shrink_is_faster_than_growth(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        shrunk = governor.evaluate(snapshot_factory(memory_percent=85.0)).batch_size
        assert shrunk == 32

        for _ in range(limits.growth_patience):
            grown = governor.evaluate(snapshot_factory(memory_percent=40.0)).batch_size
        assert grown - shrunk < 64 - shrunk


class TestCeilingAfterCritical:
    def test_critical_lowers_the_ceiling(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=128, total_items=100)
        governor.evaluate(snapshot_factory(memory_percent=95.0))

        for _ in range(limits.growth_patience * 10):
            decision = governor.evaluate(snapshot_factory(memory_percent=30.0))

        assert decision.batch_size <= 64

    def test_batch_never_exceeds_max(self, limits, snapshot_factory):
        governor = ResourceGovernor(
            limits, starting_batch_size=limits.max_batch_size, total_items=5
        )
        for _ in range(limits.growth_patience * 5):
            decision = governor.evaluate(snapshot_factory(memory_percent=10.0))
        assert decision.batch_size <= limits.max_batch_size

    def test_batch_never_drops_below_min(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=8, total_items=100)
        for _ in range(10):
            decision = governor.evaluate(snapshot_factory(memory_percent=99.0))
        assert decision.batch_size >= limits.min_batch_size


class TestDiskGuard:
    def test_halts_below_halt_threshold(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        decision = governor.evaluate(snapshot_factory(disk_free_mb=50.0))
        assert decision.should_halt
        assert "disk free" in (decision.halt_reason or "")

    def test_warns_but_continues_below_warn_threshold(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        decision = governor.evaluate(snapshot_factory(disk_free_mb=300.0))
        assert not decision.should_halt
        assert any("disk free" in n for n in decision.notes)

    def test_disk_halt_takes_priority_over_memory(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        decision = governor.evaluate(
            snapshot_factory(disk_free_mb=10.0, memory_percent=95.0)
        )
        assert decision.should_halt


class TestNetworkDegradation:
    def test_slow_latency_switches_to_minimal(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        decision = governor.evaluate(snapshot_factory(latency_ms=1500.0))
        assert decision.payload_mode is PayloadMode.MINIMAL
        assert decision.concurrency < limits.max_concurrent

    def test_fast_latency_restores_full(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        governor.evaluate(snapshot_factory(latency_ms=1500.0))
        decision = governor.evaluate(snapshot_factory(latency_ms=100.0))
        assert decision.payload_mode is PayloadMode.FULL
        assert decision.concurrency == limits.max_concurrent

    def test_middle_latency_holds_current_mode(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        governor.evaluate(snapshot_factory(latency_ms=1500.0))
        decision = governor.evaluate(snapshot_factory(latency_ms=500.0))
        assert decision.payload_mode is PayloadMode.MINIMAL

    def test_unprobed_latency_leaves_mode_alone(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=100)
        decision = governor.evaluate(snapshot_factory(latency_ms=None))
        assert decision.payload_mode is PayloadMode.FULL


class TestClientRecycling:
    def test_recycles_when_rss_exceeds_threshold(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=1000)
        decision = governor.evaluate(snapshot_factory(process_rss_mb=900.0))
        assert decision.recycle_client

    def test_no_recycle_below_threshold(self, limits, snapshot_factory):
        governor = ResourceGovernor(limits, starting_batch_size=64, total_items=1000)
        decision = governor.evaluate(snapshot_factory(process_rss_mb=400.0))
        assert not decision.recycle_client

    def test_small_workload_tolerates_more_memory(self, limits, snapshot_factory):
        small = ResourceGovernor(limits, starting_batch_size=64, total_items=5)
        large = ResourceGovernor(limits, starting_batch_size=64, total_items=1000)
        snap = snapshot_factory(process_rss_mb=900.0)
        assert not small.evaluate(snap).recycle_client
        assert large.evaluate(snap).recycle_client
