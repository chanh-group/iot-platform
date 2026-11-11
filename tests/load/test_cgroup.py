#!/usr/bin/env python3
"""Report effective CPU/memory limits for this process's cgroup."""

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest


def _unescape_mount(value):
    return value.replace("\\040", " ").replace("\\011", "\t").replace("\\134", "\\")


def _mounts(proc):
    result = []
    for line in (proc / "self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        fields, fs = before.split(), after.split()
        if fs[0] not in ("cgroup", "cgroup2"):
            continue
        root, point = Path(_unescape_mount(fields[3])), Path(_unescape_mount(fields[4]))
        controllers = set(fs[2].split(",")) | set(fields[5].split(","))
        result.append((fs[0], root, point, controllers))
    return result


def _membership(proc):
    result = {}
    for line in (proc / "self/cgroup").read_text().splitlines():
        hierarchy, controllers, path = line.split(":", 2)
        result["" if hierarchy == "0" and not controllers else controllers] = Path(path)
    return result


def _walk_cgroup(mount, member):
    _, root, point, _ = mount
    try:
        relative = member.relative_to(root)
    except ValueError:
        return
    current = point / relative
    while current == point or point in current.parents:
        yield current
        if current == point:
            break
        current = current.parent


def _read(path):
    try:
        return path.read_text().strip()
    except OSError:
        return None


def _effective(proc=Path("/proc")):
    try:
        memberships = _membership(proc)
        mounts = _mounts(proc)
    except OSError:
        return "unavailable", "unavailable", "unavailable", "unavailable", "unavailable"
    values = {"cpu": [], "memory": []}
    available = set()
    incomplete = set()
    sources = {"cpu": set(), "memory": set()}
    found = set()
    for mount in mounts:
        version, _, _, controllers = mount
        if version == "cgroup2":
            member = memberships.get("")
            categories = ("cpu", "memory")
            cgroup2_cpu_quota = _read(mount[2] / "cpu.max") is not None
            cgroup2_cpuset = _read(mount[2] / "cpuset.cpus.effective") is not None
            cgroup2_memory = _read(mount[2] / "memory.max") is not None
        else:
            member = next((memberships[key] for key in memberships
                           if key and set(key.split(",")) <= controllers
                           and set(key.split(",")) & {"cpu", "cpuacct", "memory", "cpuset"}), None)
            has_cpuset = "cpuset" in controllers
            has_cpu_quota = "cpu" in controllers
            categories = tuple(name for name in ("cpu", "memory") if name in controllers)
            if has_cpuset and "cpu" not in categories:
                categories += ("cpu",)
            if member is not None:
                # v1 has independent mounts; select the membership matching this mount.
                key = next((key for key in memberships if key and set(key.split(",")) <= controllers), None)
                member = memberships.get(key) if key else None
        if member is None:
            continue
        found.update(categories)
        cpuset_found = False
        for directory in _walk_cgroup(mount, member):
            if version == "cgroup2":
                if "cpu" in categories and cgroup2_cpu_quota:
                    raw = _read(directory / "cpu.max")
                    if raw is None:
                        incomplete.add("cpu")
                    else:
                        available.add("cpu")
                        sources["cpu"].add("v2")
                        if raw.split()[0] != "max":
                            quota, period = map(int, raw.split())
                            if quota > 0 and period > 0:
                                values["cpu"].append(quota / period)
                if "cpu" in categories and cgroup2_cpuset:
                    cpus = _read(directory / "cpuset.cpus.effective")
                    if not cpus:
                        incomplete.add("cpu")
                    else:
                        available.add("cpu")
                        sources["cpu"].add("v2")
                        count = sum(int(end or start) - int(start) + 1 for item in cpus.split(",")
                                    for start, _, end in [item.partition("-")])
                        values["cpu"].append(float(count))
                if "memory" in categories and cgroup2_memory:
                    raw = _read(directory / "memory.max")
                    if raw is None:
                        incomplete.add("memory")
                    else:
                        available.add("memory")
                        sources["memory"].add("v2")
                        if raw != "max":
                            values["memory"].append(int(raw))
            else:
                if has_cpu_quota:
                    quota, period = _read(directory / "cpu.cfs_quota_us"), _read(directory / "cpu.cfs_period_us")
                    if quota is None or period is None:
                        incomplete.add("cpu")
                    else:
                        available.add("cpu")
                        sources["cpu"].add("v1")
                    if quota and period and int(quota) > 0 and int(period) > 0:
                        values["cpu"].append(int(quota) / int(period))
                if has_cpuset:
                    cpus = _read(directory / "cpuset.cpus")
                    if cpus is None:
                        incomplete.add("cpu")
                    elif cpus:
                        cpuset_found = True
                        available.add("cpu")
                        sources["cpu"].add("v1")
                        count = sum(int(end or start) - int(start) + 1 for item in cpus.split(",")
                                    for start, _, end in [item.partition("-")])
                        values["cpu"].append(float(count))
                if "memory" in categories:
                    raw = _read(directory / "memory.limit_in_bytes")
                    if raw is None:
                        incomplete.add("memory")
                    else:
                        available.add("memory")
                        sources["memory"].add("v1")
                        if int(raw) > 0 and int(raw) < (1 << 60):
                            values["memory"].append(int(raw))
        if version == "cgroup" and has_cpuset and not cpuset_found:
            incomplete.add("cpu")

    def render(limits, category):
        if category not in found or category not in available or category in incomplete:
            return "unavailable"
        if not limits:
            return "unlimited"
        value = min(limits)
        return f"{value:.3f}" if category == "cpu" else str(value)

    used_versions = sources["cpu"] | sources["memory"]
    version = ("hybrid" if len(used_versions) > 1 else next(iter(used_versions), "unavailable"))
    return (version, render(values["cpu"], "cpu"), render(values["memory"], "memory"),
            "+".join(sorted(sources["cpu"])) or "unavailable",
            "+".join(sorted(sources["memory"])) or "unavailable")


class CgroupTests(unittest.TestCase):
    def fixture(self, cgroup, mountinfo):
        self.temp = TemporaryDirectory()
        root = Path(self.temp.name)
        (root / "self").mkdir()
        (root / "self/cgroup").write_text(cgroup)
        (root / "self/mountinfo").write_text(mountinfo.replace("@ROOT@", str(root)))
        return root

    def test_v2_reports_effective_process_limits_and_parent_limits(self):
        root = self.fixture(
            "0::/tenant/job\n",
            "1 0 0:1 / @ROOT@/sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        )
        parent = Path(self.temp.name) / "sys/fs/cgroup/tenant"
        child = parent / "job"
        child.mkdir(parents=True)
        (Path(self.temp.name) / "sys/fs/cgroup/cpu.max").write_text("max 100000")
        (Path(self.temp.name) / "sys/fs/cgroup/memory.max").write_text("max")
        (parent / "cpu.max").write_text("250000 100000")
        (parent / "memory.max").write_text("max")
        (child / "cpu.max").write_text("max 100000")
        (child / "memory.max").write_text("536870912")
        self.assertEqual(_effective(root), ("v2", "2.500", "536870912", "v2", "v2"))
        self.temp.cleanup()

    def test_v1_reports_quota_and_treats_kernel_sentinel_as_unlimited(self):
        root = self.fixture(
            "2:cpu,cpuacct:/job\n3:memory:/job\n",
            "1 0 0:1 / @ROOT@/sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu,cpuacct\n"
            "2 0 0:2 / @ROOT@/sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
        )
        cpu, memory = Path(self.temp.name) / "sys/fs/cgroup/cpu/job", Path(self.temp.name) / "sys/fs/cgroup/memory/job"
        cpu.mkdir(parents=True)
        memory.mkdir(parents=True)
        cpu_parent = Path(self.temp.name) / "sys/fs/cgroup/cpu"
        memory_parent = Path(self.temp.name) / "sys/fs/cgroup/memory"
        (cpu_parent / "cpu.cfs_quota_us").write_text("-1")
        (cpu_parent / "cpu.cfs_period_us").write_text("100000")
        (memory_parent / "memory.limit_in_bytes").write_text(str(1 << 62))
        (cpu / "cpu.cfs_quota_us").write_text("150000")
        (cpu / "cpu.cfs_period_us").write_text("100000")
        (memory / "memory.limit_in_bytes").write_text(str(1 << 62))
        self.assertEqual(_effective(root), ("v1", "1.500", "unlimited", "v1", "v1"))
        self.temp.cleanup()

    def test_hybrid_reports_only_versions_that_supply_effective_limits(self):
        root = self.fixture(
            "0::/job\n2:cpu,cpuacct:/job\n3:memory:/job\n",
            "1 0 0:1 / @ROOT@/sys/fs/cgroup/v2 rw - cgroup2 cgroup rw\n"
            "2 0 0:2 / @ROOT@/sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu,cpuacct\n"
            "3 0 0:3 / @ROOT@/sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
        )
        v2 = Path(self.temp.name) / "sys/fs/cgroup/v2/job"
        cpu = Path(self.temp.name) / "sys/fs/cgroup/cpu/job"
        memory = Path(self.temp.name) / "sys/fs/cgroup/memory/job"
        v2.mkdir(parents=True)
        cpu.mkdir(parents=True)
        memory.mkdir(parents=True)
        (Path(self.temp.name) / "sys/fs/cgroup/v2/memory.max").write_text("max")
        (Path(self.temp.name) / "sys/fs/cgroup/cpu/cpu.cfs_quota_us").write_text("-1")
        (Path(self.temp.name) / "sys/fs/cgroup/cpu/cpu.cfs_period_us").write_text("100000")
        (Path(self.temp.name) / "sys/fs/cgroup/memory/memory.limit_in_bytes").write_text(str(1 << 62))
        (memory / "memory.limit_in_bytes").write_text(str(1 << 62))
        (v2 / "memory.max").write_text("268435456")
        (cpu / "cpu.cfs_quota_us").write_text("200000")
        (cpu / "cpu.cfs_period_us").write_text("100000")
        self.assertEqual(_effective(root), ("hybrid", "2.000", "268435456", "v1", "v1+v2"))
        self.temp.cleanup()

    def test_unreadable_descendant_does_not_become_unlimited(self):
        root = self.fixture(
            "0::/job\n",
            "1 0 0:1 / @ROOT@/sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        )
        mount = Path(self.temp.name) / "sys/fs/cgroup"
        child = mount / "job"
        child.mkdir(parents=True)
        (mount / "cpu.max").write_text("max 100000")
        (mount / "memory.max").write_text("max")
        (child / "cpu.max").write_text("max 100000")
        # The descendant's missing memory.max could conceal a tighter limit.
        self.assertEqual(_effective(root)[2], "unavailable")
        self.temp.cleanup()

    def test_cpuset_v1_separate_mount_constrains_cpu(self):
        root = self.fixture(
            "2:cpu,cpuacct:/job\n3:cpuset:/job\n",
            "1 0 0:1 / @ROOT@/sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu,cpuacct\n"
            "2 0 0:2 / @ROOT@/sys/fs/cgroup/cpuset rw - cgroup cgroup rw,cpuset\n",
        )
        cpu = Path(self.temp.name) / "sys/fs/cgroup/cpu"
        cpuset = Path(self.temp.name) / "sys/fs/cgroup/cpuset"
        (cpu / "job").mkdir(parents=True)
        (cpuset / "job").mkdir(parents=True)
        for directory in (cpu, cpu / "job"):
            (directory / "cpu.cfs_quota_us").write_text("-1")
            (directory / "cpu.cfs_period_us").write_text("100000")
        (cpuset / "cpuset.cpus").write_text("0-7")
        (cpuset / "job/cpuset.cpus").write_text("2-3")
        self.assertEqual(_effective(root)[:4], ("v1", "2.000", "unavailable", "v1"))
        self.temp.cleanup()

    def test_mount_root_is_removed_before_walking_ancestors(self):
        root = self.fixture(
            "0::/tenant/job\n",
            "1 0 0:1 /tenant @ROOT@/sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        )
        mount = Path(self.temp.name) / "sys/fs/cgroup"
        (mount / "job").mkdir(parents=True)
        (mount / "cpu.max").write_text("100000 100000")
        (mount / "job/cpu.max").write_text("max 100000")
        (mount / "memory.max").write_text("max")
        (mount / "job/memory.max").write_text("max")
        self.assertEqual(_effective(root), ("v2", "1.000", "unlimited", "v2", "v2"))
        self.temp.cleanup()

    def test_missing_cgroup_information_is_unavailable(self):
        root = self.fixture("", "")
        (root / "self/cgroup").unlink()
        self.assertEqual(
            _effective(root),
            ("unavailable", "unavailable", "unavailable", "unavailable", "unavailable"),
        )
        self.temp.cleanup()


if __name__ == "__main__":
    if sys.argv[1:] == ["--report"]:
        version, cpu, memory, cpu_source, memory_source = _effective()
        print(f"cgroup_scope=benchmark_process\ncgroup_version={version}\ncgroup_cpu_source={cpu_source}\ncgroup_cpu_limit={cpu}\ncgroup_memory_source={memory_source}\ncgroup_memory_limit_bytes={memory}")
    else:
        unittest.main(argv=[sys.argv[0]])
