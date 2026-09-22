"""公共测试夹具。

每个测试函数都拿到:
- 独立临时目录下的 trace 文件与 metrics 快照路径；
- 全新的 Tracer 与 MetricsRegistry 单例，避免用例间相互污染；
- 基于 httpx ASGITransport 的异步客户端（不绑定真实端口）。
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from app import create_app
from app.config import Settings
from app.logging_setup import configure_logging
from app.metrics import get_registry
from app.tracing import SpanExporter, Tracer, reset_tracer


@pytest.fixture
def tmp_obs_paths(tmp_path: Path) -> tuple[Path, Path]:
    return tmp_path / "traces.jsonl", tmp_path / "metrics.json"


@pytest.fixture
def app(tmp_obs_paths: tuple[Path, Path]):
    trace_path, metrics_path = tmp_obs_paths
    settings = Settings(
        correlation_header="X-Correlation-ID",
        correlation_length_max=128,
        log_json=True,
        log_level="DEBUG",
        metrics_snapshot_path=str(metrics_path),
        trace_sample_rate=1.0,
        trace_export="file",
        trace_file_path=str(trace_path),
    )
    application = create_app(settings)
    # 测试日志打到默认 handler，-s 时可见；同时保持单例隔离
    configure_logging(True, "DEBUG")
    yield application
    # 用例结束走一次关闭语义：flush 指标 + 落盘未完成片段
    get_registry().write_snapshot()
    from app.tracing import get_tracer

    get_tracer().shutdown()


@pytest.fixture
async def client(app) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://observability.test"
    ) as c:
        yield c


@pytest.fixture
def trace_rows(tmp_obs_paths: tuple[Path, Path]):
    """读取并解析 trace JSONL 文件的辅助函数（延迟读取）。"""

    trace_path, _ = tmp_obs_paths

    def _read() -> list[dict]:
        if not Path(trace_path).exists():
            return []
        return [
            json.loads(line)
            for line in Path(trace_path).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    return _read


@pytest.fixture
def metrics_snapshot(tmp_obs_paths: tuple[Path, Path]):
    _, metrics_path = tmp_obs_paths

    def _read() -> dict:
        get_registry().write_snapshot()
        return json.loads(Path(metrics_path).read_text(encoding="utf-8"))

    return _read


@pytest.fixture(autouse=True)
def _reset_singletons_after():
    """每个用例后把全局单例恢复为 no-op，杜绝跨文件用例污染。"""

    yield
    reset_tracer(Tracer(sample_rate=1.0, exporter=SpanExporter("none", "")))
