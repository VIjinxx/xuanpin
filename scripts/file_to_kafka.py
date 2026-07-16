#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 JSONL 大文件（一行一条 JSON）逐行写入 Kafka Topic。
适用于 geekbiGoods 等 Flink 任务的离线数据灌入，可在 Windows 本地直接运行。

依赖安装:
    pip install -r scripts/requirements.txt
"""

from __future__ import annotations

import argparse
import logging
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    from kafka.serializer import Serializer
except ImportError as exc:
    print("缺少依赖 kafka-python，请先执行: pip install -r scripts/requirements.txt", file=sys.stderr)
    raise SystemExit(1) from exc

DEFAULT_TOPIC = "geekbiGoods"
READER_BUFFER_SIZE = 8 * 1024 * 1024
PROGRESS_LOG_INTERVAL_SEC = 5.0
GOODS_ID_PATTERN = re.compile(r'"goodsId"\s*:\s*"?(\d+)"?')

LOGGER = logging.getLogger("file_to_kafka")


class StringKeySerializer(Serializer):
    def serialize(self, topic, headers, key):
        if key is None:
            return None
        if isinstance(key, (bytes, bytearray, memoryview)):
            return bytes(key)
        return str(key).encode("utf-8")


class StringValueSerializer(Serializer):
    def serialize(self, topic, headers, value):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        return value.encode("utf-8")


@dataclass
class Options:
    file_path: Path
    topic: str = DEFAULT_TOPIC
    bootstrap_servers: str = "localhost:9092"
    start_line: int = 1
    max_lines: Optional[int] = None
    checkpoint_file: Optional[Path] = None
    checkpoint_interval: int = 10_000
    dry_run: bool = False
    key_by_goods_id: bool = False
    resume: bool = False


@dataclass
class ProgressTracker:
    start_line: int
    last_line_no: int = 0
    end_line_no: int = 0
    start_time: float = 0.0
    last_log_time: float = 0.0
    stop_requested: bool = False
    success_count: int = 0
    fail_count: int = 0
    pending_count: int = 0
    skipped_count: int = 0
    empty_count: int = 0
    _fail_logs: int = field(default=0, repr=False)

    def on_success(self, _metadata) -> None:
        self.pending_count -= 1
        self.success_count += 1

    def on_error(self, line_no: int, exc: Exception) -> None:
        self.pending_count -= 1
        self.fail_count += 1
        if self._fail_logs < 10:
            LOGGER.error("发送失败: line=%s, error=%s", line_no, exc)
            self._fail_logs += 1

    def log_summary(self) -> None:
        duration_ms = max(int((time.time() - self.start_time) * 1000), 1)
        processed = self.success_count + self.fail_count
        LOGGER.info("========== 导入完成 ==========")
        LOGGER.info("文件总行数(扫描到的最后一行): %s", self.end_line_no)
        LOGGER.info("起始行: %s", self.start_line)
        LOGGER.info("最后处理行: %s", self.last_line_no)
        LOGGER.info("成功: %s", self.success_count)
        LOGGER.info("失败: %s", self.fail_count)
        LOGGER.info("跳过空行: %s", self.empty_count)
        LOGGER.info("跳过起始前行数: %s", self.skipped_count)
        LOGGER.info("耗时: %s ms", duration_ms)
        LOGGER.info("平均吞吐: %s msg/s", processed * 1000 // duration_ms)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def load_bootstrap_servers_from_properties() -> Optional[str]:
    candidates = [
        Path(__file__).resolve().parent.parent / "src" / "main" / "resources" / "application.properties",
        Path.cwd() / "src" / "main" / "resources" / "application.properties",
    ]
    for path in candidates:
        if not path.is_file():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "bootstrapServers":
                return value.strip()
    return None


def should_skip_line(line: Optional[str]) -> bool:
    return line is None or not line.strip()


def extract_goods_id(json_line: str) -> Optional[str]:
    match = GOODS_ID_PATTERN.search(json_line)
    return match.group(1) if match else None


def read_checkpoint(checkpoint_file: Path) -> int:
    for line in checkpoint_file.read_text(encoding="utf-8").splitlines():
        trimmed = line.strip()
        if trimmed:
            return int(trimmed)
    return 0


def write_checkpoint(checkpoint_file: Path, line_no: int) -> None:
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_file.write_text(f"{line_no}\n", encoding="utf-8")
    LOGGER.info("已写入 checkpoint: line=%s", line_no)


def resolve_start_line(options: Options) -> int:
    if (
        options.resume
        and options.checkpoint_file is not None
        and options.checkpoint_file.is_file()
    ):
        checkpoint_line = read_checkpoint(options.checkpoint_file)
        if checkpoint_line >= options.start_line:
            LOGGER.info(
                "从 checkpoint 续传: %s -> 下一行 %s",
                options.checkpoint_file,
                checkpoint_line + 1,
            )
            return checkpoint_line + 1
    return options.start_line


def log_progress_if_needed(tracker: ProgressTracker) -> None:
    now = time.time()
    if now - tracker.last_log_time < PROGRESS_LOG_INTERVAL_SEC:
        return
    tracker.last_log_time = now
    elapsed_ms = max(int((now - tracker.start_time) * 1000), 1)
    processed = tracker.success_count + tracker.fail_count + tracker.pending_count
    throughput = processed * 1000 // elapsed_ms
    LOGGER.info(
        "进度: line=%s, 成功=%s, 失败=%s, 待回调=%s, 跳过空行=%s, 吞吐=%s msg/s",
        tracker.last_line_no,
        tracker.success_count,
        tracker.fail_count,
        tracker.pending_count,
        tracker.empty_count,
        throughput,
    )


def wait_for_pending(tracker: ProgressTracker, timeout_sec: float) -> None:
    deadline = time.time() + timeout_sec
    while tracker.pending_count > 0 and time.time() < deadline:
        time.sleep(0.2)
    if tracker.pending_count > 0:
        LOGGER.warning("仍有 %s 条消息未完成回调，继续退出", tracker.pending_count)


def resolve_compression_type() -> Optional[str]:
    from kafka.codec import has_gzip, has_lz4, has_snappy

    if has_lz4():
        return "lz4"
    if has_gzip():
        LOGGER.info("未安装 lz4 库，改用 gzip 压缩")
        return "gzip"
    if has_snappy():
        LOGGER.info("未安装 lz4 库，改用 snappy 压缩")
        return "snappy"
    LOGGER.warning("未安装压缩库，将以无压缩方式发送")
    return None


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    servers = [item.strip() for item in bootstrap_servers.split(",") if item.strip()]
    compression_type = resolve_compression_type()
    if compression_type:
        LOGGER.info("Kafka 压缩算法: %s", compression_type)
    return KafkaProducer(
        bootstrap_servers=servers,
        acks=1,
        compression_type=compression_type,
        linger_ms=20,
        batch_size=256 * 1024,
        retries=3,
        request_timeout_ms=120_000,
        delivery_timeout_ms=300_000,
        max_in_flight_requests_per_connection=5,
        key_serializer=StringKeySerializer(),
        value_serializer=StringValueSerializer(),
    )


def process_file(options: Options) -> ProgressTracker:
    if not options.file_path.is_file():
        raise FileNotFoundError(f"输入文件不存在或不是普通文件: {options.file_path}")

    start_line = resolve_start_line(options)
    max_lines = options.max_lines if options.max_lines is not None else sys.maxsize

    LOGGER.info("========== 文件导入 Kafka ==========")
    LOGGER.info("文件: %s", options.file_path.resolve())
    LOGGER.info("Topic: %s", options.topic)
    LOGGER.info("Bootstrap Servers: %s", options.bootstrap_servers)
    LOGGER.info("起始行: %s", start_line)
    LOGGER.info("最大发送行数: %s", max_lines if max_lines != sys.maxsize else "不限")
    LOGGER.info("Dry Run: %s", options.dry_run)
    LOGGER.info("按 goodsId 分区: %s", options.key_by_goods_id)

    tracker = ProgressTracker(start_line=start_line)
    tracker.start_time = time.time()
    tracker.last_log_time = tracker.start_time

    def handle_stop(signum, _frame):
        LOGGER.warning("收到中断信号 %s，正在刷写 Kafka 缓冲区...", signum)
        tracker.stop_requested = True

    previous_int = signal.signal(signal.SIGINT, handle_stop)
    previous_term = None
    if hasattr(signal, "SIGTERM"):
        previous_term = signal.signal(signal.SIGTERM, handle_stop)

    producer = None
    if not options.dry_run:
        producer = build_producer(options.bootstrap_servers)

    sent_in_this_run = 0
    last_checkpoint_line = start_line - 1

    try:
        with options.file_path.open("r", encoding="utf-8", buffering=READER_BUFFER_SIZE) as handle:
            for line_no, line in enumerate(handle, start=1):
                tracker.end_line_no = line_no
                if tracker.stop_requested:
                    break

                if line_no < start_line:
                    tracker.skipped_count += 1
                    continue
                if sent_in_this_run >= max_lines:
                    break
                if should_skip_line(line):
                    tracker.empty_count += 1
                    continue

                payload = line.strip()
                tracker.last_line_no = line_no

                if options.dry_run:
                    tracker.success_count += 1
                    sent_in_this_run += 1
                    log_progress_if_needed(tracker)
                    continue

                key = extract_goods_id(payload) if options.key_by_goods_id else None
                current_line_no = line_no
                tracker.pending_count += 1
                future = producer.send(options.topic, key=key, value=payload)
                future.add_callback(tracker.on_success)
                future.add_errback(lambda exc, ln=current_line_no: tracker.on_error(ln, exc))

                sent_in_this_run += 1
                log_progress_if_needed(tracker)

                if (
                    options.checkpoint_file is not None
                    and line_no - last_checkpoint_line >= options.checkpoint_interval
                ):
                    producer.flush()
                    wait_for_pending(tracker, 60.0)
                    write_checkpoint(options.checkpoint_file, line_no)
                    last_checkpoint_line = line_no

        if producer is not None:
            producer.flush()
            wait_for_pending(tracker, 300.0)
            if options.checkpoint_file is not None and tracker.last_line_no >= start_line:
                write_checkpoint(options.checkpoint_file, tracker.last_line_no)
    finally:
        if producer is not None:
            producer.close()
        signal.signal(signal.SIGINT, previous_int)
        if previous_term is not None:
            signal.signal(signal.SIGTERM, previous_term)

    tracker.log_summary()
    return tracker


def parse_args(argv: Optional[list[str]] = None) -> Options:
    default_bootstrap = load_bootstrap_servers_from_properties() or "localhost:9092"
    parser = argparse.ArgumentParser(
        description="将 JSONL 大文件逐行导入 Kafka，供 geekbiGoods Flink 任务消费。",
    )
    parser.add_argument("-f", "--file", required=True, help="输入 JSONL 文件路径，一行一条 JSON")
    parser.add_argument("-t", "--topic", default=DEFAULT_TOPIC, help=f"Kafka Topic，默认 {DEFAULT_TOPIC}")
    parser.add_argument(
        "-b",
        "--bootstrap-servers",
        default=default_bootstrap,
        help="Kafka bootstrap servers，默认读取 application.properties",
    )
    parser.add_argument("--start-line", type=int, default=1, help="从指定行号开始发送（1-based），默认 1")
    parser.add_argument("--max-lines", type=int, help="本次最多发送多少行，用于抽样测试")
    parser.add_argument("--checkpoint-file", help="checkpoint 文件路径，定期写入已成功刷盘的行号")
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=10_000,
        help="每发送多少行 flush 并写 checkpoint，默认 10000",
    )
    parser.add_argument("--resume", action="store_true", help="若 checkpoint 存在，则从 checkpoint 下一行续传")
    parser.add_argument(
        "--key-by-goods-id",
        action="store_true",
        help="使用 goodsId 作为 Kafka message key，便于同商品有序落同一分区",
    )
    parser.add_argument("--dry-run", action="store_true", help="只扫描文件统计行数，不写入 Kafka")

    args = parser.parse_args(argv)

    if args.start_line < 1:
        parser.error("--start-line 必须 >= 1")
    if args.max_lines is not None and args.max_lines < 1:
        parser.error("--max-lines 必须 >= 1")
    if args.checkpoint_interval < 1:
        parser.error("--checkpoint-interval 必须 >= 1")

    return Options(
        file_path=Path(args.file),
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        start_line=args.start_line,
        max_lines=args.max_lines,
        checkpoint_file=Path(args.checkpoint_file) if args.checkpoint_file else None,
        checkpoint_interval=args.checkpoint_interval,
        dry_run=args.dry_run,
        key_by_goods_id=args.key_by_goods_id,
        resume=args.resume,
    )


def main(argv: Optional[list[str]] = None) -> int:
    configure_logging()
    options = parse_args(argv)

    try:
        tracker = process_file(options)
    except (FileNotFoundError, ValueError, KafkaError):
        LOGGER.exception("文件导入 Kafka 失败")
        return 1

    return 1 if tracker.fail_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
