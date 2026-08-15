"""Print readings off a Sentinel topic. The proof that step 1 works.

    python scripts/tail_topic.py sentinel.system --from-start --limit 5
"""

from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Tail a Sentinel Kafka topic")
    parser.add_argument("topic")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="sentinel-tail")
    parser.add_argument("--limit", type=int, default=0, help="stop after N messages")
    parser.add_argument("--from-start", action="store_true")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--raw", action="store_true", help="print the payload unformatted")
    args = parser.parse_args()

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest" if args.from_start else "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    seen = 0
    idle = 0.0
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1.0
                if idle >= args.timeout:
                    print(f"no messages for {args.timeout:.0f}s, stopping", file=sys.stderr)
                    break
                continue
            if message.error():
                print(f"error: {message.error()}", file=sys.stderr)
                continue

            idle = 0.0
            seen += 1
            payload = message.value().decode("utf-8")
            if args.raw:
                print(payload)
            else:
                record = json.loads(payload)
                print(
                    f"[{record['ts']}] {record['host']} ({record['role']}) "
                    f"{record['kind']} p{message.partition()}@{message.offset()}"
                )
            if args.limit and seen >= args.limit:
                break
    finally:
        consumer.close()

    print(f"{seen} messages", file=sys.stderr)
    return 0 if seen else 1


if __name__ == "__main__":
    sys.exit(main())
