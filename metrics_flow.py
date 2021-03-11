#!/usr/bin/env python3
"""
# Site availability monitor

While all this might be done using plain functions and promitive types, I decided to put a little efforts in extracting several entities into classes:
- ProbeResult is a data-transfer object to keep the HTTP request result;
- Probe is an implementation of the probing scenario, incapsulating retries, timeouts, custom SSL sertificates, etc;
- PostgresqlWritter is a file-like class to maintain the connection and provide a way to write into database;

"""

import argparse
import contextlib
import dataclasses
import json
import logging
import os
import re
import sys
import time
from typing import Optional, Sequence
from urllib.request import urlopen, Request

import kafka
import psycopg2

logger = logging.getLogger(__name__)

# Bound to database's table schema
TABLE = "test"
COLUMNS = (
    "timestamp",
    "target_url",
    "http_status",
    "latency_ms",
    "error_text",
    "pattern",
    "is_pattern_found",
)


@dataclasses.dataclass
class ProbeResult:
    """Let's not use dictionaries to pass the data around."""

    timestamp: float
    target: str
    error: Optional[str]
    latency_ms: Optional[float]
    status: Optional[int]
    pattern: Optional[str]
    is_pattern_found: Optional[bool]


class Probe:
    """Incapsulates logic on reachnig the target site.

    Currently only HTTP GET methos is supported.
    The time measurements aren't very much accurate.
    If required, pattern search happens right after getting a HTTP response.
    Error handling is intentionally generic.
    """

    def __init__(self, url, timeout=None, pattern=None):
        self.target = url
        self.timeout = timeout
        self.pattern = pattern
        self.re_pattern = re.compile(pattern.encode()) if pattern else None
        self.request = Request(url)  # Extension point: method, payload, ssl

    def check(self) -> ProbeResult:
        try:
            # Shoot an HTTP request with time-measurment for the poor
            start = time.time()
            resp = urlopen(self.request, timeout=self.timeout)
            latency = time.time() - start
            logger.info(
                "Got reponse from %s as %s in %s sec", self.target, resp.status, latency
            )

            # Look up for a pattern if required
            is_pattern_found = None
            if self.pattern:
                # Read response bytes into memory only when required
                is_pattern_found = bool(self.re_pattern.search(resp.read(), re.M))

            return ProbeResult(
                timestamp=start,
                target=self.target,
                error=None,
                latency_ms=latency * 1000,
                status=resp.status,
                pattern=self.pattern,
                is_pattern_found=is_pattern_found,
            )

        # Catches network timeout, no route to host, ssl-handshake, etc
        except Exception as exc:
            logger.error("Failed to fetch %s : %r", self.target, exc)
            return ProbeResult(
                timestamp=start,
                target=self.target,
                error=str(exc),
                latency_ms=None,
                status=None,
                pattern=None,
                is_pattern_found=None,
            )


@contextlib.contextmanager
def get_kafka_producer(broker: str, **ssl_kwargs):
    producer = kafka.KafkaProducer(
        client_id="writer",
        security_protocol="SSL" if ssl_kwargs else "PLAINTEXT",
        bootstrap_servers=[broker],
        retries=3,  # Network is unreliable!
        # Serialization for the poor. A binary format should be used in prod
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="gzip",  # Compression is nice when it's "free"
        **ssl_kwargs,
    )

    yield producer
    producer.close()


def produce_forever(
    broker: str,
    topic: str,
    probe: Probe,
    timeout: Optional[float] = None,
    delay: Optional[float] = None,
    **kwargs,
):
    logger.info("Connecting to Kafka Broker at %s", broker)
    with get_kafka_producer(broker, **kwargs) as producer:
        while True:
            probe_result = probe.check()
            msg = probe_result.__dict__
            logger.info("Sending message to %s topic", topic)
            producer.send(topic, value=msg)  # async delivery
            if delay:
                logger.debug("Sleeping for %d sec before the next probe")
                time.sleep(delay)


class PostgresqlWritter:
    """A file-like interface to write to a database."""

    def __init__(self, dsn: str, table: str, columns: Sequence[str]):
        self.dsn = dsn
        self.table = table
        self.columns = columns
        columns_str = ",".join('"{}"'.format(c) for c in self.columns)
        values_str = ",".join(f"%({c})s" for c in columns)
        self.sql = f"INSERT INTO {self.table} ({columns_str}) VALUES ({values_str});"  # No ORMs :)
        self.connection = None

    def _connect(self):
        logger.debug(f"Connecting to postgres server")
        self.connection = psycopg2.connect(self.dsn)
        self.connection.autocommit = True  # The most reliable way

    def _write(self, probe_result: ProbeResult):
        # Explicit matching to db-columns
        values_dict = dict(
            timestamp=probe_result.timestamp,
            target_url=probe_result.target,
            http_status=probe_result.status,
            latency_ms=probe_result.latency_ms,
            error_text=probe_result.error,
            pattern=probe_result.pattern,
            is_pattern_found=probe_result.is_pattern_found,
        )
        with self.connection.cursor() as cursor:
            logger.debug(f"Executing SQL {self.sql} with {values_dict}")
            cursor.execute(self.sql, values_dict)

    def write(self, probe_result: ProbeResult) -> bool:
        try:
            if not self.connection:
                self._connect()
            self._write(probe_result)
            return True
        except Exception as exc:
            logger.error("Failed to write to database %s : %s", exc)
            return False


@contextlib.contextmanager
def get_kafka_consumer(broker: str, topic: str, **ssl_kwargs):
    consumer = kafka.KafkaConsumer(
        topic,
        group_id="reader",
        security_protocol="SSL" if ssl_kwargs else "PLAINTEXT",
        bootstrap_servers=[broker],
        value_deserializer=json.loads,
        enable_auto_commit=False,  # Do not acknowlege until written to DB
        auto_offset_reset="earliest",
        **ssl_kwargs,
    )

    yield consumer
    consumer.close()


def consume_forever(broker: str, topic: str, writer: PostgresqlWritter, **kwargs):
    logger.debug("Connecting", broker, "to", topic)
    with get_kafka_consumer(broker, topic, **kwargs) as consumer:
        logger.info("Connected")
        for message in consumer:
            logger.debug("Got a message %s", message)
            is_ok = writer.write(ProbeResult(**message.value))
            if is_ok:  # Commit only when written to database
                consumer.commit()


def main(args):
    if args.ssl_ca_file and args.ssl_cert_file and args.ssl_key_file:
        ssl_kwargs = dict(
            ssl_cafile=args.ssl_ca_file,
            ssl_certfile=args.ssl_cert_file,
            ssl_keyfile=args.ssl_key_file,
        )
    else:
        ssl_kwargs = {}

    if args.mode == "pub":
        probe = Probe(args.target, timeout=args.timeout, pattern=args.pattern)
        produce_forever(
            args.kafka_broker,
            args.kafka_topic,
            probe,
            delay=args.delay,
            **ssl_kwargs,
        )

    elif args.mode == "sub":
        writer = PostgresqlWritter(
            args.postgresql_dsn,
            TABLE,
            COLUMNS,
        )
        consume_forever(args.kafka_broker, args.kafka_topic, writer, **ssl_kwargs)


def build_parser():
    desc = """
    Site availability monitor to store simple metrics to a database.
    Works in PUB or SUB mode.
    PUB mode performs availability checks and reports to Kafka.
    SUB mode watches a Kafka topic and saves events to PostgreSQL table.
    """
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument(
        "mode",
        choices=["pub", "sub"],
        help="Monitor a target site and produce messages vs consume messages and store them to database.",
    )
    parser.add_argument(
        "--kafka-broker",
        default=os.getenv("KAFKA_BROKER"),
        help="Address of a Kafka Broker to use. Env var KAFKA_BROKER.",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.getenv("KAFKA_TOPIC"),
        help="A name of the topic to use with the Kafka Broker. Env var KAFKA_TOPIC",
    )

    parser.add_argument(
        "--postgresql-dsn",
        default=os.getenv("POSTGRESQL_DSN"),
        help="PostgreSQL connection string to use in SUB work mode. Env var POSTGRESQL_DSN",
    )

    parser.add_argument(
        "--target",
        default=os.getenv("TARGET"),
        help="An URL to send probes to in PUB work mode. Env var TARGET",
    )
    parser.add_argument(
        "--delay",
        default=float(os.getenv("DELAY", 30)),
        type=float,
        help="Delay between two attempts to reach the target site in the PUB mode. Defaults to 60 sec. Env var DELAY",
    )
    parser.add_argument(
        "--timeout",
        default=float(os.getenv("TIMEOUT", 10)),
        type=float,
        help="Timeout to use while querying the target site in PUB mode. Defaults to 60 sec. Env var TIMEOUT",
    )
    parser.add_argument(
        "--pattern",
        default=os.getenv("PATTERN"),
        help="A regular expression to search in the target's response. Env var PATTERN",
    )

    parser.add_argument(
        "--ssl-ca-file",
        default=os.getenv("SSL_CA_FILE"),
        help="A file holding certificate authority to use in Kafka auth. Env var SSL_CA_FILE",
    )
    parser.add_argument(
        "--ssl-cert-file",
        default=os.getenv("SSL_CERT_FILE"),
        help="A file holding the certificate to use in Kafka auth. Env var SSL_CERT_FILE",
    )
    parser.add_argument(
        "--ssl-key-file",
        default=os.getenv("SSL_KEY_FILE"),
        help="A file holding private key of the certificate to authenticate to Kafka. Env var SSL_KEY_FILE",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=os.getenv("VERBOSE"),
        help="Show more logs. Env var VERBOSE",
    )

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)

    # Those arguments might be specified via env vars as well
    all_is_set = args.kafka_broker and args.kafka_topic
    if args.mode == "pub":
        all_is_set = all_is_set and args.target
    elif args.mode == "sub":
        all_is_set = all_is_set and args.postgresql_dsn

    if not all_is_set:
        logger.critical("Specific arguments are required for %s work mode", args.mode)
        parser.print_help()
        exit(1)

    try:
        main(args)
    except KeyboardInterrupt:
        exit(0)
