#!/usr/bin/env python3
"""Generates the proposed AWS target architecture diagram with official AWS icons.

The diagram is committed as a PNG so it renders on GitHub, and this script is
committed alongside it so the picture stays editable and reviewable instead of
being an opaque binary.

Regenerate with:

    make diagram

Requires graphviz (`brew install graphviz`) and the `diagrams` package, which
bundles the official AWS Architecture Icons.

Design points the diagram is meant to make:
  1. The API tier scales horizontally; the MATCHING tier cannot. A single symbol's
     book is a single writer, so it is exactly one task, with a standby that
     rebuilds from the log.
  2. Fund reservation happens against Aurora on the request path, so the
     no-double-spend guarantee survives multiple API replicas (row-level locks).
  3. The engine never touches money. Settlement is a separate consumer, and it is
     the only component writing credits and debits.

Each tier is drawn as ONE node rather than N replicas: replication is stated in
the label, which keeps the edge count low enough that the flow stays readable.
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, Glue, ManagedStreamingForKafka
from diagrams.aws.compute import Fargate
from diagrams.aws.database import Aurora, ElasticacheForRedis
from diagrams.aws.integration import SimpleQueueServiceSqs
from diagrams.aws.management import Cloudwatch
from diagrams.aws.network import ELB, Route53
from diagrams.aws.security import WAF
from diagrams.aws.storage import S3
from diagrams.onprem.client import Users

GRAPH_ATTR = {
    "fontsize": "26",
    "labelloc": "t",
    "pad": "0.75",
    "nodesep": "0.7",
    "ranksep": "1.5",
    "bgcolor": "white",
    "splines": "spline",
    "compound": "true",
}
NODE_ATTR = {"fontsize": "13"}
EDGE_ATTR = {"fontsize": "12"}

# Edge colours carry meaning, so the three concerns stay separable at a glance.
MONEY = "#B7472A"  # moves or guards funds
EVENTS = "#3B7DD8"  # the ordered event log
FLOW = "#5A5A5A"  # plain request flow
SUPPORT = "#9A9A9A"  # build, secrets, telemetry


def build() -> None:
    with Diagram(
        "Vibranium Order Book — proposed AWS target architecture",
        filename="docs/architecture/aws-architecture",
        outformat="png",
        show=False,
        direction="LR",
        graph_attr=GRAPH_ATTR,
        node_attr=NODE_ATTR,
        edge_attr=EDGE_ATTR,
    ):
        clients = Users("Clients /\ntrading bots")

        with Cluster("Edge"):
            dns = Route53("Route 53")
            waf = WAF("AWS WAF")
            alb = ELB("Application\nLoad Balancer")

        with Cluster("API tier — stateless, scales out"):
            api = Fargate("ECS Fargate\napi tasks x N")

        with Cluster("Matching tier — ONE task per symbol"):
            engine = Fargate("VIB order book\nsingle writer")
            standby = Fargate("standby\nrebuilds from log")

        with Cluster("Ordered log"):
            msk = ManagedStreamingForKafka("Amazon MSK\n1 partition / symbol")

        with Cluster("Settlement tier"):
            settlement = Fargate("ECS Fargate\nconsumer group")
            dlq = SimpleQueueServiceSqs("SQS\ndead letter")

        with Cluster("Data — source of truth"):
            aurora = Aurora("Aurora PostgreSQL\nMulti-AZ\nwallets · trades\norders · journal")
            redis = ElasticacheForRedis("ElastiCache Redis\nwallet read cache")

        with Cluster("Traceability archive"):
            bucket = S3("S3\ntrade archive")
            glue = Glue("Glue")
            athena = Athena("Athena")

        with Cluster("Observability"):
            logs = Cloudwatch("CloudWatch\nlogs · metrics\nalarms")

        # --- edge / request path (this chain sets the left-to-right ranking) ---
        clients >> Edge(color=FLOW) >> dns >> Edge(color=FLOW) >> waf >> Edge(color=FLOW) >> alb
        alb >> Edge(color=FLOW) >> api
        api >> Edge(color=FLOW, label="submit order") >> engine

        # --- event path ---
        engine >> Edge(color=EVENTS, style="bold", label="emit events\nkeyed by symbol") >> msk
        msk >> Edge(color=EVENTS, style="bold", label="consume") >> settlement

        # --- money path: settlement is the ONLY writer of credits and debits ---
        settlement >> Edge(color=MONEY, style="bold", label="atomic\ncredits / debits") >> aurora
        settlement >> Edge(color=MONEY, style="dashed", label="unsettleable") >> dlq

        # --- archive ---
        aurora >> Edge(color=SUPPORT, style="dashed", label="archive") >> bucket
        bucket >> Edge(color=SUPPORT, style="dashed") >> glue >> Edge(color=SUPPORT, style="dashed") >> athena

        # Secondary relationships use constraint=false so they are drawn without
        # dragging nodes out of the main flow's ranking.
        #
        # Fund reservation is the double-spend guard, and it is why the API tier
        # can scale: the lock lives in the database, not in the process.
        api >> Edge(color=MONEY, style="bold", constraint="false",
                    label="reserve funds\nrow lock per user") >> aurora
        api >> Edge(color=SUPPORT, style="dashed", constraint="false",
                    label="read balances") >> redis
        settlement >> Edge(color=SUPPORT, style="dashed", constraint="false",
                           label="invalidate") >> redis
        msk >> Edge(color=EVENTS, style="dashed", constraint="false",
                    label="replay to\nrebuild book") >> standby
        # Telemetry edges stay constrained so CloudWatch is ranked after the
        # tiers that emit to it, instead of drifting to the far left.
        engine >> Edge(color=SUPPORT, style="dotted") >> logs
        settlement >> Edge(color=SUPPORT, style="dotted") >> logs


if __name__ == "__main__":
    build()
    print("wrote docs/architecture/aws-architecture.png")
