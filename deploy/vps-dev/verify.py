"""Report counts only, without printing DAG error bodies or credentials."""

import os
import urllib.error
import urllib.request
from importlib.metadata import version

from airflow.models import DagBag, DagModel, DagRun
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType

# Small, stable HTTPS endpoint; avoids depending on any one site's uptime.
# A hostname, not a bare IP: the probe must prove DNS resolution as well as
# routing, and a 204 endpoint answers without a body.
EGRESS_PROBE_URL = "https://www.google.com/generate_204"
EGRESS_PROBE_TIMEOUT_SECONDS = 5
# Maintenance DAGs that are meant to run on their own schedule before the cutover:
# they may be unpaused and may own scheduled runs. Every business DAG must stay paused.
ALWAYS_ON_DAGS = frozenset({"nas_archive"})


def check_scheduler_egress() -> None:
    """Assert scheduler outbound reachability matches EGRESS_INTERNAL.

    This process runs only inside the scheduler container (see how this
    script is invoked), which compose.yml attaches to both `runtime` and
    `egress`. `EGRESS_INTERNAL` is set on the scheduler environment for
    exactly this purpose, read the same way the other dev-tools scripts read
    their required variables (``os.environ[...]``, hard-failing if unset).

    This cannot observe another container's network namespace, so it cannot
    prove the ML sidecars stay egress-less from here: that half is verified
    by running `curl` directly inside a sidecar container (they carry curl,
    not Python), documented as existing acceptance evidence alongside this
    check rather than executed from this script.
    """
    egress_internal = os.environ["EGRESS_INTERNAL"].strip().lower() == "true"
    try:
        urllib.request.urlopen(EGRESS_PROBE_URL, timeout=EGRESS_PROBE_TIMEOUT_SECONDS)
        reachable = True
    except urllib.error.HTTPError:
        # Any HTTP status means the request left the container and a server
        # answered; HTTPError subclasses URLError, so it must be caught first.
        reachable = True
    except urllib.error.URLError:
        reachable = False
    if egress_internal:
        assert not reachable, "EGRESS_INTERNAL=true but the scheduler reached the internet"
    else:
        assert reachable, "EGRESS_INTERNAL=false but the scheduler could not reach the internet"
    print(f"Egress check: EGRESS_INTERNAL={egress_internal} reachable={reachable}")


def main():
    check_scheduler_egress()
    assert version("apache-airflow") == "2.11.1"
    bag = DagBag(include_examples=False)
    print(f"DAGs={len(bag.dags)} import_errors={len(bag.import_errors)}")
    assert bag.dags and not bag.import_errors, "DAG parsing is not clean"
    with create_session() as session:
        models = session.query(DagModel).filter(DagModel.is_active.is_(True)).all()
        assert models, "Scheduler has not registered DAGs yet"
        business = [model for model in models if model.dag_id not in ALWAYS_ON_DAGS]
        assert all(model.is_paused for model in business), "An active business DAG is unpaused"
        # Operators may trigger paused DAGs by hand on DEV; only the scheduler
        # must never have started anything on its own.
        unattended = (
            session.query(DagRun)
            .filter(DagRun.run_type != DagRunType.MANUAL, DagRun.dag_id.notin_(ALWAYS_ON_DAGS))
            .count()
        )
        assert unattended == 0, "Unexpected scheduled or backfill DAG runs"
        manual = session.query(DagRun).filter(DagRun.run_type == DagRunType.MANUAL).count()
    print(f"Airflow 2.11.1: parsed, business DAGs paused, zero unattended business runs, manual_runs={manual}")


if __name__ == "__main__":
    main()
