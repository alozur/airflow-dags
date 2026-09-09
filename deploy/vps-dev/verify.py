"""Report counts only, without printing DAG error bodies or credentials."""

from importlib.metadata import version

from airflow.models import DagBag, DagModel, DagRun
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType


def main():
    assert version("apache-airflow") == "2.11.1"
    bag = DagBag(include_examples=False)
    print(f"DAGs={len(bag.dags)} import_errors={len(bag.import_errors)}")
    assert bag.dags and not bag.import_errors, "DAG parsing is not clean"
    with create_session() as session:
        models = session.query(DagModel).filter(DagModel.is_active.is_(True)).all()
        assert models, "Scheduler has not registered DAGs yet"
        assert all(model.is_paused for model in models), "An active DAG is unpaused"
        # Operators may trigger paused DAGs by hand on DEV; only the scheduler
        # must never have started anything on its own.
        unattended = session.query(DagRun).filter(DagRun.run_type != DagRunType.MANUAL).count()
        assert unattended == 0, "Unexpected scheduled or backfill DAG runs"
        manual = session.query(DagRun).filter(DagRun.run_type == DagRunType.MANUAL).count()
    print(f"Airflow 2.11.1: parsed, paused, zero unattended DAG runs, manual_runs={manual}")


if __name__ == "__main__":
    main()
