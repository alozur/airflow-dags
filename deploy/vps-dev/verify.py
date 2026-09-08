"""Report counts only, without printing DAG error bodies or credentials."""

from importlib.metadata import version

from airflow.models import DagBag, DagModel, DagRun
from airflow.utils.session import create_session


def main():
    assert version("apache-airflow") == "2.11.1"
    bag = DagBag(include_examples=False)
    print(f"DAGs={len(bag.dags)} import_errors={len(bag.import_errors)}")
    assert bag.dags and not bag.import_errors, "DAG parsing is not clean"
    with create_session() as session:
        models = session.query(DagModel).filter(DagModel.is_active.is_(True)).all()
        assert models, "Scheduler has not registered DAGs yet"
        assert all(model.is_paused for model in models), "An active DAG is unpaused"
        assert session.query(DagRun).count() == 0, "Unexpected business DAG runs"
    print("Airflow 2.11.1: parsed, paused, zero DAG runs")


if __name__ == "__main__":
    main()
