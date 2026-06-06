"""Tests for utils/git_sync_dag.py — DAG load and task chain."""

from __future__ import annotations


class TestGitSyncDAGLoads:

    def test_dag_loads(self):
        from utils.git_sync_dag import dag
        assert dag is not None
        assert dag.dag_id == "git_sync_dag"

    def test_dag_has_four_tasks(self):
        from utils.git_sync_dag import dag
        assert len(dag.tasks) == 4

    def test_dag_schedule_is_none(self):
        from utils.git_sync_dag import dag
        assert dag.schedule is None

    def test_task_chain(self):
        from utils.git_sync_dag import dag
        configure = dag.get_task("configure_git")
        pull = dag.get_task("git_pull")
        status = dag.get_task("show_status")
        trigger = dag.get_task("trigger_migrations")

        assert pull in configure.downstream_list
        assert status in pull.downstream_list
        assert trigger in status.downstream_list

    def test_trigger_targets_run_migrations(self):
        from utils.git_sync_dag import dag
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

        trigger = dag.get_task("trigger_migrations")
        assert isinstance(trigger, TriggerDagRunOperator)
        assert trigger.trigger_dag_id == "run_migrations"

    def test_trigger_does_not_wait_for_completion(self):
        from utils.git_sync_dag import dag

        trigger = dag.get_task("trigger_migrations")
        assert trigger.wait_for_completion is False
