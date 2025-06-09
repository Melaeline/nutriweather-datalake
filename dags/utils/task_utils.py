"""
Utility functions for DAG tasks.
"""
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from config.dag_config import AIRFLOW_HOME


def create_script_task(task_id: str, script_name: str, **kwargs) -> BashOperator:
    """Create a standardized bash task for running Python scripts."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {AIRFLOW_HOME} && python include/scripts/{script_name}",
        cwd=AIRFLOW_HOME,
        **kwargs
    )


def create_trigger_task(
    task_id: str, 
    target_dag_id: str, 
    wait_for_completion: bool = False,
    **kwargs
) -> TriggerDagRunOperator:
    """Create a standardized trigger task."""
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=target_dag_id,
        wait_for_completion=wait_for_completion,
        poke_interval=30 if wait_for_completion else None,
        allowed_states=["success"] if wait_for_completion else None,
        failed_states=["failed"] if wait_for_completion else None,
        **kwargs
    )
