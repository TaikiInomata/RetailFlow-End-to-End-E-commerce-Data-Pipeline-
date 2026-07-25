"""
callbacks.py — Airflow Failure/Success Callbacks dùng chung cho tất cả DAGs.

Cách dùng:
    from utils.callbacks import on_failure_callback
    default_args = {"on_failure_callback": on_failure_callback, ...}
"""
import logging

logger = logging.getLogger("airflow.task")


def on_failure_callback(context: dict) -> None:
    """
    Callback khi Task fail hoàn toàn (đã hết retry).
    
    Hiện tại: In log chi tiết ra Airflow Task Log.
    Mở rộng: Thêm Slack Webhook / PagerDuty alert bên dưới.
    """
    dag_id      = context["dag"].dag_id
    task_id     = context["task_instance"].task_id
    run_id      = context["run_id"]
    log_url     = context["task_instance"].log_url
    exception   = context.get("exception", "N/A")

    logger.error(
        "\n"
        "═══════════════════════════════════════════════\n"
        "  ❌  AIRFLOW TASK FAILED\n"
        "═══════════════════════════════════════════════\n"
        f"  DAG     : {dag_id}\n"
        f"  Task    : {task_id}\n"
        f"  Run ID  : {run_id}\n"
        f"  Error   : {exception}\n"
        f"  Log URL : {log_url}\n"
        "═══════════════════════════════════════════════\n"
    )

    # ---------------------------------------------------------------
    # MỞ RỘNG: Gửi Slack Webhook (bỏ comment khi đã có Slack token)
    # ---------------------------------------------------------------
    # import requests
    # slack_webhook_url = Variable.get("slack_webhook_url")
    # requests.post(slack_webhook_url, json={
    #     "text": f":red_circle: *[{dag_id}]* Task `{task_id}` failed!\n<{log_url}|View Log>"
    # })
    # ---------------------------------------------------------------
