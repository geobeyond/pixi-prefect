from pathlib import Path

from prefect import flow

@flow(log_prints=True)
def my_workflow(name: str = "world") -> str:
    return f"Hello, {name.capitalize()}!"


# if __name__ == "__main__":
#     (
#         my_workflow
#         .from_source(
#             source=str(Path(__file__).parent),
#             entrypoint="main.py:my_workflow"
#         )
#         .deploy(
#             name="my_workflow_deployment",
#             work_pool_name="process-pool"
#         )
#     )