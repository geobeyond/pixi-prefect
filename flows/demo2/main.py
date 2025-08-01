import sys

from cowpy import cow
from prefect import flow


@flow(log_prints=True)
def my_workflow(name: str = "world") -> str:
    my_cow = cow.get_cow()()
    message = F"Hello, {name.capitalize()}!"
    formatted_message = my_cow.milk(message)
    print(f"running from environment at {sys.executable}")
    print(formatted_message)
    return message


if __name__ == "__main__":
    my_workflow.serve(name="my_demo2_local_deployment")
