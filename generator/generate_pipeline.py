import json
import os
import sys
from jinja2 import Environment, FileSystemLoader

from template_registry import TEMPLATE_REGISTRY


def load_workflow_plan(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def render_template(template_path: str, context: dict) -> str:
    template_dir, template_file = os.path.split(template_path)
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template(template_file)
    return template.render(**context)


def generate_task_artifacts(workflow: dict) -> None:
    tasks = workflow.get("tasks", [])

    for task in tasks:
        template_name = task["template"]

        if template_name not in TEMPLATE_REGISTRY:
            raise ValueError(f"Template '{template_name}' not found in registry")

        registry_entry = TEMPLATE_REGISTRY[template_name]

        template_path = registry_entry["template_path"]
        output_dir = registry_entry["output_dir"]
        output_ext = registry_entry["output_ext"]
        output_name_tpl = registry_entry["output_name"]

        ensure_dir(output_dir)

        output_name = output_name_tpl.format(task_id=task["id"])
        output_path = os.path.join(output_dir, output_name + output_ext)

        context = {
            "task_id": task["id"],
            **task.get("params", {})
        }

        rendered = render_template(template_path, context)

        with open(output_path, "w") as f:
            f.write(rendered)

        print(f"Generated: {output_path}")


def generate_dag(workflow: dict) -> None:
    dag_registry = TEMPLATE_REGISTRY["airflow_dag"]

    ensure_dir(dag_registry["output_dir"])

    context = {
        "dag_id": workflow["dag_id"],
        "schedule": workflow["schedule"],
        "tasks": workflow["tasks"]
    }

    rendered = render_template(
        dag_registry["template_path"],
        context
    )

    output_name = dag_registry["output_name"].format(dag_id=workflow["dag_id"])
    output_path = os.path.join(
        dag_registry["output_dir"],
        output_name + dag_registry["output_ext"]
    )

    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Generated DAG: {output_path}")


def main():
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: python generate_pipeline.py <workflow_plan.json>")

    workflow_path = sys.argv[1]
    workflow = load_workflow_plan(workflow_path)

    generate_task_artifacts(workflow)
    generate_dag(workflow)


if __name__ == "__main__":
    main()
