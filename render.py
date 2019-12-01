import os
import sys
import yaml
from jinja2 import Environment, FileSystemLoader
from yapf.yapflib.yapf_api import FormatFile

templates_path = sys.argv[1]
manifests_path = sys.argv[2]
output_path = sys.argv[3]


def load_templates():
    return Environment(loader=FileSystemLoader(templates_path))


def load_manifests(path):
    with open(path) as f:
        return yaml.load(f)


def render_dags(manifest):
    templates = load_templates()
    for dag in manifest:
        template = templates.get_template(dag.get('template'))
        dag_name = dag.get("dag_name")
        output_file = output_path + dag_name + '.py'
        rendered_dag = template.render(dag)

        with open(output_file,'wb') as fh:
            fh.write(rendered_dag.encode())
            fh.close()
            FormatFile(output_file, in_place=True, style_config='pep8')


for root, dirs, files in os.walk(os.path.abspath(manifests_path)):
    for file in files:
        current_manifest = os.path.join(root, file)
        render_dags(load_manifests(current_manifest))
