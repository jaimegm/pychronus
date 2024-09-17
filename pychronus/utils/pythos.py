import hashlib
import logging
from pathlib import Path
from typing import Dict, Optional, Union

from airflow.exceptions import AirflowNotFoundException
from jinja2 import Template


def generate_hash_uuid(string: str):
    return hashlib.sha1((string).encode("utf-8")).hexdigest()


def keyify(original_key):
    replacements = [
        ("ß", "ss"),
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("-", "_"),
        ("–", "_"),
        (" ", "_"),
        (":", ""),
        ("(", ""),
        (")", ""),
        ("#", "count"),
        ("/", ""),
        (".", ""),
        (",", ""),
        ("?", ""),
        ("+", ""),
        ("___", "_"),
        ("__", "_"),
    ]
    replaced = original_key.lower()
    for umlaut, replacement in replacements:
        replaced = replaced.replace(umlaut, replacement)
    return replaced.lower()


def render_sql(sql_file: str, context: Optional[Dict[str, Union[str, int]]] = None):
    with open(sql_file, "r") as f:
        sql = " ".join(f.readlines())
        try:
            return Template(sql).render(context or {})
        except ValueError as ve:
            logging.error(f"The context value: {context} is not correct")
            raise ValueError(ve)


def get_sql_filepath(
    filename: str, context: Optional[Dict[str, Union[str, int]]] = None
) -> str:
    path = Path(__file__).resolve().parent.parent / "sql" / filename
    if path.exists():
        return render_sql(sql_file=path, context=context)

    logging.error(f"the file {filename} does not exist in the sql directory")
    raise AirflowNotFoundException()
