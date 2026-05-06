import abc
import hashlib
import logging
import random
import string
from pathlib import Path
from typing import Dict, Optional, Union

from airflow.exceptions import AirflowNotFoundException
from jinja2 import Template


class PathDict:
    def __init__(self, data):
        self.data = data

    def __getitem__(self, path):
        path_items = path.split(".")
        sub_dict = self.data
        for path_item in path_items:
            sub_dict = sub_dict[path_item]
        return sub_dict

    def get(self, path, default_value=None):
        path_items = path.split(".")
        sub_dict = self.data
        for index, path_item in enumerate(path_items):
            sub_dict = sub_dict.get(path_item)
            if index == len(path_items) - 1:
                return sub_dict
            if sub_dict is None or not isinstance(sub_dict, abc.Mapping):
                return default_value
        return sub_dict


def generate_hash_uuid(string: str):
    return hashlib.sha1((string).encode("utf-8")).hexdigest()


def create_md5_hash(key: str) -> str:
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def get_random_suffix(suffix_length=6):
    return "".join(random.choices(string.ascii_letters, k=suffix_length))


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


def get_sql_query(
    filename: str, context: Optional[Dict[str, Union[str, int]]] = None
) -> str:
    path = Path(__file__).resolve().parent.parent / "sql" / filename
    if path.exists():
        return render_sql(sql_file=path, context=context)

    logging.error(f"the file {filename} does not exist in the sql directory")
    raise AirflowNotFoundException()
