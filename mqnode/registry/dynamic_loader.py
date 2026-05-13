from __future__ import annotations

from importlib import import_module


def load_function(module_path: str, function_name: str):
    module = import_module(module_path)
    return getattr(module, function_name)
