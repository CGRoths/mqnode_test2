from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ListResponse(BaseModel):
    count: int
    limit: int
    offset: int
    interval: str | None = None
    items: list[dict[str, Any]]
