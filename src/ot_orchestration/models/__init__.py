"""Pydantic models."""

from pydantic import BaseModel


class GCSMountModel(BaseModel):
    remote_path: str
    mount_point: str
