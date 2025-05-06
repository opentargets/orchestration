from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pyhocon
import yaml
from deepdiff.diff import DeepDiff

from orchestration.utils.path import GCSPath, IOManager

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

_parsers = {
    "yaml": yaml.safe_load,
    "conf": pyhocon.ConfigFactory.parse_string,
}


class AppConfig:
    def __init__(
        self,
        raw_config: str,
        parser: Callable | None,
        template_context: dict[str, str] | None = None,
    ):
        self.template_context = template_context or {}
        self.parser = parser or (lambda _: {})
        self.raw_config = raw_config
        self.rendered_config: str
        self.config: dict[str, Any]
        self.logger = logging.getLogger(__name__)

    def _render(self) -> None:
        self.rendered_config = self.raw_config
        self.logger.debug(f"rendering template with context {self.template_context}")
        for sentinel, value in self.template_context.items():
            self.rendered_config = self.rendered_config.replace(f"{{{{{sentinel}}}}}", value)

    def _parse(self) -> None:
        self.config = self.parser(self.rendered_config)

    @classmethod
    def from_file(
        cls,
        file_path: str | Path,
        client: Any = None,
        template_context: dict[str, str] | None = None,
    ) -> AppConfig:
        """Create an AppConfig instance from a file.

        Args:
            file_path (Path | str): Path or URI to the configuration file.
            client (Any): Optional client to use for file access. Defaults to None.
            template_context (dict[str, str], optional): Template context to use
                in rendering. Optional.

        Returns:
            AppConfig: An instance of AppConfig.
        """
        if isinstance(file_path, Path):
            file_path = str(file_path.resolve())
        m = IOManager().resolve(path=file_path)
        if client and isinstance(m, GCSPath):
            m._client = client
        c = m.load_str()
        parser = _parsers.get(file_path.split(".")[-1])

        c = cls(c, parser=parser, template_context=template_context)
        c._render()
        c._parse()
        return c

    def get(self, key_or_path: str, default: Any = None) -> Any:
        """Get a value from the parsed configuration.

        This method allows passing a path using dot notation to access nested
        values in the configuration. Arrays are not supported as there is no
        use case yet.

        Args:
            key_or_path (str): Key or path to look up in the configuration.
            default (Any): Default value to return if the key is not found. If
                not provided, `None` will be returned when the key is not found.

        Returns:
            Any: Value associated with the key.
        """
        if "." not in key_or_path:
            return self.config.get(key_or_path, default)
        keys = key_or_path.split(".")
        current = self.config
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    def __le__(self, other: AppConfig) -> bool:
        if self.parser != other.parser:
            return False

        # compare all keys but steps
        d = DeepDiff(self.config, other.config, exclude_paths=["steps"])
        if d:
            self.logger.info(f"configs are different: {d}")
            return False

        # inside of steps, compare only those present in other
        for s, v in other.get("steps", {}).items():
            d = DeepDiff(v, self.get("steps", {}).get(s))
            if d:
                self.logger.info(f"configs are different: {d}")
                return False

        self.logger.info("configs are equal")
        return True
