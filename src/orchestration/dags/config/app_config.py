from __future__ import annotations

import copy
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

_parsers: dict[str, Callable] = {
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
        self.is_rendered = False
        self.is_parsed = False

    def _render(self) -> None:
        if not self.is_rendered:
            self.rendered_config = self.raw_config
            self.logger.debug(f"rendering template with context {self.template_context}")
            for sentinel, value in self.template_context.items():
                self.rendered_config = self.rendered_config.replace(f"{{{{{sentinel}}}}}", value)
            self.is_rendered = True

    def _parse(self) -> None:
        if not self.is_parsed:
            self.config = self.parser(self.rendered_config)
            self.is_parsed = True

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
        conf = m.load_str()
        parser = _parsers.get(file_path.split(".")[-1])

        c = cls(raw_config=conf, parser=parser, template_context=template_context)
        c._render()
        c._parse()
        return c

    def overwrite(self, file_path: str | Path) -> AppConfig:
        """Overwrite the current configuration with another configuration from the given file.

        Note:
        ----
        In case the override `file_path` does not exist, current configuration is returned as is.

        Args:
            file_path (str | Path): Path or URI to the configuration file to merge with.

        Returns:
            AppConfig: A new AppConfig instance with the merged configuration.

        Note:
        -----
        This method forces the parsing and rendering of the current and other configuration.
        Other configuration is parsed using the same template_context as self.
        """
        # Ensure both configs are rendered and parsed before attempting to merge.
        if not Path(file_path).exists():
            return self
        other = AppConfig.from_file(file_path, template_context=self.template_context)

        if not self.is_rendered:
            self._render()
        if not self.is_parsed:
            self._parse()
        if not other.is_rendered:
            other._render()
        if not other.is_parsed:
            other._parse()

        return AppConfigMerger(self, other).merge()

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


class AppConfigMerger:
    def __init__(self, base_config: AppConfig, override_config: AppConfig):
        """Merge two AppConfig instances together by overriding the `base_config` with the `override_config`.

        Args:
            base_config (AppConfig): The base configuration to merge from.
            override_config (AppConfig): The configuration to merge into the base configuration.
        """
        self.logger = logging.getLogger(__name__)
        self.parser = base_config.parser
        self.original_config = copy.deepcopy(base_config.config)
        self.base_config = base_config.config.get("steps", {})
        self.config_override = override_config.config.get("steps", {})
        if not self.config_override:
            raise ValueError("Config overwrite must contain `steps` key.")
        if not self.base_config:
            raise ValueError("Base config must contain `steps` key.")

    def merge(self) -> AppConfig:
        """Merge the base configuration with the overwrite configuration.

        Note:
        ----
        This method merges the two configuration with DeepMerge.
        See https://deepmerge.readthedocs.io/en/latest/guide.html#custom-strategies for more details.

        Note:
        ----
        The merge strategy does following:
        * merge two dictionaries
        * override the list values from `base_config` with the values from `override_config`
        * In case of conflicting types between the two configurations the override is preferred.

        Note:
        ----
        The merge should be done after both configurations are rendered and parsed.

        Examples:
        --------
        >>> logging.getLogger().handlers = []  # remove all handlers
        >>> base_config = '''
        ... ---
        ... scratch: /work/dir
        ... steps:
        ...   step_a:
        ...     params:
        ...       step: STEP_A # Kept as is
        ...
        ...   step_b:
        ...     params:
        ...       step: STEP_B
        ...       step.path:
        ...         - gs://study_1 # overridden by test_gentropy.overrides.yaml
        ... '''

        >>> override_config = '''
        ... ---
        ... steps:
        ...   step_b:
        ...     params:
        ...       step.path:
        ...         - gs://study_2 # Overriding the original step_b.step.path configuration
        ...       step.write_mode: overwrite # Amending to the original step_b.step.write_mode configuration
        ... '''

        >>> merged_config = '''
        ... ---
        ... scratch: /work/dir
        ... steps:
        ...   step_a:
        ...     params:
        ...       step: STEP_A
        ...   step_b:
        ...     params:
        ...       step: STEP_B
        ...       step.path:
        ...         - gs://study_2
        ...       step.write_mode: overwrite
        ... '''

        >>> expected_config = yaml.safe_load(merged_config)
        >>> bc = AppConfig(base_config, yaml.safe_load)
        >>> oc = AppConfig(override_config, yaml.safe_load)
        >>> oc._render()
        >>> oc._parse()
        >>> bc._render()
        >>> bc._parse()
        >>> merged_config = AppConfigMerger(bc, oc).merge()
        >>> assert merged_config.is_parsed and merged_config.is_rendered
        >>> assert not DeepDiff(expected_config, merged_config.config)
        """
        from deepmerge.merger import Merger

        merger = Merger([(list, ["override"]), (dict, ["merge"])], ["override"], ["override"])
        self.logger.info("Creating deep copy of the base config before merging.")
        # NOTE: The merge results in the in-place modification of the `base_config`.
        # See https://deepmerge.readthedocs.io/en/latest/guide.html#merges-are-destructive
        self.logger.info(
            "Merging base config with overwrite config using DeepMerge. "
            "This will override the list values from base config with the values from overwrite config."
        )
        merged_config = merger.merge(self.base_config, self.config_override)

        diff = DeepDiff(self.original_config.get("steps"), merged_config)
        if not diff:
            self.logger.warning("No differences found after merging the configurations.")
        else:
            self.logger.info(f"Configurations overwritten in: {diff.affected_root_keys}")
            self.original_config.update(steps=merged_config)
            self.logger.info("Configuration successfully merged.")

        self.logger.info("Reconstructing top level fields of the original AppConfig.")
        raw_config = yaml.dump(self.original_config)
        ac = AppConfig(raw_config=raw_config, parser=self.parser)
        ac._render()
        ac._parse()
        return ac
