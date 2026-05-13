"""Runnable specification for Google Batch tasks."""

from __future__ import annotations

import re
from functools import cached_property
from importlib.resources import files
from typing import Annotated

from google.cloud import batch_v1
from pydantic import BaseModel, StringConstraints, model_validator


class RunnableSpec(BaseModel):
    """Runnable specification."""

    image_uri: Annotated[str, StringConstraints(min_length=1)]
    """Container image to be used for the batch task."""

    entrypoint: str = "/bin/bash"
    """Entrypoint for the batch task. Default is /bin/bash."""

    inline_commands: list[str] | None = None
    """Commands to be executed in the batch task.
        Mutually exclusive with `script_file` field. If both are provided, a validation error will be raised.
        If neither are provided, a validation error will be raised."""

    script_file: str | None = None
    """Name of the script file relative to `src.orchestration.assets` directory to use for task execution.
        Mutually exclusive with `commands` field. If both are provided, a validation error will be raised.
        If neither is provided, a validation error will be raised."""

    script_variables: dict[str, str] | None = None
    """Optional mapping of variable names to values to substitute into the script file.
        Sentinels in the script must follow the bash variable syntax: ``${variable_name}``."""

    @model_validator(mode="after")
    def _validate_oneof_script_or_commands(self) -> RunnableSpec:
        if not ((self.inline_commands is not None) ^ (self.script_file is not None)):
            raise ValueError("Exactly one of 'inline_commands' or 'script_file' must be provided, not both or neither.")
        return self

    @model_validator(mode="after")
    def _validate_script_file_exists(self) -> RunnableSpec:
        if self.script_file:
            script_path = files("orchestration.assets").joinpath(self.script_file)
            if not script_path.is_file():
                raise FileNotFoundError(
                    f"Script file '{self.script_file}' not found in 'orchestration.assets' package."
                )
        return self

    @cached_property
    def commands(self) -> list[str]:
        if self.inline_commands:
            return self.inline_commands
        elif self.script_file:
            # Assuming that the script file is located in the `src.orchestration.assets` package
            script_content = self._find_script_file(self.script_file)
            if self.script_variables:
                script_content = self._apply_template(script_content, self.script_variables)
            script_commands = self._parse_script_file(script_content)
            return ["-c", script_commands]
        else:
            raise ValueError("Either 'inline_commands' or 'script_file' must be provided to create a Runnable.")

    def build(self) -> batch_v1.Runnable:
        """Get the commands to be executed in the batch task.

        The method constructs a `google.cloud.batch_v1.Runnable` of type `CONTAINER`
        using the provided `image_uri`, `commands` or `script_file`, and `entrypoint`.


        If `commands` are provided, they will be used directly in the `Runnable`.
        If a `script_file` is provided instead, the method will read the script file from
        the `src.orchestration.assets` package, parse it to extract the commands, and use those commands in the `Runnable`.


        Returns:
            batch_v1.Runnable: A `google.cloud.batch_v1.Runnable` object constructed from the provided specifications.

        Example:
        ---
        >>> spec = RunnableSpec(image_uri="gcr.io/project/image:latest", inline_commands=["echo", "hello"])
        >>> runnable = spec.build()
        >>> isinstance(runnable, batch_v1.Runnable)
        True
        >>> runnable.container.image_uri
        'gcr.io/project/image:latest'
        >>> list(runnable.container.commands)
        ['echo', 'hello']
        """
        container = batch_v1.Runnable.Container(
            image_uri=self.image_uri,
            entrypoint=self.entrypoint,
            commands=self.commands,
        )

        return batch_v1.Runnable(container=container)

    @classmethod
    def _apply_template(cls, script_content: str, variables: dict[str, str]) -> str:
        """Replace ``${key}`` sentinels in the script content with the provided values.

        Example:
        ---
        >>> RunnableSpec._apply_template("echo ${greeting}", {"greeting": "hello"})
        'echo hello'
        """
        for key, value in variables.items():
            script_content = script_content.replace(f"${{{key}}}", value)
        return script_content

    @classmethod
    def _find_script_file(cls, script_name: str) -> str:
        """Find the script file in the `src.orchestration.assets` package and return its content."""
        script_path = files("orchestration.assets").joinpath(script_name)
        if not script_path.is_file():
            raise FileNotFoundError(f"Script file '{script_name}' not found in 'orchestration.assets' package.")
        return script_path.read_text()

    @classmethod
    def _parse_script_file(cls, script_content: str) -> str:
        r"""Parse the entry script file to extract all of the commands.

        This method
        1. removes comments and empty lines,
        2. joins lines that are continued with a backslash (\\),
        3. splits each joined line on ``;`` to separate co-located commands,
        4. joins all resulting commands with ``&&``.

        ``||`` and ``&&`` operators within a line are never split, so guard
        clauses of the form ``[[ check ]] || { echo error; exit 1; }`` are
        preserved as a single logical unit and their conditional semantics
        remain intact.

        Example:
        ---
        >>> script = "# comment\necho hello\\\n    world\nfoo; bar"
        >>> RunnableSpec._parse_script_file(script)
        'echo hello world && foo && bar'
        """
        lines = cls._remove_comments(script_content)
        joined_lines = cls._join_continuation_lines(lines)
        commands = cls._split_commands(joined_lines)
        return " && ".join(commands)

    @staticmethod
    def _remove_comments(script: str) -> list[str]:
        r"""Remove comment lines (starting with ``#``) and empty lines from a shell script.

        Example:
        ---
        >>> RunnableSpec._remove_comments("# comment\necho hello\n\necho world")
        ['echo hello', 'echo world']
        """
        lines = []
        for line in script.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or not stripped:
                continue
            lines.append(line)
        return lines

    @staticmethod
    def _join_continuation_lines(lines: list[str]) -> list[str]:
        r"""Join lines that end with a backslash continuation into a single line.

        Example:
        ---
        >>> RunnableSpec._join_continuation_lines(["echo hello \\", "    world", "foo"])
        ['echo hello world', 'foo']
        """
        joined_lines = []
        buffer = []
        for line in lines:
            # Start new buffer if line ends with \, otherwise add to current buffer and join
            if line.rstrip().endswith("\\"):
                buffer.append(line.rstrip()[:-1].strip())
            else:
                buffer.append(line.strip())
                joined_lines.append(" ".join(buffer).strip())
                buffer = []
        if buffer:
            joined_lines.append(" ".join(buffer).strip())
        return joined_lines

    @staticmethod
    def _split_commands(joined_lines: list[str]) -> list[str]:
        """Split on ; only — preserving || and && to keep conditional logic and pipelines intact.

        Example:
        ---
        >>> RunnableSpec._split_commands(["foo; bar && baz", "cmd1 || cmd2", "a | b"])
        ['foo', 'bar && baz', 'cmd1 || cmd2', 'a | b']
        """
        commands = []
        for line in joined_lines:
            parts = re.split(r"\s*;\s*", line)
            for part in parts:
                part = part.strip()
                if part:
                    commands.append(part)
        return commands
