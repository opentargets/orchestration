"""Labels for resources in Google Cloud."""

import re

from ot_orchestration.utils.common import GCP_PROJECT_PLATFORM, shared_labels


class Labels:
    """A collection of labels for Google Cloud resources.

    Includes a set of default labels, and ensures that all labels are correctly
    formatted.

    Refer to the `controlled vocabularies <https://github.com/opentargets/controlled-vocabularies/blob/main/infrastructure.yaml>`__
        repository for a list of example values.

    See the `shared_labels` dict in `common.py` module for the default labels.

    Args:
        extra: A dict of extra labels to add on top of the defaults.
        repository for a list of valid values. Defaults to "platform".
        project: The GCP project to use for the labels. This will determine the
            content of the default "environment" label. Defaults to
            GCP_PROJECT_PLATFORM.
    """

    def __init__(
        self,
        extra: dict[str, str] | None = None,
        project: str = GCP_PROJECT_PLATFORM,
    ) -> None:
        self.project = project
        self.extra = extra or {}
        self.label_dict = shared_labels(project)
        self.label_dict.update({k: self.clean_label(v) for k, v in self.extra.items()})

    def clean_label(self, label: str) -> str:
        """Clean a label for use in google cloud.

        According to the docs: The value can only contain lowercase letters, numeric
        characters, underscores and dashes. The value can be at most 63 characters
        long.
        """
        return re.sub(r"[^a-z0-9-_]", "-", label.lower())[0:63]

    def add(self, extra: dict[str, str]) -> None:
        """Add labels to a collection."""
        self.label_dict.update({k: self.clean_label(v) for k, v in extra.items()})

    def get(self) -> dict[str, str]:
        """Return a dict of clean labels."""
        return self.label_dict

    def clone(self, extra: dict[str, str] | None = None) -> "Labels":
        """Return a copy with additional labels."""
        extra = extra or {}
        return Labels({**self.label_dict, **extra}, self.project)
