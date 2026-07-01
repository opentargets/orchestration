"""Tests for Spark ``spark.jars`` staging resolution.

``resolve_jar_staging`` turns a cluster's rendered ``spark.jars`` value into the
list of ``(src_url, dst_uri)`` pairs orchestration must stage before the cluster
is created. A jar living under the managed staging prefix must have a registered
source; one that does not is a misconfiguration and fails loudly. Jars outside
the managed prefix are assumed to be provided elsewhere and are left alone.
"""

import pytest

from orchestration.utils import resolve_jar_staging

PREFIX = "gs://opentargets-pipelines/up/pts/jars/"
NLP_DST = f"{PREFIX}spark-nlp-assembly-6.1.5.jar"
NLP_SRC = "https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/jars/spark-nlp-assembly-6.1.5.jar"
REGISTRY = {NLP_DST: NLP_SRC}


def test_registered_jar_is_staged():
    """A jar present in the registry resolves to its (src, dst) pair."""
    assert resolve_jar_staging(NLP_DST, REGISTRY, PREFIX) == [(NLP_SRC, NLP_DST)]


def test_external_jar_is_ignored():
    """A jar outside the managed prefix is not our responsibility: skip it."""
    external = "gs://some-other-bucket/foo.jar"
    assert resolve_jar_staging(external, REGISTRY, PREFIX) == []


def test_unregistered_jar_under_prefix_raises():
    """A jar under the managed prefix with no registered source is an error."""
    orphan = f"{PREFIX}mystery-1.0.jar"
    with pytest.raises(ValueError, match="no registered source"):
        resolve_jar_staging(orphan, REGISTRY, PREFIX)


def test_comma_separated_mixed_list():
    """Multiple jars: each is resolved independently; external ones drop out."""
    external = "gs://some-other-bucket/foo.jar"
    spark_jars = f"{NLP_DST}, {external}"
    assert resolve_jar_staging(spark_jars, REGISTRY, PREFIX) == [(NLP_SRC, NLP_DST)]


def test_empty_and_whitespace_yield_nothing():
    """No jars declared (empty or blank) stages nothing."""
    assert resolve_jar_staging("", REGISTRY, PREFIX) == []
    assert resolve_jar_staging("  , ", REGISTRY, PREFIX) == []
