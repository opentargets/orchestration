"""Module for Google Cloud Path naive parsers."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import multiprocessing
import re
from abc import abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Any, Protocol, TypedDict

import yaml
from google.cloud import storage
from requests.adapters import HTTPAdapter

CHUNK_SIZE = 1024 * 256
MAX_N_THREADS = 32
POSIX_PATH_PATTERN = r"^^((?P<protocol>.*)://)?(?P<root>[(\w)-]+)/(?P<path>([(\w)-/])+)"


class PathSegments(TypedDict):
    """Path segments.

    Args:
        protocol(str): For example gs, s3, file.
        root(str): The first path segment.
        prefix(str): The path segment between the basename and final filename.
        filename(str): The final path segment (basename).
    """

    protocol: str
    root: str
    prefix: str
    filename: str


class ProtoPath(Protocol):
    segments: PathSegments

    @abstractmethod
    def dump(self, data: Any) -> None:
        """Dump data to storage.

        Args:
            data (Any): Data to dump.

        Raises:
            NotImplementedError: this method is required for classes to follow the protocol.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self) -> Any:
        """Load data from storage.

        Raises:
            NotImplementedError: this method is required for classes to follow the protocol.

        Returns:
            Any: Loaded data.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self) -> bool:
        """Check if path exists.

        Raises:
            NotImplementedError: this method is required for classes to follow the protocol.

        Returns:
            bool: True if path exists.
        """
        raise NotImplementedError


class NativePath(ProtoPath):
    """Create Native Path object.

    This represents a local file path.

    Args:
        path (str): Local file path.
    """

    def __init__(self, path: str):
        self.native_path = Path(path)

    def dump(self, data: Any) -> None:
        """Write data to Native storage.

        Args:
            data (Any): Data to write.
        """
        dirname = self.native_path.parent
        dirname.mkdir(parents=True, exist_ok=True)
        with open(self.native_path, "w") as fp:
            match self.native_path.suffix:
                case ".json":
                    json.dump(data, fp)
                case ".yaml" | ".yml":
                    yaml.safe_dump(data, fp)
                case _:
                    fp.write(data)

    def load(self) -> Any:
        """Read data from Native storage.

        Returns:
            Any: Loaded data.
        """
        with open(self.native_path, "r") as fp:
            match self.native_path.suffix:
                case ".json":
                    return json.load(fp)
                case ".yaml" | ".yml":
                    return yaml.safe_load(fp)
                case _:
                    return fp.read()

    def exists(self) -> bool:
        """Check if path exists preserved for the interface of ProtoPath implementation.

        Returns:
            bool: True if path exists.
        """
        return self.native_path.exists()

    @cached_property
    def segments(self) -> PathSegments:
        """Get path segments.

        Returns:
            PathSegments: Object with path segments.
        """
        return {
            "protocol": "file",
            "root": self.native_path.root,
            "prefix": str(self.native_path.parent),
            "filename": self.native_path.name,
        }


class GCSPath(ProtoPath):
    """Create GCS Path object.

    Args:
        gcs_path (str): Google Cloud Storage path.
        chunk_size (int): Chunk size for reading and writing. Defaults to 1024*256.
        client (storage.Client | None): Google Cloud Storage client. Defaults to storage.Client().
    """

    def __init__(
        self,
        gcs_path: str,
        chunk_size: int = CHUNK_SIZE,
        client: storage.Client | None = None,
    ):
        self._client = client
        self.gcs_path = gcs_path
        self.path_pattern = re.compile(POSIX_PATH_PATTERN)
        self.chunk_size = chunk_size

    @cached_property
    def client(self) -> storage.Client:
        """Get existing or set new the GCS client.

        This is a post init method that allows to expand the number of
        concurrent tasks that can be run.

        Returns:
            storage.Client: GCS client.
        """
        client = self._client or storage.Client()
        adapter = HTTPAdapter(pool_connections=128, pool_maxsize=1024, max_retries=3)
        client._http.mount("https://", adapter)
        client._http._auth_request.session.mount("https://", adapter)
        return

    @cached_property
    def _match(self) -> re.Match:
        """Match the path with the pattern.

        Raises:
            ValueError: When path does not match the pattern.

        Returns:
            re.Match: Match object.
        """
        _match = self.path_pattern.match(self.gcs_path)
        if _match is None:
            raise ValueError("Invalid GCS path %s", self.gcs_path)
        return _match

    @property
    def bucket(self) -> str:
        """Return Bucket Name.

        Returns:
            str: Bucket name.
        """
        return self._match.group("root")

    @property
    def path(self) -> str:
        """Get path segment after the bucket name.

        Returns:
            str: Path segment after Bucket Name.
        """
        return self._match.group("path")

    def exists(self) -> bool:
        """Check if file exists in Google Cloud Storage.

        Returns:
            bool: True if file exists.
        """
        bucket = storage.Bucket(client=self.client, name=self.bucket)
        blob = storage.Blob(name=self.path, bucket=bucket, chunk_size=self.chunk_size)
        return blob.exists()

    def split(self) -> tuple[str, str]:
        """Return both, bucket and path after split.

        Returns:
            tuple[str, str]: Bucket name and path.
        """
        return self.bucket, self.path

    def __repr__(self) -> str:
        """Path representation."""
        return self.gcs_path

    def dump(self, data: Any) -> None:
        """Write data to Google Cloud Storage.

        Args:
            data (Any): Data to write.
        """
        bucket = storage.Bucket(client=self.client, name=self.bucket)
        blob = storage.Blob(name=self.path, bucket=bucket)
        with blob.open("w") as fp:
            match Path(self.path).suffix:
                case ".json":
                    json.dump(data, fp)  # type: ignore
                case ".yaml" | ".yml":
                    yaml.safe_dump(data, fp, indent=2)
                case _:
                    fp.write(data)

    def load(self) -> Any:
        """Read data from Google Cloud Storage.

        Returns:
            Any: Loaded data.
        """
        bucket = storage.Bucket(client=self.client, name=self.bucket)
        blob = storage.Blob(name=self.path, bucket=bucket)

        with blob.open("r") as fp:
            match Path(self.path).suffix:
                case ".json":
                    return json.load(fp)
                case ".yaml" | ".yml":
                    return yaml.safe_load(fp)
                case _:
                    return fp.read()

    @cached_property
    def segments(self) -> PathSegments:
        """Get path segments.

        Returns:
            PathSegments: Object with path segments.
        """
        split = self._match.group("path").split("/")
        return {
            "protocol": self._match.group("protocol"),
            "root": self._match.group("root"),
            "prefix": "/".join(split[0:-1]),
            "filename": split[-1],
        }


class IOManager:
    """Input Output manager class."""

    def resolve(self, path: str) -> ProtoPath:
        """Resolve path by its protocol.

        Depending on the protocol the following are defined

        - gs:// for Google Cloud Storage
        - file:// for local file system
        - no protocol for local file system
        - s3:// (currently not implemented)
        - s3a:// (currently not implemented)
        - http:// (currently not implemented)
        - https:// (currently not implemented)
        - ftp:// (currently not implemented)

        Based on registred protocols, the path is resolved to the appropriate Path object.
        Works only with POSIX path objects.

        Args:
            path (str): Path to resolve.

        Returns:
            ProtoPath: Path object instance following ProtoPath protocol.

        Raises:
            NotImplementedError: When not supported file protocol is provided as input path.
        """
        match path.split("://"):
            case ["gs", _]:
                return GCSPath(path)
            case [_] | ["file", _]:
                return NativePath(path)
            case [protocol, _]:
                raise NotImplementedError(
                    "IOManager.resolve does not implement %s protocol resolution",
                    protocol,
                )
            case _:
                raise NotImplementedError(
                    "IOManager.resolve is not implemented for path=%s", path
                )

    def resolve_paths(self, paths: list[str]) -> list[ProtoPath]:
        """Resolve multiple paths by the protocols.

        Args:
            paths (list[str]): Paths to resolve.

        Returns:
            list[ProtoPath]: List of path objects following ProtoPath protocol.
        """
        return [self.resolve(p) for p in paths]

    def load_many(self, paths: list[str], n_threads: int | None = None) -> list[Any]:
        """Load many objects by concurrent operations. Thread safe.

        By default this method spawns 32 or num of cpus treads depending on which number is
        closer to the oveall concurrent process count.

        Args:
            paths (list[str]): Paths to read from.
            n_threads (int | None): number of concurrent threads to use. If None, default is used. See description.

        Returns:
            list[Any]: Objects that were read.
        """
        if not paths:
            logging.warning("Nothing to load.")
            return []

        resolved_paths = self.resolve_paths(paths)
        if not n_threads:
            n_threads = self._find_optimal_thread_num(len(paths))
        logging.info(f"LOADING %s OBJECTS by %s THREADS.", len(paths), n_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures: list[concurrent.futures.Future] = []
            for path in resolved_paths:
                method = lambda: path.load()
                futures.append(executor.submit(method))
        results = []

        concurrent.futures.wait(futures)
        for idx, future in enumerate(futures):
            exe = future.exception()
            if exe:
                logging.error(exe)
            else:
                logging.info(f"Successfully loaded {idx + 1}/{len(futures)} objects.")
                results.append(future.result())
        return results

    def dump_many(
        self, objects: list[Any], paths: list[str], n_threads: int | None = None
    ) -> None:
        """Dump many objects by concurrent operations. Not thread safe.

        When dumping many objects make sure, you are not writing to the same object multiple times.

        By default this method spawns 32 or num of cpus treads depending on which number is
        closer to the oveall concurrent process count.

        Args:
            objects (list[Any]): Objects to write.
            paths (list[str]): Paths to write the objects to.
            n_threads (int | None): number of concurrent threads to use. If None, default is used. See description.

        Raises:
            ValueError: When number of objects does not match paths.
            ThreadSafetyError: When attempting to write to the same file from multiple threads.

        """
        if len(paths) != len(objects):
            raise ValueError("Empty paths or unequal number of objects")
        if not paths:
            logging.warning("Nothing to dump.")
            return None

        resolved_paths = self.resolve_paths(paths)
        unique_paths = set(paths)
        if len(paths) != len(unique_paths):
            duplicated_paths = [p for p in paths if p not in unique_paths]
            raise ThreadSafetyError(
                "You are trying to write to the same file multiple times %s",
                duplicated_paths,
            )
        if not n_threads:
            n_threads = self._find_optimal_thread_num(len(paths))
        logging.info(f"DUMPING %s OBJECTS by %s THREADS.", len(paths), n_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures: list[concurrent.futures.Future] = []
            for path, ob in zip(resolved_paths, objects, strict=True):
                method = lambda: path.dump(ob)
                futures.append(executor.submit(method))

        concurrent.futures.wait(futures)
        for idx, future in enumerate(futures):
            exe = future.exception()
            if exe:
                logging.error(exe)
            else:
                logging.info(f"Successfully dumped {idx + 1}/{len(futures)} objects.")

    @staticmethod
    def _find_optimal_thread_num(
        n_processes: int, max_n_threads: int = MAX_N_THREADS
    ) -> int:
        """Find optimal number of threads to spawn for the concurrent IO operations.

        Args:
            n_processes(int): Number of concurrent processes.
            max_n_threads(int): Max number of threads to spawn.

        Returns:
            int: Either max_n_threads or number of cpu cores depending on which is closer to the number of n_processes.
        """
        n_cpus = multiprocessing.cpu_count()
        if abs(n_cpus - n_processes) < abs(max_n_threads - n_processes):
            return n_cpus
        else:
            return max_n_threads


class ThreadSafetyError(Exception):
    """Exception raised for errors in thread safety."""

    pass
