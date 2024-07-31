"""Module for Google Cloud Path naive parsers."""

import re
import concurrent.futures
from google.cloud.storage.blob import Blob, TextIOWrapper
from google.cloud.storage.client import Client
from google.cloud.storage.bucket import Bucket
from requests.adapters import HTTPAdapter
import json
import yaml
from pathlib import Path
from typing import Any
import logging
from typing import Protocol
from abc import abstractmethod


class ProtoPath(Protocol):
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


class GCSPath(ProtoPath):
    """Create GCS Path object.

    Args:
        gcs_path (str): Google Cloud Storage path.
        chunk_size (int, optional): Chunk size for reading and writing. Defaults to 1024*256.
    """

    def __init__(self, gcs_path: str, chunk_size: int = 1024 * 256):
        self.gcs_path = gcs_path
        self.path_pattern = re.compile("^(gs://)?(?P<bucket_name>[(\\w)-]+)")
        self.chunk_size = chunk_size

    @property
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

    def __post_init__(self):
        """Create a client object.

        This is a post init method that allows to expand the number of concurrent tasks that can be run.
        """
        self.client = Client()
        adapter = HTTPAdapter(pool_connections=128, pool_maxsize=1024, max_retries=3)
        self.client._http.mount("https://", adapter)
        self.client._http._auth_request.session.mount("https://", adapter)

    @property
    def bucket(self) -> str:
        """Return Bucket Name.

        Returns:
            str: Bucket name.

        """
        return self._match.group("bucket_name")

    @property
    def path(self) -> str:
        """Get path segment after the bucket name.

        Returns:
            str: Path segment after Bucket Name.
        """
        # +1 to remove "/" afer bucket name
        return self.gcs_path[self._match.end() + 1 :]

    def exists(self) -> bool:
        """Check if file exists in Google Cloud Storage.

        Returns:
            bool: True if file exists.
        """
        bucket = Bucket(client=self.client, name=self.bucket)
        blob = Blob(name=self.path, bucket=bucket, chunk_size=self.chunk_size)
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
        bucket = Bucket(client=self.client, name=self.bucket)
        blob = Blob(name=self.path, bucket=bucket)
        with blob.open("w") as fp:
            assert isinstance(fp, TextIOWrapper)
            match Path(self.path).suffix:
                case ".json":
                    json.dump(data, fp)
                case ".yaml" | ".yml":
                    yaml.safe_dump(data, fp, indent=2)
                case _:
                    fp.write(data)

    def load(self) -> Any:
        """Read data from Google Cloud Storage.

        Returns:
            Any: Loaded data.
        """
        bucket = Bucket(client=self.client, name=self.bucket)
        blob = Blob(name=self.path, bucket=bucket)

        with blob.open("r") as fp:
            match Path(self.path).suffix:
                case ".json":
                    return json.load(fp)
                case ".yaml" | ".yml":
                    return yaml.safe_load(fp)
                case _:
                    return fp.read()


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

        Based on registred protocols, the path is resolved to the appropriate Path object

        Args:
            path (str): Path to resolve.

        Returns:
            ProtoPath: Path object instance following ProtoPath protocol.
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

    def load_many(self, paths: list[str], n_threads=100) -> list[Any]:
        """Load many objects by concurrent operations.

        Args:
            paths (list[str]): Paths to read from.
            n_threads (int): number of concurrent threads to use.

        Returns:
            list[Any]: Objects that were read.
        """
        if not paths:
            logging.warning("Nothing to load.")
            return []

        resolved_paths = self.resolve_paths(paths)

        logging.info(f"LOADING {len(paths)} OBJECTS.")
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

    def dump_many(self, objects: list[Any], paths: list[str], n_threads=100) -> None:
        """Dump many objects by concurrent operations.

        Args:
            objects (list[Any]): Objects to write.
            paths (list[str]): Paths to write the objects to.
            n_threads (int): number of concurrent threads to use.

        Raises:
            ValueError: When number of objects does not match paths.
        """
        if len(paths) != len(objects):
            raise ValueError("Empty paths or unequal number of objects")
        if not paths:
            logging.warning("Nothing to dump.")
            return None

        resolved_paths = self.resolve_paths(paths)
        logging.info(f"DUMPING {len(paths)} OBJECTS.")
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


__all__ = ["GCSPath", "IOManager", "NativePath"]
