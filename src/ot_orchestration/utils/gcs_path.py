"""Module for Google Cloud Path naive parsers."""

import re
import concurrent.futures
from google.cloud.storage.blob import Blob
from google.cloud.storage.client import Client
from google.cloud.storage.bucket import Bucket
from requests.adapters import HTTPAdapter
from ot_orchestration.types import GCS_Bucket_Name, GCS_Path_Suffix
import json
from ot_orchestration.types import Manifest_Object


class GCSPath:
    """Google Cloud Storage Path."""

    def __init__(self, gcs_path: str):
        self.gcs_path = gcs_path
        path_pattern = re.compile("^(gs://)?(?P<bucket_name>[(\\w)-]+)")
        self._match = path_pattern.match(gcs_path)
        if self._match is None:
            raise ValueError("Invalid GCS path %s", gcs_path)

    @property
    def bucket(self) -> GCS_Bucket_Name:
        """Return Bucket Name."""
        return self._match.group("bucket_name")

    @property
    def path(self) -> GCS_Path_Suffix:
        """Return Path segment after Bucket Name."""
        # +1 to remove "/" afer bucket name
        return self.gcs_path[self._match.end() + 1 :]

    def split(self) -> tuple[GCS_Bucket_Name, GCS_Path_Suffix]:
        """Return both, bucket and path."""
        return self.bucket, self.path

    def __repr__(self) -> str:
        """Reprint object."""
        return f"gcs_path={self.gcs_path}, bucket={self.bucket}, path={self.path}"


class GCSIOManager:
    """Input Output manager class."""

    def __init__(self):
        self.client = Client()
        adapter = HTTPAdapter(pool_connections=128, pool_maxsize=1024, max_retries=3)
        self.client._http.mount("https://", adapter)
        self.client._http._auth_request.session.mount("https://", adapter)

    def load_many(self, gcs_paths: list[str]) -> list[Manifest_Object]:
        """Load many json objects from google cloud by concurrent operations.

        Args:
            gcs_paths (list[str]): Google Cloud Storage Paths.

        Returns:
            list[Manifest_Object]: Manifest objects
        """
        if not gcs_paths:
            return []
        n_threads = len(gcs_paths)
        print(f"LOADING {len(gcs_paths)} MANIFESTS.")
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures: list[concurrent.futures.Future] = []
            for gcs_path in gcs_paths:
                gcs_path = GCSPath(gcs_path)
                bucket_name, file_name = gcs_path.split()
                bucket = Bucket(name=bucket_name, client=self.client)
                blob = Blob(name=file_name, bucket=bucket)

                def load_manifest(blob: Blob) -> Manifest_Object:
                    """Load manifest with blob."""
                    with blob.open("r") as fp:
                        manifest: Manifest_Object = json.load(fp)
                        return manifest

                method = lambda: load_manifest(blob)
                futures.append(executor.submit(method))
        results: list[Manifest_Object] = []
        concurrent.futures.wait(futures)
        for future in futures:
            exe = future.exception()
            if exe:
                continue
        results.append(future.result())
        return results

    def dump_many(
        self, manifest_objects: list[Manifest_Object], gcs_paths: list[str]
    ) -> None:
        """Dump many manifest objects to corresponding Google Cloud Storage paths.

        Args:
            manifest_objects (list[Manifest_Object]): Manifest objects
            gcs_paths (list[str]): Google Cloud Storage paths.

        Raises:
            ValueError: When number of manifest objects does not match gcs paths.

        """
        if len(gcs_paths) != len(manifest_objects):
            raise ValueError("Empty gcs_paths or unequal number of file_blobs")
        if not gcs_paths:
            return None
        print(f"DUMPING {len(gcs_paths)} MANIFESTS.")
        n_threads = len(gcs_paths)
        chunk_size = 1024 * 256
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures: list[concurrent.futures.Future] = []
            for gcs_path, manifest_object in zip(
                gcs_paths, manifest_objects, strict=True
            ):
                gcs_path = GCSPath(gcs_path)
                bucket_name, file_name = gcs_path.split()
                bucket = Bucket(client=self.client, name=bucket_name)
                blob = Blob(name=file_name, bucket=bucket, chunk_size=chunk_size)

                def dump_manifest(blob: Blob, manifest: Manifest_Object) -> None:
                    """Dump manifest to blob."""
                    with blob.open("w") as fp:
                        json.dump(manifest, fp, indent=2)

                method = lambda: dump_manifest(blob, manifest_object)
                futures.append(executor.submit(method))
        results = []
        concurrent.futures.wait(futures)
        for future in futures:
            exe = future.exception()
            if exe:
                continue
        results.append(future.result())
        return results


__all__ = ["GCSPath", "GCSIOManager"]
