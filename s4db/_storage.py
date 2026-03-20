import re

import boto3
from botocore.exceptions import ClientError


_DATA_FILE_RE = re.compile(r"^data_(\d{6})\.s4db$")


class S3Storage:
    def __init__(self, bucket: str, prefix: str, **boto_kwargs):
        """Creates an S3Storage backed by the given bucket and key prefix.

        Any extra boto_kwargs (e.g. region_name, endpoint_url) are forwarded directly
        to boto3.client, making it easy to target non-AWS S3-compatible services.
        """
        self.bucket = bucket
        self.prefix = prefix
        self._client = boto3.client("s3", **boto_kwargs)

    def _key(self, filename: str) -> str:
        """Returns the full S3 key for filename by prepending the configured prefix."""
        return self.prefix + filename

    def upload(self, local_path: str, filename: str) -> None:
        """Uploads a file from disk to S3 using multipart transfer for large files."""
        self._client.upload_file(local_path, self.bucket, self._key(filename))

    def upload_bytes(self, data: bytes, filename: str) -> None:
        """Uploads raw bytes to S3 as a single PUT request."""
        self._client.put_object(Bucket=self.bucket, Key=self._key(filename), Body=data)

    def download_file(self, filename: str, local_path: str) -> None:
        """Downloads an S3 object and writes it to local_path on disk."""
        self._client.download_file(self.bucket, self._key(filename), local_path)

    def download_bytes(self, filename: str) -> bytes:
        """Downloads an S3 object and returns its full contents as bytes."""
        response = self._client.get_object(Bucket=self.bucket, Key=self._key(filename))
        return response["Body"].read()

    def read_range(self, filename: str, start: int, length: int) -> bytes:
        """Fetches a byte range from an S3 object using an HTTP Range request.

        start and length are both in bytes; the range is inclusive on both ends per S3 semantics.
        Use this to read a single entry without downloading the entire data file.
        """
        end = start + length - 1
        response = self._client.get_object(
            Bucket=self.bucket,
            Key=self._key(filename),
            Range=f"bytes={start}-{end}",
        )
        return response["Body"].read()

    def exists(self, filename: str) -> bool:
        """Returns True if the file exists in S3, False on 404/NoSuchKey. Re-raises other errors."""
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(filename))
            return True
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return False
            raise

    def delete(self, filename: str) -> None:
        """Deletes the S3 object for filename. Silent no-op if the object does not exist."""
        self._client.delete_object(Bucket=self.bucket, Key=self._key(filename))

    def list_data_files(self) -> list[str]:
        """Lists all data files under the prefix, returning filenames sorted by number.

        Only returns files matching the data_NNNNNN.s4db pattern; index and other files
        are ignored. Uses pagination to handle buckets with more than 1000 objects.
        """
        paginator = self._client.get_paginator("list_objects_v2")
        filenames = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key[len(self.prefix):]
                if _DATA_FILE_RE.match(name):
                    filenames.append(name)
        filenames.sort()
        return filenames
