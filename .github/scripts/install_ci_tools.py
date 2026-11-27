"""Install the existing CI scanners from hash-pinned official release assets."""

import hashlib
import io
import os
import platform
import subprocess
import sys
import tarfile
import urllib.request
from pathlib import Path

TOOLS = {
    "pinact": {
        "repo": "suzuki-shunsuke/pinact",
        "version": "v5.0.0",
        "asset": "pinact_linux_{arch}.tar.gz",
        "amd64": "d005bbb85da80dacdc07816f24a5da723a9f6d1e9f3d3e7e73df33f9caa1358f",
        "arm64": "d28ca5e9ddd7950da4a808288f8a83f84fc3ca40e86bab970f3adeef33578c0b",
    },
    "actionlint": {
        "repo": "rhysd/actionlint",
        "version": "v1.7.12",
        "asset": "actionlint_1.7.12_linux_{arch}.tar.gz",
        "amd64": "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8",
        "arm64": "325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6",
    },
}


def unpack_verified(data, expected_hash, binary):
    if hashlib.sha256(data).hexdigest() != expected_hash:
        raise ValueError("CI tool archive checksum mismatch")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        member = archive.getmember(binary)
        if not member.isfile():
            raise ValueError("CI tool is not a regular archive member")
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError("CI tool archive member is empty")
        return stream.read()


def install(tool):
    if platform.system() != "Linux":
        raise ValueError("This CI installer requires Linux")
    arches = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
    arch = arches[platform.machine()]
    spec = TOOLS[tool]
    root = Path(os.environ["RUNNER_TEMP"]) / "alloy-ci-tools" / tool
    root.mkdir(parents=True, exist_ok=True)
    asset = spec["asset"].format(arch=arch)
    url = (
        f"https://github.com/{spec['repo']}/releases/download/{spec['version']}/{asset}"
    )
    with urllib.request.urlopen(url, timeout=30) as response:
        data = response.read()
    binary = unpack_verified(data, spec[arch], tool)
    if tool == "pinact":
        archive_path = root / asset
        archive_path.write_bytes(data)
        subprocess.run(
            [
                "gh",
                "attestation",
                "verify",
                str(archive_path),
                "--repo",
                spec["repo"],
                "--signer-workflow",
                "suzuki-shunsuke/go-release-workflow/.github/workflows/release.yaml",
                "--source-ref",
                "refs/tags/" + spec["version"],
            ],
            check=True,
        )
    target = root / tool
    target.write_bytes(binary)
    target.chmod(0o755)
    return target


if __name__ == "__main__":
    install(sys.argv[1])
