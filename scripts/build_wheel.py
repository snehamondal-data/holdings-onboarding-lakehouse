"""Build a pure-Python wheel of src/keelstone with the standard library only
(no setuptools/build install needed). Output: dist/keelstone_holdings-<ver>-py3-none-any.whl"""
import base64
import hashlib
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NAME, VERSION = "keelstone_holdings", "0.1.0"
WHEEL_NAME = f"{NAME}-{VERSION}-py3-none-any.whl"


def _digest(data: bytes) -> str:
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


def source_files() -> dict[str, bytes]:
    return {str(p.relative_to(ROOT / "src")): p.read_bytes()
            for p in sorted((ROOT / "src" / "keelstone").rglob("*.py"))}


def content_version() -> str:
    """0.1.0+<hash of the package source>: a new version whenever the code changes."""
    h = hashlib.sha256()
    for k, v in source_files().items():
        h.update(k.encode()); h.update(v)
    return f"{VERSION}+{h.hexdigest()[:10]}"


def wheel_name(version: str = VERSION) -> str:
    return f"{NAME}-{version}-py3-none-any.whl"


def build(out_dir: Path = ROOT / "dist", version: str = VERSION) -> Path:
    out_dir.mkdir(exist_ok=True)
    dist_info = f"{NAME}-{version}.dist-info"
    files: dict[str, bytes] = source_files()
    files[f"{dist_info}/METADATA"] = (f"Metadata-Version: 2.1\nName: {NAME}\nVersion: {version}\n"
                                      "Requires-Dist: pyyaml>=6\n").encode()
    files[f"{dist_info}/WHEEL"] = b"Wheel-Version: 1.0\nGenerator: build_wheel.py\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    record = "".join(f"{k},{_digest(v)},{len(v)}\n" for k, v in files.items()) + f"{dist_info}/RECORD,,\n"
    files[f"{dist_info}/RECORD"] = record.encode()
    path = out_dir / wheel_name(version)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for k, v in files.items():
            info = zipfile.ZipInfo(k, date_time=(2026, 1, 1, 0, 0, 0))   # fixed time: reproducible
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, v)
    return path


if __name__ == "__main__":
    print(build())
    sys.exit(0)
