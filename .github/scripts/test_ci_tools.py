import hashlib
import io
import tarfile
import unittest

from install_ci_tools import unpack_verified


class ToolArchiveTests(unittest.TestCase):
    def archive(self, *, symlink=False, name="scanner"):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz") as stream:
            member = tarfile.TarInfo(name)
            if symlink:
                member.type = tarfile.SYMTYPE
                member.linkname = "../../outside"
                stream.addfile(member)
            else:
                member.size = len(b"native tool")
                stream.addfile(member, io.BytesIO(b"native tool"))
        return output.getvalue()

    def test_verified_regular_binary(self):
        data = self.archive()
        self.assertEqual(
            unpack_verified(data, hashlib.sha256(data).hexdigest(), "scanner"),
            b"native tool",
        )

    def test_checksum_mismatch(self):
        data = self.archive()
        with self.assertRaises(ValueError):
            unpack_verified(data, "0" * 64, "scanner")

    def test_reject_link(self):
        data = self.archive(symlink=True)
        with self.assertRaises(ValueError):
            unpack_verified(data, hashlib.sha256(data).hexdigest(), "scanner")

    def test_missing_expected_binary(self):
        data = self.archive(name="different")
        with self.assertRaises(KeyError):
            unpack_verified(data, hashlib.sha256(data).hexdigest(), "scanner")
