import hashlib
import tarfile
from pathlib import Path

import pytest
import requests
import requests_mock

from chembl_sim.chembl import dump
from chembl_sim.settings import DumpSettings

CHECKSUMS = (
    "SHA256\tfile\n"
    "\n"
    "aaa\tchembl_37_mysql.tar.gz\n"
    "d5fb08\tchembl_37_postgresql.tar.gz\n"
    "bbb\tchembl_37_sqlite.tar.gz\n"
)


@pytest.fixture
def settings(tmp_path):
    return DumpSettings(
        base_url="https://ftp.test/latest",
        download_dir=tmp_path,
        chunk_size=1024,
        timeout_seconds=5,
    )


@pytest.fixture
def session():
    session = requests.Session()
    with requests_mock.Mocker(session=session) as mocker:
        session.mocker = mocker
        yield session


def test_artifact_is_read_from_the_checksum_file(session, settings):
    session.mocker.get("https://ftp.test/latest/checksums.txt", text=CHECKSUMS)
    artifact = dump.discover_artifact(session, settings)
    assert artifact.filename == "chembl_37_postgresql.tar.gz"
    assert artifact.sha256 == "d5fb08"
    assert artifact.release == "ChEMBL_37"
    assert artifact.url == "https://ftp.test/latest/chembl_37_postgresql.tar.gz"


def test_missing_archive_is_reported(session, settings):
    session.mocker.get("https://ftp.test/latest/checksums.txt", text="SHA256\tfile\nx\tother.gz\n")
    with pytest.raises(dump.DumpError, match="no postgresql archive"):
        dump.discover_artifact(session, settings)


def artifact_for(settings):
    return dump.DumpArtifact(
        filename="chembl_37_postgresql.tar.gz",
        url="https://ftp.test/latest/chembl_37_postgresql.tar.gz",
        sha256="unused",
        release="ChEMBL_37",
    )


def test_complete_file_is_not_downloaded_again(session, settings):
    target = settings.download_dir / "chembl_37_postgresql.tar.gz"
    target.write_bytes(b"0123456789")
    session.mocker.head(artifact_for(settings).url, headers={"Content-Length": "10"})
    result = dump.download_archive(session, artifact_for(settings), settings)
    assert result.read_bytes() == b"0123456789"
    assert session.mocker.call_count == 1


def test_partial_file_is_resumed(session, settings):
    target = settings.download_dir / "chembl_37_postgresql.tar.gz"
    target.write_bytes(b"01234")
    url = artifact_for(settings).url
    session.mocker.head(url, headers={"Content-Length": "10"})
    session.mocker.get(url, content=b"56789", status_code=206)
    result = dump.download_archive(session, artifact_for(settings), settings)
    assert result.read_bytes() == b"0123456789"
    assert session.mocker.last_request.headers["Range"] == "bytes=5-"


def test_ignored_range_request_is_refused(session, settings):
    target = settings.download_dir / "chembl_37_postgresql.tar.gz"
    target.write_bytes(b"01234")
    url = artifact_for(settings).url
    session.mocker.head(url, headers={"Content-Length": "10"})
    session.mocker.get(url, content=b"0123456789", status_code=200)
    with pytest.raises(dump.DumpError, match="ignored the range request"):
        dump.download_archive(session, artifact_for(settings), settings)


def test_checksum_accepts_a_matching_file(tmp_path):
    path = tmp_path / "f.bin"
    path.write_bytes(b"chembl")
    dump.verify_checksum(path, hashlib.sha256(b"chembl").hexdigest(), 1024)


def test_checksum_rejects_a_corrupt_file(tmp_path):
    path = tmp_path / "f.bin"
    path.write_bytes(b"chembl")
    with pytest.raises(dump.DumpError, match="checksum mismatch"):
        dump.verify_checksum(path, "deadbeef", 1024)


def build_archive(tmp_path, members):
    payload = tmp_path / "payload"
    payload.mkdir(exist_ok=True)
    archive = tmp_path / "archive.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name, content in members.items():
            item = payload / Path(name).name
            item.write_bytes(content)
            tar.add(item, arcname=name)
    return archive


def test_dump_member_is_extracted(tmp_path):
    archive = build_archive(
        tmp_path,
        {
            "chembl_37/chembl_37_postgresql/INSTALL_postgresql": b"readme",
            "chembl_37/chembl_37_postgresql/chembl_37_postgresql.dmp": b"PGDMP-data",
        },
    )
    out = tmp_path / "out"
    out.mkdir()
    extracted = dump.extract_dump(archive, out)
    assert extracted.name == "chembl_37_postgresql.dmp"
    assert extracted.read_bytes() == b"PGDMP-data"


def test_archive_without_a_dump_is_reported(tmp_path):
    archive = build_archive(tmp_path, {"chembl_37/README": b"nothing here"})
    with pytest.raises(dump.DumpError, match="no .dmp member"):
        dump.extract_dump(archive, tmp_path)


def test_restore_command_skips_indexes_and_constraints(monkeypatch, tmp_path):
    captured = {}

    class Completed:
        returncode = 0
        stderr = ""

    monkeypatch.setattr(
        dump.subprocess, "run", lambda cmd, **kw: captured.update(cmd=cmd) or Completed()
    )
    dump.restore_table(tmp_path / "d.dmp", "compound_structures", "postgresql://x/y")
    command = captured["cmd"]
    assert command[0] == "pg_restore"
    assert "--table" in command and "compound_structures" in command
    assert command.count("--section") == 2
    assert "pre-data" in command and "data" in command
    assert "--no-owner" in command and "--exit-on-error" in command


def test_failed_restore_surfaces_stderr(monkeypatch, tmp_path):
    class Failed:
        returncode = 1
        stderr = "relation already exists"

    monkeypatch.setattr(dump.subprocess, "run", lambda cmd, **kw: Failed())
    with pytest.raises(dump.DumpError, match="relation already exists"):
        dump.restore_table(tmp_path / "d.dmp", "molecule_dictionary", "postgresql://x/y")


def test_molecule_dictionary_is_loaded_first():
    assert dump.TABLE_LOADS[0].table == "molecule_dictionary"
    assert dump.TABLE_LOADS[0].prepare_sql is not None
    assert all(load.prepare_sql is None for load in dump.TABLE_LOADS[1:])


def test_every_bronze_table_is_covered():
    assert {load.table for load in dump.TABLE_LOADS} == {
        "molecule_dictionary",
        "compound_properties",
        "compound_structures",
        "chembl_id_lookup",
    }


class CountingCursor:
    def __init__(self, complete_count):
        self.complete_count = complete_count
        self.params = None

    def execute(self, statement, params=None):
        self.params = params

    def fetchone(self):
        return (self.complete_count,)


def test_release_counts_as_loaded_when_every_resource_is_complete():
    assert dump.already_loaded(CountingCursor(2), "ChEMBL_37") is True


def test_release_is_not_loaded_when_a_resource_is_missing():
    assert dump.already_loaded(CountingCursor(1), "ChEMBL_37") is False


def test_watermark_resources_match_the_api_resources():
    from chembl_sim.chembl.records import RESOURCES

    assert set(dump.WATERMARK_RESOURCES) == {resource.name for resource in RESOURCES}
