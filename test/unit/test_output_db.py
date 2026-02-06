import brotli
import pytest

from apsis.sqlite import SqliteDB
from apsis.program import OutputMetadata, Output

# -------------------------------------------------------------------------------


def test_basic(tmp_path):
    path = tmp_path / "apsis.db"

    SqliteDB.create(path=path)
    db = SqliteDB.open(path).output_db

    assert len(db.get_metadata("r42")) == 0

    with pytest.raises(LookupError):
        db.get_output("r42", "output")

    data = b"The quick brown fox jumped over the lazy dogs.\x01\x02\x03"
    output = Output(OutputMetadata("combined output", len(data)), data)
    db.upsert("r42", "output", output)

    meta = db.get_metadata("r42")
    assert list(meta.keys()) == ["output"]
    assert meta["output"].name == "combined output"

    assert db.get_output("r42", "output").data == data


def test_br(tmp_path):
    DATA = bytes(range(256)) * 40960  # 10 MB; definitely not UTF-8.
    path = tmp_path / "apsis.db"

    SqliteDB.create(path=path)
    db = SqliteDB.open(path).output_db
    db.upsert(
        "r99",
        "test",
        Output(
            OutputMetadata("program output", len(DATA)),
            brotli.compress(DATA),
            "br",
        ),
    )

    db = SqliteDB.open(path).output_db
    output = db.get_output("r99", "test")
    assert output.metadata.name == "program output"
    assert output.metadata.length == 10485760
    assert output.metadata.content_type == "application/octet-stream"
    assert output.compression == "br"
    assert len(output.data) < 1024
    data = output.get_uncompressed_data()
    assert data == DATA
