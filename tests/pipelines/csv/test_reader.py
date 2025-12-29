import os
import uuid
from pathlib import Path

import pytest

from pipelines.csv.reader import CSVReader


def _write_csv(path: Path) -> None:
    content = (
        "id;timestamp;amount;note\n"
        "1;2023-11-28 06:35:12;-952,40;Card expense\n"
        "2;2023-05-16 01:27:52;-749,56;Purchase\n"
        "3;2023-11-03 11:49:37;184,91;Receipt\n"
    )
    path.write_text(content, encoding="utf-8")


@pytest.mark.unit
class TestCSVReader:

    def test_read_batches_splits_into_chunks(self, tmp_path: Path) -> None:
        csv_path = Path(os.path.join(tmp_path, "transactions.csv"))
        _write_csv(csv_path)

        reader = CSVReader(file_path=str(csv_path))
        batches = list(reader.read_batches(chunk_size=2))

        assert len(batches) == 2
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1

    def test_id_is_converted_to_uuid_string(self, tmp_path: Path) -> None:
        csv_path = Path(os.path.join(tmp_path, "transactions.csv"))
        _write_csv(csv_path)

        reader = CSVReader(file_path=str(csv_path))
        df = next(reader.read_batches(chunk_size=1000))

        expected = str(uuid.UUID(int=1))
        assert df.loc[0, "id"] == expected

        parsed = uuid.UUID(str(df.loc[0, "id"]))
        assert str(parsed) == expected

    def test_timestamp_converted_to_isoformat_string(self, tmp_path: Path) -> None:
        csv_path = Path(os.path.join(tmp_path, "transactions.csv"))
        _write_csv(csv_path)

        reader = CSVReader(file_path=str(csv_path))
        df = next(reader.read_batches(chunk_size=1000))

        assert isinstance(df.loc[0, "timestamp"], str)
        assert str(df.loc[0, "timestamp"]).startswith("2023-11-28T06:35:12")

    def test_decimal_comma_parsed_as_float(self, tmp_path: Path) -> None:
        csv_path = Path(os.path.join(tmp_path, "transactions.csv"))
        _write_csv(csv_path)

        reader = CSVReader(file_path=str(csv_path))
        df = next(reader.read_batches(chunk_size=1000))

        assert df.loc[0, "amount"] == pytest.approx(-952.40)
        assert df["amount"].dtype.kind in ("f", "i")

    def test_returns_dataframe_with_expected_columns(self, tmp_path: Path) -> None:
        csv_path = Path(os.path.join(tmp_path, "transactions.csv"))
        _write_csv(csv_path)

        reader = CSVReader(file_path=str(csv_path))
        df = next(reader.read_batches(chunk_size=1000))

        assert set(["id", "timestamp", "amount", "note"]).issubset(df.columns)
