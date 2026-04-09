from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile
import sqlite3
import unicodedata
import xml.etree.ElementTree as ET


NAMESPACES = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
}

REQUIRED_COVERAGE_SHEET = "COBERTURA TOTAL"
REQUIRED_DISTANCE_SHEET = "DISTANCIA ENTRE SUCURSALES"


@dataclass(frozen=True)
class BranchOffice:
    label: str
    city: str
    state: str
    sucursal: str
    plaza: str
    postal_code: str

    @property
    def description(self) -> str:
        return f"{self.city}, {self.state} | {self.sucursal}/{self.plaza} | CP {self.postal_code}"


@dataclass(frozen=True)
class CoverageResult:
    postal_code: str
    state: str
    city: str
    municipality: str
    neighborhood: str
    coverage: str
    destination_branch: str
    destination_plaza: str
    distance_km: int | None

    @property
    def within_limit(self) -> bool:
        return self.distance_km is not None and self.distance_km <= 1600

    @property
    def distance_label(self) -> str:
        if self.distance_km is None:
            return "Sin dato"
        return f"{self.distance_km} km"

    @property
    def limit_label(self) -> str:
        if self.distance_km is None:
            return "Sin distancia"
        return "Si" if self.within_limit else "No"


@dataclass(frozen=True)
class DatabaseSummary:
    database_path: Path
    source_name: str
    source_path: str
    total_postal_codes: int
    total_coverage_records: int
    duplicate_postal_codes: int
    duplicate_rows: int
    total_routes: int


@dataclass(frozen=True)
class SearchOutcome:
    postal_code: str
    raw_match_count: int
    duplicate_postal_code: bool
    results: list[CoverageResult]


@dataclass(frozen=True)
class ParsedWorkbookData:
    source_path: Path
    coverage_rows: list[dict[str, str]]
    distance_rows: list[tuple[str, str, int]]
    total_postal_codes: int
    total_coverage_records: int
    duplicate_postal_codes: int
    duplicate_rows: int
    total_routes: int


BRANCH_OFFICES: tuple[BranchOffice, ...] = (
    BranchOffice(
        label="Guadalajara",
        city="Guadalajara",
        state="Jalisco",
        sucursal="GDL02",
        plaza="GDL",
        postal_code="44940",
    ),
    BranchOffice(
        label="Culiacan",
        city="Culiacan",
        state="Sinaloa",
        sucursal="CUL01",
        plaza="CUL",
        postal_code="80220",
    ),
    BranchOffice(
        label="Los Mochis",
        city="Los Mochis",
        state="Sinaloa",
        sucursal="LMM01",
        plaza="LMM",
        postal_code="81200",
    ),
    BranchOffice(
        label="Monterrey",
        city="Monterrey",
        state="Nuevo Leon",
        sucursal="MTY02",
        plaza="MTY",
        postal_code="64536",
    ),
)


def normalize_postal_code(raw_value: str) -> str:
    digits = "".join(character for character in raw_value if character.isdigit())
    if not digits or len(digits) > 5:
        raise ValueError("El codigo postal debe contener entre 1 y 5 digitos.")
    return digits.zfill(5)


class CoverageDatabase:
    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path).expanduser().resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    @classmethod
    def build_from_excel(cls, database_path: str | Path, excel_path: str | Path) -> "CoverageDatabase":
        database = cls(database_path)
        database.import_from_excel(excel_path)
        return database

    def import_from_excel(self, excel_path: str | Path) -> DatabaseSummary:
        parsed = WorkbookImporter.from_excel(excel_path)
        with self._connect() as connection:
            connection.execute("DELETE FROM coverage")
            connection.execute("DELETE FROM distances")
            connection.execute("DELETE FROM metadata")
            connection.executemany(
                """
                INSERT INTO coverage (
                    postal_code,
                    branch,
                    plaza,
                    municipality,
                    city,
                    neighborhood,
                    state,
                    coverage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["postal_code"],
                        row["branch"],
                        row["plaza"],
                        row["municipality"],
                        row["city"],
                        row["neighborhood"],
                        row["state"],
                        row["coverage"],
                    )
                    for row in parsed.coverage_rows
                ],
            )
            connection.executemany(
                """
                INSERT INTO distances (origin, destination, distance_km)
                VALUES (?, ?, ?)
                """,
                parsed.distance_rows,
            )
            connection.executemany(
                "INSERT INTO metadata (key, value) VALUES (?, ?)",
                [
                    ("source_name", parsed.source_path.name),
                    ("source_path", str(parsed.source_path)),
                    ("total_postal_codes", str(parsed.total_postal_codes)),
                    ("total_coverage_records", str(parsed.total_coverage_records)),
                    ("duplicate_postal_codes", str(parsed.duplicate_postal_codes)),
                    ("duplicate_rows", str(parsed.duplicate_rows)),
                    ("total_routes", str(parsed.total_routes)),
                ],
            )
        return self.get_summary()

    def add_manual_record(self, record: dict[str, str]) -> tuple[DatabaseSummary, int]:
        normalized_record = self._normalize_manual_record(record)
        with self._connect() as connection:
            duplicate_count = connection.execute(
                "SELECT COUNT(*) AS total FROM coverage WHERE postal_code = ?",
                (normalized_record["postal_code"],),
            ).fetchone()["total"]

            connection.execute(
                """
                INSERT INTO coverage (
                    postal_code,
                    branch,
                    plaza,
                    municipality,
                    city,
                    neighborhood,
                    state,
                    coverage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_record["postal_code"],
                    normalized_record["branch"],
                    normalized_record["plaza"],
                    normalized_record["municipality"],
                    normalized_record["city"],
                    normalized_record["neighborhood"],
                    normalized_record["state"],
                    normalized_record["coverage"],
                ),
            )
            self._refresh_metadata(connection)
        return self.get_summary(), int(duplicate_count)

    def has_data(self) -> bool:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM coverage").fetchone()
        return bool(row["total"])

    def get_summary(self) -> DatabaseSummary:
        metadata = self._metadata()
        return DatabaseSummary(
            database_path=self.database_path,
            source_name=metadata.get("source_name", self.database_path.name),
            source_path=metadata.get("source_path", str(self.database_path)),
            total_postal_codes=int(metadata.get("total_postal_codes", "0")),
            total_coverage_records=int(metadata.get("total_coverage_records", "0")),
            duplicate_postal_codes=int(metadata.get("duplicate_postal_codes", "0")),
            duplicate_rows=int(metadata.get("duplicate_rows", "0")),
            total_routes=int(metadata.get("total_routes", "0")),
        )

    def search(self, origin_plaza: str, postal_code: str) -> SearchOutcome:
        normalized_postal_code = normalize_postal_code(postal_code)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    postal_code,
                    branch,
                    plaza,
                    municipality,
                    city,
                    neighborhood,
                    state,
                    coverage
                FROM coverage
                WHERE postal_code = ?
                ORDER BY plaza, neighborhood, city
                """,
                (normalized_postal_code,),
            ).fetchall()

            results: list[CoverageResult] = []
            seen_keys: set[tuple[str, ...]] = set()
            for row in rows:
                result_key = (
                    row["postal_code"],
                    row["branch"],
                    row["plaza"],
                    row["municipality"],
                    row["city"],
                    row["neighborhood"],
                    row["state"],
                    row["coverage"],
                )
                if result_key in seen_keys:
                    continue
                seen_keys.add(result_key)

                distance_km = self._get_distance(connection, origin_plaza, row["plaza"])
                results.append(
                    CoverageResult(
                        postal_code=row["postal_code"],
                        state=row["state"],
                        city=row["city"],
                        municipality=row["municipality"],
                        neighborhood=row["neighborhood"],
                        coverage=row["coverage"],
                        destination_branch=row["branch"],
                        destination_plaza=row["plaza"],
                        distance_km=distance_km,
                    )
                )

        results.sort(
            key=lambda item: (
                item.distance_km is None,
                item.distance_km if item.distance_km is not None else 999999,
                item.destination_plaza,
                item.neighborhood,
            )
        )
        return SearchOutcome(
            postal_code=normalized_postal_code,
            raw_match_count=len(rows),
            duplicate_postal_code=len(rows) > 1,
            results=results,
        )

    def _get_distance(
        self,
        connection: sqlite3.Connection,
        origin_plaza: str,
        destination_plaza: str,
    ) -> int | None:
        if origin_plaza == destination_plaza:
            return 0
        row = connection.execute(
            """
            SELECT distance_km
            FROM distances
            WHERE (origin = ? AND destination = ?)
               OR (origin = ? AND destination = ?)
            LIMIT 1
            """,
            (origin_plaza, destination_plaza, destination_plaza, origin_plaza),
        ).fetchone()
        return None if row is None else int(row["distance_km"])

    def _metadata(self) -> dict[str, str]:
        with self._connect() as connection:
            rows = connection.execute("SELECT key, value FROM metadata").fetchall()
        return {row["key"]: row["value"] for row in rows}

    def _refresh_metadata(self, connection: sqlite3.Connection) -> None:
        metadata = {
            row["key"]: row["value"]
            for row in connection.execute("SELECT key, value FROM metadata").fetchall()
        }
        source_name = metadata.get("source_name", self.database_path.name)
        source_path = metadata.get("source_path", str(self.database_path))

        total_coverage_records = connection.execute(
            "SELECT COUNT(*) AS total FROM coverage"
        ).fetchone()["total"]
        total_routes = connection.execute(
            "SELECT COUNT(*) AS total FROM distances"
        ).fetchone()["total"]

        duplicate_row = connection.execute(
            """
            SELECT
                COUNT(*) AS duplicate_postal_codes,
                COALESCE(SUM(total - 1), 0) AS duplicate_rows
            FROM (
                SELECT postal_code, COUNT(*) AS total
                FROM coverage
                GROUP BY postal_code
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()

        total_postal_codes = connection.execute(
            "SELECT COUNT(DISTINCT postal_code) AS total FROM coverage"
        ).fetchone()["total"]

        connection.execute("DELETE FROM metadata")
        connection.executemany(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            [
                ("source_name", source_name),
                ("source_path", source_path),
                ("total_postal_codes", str(total_postal_codes)),
                ("total_coverage_records", str(total_coverage_records)),
                ("duplicate_postal_codes", str(duplicate_row["duplicate_postal_codes"])),
                ("duplicate_rows", str(duplicate_row["duplicate_rows"])),
                ("total_routes", str(total_routes)),
            ],
        )

    def _normalize_manual_record(self, record: dict[str, str]) -> dict[str, str]:
        normalized = {
            "postal_code": normalize_postal_code(record.get("postal_code", "")),
            "branch": record.get("branch", "").strip().upper(),
            "plaza": record.get("plaza", "").strip().upper(),
            "municipality": record.get("municipality", "").strip(),
            "city": record.get("city", "").strip(),
            "neighborhood": record.get("neighborhood", "").strip(),
            "state": record.get("state", "").strip(),
            "coverage": record.get("coverage", "").strip(),
        }

        required_labels = {
            "branch": "Sucursal destino",
            "plaza": "Plaza destino",
            "municipality": "Municipio",
            "city": "Ciudad",
            "neighborhood": "Colonia",
            "state": "Estado",
            "coverage": "Cobertura",
        }
        for key, label in required_labels.items():
            if not normalized[key]:
                raise ValueError(f"El campo '{label}' es obligatorio.")
        return normalized

    def _initialize_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS coverage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    postal_code TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    plaza TEXT NOT NULL,
                    municipality TEXT NOT NULL,
                    city TEXT NOT NULL,
                    neighborhood TEXT NOT NULL,
                    state TEXT NOT NULL,
                    coverage TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS distances (
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    distance_km INTEGER NOT NULL,
                    PRIMARY KEY (origin, destination)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_coverage_postal_code ON coverage (postal_code)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_coverage_plaza ON coverage (plaza)")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection


class WorkbookImporter:
    @classmethod
    def from_excel(cls, excel_path: str | Path) -> ParsedWorkbookData:
        source_path = Path(excel_path).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"No se encontro el archivo: {source_path}")

        workbook = XlsxWorkbook(source_path)
        coverage_rows = workbook.read_sheet(REQUIRED_COVERAGE_SHEET)
        distance_rows = workbook.read_sheet(REQUIRED_DISTANCE_SHEET)
        if not coverage_rows:
            raise ValueError(f"La hoja '{REQUIRED_COVERAGE_SHEET}' esta vacia.")
        if not distance_rows:
            raise ValueError(f"La hoja '{REQUIRED_DISTANCE_SHEET}' esta vacia.")

        coverage_headers = coverage_rows[0]
        distance_headers = distance_rows[0]

        required_coverage_columns = {
            "CODIGO POSTAL",
            "SUCURSAL",
            "PLAZA",
            "DELEGACION/MUNICIPIO",
            "CIUDAD",
            "COLONIA/ASENTAMIENTO",
            "ESTADO",
            "COBERTURA",
        }
        required_distance_columns = {"ORIGEN", "DESTINO"}

        normalized_coverage_headers = {normalize_header(header): header for header in coverage_headers}
        normalized_distance_headers = {normalize_header(header): header for header in distance_headers}

        missing_coverage = required_coverage_columns - set(normalized_coverage_headers)
        missing_distance = required_distance_columns - set(normalized_distance_headers)

        if missing_coverage:
            missing = ", ".join(sorted(missing_coverage))
            raise ValueError(f"Faltan columnas en '{REQUIRED_COVERAGE_SHEET}': {missing}")
        if missing_distance:
            missing = ", ".join(sorted(missing_distance))
            raise ValueError(f"Faltan columnas en '{REQUIRED_DISTANCE_SHEET}': {missing}")

        distance_column_name = None
        for candidate in ("DISTACIA", "DISTANCIA"):
            if candidate in normalized_distance_headers:
                distance_column_name = normalized_distance_headers[candidate]
                break
        if distance_column_name is None:
            raise ValueError(f"Falta la columna de distancia en '{REQUIRED_DISTANCE_SHEET}'.")

        parsed_coverage_rows: list[dict[str, str]] = []
        postal_code_counter: Counter[str] = Counter()
        for row in coverage_rows[1:]:
            row_map = map_row(coverage_headers, row)
            raw_postal_code = row_map[normalized_coverage_headers["CODIGO POSTAL"]]
            if not raw_postal_code.strip():
                continue
            try:
                postal_code = normalize_postal_code(raw_postal_code)
            except ValueError:
                continue
            normalized_row = {
                "postal_code": postal_code,
                "branch": row_map[normalized_coverage_headers["SUCURSAL"]].strip(),
                "plaza": row_map[normalized_coverage_headers["PLAZA"]].strip(),
                "municipality": row_map[normalized_coverage_headers["DELEGACION/MUNICIPIO"]].strip(),
                "city": row_map[normalized_coverage_headers["CIUDAD"]].strip(),
                "neighborhood": row_map[normalized_coverage_headers["COLONIA/ASENTAMIENTO"]].strip(),
                "state": row_map[normalized_coverage_headers["ESTADO"]].strip(),
                "coverage": row_map[normalized_coverage_headers["COBERTURA"]].strip(),
            }
            parsed_coverage_rows.append(normalized_row)
            postal_code_counter[postal_code] += 1

        parsed_distance_rows: dict[tuple[str, str], int] = {}
        for row in distance_rows[1:]:
            row_map = map_row(distance_headers, row)
            origin = row_map[normalized_distance_headers["ORIGEN"]].strip()
            destination = row_map[normalized_distance_headers["DESTINO"]].strip()
            raw_distance = row_map[distance_column_name].strip()
            if not origin or not destination or not raw_distance:
                continue
            try:
                parsed_distance_rows[(origin, destination)] = int(float(raw_distance))
            except ValueError:
                continue

        duplicate_postal_codes = sum(1 for count in postal_code_counter.values() if count > 1)
        duplicate_rows = sum(count - 1 for count in postal_code_counter.values() if count > 1)

        return ParsedWorkbookData(
            source_path=source_path,
            coverage_rows=parsed_coverage_rows,
            distance_rows=[(origin, destination, distance) for (origin, destination), distance in parsed_distance_rows.items()],
            total_postal_codes=len(postal_code_counter),
            total_coverage_records=len(parsed_coverage_rows),
            duplicate_postal_codes=duplicate_postal_codes,
            duplicate_rows=duplicate_rows,
            total_routes=len(parsed_distance_rows),
        )


def map_row(headers: list[str], row: list[str]) -> dict[str, str]:
    padded_row = row + [""] * (len(headers) - len(row))
    return dict(zip(headers, padded_row))


def normalize_header(header: str) -> str:
    normalized = unicodedata.normalize("NFKD", header.strip().upper())
    without_accents = "".join(character for character in normalized if not unicodedata.combining(character))
    return without_accents


class XlsxWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path
        with ZipFile(path) as zip_file:
            self._shared_strings = self._load_shared_strings(zip_file)
            self._sheet_paths = self._load_sheet_paths(zip_file)

    def read_sheet(self, sheet_name: str) -> list[list[str]]:
        target = self._sheet_paths.get(sheet_name)
        if target is None:
            raise ValueError(f"No se encontro la hoja '{sheet_name}' en el archivo.")

        with ZipFile(self.path) as zip_file:
            root = ET.fromstring(zip_file.read(target))

        rows: list[list[str]] = []
        for row in root.findall("main:sheetData/main:row", NAMESPACES):
            values: list[str] = []
            for cell in row.findall("main:c", NAMESPACES):
                index = column_index_from_reference(cell.attrib.get("r", "A1"))
                while len(values) <= index:
                    values.append("")
                values[index] = self._cell_value(cell)
            rows.append(values)
        return rows

    def _cell_value(self, cell: ET.Element) -> str:
        cell_type = cell.attrib.get("t")
        value = cell.find("main:v", NAMESPACES)
        if cell_type == "s" and value is not None and value.text:
            return self._shared_strings[int(value.text)]
        if cell_type == "inlineStr":
            return "".join(node.text or "" for node in cell.iterfind(".//main:t", NAMESPACES))
        if value is not None and value.text:
            return value.text
        return ""

    def _load_shared_strings(self, zip_file: ZipFile) -> list[str]:
        if "xl/sharedStrings.xml" not in zip_file.namelist():
            return []

        root = ET.fromstring(zip_file.read("xl/sharedStrings.xml"))
        strings: list[str] = []
        for item in root.findall("main:si", NAMESPACES):
            strings.append("".join(node.text or "" for node in item.iterfind(".//main:t", NAMESPACES)))
        return strings

    def _load_sheet_paths(self, zip_file: ZipFile) -> dict[str, str]:
        workbook_root = ET.fromstring(zip_file.read("xl/workbook.xml"))
        rels_root = ET.fromstring(zip_file.read("xl/_rels/workbook.xml.rels"))
        relationship_map = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels_root.findall("pkgrel:Relationship", NAMESPACES)
        }

        sheet_paths: dict[str, str] = {}
        for sheet in workbook_root.findall("main:sheets/main:sheet", NAMESPACES):
            relationship_id = sheet.attrib[
                "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
            ]
            target = relationship_map[relationship_id]
            if not target.startswith("xl/"):
                target = f"xl/{target}"
            sheet_paths[sheet.attrib["name"]] = target
        return sheet_paths


def column_index_from_reference(reference: str) -> int:
    letters = "".join(character for character in reference if character.isalpha())
    index = 0
    for character in letters:
        index = (index * 26) + (ord(character.upper()) - 64)
    return max(index - 1, 0)


def iter_branch_labels(branches: Iterable[BranchOffice]) -> list[str]:
    return [branch.label for branch in branches]
