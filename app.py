from __future__ import annotations

import json
import mimetypes
import os
import secrets
import threading
from dataclasses import asdict
from email.parser import BytesParser
from email.policy import default
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from coverage_data import BRANCH_OFFICES, CoverageDatabase, DatabaseSummary


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
STATIC_DIR = APP_DIR / "static"
DEFAULT_DATABASE_FILE = DATA_DIR / "current_database.db"
DEFAULT_IMPORT_FILE = DATA_DIR / "current_database.xlsx"
HOST = os.getenv("APP_HOST", "127.0.0.1")
PORT = int(os.getenv("APP_PORT", "8000"))
AUTH_USERNAME = "admin"
AUTH_PASSWORD = "admin"
SESSION_COOKIE_NAME = "coverage_session"


class AppState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.database: CoverageDatabase | None = None
        self.last_error = ""
        self.last_notice = ""
        self.branches = {branch.label: branch for branch in BRANCH_OFFICES}

    def initialize(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        database = CoverageDatabase(DEFAULT_DATABASE_FILE)
        try:
            if database.has_data():
                self.database = database
                self.last_notice = "Base SQLite cargada correctamente."
                return
            if DEFAULT_IMPORT_FILE.exists():
                summary = database.import_from_excel(DEFAULT_IMPORT_FILE)
                self.database = database
                self.last_notice = self._import_notice(summary)
                return
            self.database = database
            self.last_error = "No se encontro una base de datos ni un archivo Excel para importar."
        except Exception as error:  # noqa: BLE001
            self.last_error = str(error)
            self.database = database

    def import_data(self, filename: str, payload: bytes) -> DatabaseSummary:
        if self.database is None:
            self.database = CoverageDatabase(DEFAULT_DATABASE_FILE)

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        suffix = Path(filename).suffix.lower() or ".xlsx"
        with NamedTemporaryFile(delete=False, suffix=suffix, dir=DATA_DIR) as temporary_file:
            temporary_file.write(payload)
            temporary_path = Path(temporary_file.name)

        try:
            summary = self.database.import_from_excel(temporary_path)
            DEFAULT_IMPORT_FILE.write_bytes(payload)
            self.last_notice = self._import_notice(summary)
            self.last_error = ""
            return summary
        finally:
            temporary_path.unlink(missing_ok=True)

    def add_manual_data(self, payload: dict[str, str]) -> tuple[DatabaseSummary, str]:
        if self.database is None:
            self.database = CoverageDatabase(DEFAULT_DATABASE_FILE)

        summary, duplicate_count = self.database.add_manual_record(payload)
        self.last_error = ""
        if duplicate_count > 0:
            self.last_notice = (
                f"Registro agregado. El CP {payload.get('postal_code', '').strip()} "
                f"ya existia {duplicate_count} vez/veces en la base."
            )
        else:
            self.last_notice = "Registro agregado correctamente a la base de datos."
        return summary, self.last_notice

    def status_payload(self) -> dict[str, object]:
        summary = self._summary()
        return {
            "databaseLoaded": summary is not None and summary.total_coverage_records > 0,
            "databaseName": summary.database_path.name if summary else DEFAULT_DATABASE_FILE.name,
            "databasePath": str(summary.database_path if summary else DEFAULT_DATABASE_FILE),
            "sourceName": summary.source_name if summary else "",
            "sourcePath": summary.source_path if summary else "",
            "totalPostalCodes": summary.total_postal_codes if summary else 0,
            "totalCoverageRecords": summary.total_coverage_records if summary else 0,
            "duplicatePostalCodes": summary.duplicate_postal_codes if summary else 0,
            "duplicateRows": summary.duplicate_rows if summary else 0,
            "totalRoutes": summary.total_routes if summary else 0,
            "lastError": self.last_error,
            "lastNotice": self.last_notice,
            "branches": [
                {
                    "label": branch.label,
                    "description": branch.description,
                    "city": branch.city,
                    "state": branch.state,
                    "sucursal": branch.sucursal,
                    "plaza": branch.plaza,
                    "postalCode": branch.postal_code,
                }
                for branch in BRANCH_OFFICES
            ],
        }

    def search(self, branch_label: str, postal_code: str) -> dict[str, object]:
        if self.database is None:
            raise ValueError("No hay una base de datos cargada.")
        branch = self.branches.get(branch_label)
        if branch is None:
            raise ValueError("La sucursal seleccionada no es valida.")

        outcome = self.database.search(branch.plaza, postal_code)
        within_limit = sum(1 for result in outcome.results if result.within_limit)
        duplicate_notice = ""
        if outcome.duplicate_postal_code:
            duplicate_notice = (
                f"Aviso: el codigo postal {outcome.postal_code} aparece "
                f"{outcome.raw_match_count} veces en la base de datos."
            )

        if outcome.results:
            summary = (
                f"Sucursal origen: {branch.sucursal}/{branch.plaza}. "
                f"Coincidencias: {len(outcome.results)}. "
                f"Dentro del limite de 1600 km: {within_limit}."
            )
        else:
            summary = f"El codigo postal {outcome.postal_code} no tiene cobertura registrada en la base actual."

        return {
            "branch": {
                "label": branch.label,
                "description": branch.description,
                "sucursal": branch.sucursal,
                "plaza": branch.plaza,
            },
            "postalCode": outcome.postal_code,
            "summary": summary,
            "duplicateNotice": duplicate_notice,
            "duplicatePostalCode": outcome.duplicate_postal_code,
            "rawMatchCount": outcome.raw_match_count,
            "results": [
                {
                    **asdict(result),
                    "distanceLabel": result.distance_label,
                    "limitLabel": result.limit_label,
                    "withinLimit": result.within_limit,
                }
                for result in outcome.results
            ],
        }

    def _summary(self) -> DatabaseSummary | None:
        if self.database is None or not self.database.has_data():
            return None
        return self.database.get_summary()

    def _import_notice(self, summary: DatabaseSummary) -> str:
        if summary.duplicate_postal_codes:
            return (
                f"Datos importados en SQLite. "
                f"Se detectaron {summary.duplicate_postal_codes} codigos postales repetidos "
                f"y {summary.duplicate_rows} filas adicionales con el mismo CP."
            )
        return "Datos importados en SQLite sin codigos postales repetidos."


APP_STATE = AppState()


class SessionStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, str] = {}

    def create(self, username: str) -> str:
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._sessions[token] = username
        return token

    def get_username(self, token: str | None) -> str | None:
        if not token:
            return None
        with self._lock:
            return self._sessions.get(token)

    def destroy(self, token: str | None) -> None:
        if not token:
            return
        with self._lock:
            self._sessions.pop(token, None)


SESSION_STORE = SessionStore()


class CoverageRequestHandler(BaseHTTPRequestHandler):
    server_version = "CoverageApp/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed_path = urlparse(self.path)
        if parsed_path.path == "/api/session":
            self._send_json(
                {
                    "authenticated": self.current_user is not None,
                    "username": self.current_user or "",
                }
            )
            return
        if parsed_path.path == "/api/status":
            if not self._require_auth():
                return
            self._send_json(APP_STATE.status_payload())
            return
        self._serve_static(parsed_path.path)

    def do_POST(self) -> None:  # noqa: N802
        parsed_path = urlparse(self.path)
        if parsed_path.path == "/api/login":
            self._handle_login()
            return
        if parsed_path.path == "/api/logout":
            self._handle_logout()
            return
        if parsed_path.path == "/api/search":
            if not self._require_auth():
                return
            self._handle_search()
            return
        if parsed_path.path == "/api/import":
            if not self._require_auth():
                return
            self._handle_import()
            return
        if parsed_path.path == "/api/manual-entry":
            if not self._require_auth():
                return
            self._handle_manual_entry()
            return
        self._send_error_payload(HTTPStatus.NOT_FOUND, "Ruta no encontrada.")

    def log_message(self, format: str, *args: object) -> None:
        return

    @property
    def current_user(self) -> str | None:
        cookie_header = self.headers.get("Cookie", "")
        if not cookie_header:
            return None
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get(SESSION_COOKIE_NAME)
        token = morsel.value if morsel else None
        return SESSION_STORE.get_username(token)

    def _require_auth(self) -> bool:
        if self.current_user:
            return True
        self._send_error_payload(HTTPStatus.UNAUTHORIZED, "Debes iniciar sesion para continuar.")
        return False

    def _handle_login(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            payload = json.loads(body.decode("utf-8"))
            username = str(payload.get("username", "")).strip()
            password = str(payload.get("password", "")).strip()
            if username != AUTH_USERNAME or password != AUTH_PASSWORD:
                raise ValueError("Usuario o contraseña incorrectos.")

            session_token = SESSION_STORE.create(username)
            self._send_json(
                {"authenticated": True, "username": username},
                extra_headers=[
                    (
                        "Set-Cookie",
                        f"{SESSION_COOKIE_NAME}={session_token}; HttpOnly; Path=/; SameSite=Lax",
                    )
                ],
            )
        except ValueError as error:
            self._send_error_payload(HTTPStatus.UNAUTHORIZED, str(error))
        except json.JSONDecodeError:
            self._send_error_payload(HTTPStatus.BAD_REQUEST, "La solicitud de login no tiene JSON valido.")

    def _handle_logout(self) -> None:
        cookie_header = self.headers.get("Cookie", "")
        if cookie_header:
            cookie = SimpleCookie()
            cookie.load(cookie_header)
            morsel = cookie.get(SESSION_COOKIE_NAME)
            SESSION_STORE.destroy(morsel.value if morsel else None)
        self._send_json(
            {"authenticated": False},
            extra_headers=[
                (
                    "Set-Cookie",
                    f"{SESSION_COOKIE_NAME}=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax",
                )
            ],
        )

    def _handle_search(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            payload = json.loads(body.decode("utf-8"))
            response = APP_STATE.search(payload.get("branch", ""), payload.get("postalCode", ""))
            self._send_json(response)
        except ValueError as error:
            self._send_error_payload(HTTPStatus.BAD_REQUEST, str(error))
        except json.JSONDecodeError:
            self._send_error_payload(HTTPStatus.BAD_REQUEST, "La solicitud de busqueda no tiene JSON valido.")

    def _handle_import(self) -> None:
        try:
            filename, payload = self._extract_upload()
            if not filename.lower().endswith(".xlsx"):
                raise ValueError("Solo se permiten archivos .xlsx.")
            summary = APP_STATE.import_data(filename, payload)
            self._send_json(
                {
                    "message": APP_STATE.last_notice,
                    "importSummary": {
                        "sourceName": summary.source_name,
                        "totalPostalCodes": summary.total_postal_codes,
                        "totalCoverageRecords": summary.total_coverage_records,
                        "duplicatePostalCodes": summary.duplicate_postal_codes,
                        "duplicateRows": summary.duplicate_rows,
                        "totalRoutes": summary.total_routes,
                    },
                    "status": APP_STATE.status_payload(),
                }
            )
        except ValueError as error:
            self._send_error_payload(HTTPStatus.BAD_REQUEST, str(error))

    def _handle_manual_entry(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            payload = json.loads(body.decode("utf-8"))
            summary, message = APP_STATE.add_manual_data(payload)
            self._send_json(
                {
                    "message": message,
                    "manualSummary": {
                        "sourceName": summary.source_name,
                        "totalPostalCodes": summary.total_postal_codes,
                        "totalCoverageRecords": summary.total_coverage_records,
                        "duplicatePostalCodes": summary.duplicate_postal_codes,
                        "duplicateRows": summary.duplicate_rows,
                        "totalRoutes": summary.total_routes,
                    },
                    "status": APP_STATE.status_payload(),
                }
            )
        except ValueError as error:
            self._send_error_payload(HTTPStatus.BAD_REQUEST, str(error))
        except json.JSONDecodeError:
            self._send_error_payload(HTTPStatus.BAD_REQUEST, "La solicitud manual no tiene JSON valido.")

    def _extract_upload(self) -> tuple[str, bytes]:
        content_type = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            raise ValueError("La carga debe enviarse como multipart/form-data.")

        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length)
        message = BytesParser(policy=default).parsebytes(
            f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8") + raw_body
        )

        for part in message.iter_parts():
            if part.get_content_disposition() != "form-data":
                continue
            field_name = part.get_param("name", header="content-disposition")
            if field_name != "database":
                continue
            filename = part.get_filename() or "database.xlsx"
            payload = part.get_payload(decode=True) or b""
            if not payload:
                raise ValueError("El archivo enviado esta vacio.")
            return filename, payload

        raise ValueError("No se encontro el archivo en la solicitud.")

    def _serve_static(self, route_path: str) -> None:
        requested_path = route_path if route_path not in {"", "/"} else "/index.html"
        static_file = (STATIC_DIR / requested_path.lstrip("/")).resolve()
        if STATIC_DIR not in static_file.parents and static_file != STATIC_DIR:
            self._send_error_payload(HTTPStatus.NOT_FOUND, "Archivo no encontrado.")
            return
        if not static_file.exists() or not static_file.is_file():
            self._send_error_payload(HTTPStatus.NOT_FOUND, "Archivo no encontrado.")
            return

        content_type = mimetypes.guess_type(static_file.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(static_file.read_bytes())

    def _send_json(
        self,
        payload: dict[str, object],
        status: HTTPStatus = HTTPStatus.OK,
        extra_headers: list[tuple[str, str]] | None = None,
    ) -> None:
        response = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response)))
        self.send_header("Cache-Control", "no-store")
        for header_name, header_value in extra_headers or []:
            self.send_header(header_name, header_value)
        self.end_headers()
        self.wfile.write(response)

    def _send_error_payload(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"error": message}, status=status)


def run() -> None:
    APP_STATE.initialize()
    server = ThreadingHTTPServer((HOST, PORT), CoverageRequestHandler)
    print(f"Servidor disponible en http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")
    finally:
        server.server_close()


if __name__ == "__main__":
    run()
