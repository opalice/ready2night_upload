import os
import re
import sqlite3
import threading
import subprocess
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename

APP_ROOT = Path(os.environ.get("UPLOAD_API_ROOT", "/home/r2n/apps/upload-api-v1")).resolve()
STORAGE_RAW = Path(os.environ.get("UPLOAD_API_STORAGE_RAW", str(APP_ROOT / "storage" / "raw"))).resolve()
STORAGE_WORK = Path(os.environ.get("UPLOAD_API_STORAGE_WORK", str(APP_ROOT / "storage" / "work"))).resolve()
STORAGE_TMP = Path(os.environ.get("UPLOAD_API_STORAGE_TMP", str(APP_ROOT / "storage" / "tmp"))).resolve()
STATIC_EVENTS_ROOT = Path(os.environ.get("UPLOAD_API_STATIC_EVENTS_ROOT", "/home/r2n/domains/static.ready2night.be/public_html/events")).resolve()
STATIC_PUBLIC_ROOT = STATIC_EVENTS_ROOT.parent
DB_PATH = Path(os.environ.get("UPLOAD_API_DB_PATH", str(APP_ROOT / "upload_api.sqlite3"))).resolve()
ADMIN_TOKEN = os.environ.get("UPLOAD_API_ADMIN_TOKEN", "").strip()
FFMPEG_BIN = os.environ.get("UPLOAD_API_FFMPEG_BIN", "ffmpeg").strip() or "ffmpeg"
CWEBP_BIN = os.environ.get("UPLOAD_API_CWEBP_BIN", "cwebp").strip() or "cwebp"
MAX_FILE_MB = int(os.environ.get("UPLOAD_API_MAX_FILE_MB", "512"))
POLL_INTERVAL = float(os.environ.get("UPLOAD_API_POLL_INTERVAL", "0.7"))

for d in [STORAGE_RAW, STORAGE_WORK, STORAGE_TMP, STATIC_EVENTS_ROOT.parent, DB_PATH.parent]:
    d.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024
_db_lock = threading.Lock()
_wake_event = threading.Event()
_worker_started = False


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(value: str, upper: bool = False, max_len: int = 80) -> str:
    s = str(value or "").strip().lower()
    s = re.sub(r"[^a-z0-9-]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    s = s[:max_len] if max_len > 0 else s
    if upper:
      s = s.upper()
    return s


def parse_album_parts(album_folder: str) -> Tuple[str, str, str]:
    raw = str(album_folder or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})[_-](.+)$", raw)
    if not m:
        dt = datetime.now()
        ymd = dt.strftime("%Y-%m-%d")
        label = slugify(raw or "ALBUM", upper=True, max_len=80) or "ALBUM"
        ddmmyyyy = dt.strftime("%d%m%Y")
        return ymd, label, ddmmyyyy
    ymd = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    label = slugify(m.group(4), upper=True, max_len=80) or "ALBUM"
    ddmmyyyy = f"{m.group(3)}{m.group(2)}{m.group(1)}"
    return ymd, label, ddmmyyyy


def init_db() -> None:
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_jobs (
                  id TEXT PRIMARY KEY,
                  room_slug TEXT NOT NULL,
                  album_folder TEXT NOT NULL,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  started_at TEXT,
                  finished_at TEXT,
                  total_files INTEGER NOT NULL DEFAULT 0,
                  processed_files INTEGER NOT NULL DEFAULT 0,
                  success_files INTEGER NOT NULL DEFAULT 0,
                  failed_files INTEGER NOT NULL DEFAULT 0,
                  canceled INTEGER NOT NULL DEFAULT 0,
                  error_text TEXT,
                  requester_ip TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_job_files (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id TEXT NOT NULL,
                  sort_order INTEGER NOT NULL,
                  original_name TEXT,
                  mime_type TEXT,
                  size_bytes INTEGER,
                  input_path TEXT,
                  status TEXT NOT NULL,
                  output_url TEXT,
                  output_webp_url TEXT,
                  output_avif_url TEXT,
                  poster_url TEXT,
                  error_text TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  FOREIGN KEY(job_id) REFERENCES upload_jobs(id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_job_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id TEXT NOT NULL,
                  phase TEXT NOT NULL,
                  message TEXT,
                  current_step INTEGER,
                  total_steps INTEGER,
                  file_name TEXT,
                  created_at TEXT NOT NULL,
                  FOREIGN KEY(job_id) REFERENCES upload_jobs(id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_jobs_status_created ON upload_jobs(status, created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_job_files_job_status ON upload_job_files(job_id, status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_job_events_job_created ON upload_job_events(job_id, created_at DESC)")
            conn.commit()
        finally:
            conn.close()


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def require_admin() -> Optional[Tuple[dict, int]]:
    if not ADMIN_TOKEN or str(ADMIN_TOKEN).strip().lower() in {"disabled", "off", "none"}:
        return None
    token = request.headers.get("x-admin-token", "").strip()
    if token != ADMIN_TOKEN:
        return ({"ok": False, "error": "unauthorized"}, 401)
    return None


def media_type_for(mime: str, filename: str) -> str:
    m = (mime or "").lower()
    n = (filename or "").lower()
    if m.startswith("video/") or re.search(r"\.(mp4|mov|m4v|webm|mkv|avi)$", n):
        return "video"
    return "image"


def run_cmd(args: List[str]) -> None:
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raw = (proc.stderr or proc.stdout or "command_failed").strip()
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        tail = "\n".join(lines[-16:]) if lines else raw
        raise RuntimeError((f"cmd_failed[{args[0]}]: {tail}").strip()[:2000])


def scale_filter(max_dim: int) -> str:
    return f"scale=w={int(max_dim)}:h={int(max_dim)}:force_original_aspect_ratio=decrease:flags=lanczos"


def encode_webp(input_path: Path, output_path: Path, quality: int = 82) -> None:
    try:
        run_cmd([CWEBP_BIN, "-quiet", "-q", str(int(quality)), str(input_path), "-o", str(output_path)])
        return
    except Exception:
        pass
    # Fallback to ImageMagick convert when cwebp is unavailable.
    run_cmd(["convert", str(input_path), str(output_path)])


def insert_event(conn: sqlite3.Connection, job_id: str, phase: str, message: str, current: int, total: int, file_name: str = "") -> None:
    conn.execute(
        """
        INSERT INTO upload_job_events(job_id, phase, message, current_step, total_steps, file_name, created_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        [job_id, phase, message, int(current), int(total), file_name, now_iso()],
    )


def build_output_paths(room_slug: str, album_folder: str, short_id: str) -> Dict[str, Path]:
    ymd, album_label, ddmmyyyy = parse_album_parts(album_folder)
    folder = f"{ymd}_{album_label}"
    room_clean = slugify(room_slug, upper=False, max_len=80) or "site"
    base = f"{album_label}_{room_clean}_{ddmmyyyy}-{short_id}"

    out_dir = STATIC_EVENTS_ROOT / room_clean / "albums" / folder
    out_dir.mkdir(parents=True, exist_ok=True)

    return {
        "dir": out_dir,
        "webp": out_dir / f"{base}.webp",
        "avif": out_dir / f"{base}.avif",
        "mp4": out_dir / f"{base}.mp4",
        "poster": out_dir / f"{base}-poster.webp",
    }


def to_public_url(path: Path) -> str:
    path = path.resolve()
    rel = path.relative_to(STATIC_PUBLIC_ROOT)
    return f"https://static.ready2night.be/{str(rel).replace(os.sep, '/')}"


def process_image(input_path: Path, outputs: Dict[str, Path]) -> Dict[str, str]:
    tmp_png = STORAGE_TMP / f"img_{uuid.uuid4().hex[:12]}.png"
    try:
        run_cmd([
            FFMPEG_BIN, "-y", "-i", str(input_path),
            "-vf", scale_filter(2160),
            "-frames:v", "1",
            str(tmp_png),
        ])
        encode_webp(tmp_png, outputs["webp"], quality=82)

        avif_ok = True
        try:
            run_cmd([
                FFMPEG_BIN, "-y", "-i", str(tmp_png),
                "-c:v", "libaom-av1", "-still-picture", "1", "-crf", "35", "-b:v", "0", str(outputs["avif"]),
            ])
        except Exception:
            avif_ok = False

        result = {"url": to_public_url(outputs["webp"]), "webpUrl": to_public_url(outputs["webp"]), "posterUrl": "", "avifUrl": ""}
        if avif_ok and outputs["avif"].exists():
            result["avifUrl"] = to_public_url(outputs["avif"])
            result["url"] = result["avifUrl"]
        return result
    finally:
        try:
            tmp_png.unlink(missing_ok=True)
        except Exception:
            pass


def process_video(input_path: Path, outputs: Dict[str, Path]) -> Dict[str, str]:
    run_cmd([
        FFMPEG_BIN, "-y", "-i", str(input_path),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        "-c:a", "aac", "-b:a", "128k", str(outputs["mp4"]),
    ])
    tmp_poster = STORAGE_TMP / f"poster_{uuid.uuid4().hex[:12]}.png"
    try:
        run_cmd([
            FFMPEG_BIN, "-y", "-i", str(outputs["mp4"]), "-ss", "00:00:01.000", "-vframes", "1",
            "-vf", scale_filter(1280),
            str(tmp_poster),
        ])
        encode_webp(tmp_poster, outputs["poster"], quality=85)
    finally:
        try:
            tmp_poster.unlink(missing_ok=True)
        except Exception:
            pass
    return {"url": to_public_url(outputs["mp4"]), "webpUrl": "", "avifUrl": "", "posterUrl": to_public_url(outputs["poster"])}


def process_file(row: sqlite3.Row, room_slug: str, album_folder: str) -> Dict[str, str]:
    input_path = Path(str(row["input_path"])).resolve()
    if not input_path.exists():
        raise RuntimeError("raw_file_missing")
    short_id = str(uuid.uuid4()).split("-")[0]
    outputs = build_output_paths(room_slug, album_folder, short_id)
    mtype = media_type_for(str(row["mime_type"] or ""), str(row["original_name"] or ""))
    if mtype == "video":
        result = process_video(input_path, outputs)
        result["mediaType"] = "video"
    else:
        result = process_image(input_path, outputs)
        result["mediaType"] = "image"
    return result


def fetch_job_summary(conn: sqlite3.Connection, job_id: str) -> Dict:
    job = conn.execute("SELECT * FROM upload_jobs WHERE id = ?", [job_id]).fetchone()
    if not job:
        return {}
    files = conn.execute("SELECT * FROM upload_job_files WHERE job_id = ? ORDER BY sort_order ASC, id ASC", [job_id]).fetchall()
    uploaded = []
    rejected = []
    for f in files:
        if str(f["status"]) == "done":
            uploaded.append({
                "id": f["id"],
                "name": f["original_name"],
                "mediaType": media_type_for(str(f["mime_type"] or ""), str(f["original_name"] or "")),
                "url": f["output_url"] or f["output_webp_url"] or f["output_avif_url"] or "",
                "webpUrl": f["output_webp_url"] or "",
                "avifUrl": f["output_avif_url"] or "",
                "posterUrl": f["poster_url"] or "",
                "mimeType": f["mime_type"] or "",
                "createdAt": job["created_at"],
            })
        elif str(f["status"]) == "failed":
            rejected.append({"name": f["original_name"] or "", "reason": "upload_failed", "detail": f["error_text"] or ""})
    return {
        "ok": True,
        "jobId": job["id"],
        "roomSlug": job["room_slug"],
        "album": job["album_folder"],
        "status": job["status"],
        "createdAt": job["created_at"],
        "startedAt": job["started_at"],
        "finishedAt": job["finished_at"],
        "progress": {
            "total": int(job["total_files"] or 0),
            "processed": int(job["processed_files"] or 0),
            "success": int(job["success_files"] or 0),
            "failed": int(job["failed_files"] or 0),
        },
        "uploaded": uploaded,
        "rejected": rejected,
    }


def worker_loop() -> None:
    while True:
        job_id = None
        with _db_lock:
            conn = db_conn()
            try:
                row = conn.execute("SELECT id FROM upload_jobs WHERE status = 'queued' AND canceled = 0 ORDER BY created_at ASC LIMIT 1").fetchone()
                if row:
                    job_id = str(row["id"])
                    conn.execute("UPDATE upload_jobs SET status = 'processing', started_at = COALESCE(started_at, ?) WHERE id = ?", [now_iso(), job_id])
                    conn.commit()
            finally:
                conn.close()

        if not job_id:
            _wake_event.wait(timeout=POLL_INTERVAL)
            _wake_event.clear()
            continue

        with _db_lock:
            conn = db_conn()
            try:
                job = conn.execute("SELECT * FROM upload_jobs WHERE id = ?", [job_id]).fetchone()
                if not job:
                    continue
                files = conn.execute("SELECT * FROM upload_job_files WHERE job_id = ? ORDER BY sort_order ASC, id ASC", [job_id]).fetchall()
                insert_event(conn, job_id, "process", "Job started", 0, len(files))
                conn.commit()
            finally:
                conn.close()

        processed = 0
        success = 0
        failed = 0
        canceled = False

        for idx, f in enumerate(files, start=1):
            with _db_lock:
                conn = db_conn()
                try:
                    current_job = conn.execute("SELECT canceled, room_slug, album_folder FROM upload_jobs WHERE id = ?", [job_id]).fetchone()
                    if not current_job or int(current_job["canceled"] or 0) == 1:
                        canceled = True
                        conn.commit()
                        break
                    room_slug = str(current_job["room_slug"])
                    album_folder = str(current_job["album_folder"])
                    conn.execute("UPDATE upload_job_files SET status = 'processing', updated_at = ? WHERE id = ?", [now_iso(), int(f["id"])])
                    insert_event(conn, job_id, "process", "Processing file", idx, len(files), str(f["original_name"] or ""))
                    conn.commit()
                finally:
                    conn.close()

            try:
                result = process_file(f, room_slug, album_folder)
                with _db_lock:
                    conn = db_conn()
                    try:
                        conn.execute(
                            "UPDATE upload_job_files SET status = 'done', output_url = ?, output_webp_url = ?, output_avif_url = ?, poster_url = ?, updated_at = ? WHERE id = ?",
                            [result.get("url", ""), result.get("webpUrl", ""), result.get("avifUrl", ""), result.get("posterUrl", ""), now_iso(), int(f["id"])],
                        )
                        conn.commit()
                    finally:
                        conn.close()
                success += 1
            except Exception as e:
                with _db_lock:
                    conn = db_conn()
                    try:
                        conn.execute("UPDATE upload_job_files SET status = 'failed', error_text = ?, updated_at = ? WHERE id = ?", [str(e)[:1200], now_iso(), int(f["id"])])
                        insert_event(conn, job_id, "error", f"File failed: {str(e)[:180]}", idx, len(files), str(f["original_name"] or ""))
                        conn.commit()
                    finally:
                        conn.close()
                failed += 1
            finally:
                try:
                    in_path = Path(str(f["input_path"])).resolve()
                    if in_path.exists():
                        in_path.unlink(missing_ok=True)
                except Exception:
                    pass

            processed += 1
            with _db_lock:
                conn = db_conn()
                try:
                    conn.execute("UPDATE upload_jobs SET processed_files = ?, success_files = ?, failed_files = ? WHERE id = ?", [processed, success, failed, job_id])
                    conn.commit()
                finally:
                    conn.close()

        final_status = "done"
        if canceled:
            final_status = "canceled"
        elif success == 0 and failed > 0:
            final_status = "failed"
        elif failed > 0:
            final_status = "partial_failed"

        with _db_lock:
            conn = db_conn()
            try:
                conn.execute("UPDATE upload_jobs SET status = ?, finished_at = ?, processed_files = ?, success_files = ?, failed_files = ? WHERE id = ?", [final_status, now_iso(), processed, success, failed, job_id])
                insert_event(conn, job_id, "done" if final_status == "done" else "error", f"Job {final_status}", processed, len(files))
                conn.commit()
            finally:
                conn.close()


def ensure_worker() -> None:
    global _worker_started
    if _worker_started:
        return
    t = threading.Thread(target=worker_loop, daemon=True, name="upload-api-worker")
    t.start()
    _worker_started = True


@app.after_request
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, x-admin-token"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


@app.route("/v1/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "upload-api", "time": now_iso(), "db": str(DB_PATH)})


@app.route("/v1/admin/rooms/<slug>/albums/upload", methods=["POST", "OPTIONS"])
def enqueue_album_upload(slug: str):
    if request.method == "OPTIONS":
        return ("", 204)
    auth = require_admin()
    if auth:
        return auth

    room_slug = slugify(slug, upper=False, max_len=120)
    if not room_slug:
        return jsonify({"ok": False, "error": "invalid_slug"}), 400

    album = str(request.args.get("album") or request.form.get("album") or "").strip()
    if not album:
        return jsonify({"ok": False, "error": "missing_album"}), 400

    files = [f for f in request.files.getlist("files") if f and f.filename]
    if not files:
        return jsonify({"ok": False, "error": "missing_files"}), 400

    job_id = f"job_{uuid.uuid4().hex[:16]}"
    created_at = now_iso()
    raw_job_dir = STORAGE_RAW / room_slug / job_id
    raw_job_dir.mkdir(parents=True, exist_ok=True)

    with _db_lock:
        conn = db_conn()
        try:
            conn.execute("INSERT INTO upload_jobs(id, room_slug, album_folder, status, created_at, total_files, requester_ip) VALUES(?,?,?,?,?,?,?)", [job_id, room_slug, album, "queued", created_at, len(files), request.remote_addr or ""])
            for idx, f in enumerate(files):
                original_name = secure_filename(f.filename or f"file_{idx+1}") or f"file_{idx+1}"
                guessed_mime = (f.mimetype or "application/octet-stream")
                ext = Path(original_name).suffix or ".bin"
                raw_path = raw_job_dir / f"{idx+1:04d}_{uuid.uuid4().hex[:8]}{ext}"
                f.save(str(raw_path))
                size_bytes = raw_path.stat().st_size if raw_path.exists() else 0
                conn.execute(
                    "INSERT INTO upload_job_files(job_id, sort_order, original_name, mime_type, size_bytes, input_path, status, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    [job_id, idx + 1, original_name, guessed_mime, int(size_bytes), str(raw_path), "queued", created_at, created_at],
                )
            insert_event(conn, job_id, "queued", "Files queued", 0, len(files))
            conn.commit()
        finally:
            conn.close()

    _wake_event.set()
    return jsonify({"ok": True, "jobId": job_id, "album": album, "queued": len(files), "status": "queued"})


@app.route("/v1/admin/upload/jobs/<job_id>", methods=["GET", "OPTIONS"])
def get_upload_job(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    auth = require_admin()
    if auth:
        return auth
    with _db_lock:
        conn = db_conn()
        try:
            summary = fetch_job_summary(conn, job_id)
            if not summary:
                return jsonify({"ok": False, "error": "job_not_found"}), 404
            return jsonify(summary)
        finally:
            conn.close()


@app.route("/v1/admin/upload/jobs/<job_id>/events", methods=["GET"])
def get_upload_job_events(job_id: str):
    auth = require_admin()
    if auth:
        return auth
    limit = max(1, min(int(request.args.get("limit", "100")), 500))
    with _db_lock:
        conn = db_conn()
        try:
            rows = conn.execute("SELECT phase, message, current_step, total_steps, file_name, created_at FROM upload_job_events WHERE job_id = ? ORDER BY id DESC LIMIT ?", [job_id, limit]).fetchall()
            return jsonify({"ok": True, "jobId": job_id, "events": [dict(r) for r in reversed(rows)]})
        finally:
            conn.close()


@app.route("/v1/admin/upload/queues", methods=["GET"])
def list_upload_queues():
    auth = require_admin()
    if auth:
        return auth
    status = str(request.args.get("status", "")).strip().lower()
    limit = max(1, min(int(request.args.get("limit", "100")), 500))

    where = ""
    params = []
    if status:
        where = " WHERE status = ? "
        params.append(status)

    with _db_lock:
        conn = db_conn()
        try:
            rows = conn.execute(f"SELECT * FROM upload_jobs {where} ORDER BY created_at DESC LIMIT ?", [*params, limit]).fetchall()
            stats_row = conn.execute(
                "SELECT SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued, SUM(CASE WHEN status='processing' THEN 1 ELSE 0 END) AS processing, SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done, SUM(CASE WHEN status='partial_failed' THEN 1 ELSE 0 END) AS partial_failed, SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed, SUM(CASE WHEN status='canceled' THEN 1 ELSE 0 END) AS canceled FROM upload_jobs"
            ).fetchone()
            return jsonify({"ok": True, "stats": dict(stats_row or {}), "jobs": [dict(r) for r in rows]})
        finally:
            conn.close()


@app.route("/v1/admin/upload/jobs/<job_id>/retry", methods=["POST", "OPTIONS"])
def retry_upload_job(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    auth = require_admin()
    if auth:
        return auth
    with _db_lock:
        conn = db_conn()
        try:
            job = conn.execute("SELECT * FROM upload_jobs WHERE id = ?", [job_id]).fetchone()
            if not job:
                return jsonify({"ok": False, "error": "job_not_found"}), 404
            conn.execute("UPDATE upload_job_files SET status = 'queued', error_text = NULL, updated_at = ? WHERE job_id = ? AND status IN ('failed','canceled')", [now_iso(), job_id])
            conn.execute("UPDATE upload_jobs SET status='queued', canceled=0, finished_at=NULL, started_at=NULL, processed_files=0, success_files=0, failed_files=0, error_text=NULL WHERE id = ?", [job_id])
            insert_event(conn, job_id, "queued", "Retry requested", 0, int(job["total_files"] or 0))
            conn.commit()
        finally:
            conn.close()
    _wake_event.set()
    return jsonify({"ok": True, "jobId": job_id, "status": "queued"})


@app.route("/v1/admin/upload/jobs/<job_id>/cancel", methods=["POST", "OPTIONS"])
def cancel_upload_job(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    auth = require_admin()
    if auth:
        return auth
    with _db_lock:
        conn = db_conn()
        try:
            job = conn.execute("SELECT * FROM upload_jobs WHERE id = ?", [job_id]).fetchone()
            if not job:
                return jsonify({"ok": False, "error": "job_not_found"}), 404
            conn.execute("UPDATE upload_jobs SET canceled = 1 WHERE id = ?", [job_id])
            conn.execute("UPDATE upload_job_files SET status='canceled', updated_at=? WHERE job_id=? AND status IN ('queued','processing')", [now_iso(), job_id])
            insert_event(conn, job_id, "error", "Cancel requested", int(job["processed_files"] or 0), int(job["total_files"] or 0))
            conn.commit()
        finally:
            conn.close()
    return jsonify({"ok": True, "jobId": job_id, "status": "canceled"})


init_db()
ensure_worker()

application = app
