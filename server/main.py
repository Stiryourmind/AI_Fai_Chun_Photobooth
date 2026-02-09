import os
import io
import json
import base64
import random
import uuid
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, List

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Form, Query, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Google Drive OAuth
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError


# =========================
# ENV
# =========================
load_dotenv()

API_KEY = (os.getenv("RUNNINGHUB_API_KEY") or "").strip()
WORKFLOW_ID = (os.getenv("RUNNINGHUB_WORKFLOW_ID") or "").strip()
BASE = (os.getenv("RUNNINGHUB_BASE") or "https://www.runninghub.ai").strip()
PORT = int(os.getenv("PORT") or "8080")

RH_CREATE = f"{BASE}/task/openapi/create"
RH_OUTPUTS = f"{BASE}/task/openapi/outputs"

# Drive
GDRIVE_ENABLED = (os.getenv("GDRIVE_ENABLED") or "0").strip() == "1"
GDRIVE_ROOT_FOLDER_ID = (os.getenv("GDRIVE_ROOT_FOLDER_ID") or "").strip()
GDRIVE_TOKEN_PATH = (os.getenv("GDRIVE_TOKEN_PATH") or "").strip()
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


# =========================
# PATHS
# =========================
SERVER_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SERVER_DIR.parent
WEB_DIR = PROJECT_ROOT / "web"

ARCHIVE_DIR = SERVER_DIR / "archive"
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# Token path: Render secret file should be /etc/secrets/token.json
TOKEN_PATH = Path(GDRIVE_TOKEN_PATH) if GDRIVE_TOKEN_PATH else (SERVER_DIR / "token.json")


# =========================
# APP
# =========================
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # demo-friendly
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# HELPERS
# =========================
def require_env():
    if not API_KEY or not WORKFLOW_ID:
        return JSONResponse(
            status_code=500,
            content={"error": "Missing RUNNINGHUB_API_KEY or RUNNINGHUB_WORKFLOW_ID"},
        )
    return None


async def rh_post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=180) as client:
        r = await client.post(url, json=payload)
        try:
            j = r.json()
        except Exception:
            j = {"raw": r.text}
        if r.status_code >= 400:
            raise RuntimeError(f"RunningHub HTTP {r.status_code}: {j}")
        return j


def extract_task_id(resp: Dict[str, Any]) -> Optional[str]:
    data = resp.get("data")
    if isinstance(data, dict):
        return data.get("taskId") or data.get("task_id") or data.get("id")
    if isinstance(data, str):
        return data
    return resp.get("taskId") or resp.get("task_id")


def find_first_http_url(obj: Any) -> Optional[str]:
    found: List[str] = []

    def walk(x: Any):
        if isinstance(x, list):
            for i in x:
                walk(i)
        elif isinstance(x, dict):
            for k in ["fileUrl", "url", "file", "path"]:
                v = x.get(k)
                if isinstance(v, str) and v.startswith("http"):
                    found.append(v)
            for v in x.values():
                walk(v)

    walk(obj)
    return found[0] if found else None


def make_archive_id() -> str:
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"{ts}_{uid}"


def get_play_dir(archive_id: str) -> Path:
    d = ARCHIVE_DIR / archive_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# =========================
# Google Drive (OAuth)
# =========================
def get_drive():
    if not TOKEN_PATH.exists():
        raise RuntimeError(f"token.json not found at {TOKEN_PATH}")
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    return build("drive", "v3", credentials=creds)


def drive_create_folder(drive, name: str, parent_id: str = "") -> str:
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        body["parents"] = [parent_id]
    folder = drive.files().create(body=body, fields="id").execute()
    return folder["id"]


def drive_upload_bytes(drive, name: str, data: bytes, mime: str, parent_id: str) -> Dict[str, Any]:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    file = drive.files().create(
        body={"name": name, "parents": [parent_id]},
        media_body=media,
        fields="id, webViewLink",
    ).execute()
    return file


def drive_upload_text(drive, name: str, text: str, parent_id: str) -> Dict[str, Any]:
    return drive_upload_bytes(drive, name, text.encode("utf-8"), "application/json", parent_id)


# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/api/run")
async def api_run(
    background_tasks: BackgroundTasks,
    photo: UploadFile = File(...),
    templateId: str = Form("fai_chun_01"),
    seed: Optional[int] = Form(None),
):
    missing = require_env()
    if missing:
        return missing

    TEMPLATE_TO_INT = {
        "fai_chun_01": 1,
        "fai_chun_02": 2,
        "fai_chun_03": 3,
        "fai_chun_04": 4,
    }

    try:
        img_bytes = await photo.read()
        img_b64 = base64.b64encode(img_bytes).decode("utf-8")

        archive_id = make_archive_id()
        play_dir = get_play_dir(archive_id)

        # local archive input
        (play_dir / "input.jpg").write_bytes(img_bytes)

        if seed is None:
            seed = random.randint(100_000_000, 999_999_999)

        switch_value = TEMPLATE_TO_INT.get(templateId, 1)

        # ---- Drive folder + input upload (IMPORTANT) ----
        drive_folder_id = None
        if GDRIVE_ENABLED:
            try:
                drive = get_drive()
                drive_folder_id = drive_create_folder(drive, archive_id, GDRIVE_ROOT_FOLDER_ID)
                print("[RUN] Drive folder OK:", drive_folder_id, "root=", GDRIVE_ROOT_FOLDER_ID, "tokenPath=", str(TOKEN_PATH))
                drive_upload_bytes(drive, "input.jpg", img_bytes, "image/jpeg", drive_folder_id)
            except Exception as e:
                print("[RUN] Drive folder/upload FAILED:", repr(e))
                drive_folder_id = None

        # ---- RunningHub run ----
        node_info_list = [
            {"nodeId": "627", "fieldName": "data", "fieldValue": img_b64},
            {"nodeId": "582", "fieldName": "value", "fieldValue": str(int(switch_value))},
            {"nodeId": "471", "fieldName": "noise_seed", "fieldValue": str(int(seed))},
        ]

        run_resp = await rh_post_json(
            RH_CREATE,
            {"apiKey": API_KEY, "workflowId": WORKFLOW_ID, "nodeInfoList": node_info_list},
        )

        task_id = extract_task_id(run_resp)
        if not task_id:
            raise RuntimeError(f"Missing taskId from run response: {run_resp}")

        # meta.json local
        meta = {
            "archiveId": archive_id,
            "taskId": task_id,
            "templateId": templateId,
            "seed": seed,
            "createdAt": datetime.now().isoformat(),
            "driveFolderId": drive_folder_id,  # MUST not be None if drive is working
        }
        meta_path = play_dir / "meta.json"
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

        # meta.json Drive (optional, but you want it)
        if GDRIVE_ENABLED and drive_folder_id:
            try:
                drive = get_drive()
                drive_upload_text(drive, "meta.json", json.dumps(meta, indent=2, ensure_ascii=False), drive_folder_id)
            except Exception as e:
                print("[RUN] Upload meta.json FAILED:", repr(e))

        # ---- Background auto-finalize (archives output even without download) ----
        background_tasks.add_task(finalize_job, task_id, archive_id)

        return {"taskId": task_id, "archiveId": archive_id}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/result")
async def api_result(taskId: str = Query(...)):
    missing = require_env()
    if missing:
        return missing

    try:
        out = await rh_post_json(RH_OUTPUTS, {"apiKey": API_KEY, "taskId": taskId})
        data = out.get("data", out)
        image_url = find_first_http_url(data)

        if not image_url:
            return {"ready": False, "imageUrl": None}

        return {"ready": True, "imageUrl": image_url}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/finalize")
async def api_finalize(taskId: str = Form(...), archiveId: str = Form(...)):
    # manual fallback endpoint (frontend can still call it)
    await finalize_job(taskId, archiveId)
    return {"ok": True}


@app.get("/api/download")
async def api_download(taskId: str, archiveId: str):
    """Download local output if already finalized; otherwise finalize now then download."""
    play_dir = get_play_dir(archiveId)
    output_path = play_dir / "output.png"
    if not output_path.exists():
        await finalize_job(taskId, archiveId)

    if not output_path.exists():
        return JSONResponse(status_code=404, content={"error": "Output not ready yet"})

    return FileResponse(
        output_path,
        media_type="image/png",
        filename=f"AI_FaiChun_{archiveId}.png",
    )


# =========================
# AUTO FINALIZE JOB
# =========================
async def finalize_job(task_id: str, archive_id: str):
    """Poll RunningHub output, then archive output locally + Drive, and upload meta.json."""
    play_dir = get_play_dir(archive_id)
    meta_path = play_dir / "meta.json"

    meta: Dict[str, Any] = {}
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

    output_path = play_dir / "output.png"
    if output_path.exists():
        return  # already finalized

    # Poll outputs up to ~10 minutes (300 * 2s)
    image_url = None
    for _ in range(300):
        try:
            out = await rh_post_json(RH_OUTPUTS, {"apiKey": API_KEY, "taskId": task_id})
            data = out.get("data", out)
            image_url = find_first_http_url(data)
            if image_url:
                break
        except Exception:
            pass
        await asyncio.sleep(2)

    if not image_url:
        meta["finalizeError"] = "Timeout waiting for RunningHub output"
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        print("[FINALIZE] Timeout, no output URL.")
        return

    # Download output bytes
    async with httpx.AsyncClient(timeout=180) as client:
        r = await client.get(image_url)
        r.raise_for_status()
        img_bytes = r.content

    # Save locally
    output_path.write_bytes(img_bytes)

    # Upload to Drive (if folder exists)
    drive_folder_id = meta.get("driveFolderId")
    print("[FINALIZE] enabled=", GDRIVE_ENABLED, "driveFolderId=", drive_folder_id, "tokenPath=", str(TOKEN_PATH))

    if GDRIVE_ENABLED and drive_folder_id:
        try:
            drive = get_drive()
            drive_upload_bytes(drive, "output.png", img_bytes, "image/png", drive_folder_id)
            meta["driveOutputUploadedAt"] = datetime.now().isoformat()
            print("[FINALIZE] output.png uploaded to Drive OK")
        except HttpError as e:
            print("[FINALIZE] Drive HttpError:", str(e))
        except Exception as e:
            print("[FINALIZE] Drive upload Exception:", repr(e))

    # Update meta and save locally
    meta["outputUrl"] = image_url
    meta["finalizedAt"] = datetime.now().isoformat()
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    # Upload updated meta.json to Drive too
    if GDRIVE_ENABLED and drive_folder_id:
        try:
            drive = get_drive()
            drive_upload_text(drive, "meta.json", json.dumps(meta, indent=2, ensure_ascii=False), drive_folder_id)
            print("[FINALIZE] meta.json uploaded to Drive OK")
        except Exception as e:
            print("[FINALIZE] meta.json upload FAILED:", repr(e))


# =========================
# SERVE WEBSITE
# =========================
app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="localhost", port=PORT, reload=True)
