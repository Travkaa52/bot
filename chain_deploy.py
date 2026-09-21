from __future__ import annotations

import base64
import logging
import os
import pathlib
import re
import time
import uuid
from datetime import datetime, timezone

import qrcode
import requests

logger = logging.getLogger("chain_deploy")

API = "https://api.github.com"
FOLDER1_DIR = "1"
FOLDER2_DIR = "2"
QR_REL_PATH = "assets/q.png"
INDEX_REL_PATH = "index.html"
BRANCH = "main"


class DeployError(Exception):
    pass


def _headers(token: str) -> dict:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _gh(token: str, method: str, path: str, **kwargs):
    resp = requests.request(
        method,
        f"{API}{path}",
        headers=_headers(token),
        timeout=30,
        **kwargs,
    )
    if not resp.ok:
        logger.error("[GH] %s %s -> %s: %s", method, path, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    return resp.json() if resp.text else {}


def _get_username(token: str, override: str | None) -> str:
    if override and override.strip():
        return override.strip()
    return _gh(token, "GET", "/user")["login"]


def _make_repo_name(prefix: str, order_id: str | int | None) -> str:
    """Генерує УНІКАЛЬНЕ ім'я репо. Якщо ID порожній — додає унікальний UUID/Timestamp."""
    clean_prefix = (prefix or "site2").strip()

    uid = uuid.uuid4().hex[:6]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    if order_id:
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", str(order_id))[:30]
        name = f"{clean_prefix}-{safe_id}-{uid}"
    else:
        name = f"{clean_prefix}-{ts}-{uid}"

    return name[:100]


def _create_new_repo_strict(token: str, username: str, repo_name: str) -> None:
    """Створює СВІЖИЙ репозиторій. Якщо такий вже існує — кидає DeployError."""
    if not repo_name or not repo_name.strip():
        raise DeployError("Назва репозиторію порожня!")

    repo_name = repo_name.strip()

    # Перевіряємо, чи існує репо
    try:
        _gh(token, "GET", f"/repos/{username}/{repo_name}")
        raise DeployError(f"Репозиторій {username}/{repo_name} ВЖЕ ІСНУЄ! Перезапис заборонено.")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code != 404:
            raise

    # Створюємо новий з auto_init=True, щоб створити дефолтну гілку
    logger.info("Creating NEW repository: %s/%s", username, repo_name)
    _gh(
        token,
        "POST",
        "/user/repos",
        json={
            "name": repo_name,
            "description": "FunsDiia order deploy",
            "private": False,
            "auto_init": True,
            "has_issues": False,
            "has_projects": False,
            "has_wiki": False,
        },
    )
    logger.info("Repo created: %s/%s", username, repo_name)
    time.sleep(2)


def _push_file(token: str, username: str, repo: str, rel_path: str, content: bytes) -> None:
    path = f"/repos/{username}/{repo}/contents/{rel_path}"
    sha = None

    # Перевіряємо, чи файл вже існує (потрібно sha для оновлення)
    try:
        res = _gh(token, "GET", path)
        if isinstance(res, dict) and "sha" in res:
            sha = res["sha"]
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code != 404:
            raise

    payload = {
        "message": f"deploy: {rel_path}",
        "content": base64.b64encode(content).decode("utf-8"),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha

    _gh(token, "PUT", path, json=payload)


def _collect_files(local_dir: str) -> dict[str, pathlib.Path]:
    root = pathlib.Path(local_dir)
    if not root.is_dir():
        raise DeployError(f"Папка '{local_dir}' не знайдена")
    files = {
        str(f.relative_to(root)).replace("\\", "/"): f
        for f in root.rglob("*")
        for f_part in f.parts
        if f.is_file() and ".git" not in f.parts and "__pycache__" not in f.parts
    }
    if not files:
        raise DeployError(f"У папці '{local_dir}' немає файлів")
    return files


def _enable_pages(token: str, username: str, repo: str) -> str:
    url = f"https://{username}.github.io/{repo}/"
    try:
        _gh(
            token,
            "POST",
            f"/repos/{username}/{repo}/pages",
            json={"source": {"branch": BRANCH, "path": "/"}},
        )
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 409:
            logger.info("Pages already enabled for %s/%s", username, repo)
        else:
            raise
    return url


def _push_folder(
    token: str,
    username: str,
    repo: str,
    files: dict[str, pathlib.Path],
    overrides: dict[str, bytes] | None = None,
) -> None:
    overrides = overrides or {}
    logger.info("Pushing %d files to %s/%s", len(files), username, repo)
    for rel_path, abs_path in files.items():
        content = overrides.get(rel_path, abs_path.read_bytes())
        _push_file(token, username, repo, rel_path, content)


def get_rendered_index_content(values_data: dict) -> bytes:
    """Підставляє значення в 2/index.html В ПАМ'ЯТІ, НЕ ТОРКАЮЧИСЬ диска."""
    index_path = pathlib.Path(FOLDER2_DIR) / INDEX_REL_PATH
    if not index_path.exists():
        logger.warning("2/index.html не знайдено")
        return b""

    content = index_path.read_text(encoding="utf-8")
    for key, val in values_data.items():
        content = content.replace(f"{{{{{key}}}}}", str(val))

    return content.encode("utf-8")


def deploy_folder2_for_order(
    values_data: dict | None = None,
    order_id: str | None = None,
) -> tuple[str, str]:
    """Деплоїть папку 2/ у НОВИЙ унікальний репозиторій. Повертає (url, repo_name)"""
    token = os.getenv("GH_TOKEN_2", "").strip()
    if not token:
        raise DeployError("GH_TOKEN_2 не встановлено")

    username = _get_username(token, os.getenv("GH_USERNAME_2"))
    prefix = os.getenv("PAGES_REPO_2", "").strip() or "site2"

    repo_name = _make_repo_name(prefix, order_id)
    logger.info("Order %s -> BRAND NEW repo: %s/%s", order_id, username, repo_name)

    overrides = {}
    if values_data:
        overrides[INDEX_REL_PATH] = get_rendered_index_content(values_data)

    _create_new_repo_strict(token, username, repo_name)

    _push_folder(token, username, repo_name, _collect_files(FOLDER2_DIR), overrides=overrides)

    url = _enable_pages(token, username, repo_name)
    logger.info("Order %s Pages URL: %s", order_id, url)
    return url, repo_name


def generate_qr(target_url: str) -> str:
    qr_path = pathlib.Path(FOLDER1_DIR) / QR_REL_PATH
    qr_path.parent.mkdir(parents=True, exist_ok=True)
    qrcode.make(target_url).save(qr_path)
    logger.info("QR saved: %s -> %s", target_url, qr_path)
    return str(qr_path)


def deploy_folder1() -> str:
    """Папка 1/ пушиться в постійний репо (тут оновлення дозволено)."""
    token = os.getenv("PAGES_GH_TOKEN", "").strip()
    if not token:
        raise DeployError("PAGES_GH_TOKEN не встановлено")

    username = _get_username(token, os.getenv("GH_USERNAME"))
    repo = os.getenv("PAGES_REPO_1", "").strip() or "diia-main-pages"

    try:
        _gh(token, "GET", f"/repos/{username}/{repo}")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            _gh(
                token,
                "POST",
                "/user/repos",
                json={"name": repo, "private": False, "auto_init": True},
            )
            time.sleep(2)
        else:
            raise

    _push_folder(token, username, repo, _collect_files(FOLDER1_DIR))

    try:
        res = _gh(token, "GET", f"/repos/{username}/{repo}/pages")
        return res.get("html_url", f"https://{username}.github.io/{repo}/")
    except requests.HTTPError:
        return _enable_pages(token, username, repo)


def run_full_chain(values_data: dict | None = None, order_id: str | None = None) -> dict:
    folder2_url, repo2_name = deploy_folder2_for_order(values_data=values_data, order_id=order_id)

    qr_path = generate_qr(folder2_url)

    folder1_url = deploy_folder1()

    return {
        "folder2_url": folder2_url,
        "folder1_url": folder1_url,
        "qr_path": qr_path,
        "repo2_name": repo2_name,
        "order_id": order_id,
    }
