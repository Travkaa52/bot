"""
chain_deploy.py — повний ланцюжок деплою:

  1. Оновлює index.html у папці «2/» даними замовлення (values_data)
  2. Пушить папку «2/» на ІНШИЙ GitHub акаунт (GH_TOKEN_2), вмикає Pages
  3. Генерує QR-код з отриманого URL → зберігає у 1/assets/q.png
  4. Пушить папку «1/» (з оновленим QR) на основний акаунт (PAGES_GH_TOKEN)
  5. Повертає обидва URL

Необхідні env vars:
  GH_TOKEN_2      — токен ІНШОГО акаунта (repo + pages scopes)
  GH_USERNAME_2   — (опційно) логін іншого акаунта; якщо порожньо — визначається через /user
  PAGES_REPO_2    — назва репо для папки 2 (default: "site2-pages")

  PAGES_GH_TOKEN  — токен основного акаунта
  GH_USERNAME     — (опційно) логін основного акаунта
  PAGES_REPO_1    — назва репо для папки 1 (default: "diia-main-pages")
"""

from __future__ import annotations

import base64
import logging
import os
import pathlib
import time

import qrcode
import requests

logger = logging.getLogger("chain_deploy")

API = "https://api.github.com"
FOLDER1_DIR = "1"
FOLDER2_DIR = "2"
QR_REL_PATH = "assets/q.png"       # відносно FOLDER1_DIR  →  1/assets/q.png
INDEX_REL_PATH = "index.html"       # файл в FOLDER2_DIR, який містить {{PLACEHOLDER}}
BRANCH = "main"


class DeployError(Exception):
    pass


# ── GitHub helpers ─────────────────────────────────────────────────────────────

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
        logger.error("[GH] %s %s → %s: %s", method, path, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    return resp.json() if resp.text else {}


def _get_username(token: str, override: str | None) -> str:
    if override:
        return override
    return _gh(token, "GET", "/user")["login"]


def _ensure_repo(token: str, username: str, repo_name: str) -> None:
    try:
        _gh(token, "GET", f"/repos/{username}/{repo_name}")
        logger.info("Repo exists: %s/%s", username, repo_name)
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise
        _gh(token, "POST", "/user/repos", json={
            "name": repo_name,
            "description": "Automated deploy",
            "private": False,
            "auto_init": False,
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
        raise DeployError(f"Папка '{local_dir}' не знайдена поруч з bot.py")
    files = {
        str(f.relative_to(root)).replace("\\", "/"): f
        for f in root.rglob("*")
        if f.is_file()
        and ".git" not in f.parts
        and "__pycache__" not in f.parts
    }
    if not files:
        raise DeployError(f"У папці '{local_dir}' немає файлів для деплою")
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


def _push_folder(token: str, username: str, repo: str, files: dict[str, pathlib.Path],
                 overrides: dict[str, bytes] | None = None) -> None:
    """Push files to GitHub repo. overrides allows replacing file content by rel_path."""
    overrides = overrides or {}
    logger.info("Pushing %d files to %s/%s", len(files), username, repo)
    for rel_path, abs_path in files.items():
        content = overrides.get(rel_path, abs_path.read_bytes())
        _push_file(token, username, repo, rel_path, content)


# ── Public API ─────────────────────────────────────────────────────────────────

def update_index_in_folder2(values_data: dict) -> None:
    """
    Замінює плейсхолдери у 2/index.html значеннями зі словника values_data.
    Шаблон може використовувати {{key}} для підстановки.
    Якщо index.html відсутній — нічого не робить (папка 2 може бути без шаблону).
    """
    index_path = pathlib.Path(FOLDER2_DIR) / INDEX_REL_PATH
    if not index_path.exists():
        logger.warning("2/index.html не знайдено, пропускаємо підстановку значень")
        return

    content = index_path.read_text(encoding="utf-8")
    for key, val in values_data.items():
        content = content.replace(f"{{{{{key}}}}}", str(val))
    index_path.write_text(content, encoding="utf-8")
    logger.info("2/index.html оновлено (%d значень)", len(values_data))


def deploy_folder2(values_data: dict | None = None) -> str:
    """
    1. Якщо передано values_data — оновлює 2/index.html
    2. Пушить папку '2/' на інший акаунт
    3. Вмикає GitHub Pages
    Повертає Pages URL.
    """
    token = os.getenv("GH_TOKEN_2", "").strip()
    if not token:
        raise DeployError("GH_TOKEN_2 не встановлено (додайте в GitHub Secrets)")

    username = _get_username(token, os.getenv("GH_USERNAME_2", "").strip() or None)
    repo = os.getenv("PAGES_REPO_2", "site2-pages").strip()

    if values_data:
        update_index_in_folder2(values_data)

    _ensure_repo(token, username, repo)
    _push_folder(token, username, repo, _collect_files(FOLDER2_DIR))

    url = _enable_pages(token, username, repo)
    logger.info("Folder2 Pages URL: %s", url)
    return url


def generate_qr(target_url: str) -> str:
    """
    Генерує QR-код з target_url і зберігає у 1/assets/q.png.
    Повертає шлях до файлу.
    """
    qr_path = pathlib.Path(FOLDER1_DIR) / QR_REL_PATH
    qr_path.parent.mkdir(parents=True, exist_ok=True)
    qrcode.make(target_url).save(qr_path)
    logger.info("QR збережено: %s → %s", target_url, qr_path)
    return str(qr_path)


def deploy_folder1() -> str:
    """
    Пушить папку '1/' (включаючи оновлений 1/assets/q.png) на основний акаунт.
    Повертає Pages URL.
    """
    token = os.getenv("PAGES_GH_TOKEN", "").strip()
    if not token:
        raise DeployError("PAGES_GH_TOKEN не встановлено (додайте в GitHub Secrets)")

    username = _get_username(token, os.getenv("GH_USERNAME", "").strip() or None)
    repo = os.getenv("PAGES_REPO_1", "diia-main-pages").strip()

    _ensure_repo(token, username, repo)
    _push_folder(token, username, repo, _collect_files(FOLDER1_DIR))

    url = _enable_pages(token, username, repo)
    logger.info("Folder1 Pages URL: %s", url)
    return url


def run_full_chain(values_data: dict | None = None) -> dict:
    """
    Повний ланцюжок:
      1. Оновлює 2/index.html (якщо передано values_data)
      2. Пушить папку 2/ → отримує URL
      3. Генерує QR з URL → 1/assets/q.png
      4. Пушить папку 1/ (з новим QR)

    Повертає {"folder2_url": ..., "folder1_url": ..., "qr_path": ...}
    """
    folder2_url = deploy_folder2(values_data)
    qr_path = generate_qr(folder2_url)
    folder1_url = deploy_folder1()
    return {
        "folder2_url": folder2_url,
        "folder1_url": folder1_url,
        "qr_path": qr_path,
    }
