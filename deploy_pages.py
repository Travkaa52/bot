#!/usr/bin/env python3
"""
deploy_pages.py — створює GitHub repo, пушить файли з кореня проекту,
вмикає GitHub Pages і повертає посилання.

Env vars:
  GH_TOKEN      — GitHub Personal Access Token (repo + pages scopes)
  GH_USERNAME   — GitHub username (якщо не задано — беремо з API)
  PAGES_REPO    — назва репо (default: funsDiia-pages)
  PAGES_BRANCH  — гілка для Pages (default: main)
  DEPLOY_FILES  — через кому, які файли/папки пушити (default: всі в корені крім .git)
"""

import os
import sys
import base64
import json
import time
import pathlib
import requests

# ─── Config ────────────────────────────────────────────────────────────────────
GH_TOKEN    = os.environ["GH_TOKEN"]          # обов'язково
GH_USERNAME = os.environ.get("GH_USERNAME")   # якщо не задано — визначається з токена
REPO_NAME   = os.environ.get("PAGES_REPO", "funsDiia-pages")
BRANCH      = os.environ.get("PAGES_BRANCH", "main")
DEPLOY_FILES = os.environ.get("DEPLOY_FILES", "")  # через кому; порожньо = всі

HEADERS = {
    "Authorization": f"token {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
API = "https://api.github.com"


def gh(method, path, **kwargs):
    url = f"{API}{path}"
    resp = requests.request(method, url, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"[GH API] {method.upper()} {path} → {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
    return resp.json() if resp.text else {}


# ─── 1. Визначити username ──────────────────────────────────────────────────────
def get_username():
    global GH_USERNAME
    if GH_USERNAME:
        return GH_USERNAME
    data = gh("GET", "/user")
    GH_USERNAME = data["login"]
    return GH_USERNAME


# ─── 2. Створити або отримати репо ─────────────────────────────────────────────
def ensure_repo(username):
    try:
        repo = gh("GET", f"/repos/{username}/{REPO_NAME}")
        print(f"✅ Репо вже існує: {repo['html_url']}")
        return repo
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise
    # Створюємо нове
    repo = gh("POST", "/user/repos", json={
        "name": REPO_NAME,
        "description": "GitHub Pages deployment",
        "private": False,
        "auto_init": False,
    })
    print(f"🆕 Репо створено: {repo['html_url']}")
    time.sleep(2)  # дати GitHub час ініціалізувати
    return repo


# ─── 3. Зібрати файли для деплою ───────────────────────────────────────────────
def collect_files():
    root = pathlib.Path(".")
    skip = {".git", ".github", "__pycache__", ".env", "deploy_pages.py"}

    if DEPLOY_FILES:
        targets = [root / f.strip() for f in DEPLOY_FILES.split(",") if f.strip()]
    else:
        targets = [p for p in root.iterdir() if p.name not in skip]

    files = {}
    for target in targets:
        if target.is_file():
            files[str(target.relative_to(root))] = target
        elif target.is_dir():
            for f in target.rglob("*"):
                if f.is_file() and ".git" not in f.parts:
                    files[str(f.relative_to(root))] = f

    print(f"📁 Файлів для деплою: {len(files)}")
    return files


# ─── 4. Отримати SHA файлу (якщо вже є в репо) ─────────────────────────────────
def get_file_sha(username, path):
    try:
        data = gh("GET", f"/repos/{username}/{REPO_NAME}/contents/{path}",
                  params={"ref": BRANCH})
        return data.get("sha")
    except requests.HTTPError:
        return None


# ─── 5. Запушити всі файли ──────────────────────────────────────────────────────
def push_files(username, files):
    total = len(files)
    for i, (rel_path, abs_path) in enumerate(files.items(), 1):
        content = abs_path.read_bytes()
        b64 = base64.b64encode(content).decode()
        sha = get_file_sha(username, rel_path)

        payload = {
            "message": f"deploy: {rel_path}",
            "content": b64,
            "branch": BRANCH,
        }
        if sha:
            payload["sha"] = sha

        gh("PUT", f"/repos/{username}/{REPO_NAME}/contents/{rel_path}", json=payload)
        print(f"  [{i}/{total}] ✔ {rel_path}")


# ─── 6. Увімкнути GitHub Pages ─────────────────────────────────────────────────
def enable_pages(username):
    # Перевіряємо, чи вже увімкнено
    try:
        pages = gh("GET", f"/repos/{username}/{REPO_NAME}/pages")
        url = pages.get("html_url") or f"https://{username}.github.io/{REPO_NAME}/"
        print(f"📄 GitHub Pages вже активний: {url}")
        return url
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise

    # Вмикаємо
    gh("POST", f"/repos/{username}/{REPO_NAME}/pages", json={
        "source": {"branch": BRANCH, "path": "/"}
    })
    url = f"https://{username}.github.io/{REPO_NAME}/"
    print(f"🚀 GitHub Pages увімкнено: {url}")
    return url


# ─── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("═" * 50)
    print("  GitHub Pages Deploy")
    print("═" * 50)

    username = get_username()
    print(f"👤 GitHub user: {username}")

    ensure_repo(username)
    files = collect_files()

    if not files:
        print("⚠️  Немає файлів для деплою. Перевірте DEPLOY_FILES.", file=sys.stderr)
        sys.exit(1)

    push_files(username, files)
    pages_url = enable_pages(username)

    print()
    print("═" * 50)
    print(f"✅ Готово!")
    print(f"🔗 GitHub Pages: {pages_url}")
    print("═" * 50)

    # Записуємо URL у файл (щоб workflow міг зберегти як artifact)
    with open("pages_url.txt", "w") as f:
        f.write(pages_url)

    # Якщо є GITHUB_OUTPUT (GitHub Actions) — зберігаємо як output
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"pages_url={pages_url}\n")


if __name__ == "__main__":
    main()
