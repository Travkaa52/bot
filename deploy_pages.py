#!/usr/bin/env python3
"""
deploy_pages.py — створює GitHub repo, пушить файли та вмикає GitHub Pages.

Env vars:
  GH_TOKEN      — GitHub Personal Access Token (repo + pages scopes) [ОБОВ'ЯЗКОВО]
  GH_USERNAME   — GitHub username (якщо не задано — береться з API)
  PAGES_REPO    — назва репо (default: funsDiia-pages)
  PAGES_BRANCH  — гілка для Pages (default: main)
  DEPLOY_FILES  — через кому, які файли/папки пушити (default: всі в корені крім .git)
"""

import os
import sys
import base64
import time
import pathlib
import requests

# ─── Config ────────────────────────────────────────────────────────────────────
GH_TOKEN = os.environ.get("GH_TOKEN", "").strip()
if not GH_TOKEN:
    print("❌ Помилка: Змінна середовища GH_TOKEN не встановлена!", file=sys.stderr)
    sys.exit(1)

GH_USERNAME = os.environ.get("GH_USERNAME", "").strip() or None

_raw_repo = os.environ.get("PAGES_REPO", "").strip()
REPO_NAME = _raw_repo if _raw_repo else "funsDiia-pages"

BRANCH = os.environ.get("PAGES_BRANCH", "").strip() or "main"
DEPLOY_FILES = os.environ.get("DEPLOY_FILES", "").strip()

HEADERS = {
    "Authorization": f"token {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
API = "https://api.github.com"


def gh(method: str, path: str, **kwargs):
    url = f"{API}{path}"
    resp = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
    if not resp.ok:
        print(f"[GH API Error] {method.upper()} {path} → {resp.status_code}: {resp.text[:300]}", file=sys.stderr)
        resp.raise_for_status()
    return resp.json() if resp.text else {}


# ─── 1. Визначити username ──────────────────────────────────────────────────────
def get_username() -> str:
    global GH_USERNAME
    if GH_USERNAME:
        return GH_USERNAME
    data = gh("GET", "/user")
    GH_USERNAME = data["login"]
    return GH_USERNAME


# ─── 2. Створити або отримати репо ─────────────────────────────────────────────
def ensure_repo(username: str):
    if not REPO_NAME or not REPO_NAME.strip():
        print("❌ Помилка: REPO_NAME не може бути порожнім!", file=sys.stderr)
        sys.exit(1)

    try:
        repo = gh("GET", f"/repos/{username}/{REPO_NAME}")
        print(f"✅ Репозиторій існує: {repo['html_url']}")
        return repo
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise

    # Створюємо новий репозиторій
    print(f"🆕 Створення нового репозиторію '{REPO_NAME}'...")
    repo = gh("POST", "/user/repos", json={
        "name": REPO_NAME,
        "description": "GitHub Pages deployment",
        "private": False,
        "auto_init": False,
        "has_issues": False,
        "has_projects": False,
        "has_wiki": False,
    })
    print(f"🆕 Репозиторій створено: {repo['html_url']}")
    time.sleep(2)  # Даємо GitHub час для ініціалізації
    return repo


# ─── 3. Зібрати файли для деплою ───────────────────────────────────────────────
def collect_files() -> dict[str, pathlib.Path]:
    root = pathlib.Path(".")
    skip = {".git", ".github", "__pycache__", ".env", "deploy_pages.py", "pages_url.txt"}

    if DEPLOY_FILES:
        targets = [root / f.strip() for f in DEPLOY_FILES.split(",") if f.strip()]
    else:
        targets = [p for p in root.iterdir() if p.name not in skip]

    files = {}
    for target in targets:
        if not target.exists():
            continue
        if target.is_file():
            files[str(target.relative_to(root)).replace("\\", "/")] = target
        elif target.is_dir():
            for f in target.rglob("*"):
                if f.is_file() and not any(part in skip for part in f.parts):
                    files[str(f.relative_to(root)).replace("\\", "/")] = f

    print(f"📁 Зібрано файлів для деплою: {len(files)}")
    return files


# ─── 4. Оптимізований пуш через Git Trees API (За один комміт) ─────────────────
def push_files_via_trees(username: str, files: dict[str, pathlib.Path]):
    """Пушить всі файли за один атомарний комміт через Git Data API."""
    print("🚀 Завантаження файлів та формування Git Tree...")
    tree_items = []

    for rel_path, abs_path in files.items():
        content = abs_path.read_bytes()
        
        # Створюємо blob для кожного файлу
        blob_resp = gh("POST", f"/repos/{username}/{REPO_NAME}/git/blobs", json={
            "content": base64.b64encode(content).decode("utf-8"),
            "encoding": "base64"
        })
        
        tree_items.append({
            "path": rel_path,
            "mode": "100644",
            "type": "blob",
            "sha": blob_resp["sha"]
        })

    # Отримуємо останній комміт (якщо гілка існує)
    parent_sha = None
    try:
        ref_data = gh("GET", f"/repos/{username}/{REPO_NAME}/git/ref/heads/{BRANCH}")
        parent_sha = ref_data["object"]["sha"]
    except requests.HTTPError:
        pass  # Репо ще порожній

    # Створюємо Tree
    tree_payload = {"tree": tree_items}
    if parent_sha:
        # Отримуємо sha самого tree від батьківського комміту
        parent_commit = gh("GET", f"/repos/{username}/{REPO_NAME}/git/commits/{parent_sha}")
        tree_payload["base_tree"] = parent_commit["tree"]["sha"]

    tree_resp = gh("POST", f"/repos/{username}/{REPO_NAME}/git/trees", json=tree_payload)

    # Створюємо Commit
    commit_payload = {
        "message": "deploy: update GitHub Pages content",
        "tree": tree_resp["sha"],
    }
    if parent_sha:
        commit_payload["parents"] = [parent_sha]

    commit_resp = gh("POST", f"/repos/{username}/{REPO_NAME}/git/commits", json=commit_payload)
    new_commit_sha = commit_resp["sha"]

    # Оновлюємо або створюємо посилання на гілку (ref)
    if parent_sha:
        gh("PATCH", f"/repos/{username}/{REPO_NAME}/git/refs/heads/{BRANCH}", json={
            "sha": new_commit_sha,
            "force": True
        })
    else:
        gh("POST", f"/repos/{username}/{REPO_NAME}/git/refs", json={
            "ref": f"refs/heads/{BRANCH}",
            "sha": new_commit_sha
        })

    print(f"✔ Успішно запушено {len(files)} файлів (Commit: {new_commit_sha[:7]})")


# ─── 5. Увімкнути GitHub Pages ─────────────────────────────────────────────────
def enable_pages(username: str) -> str:
    url = f"https://{username}.github.io/{REPO_NAME}/"
    
    # Перевіряємо, чи вже увімкнено
    try:
        pages = gh("GET", f"/repos/{username}/{REPO_NAME}/pages")
        active_url = pages.get("html_url") or url
        print(f"📄 GitHub Pages вже активний: {active_url}")
        return active_url
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise

    print("🚀 Активація GitHub Pages...")
    # Спроби активації з невеликим очікуванням (GitHub іноді потребує часу)
    for attempt in range(1, 4):
        try:
            gh("POST", f"/repos/{username}/{REPO_NAME}/pages", json={
                "source": {"branch": BRANCH, "path": "/"}
            })
            print(f"🚀 GitHub Pages увімкнено: {url}")
            return url
        except requests.HTTPError as e:
            if attempt == 3:
                raise
            time.sleep(2)

    return url


# ─── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("═" * 50)
    print("  GitHub Pages Deploy")
    print("═" * 50)

    username = get_username()
    print(f"👤 GitHub user: {username}")
    print(f"📦 Target Repo: {username}/{REPO_NAME} (Branch: {BRANCH})")

    ensure_repo(username)
    files = collect_files()

    if not files:
        print("⚠️ Немає файлів для деплою. Перевірте DEPLOY_FILES.", file=sys.stderr)
        sys.exit(1)

    push_files_via_trees(username, files)
    pages_url = enable_pages(username)

    print()
    print("═" * 50)
    print("✅ Деплой успішно заверКод скрипта виглядає робочим, але в ньому є **три ключові проблеми** (дві потенційні помилки завантаження файлів і одна проблема продуктивності), які краще виправити.

---

### Основні зауваження та виправлення

#### 1. Проблема `get_file_sha` для нових репозиторіїв та гілок
Якщо репозиторій тільки-но створено (або гілка `main` ще не існує), запит `GET /contents/{path}` для перевірки `sha` поверне помилку **`404 Not Found`**, яку ваша функція `get_file_sha` обробить через `except requests.HTTPError`. 

Однак, якщо для пустого репозиторію передати `branch: "main"` у першому ж запиті `PUT /contents/{path}`, GitHub API може видати помилку **`404 Branch Not Found`**, оскільки гілка `main` ще не існує (її створить перший коміт).
* **Рішення:** Для першого файлу не слід передавати параметер `"branch": BRANCH`, якщо репо зовсім порожнє, або ж варто обробляти створення default-гілки автоматично.

#### 2. Низька швидкість (N+1 запитів до API)
Зараз для *кожного* файлу робиться **2 HTTP-запити**: `GET` (для SHA) і `PUT` (для завантаження). Якщо у вас 50 файлів, це **100 запитів**. Для безкоштовного API-ліміту це довго, повільно та ризиковано потрапити під Secondary Rate Limits.
* **Рішення:** Отримувати деревархітектуру усього репозиторію за **один запит** через `GET /repos/{owner}/{repo}/git/trees/{BRANCH}?recursive=1` і будувати карту `path -> sha` у пам'яті.

#### 3. Шляхи у Windows / POSIX (`Path` роздільники)
На Windows `relative_to()` повертає шляхи з зворотними слешами (`folder\file.ext`), тоді як GitHub API вимагає строго POSIX-формат (`folder/file.ext`).
* **Рішення:** Примусово конвертувати шляхи через `.as_posix()`.

---

### Оптимізований та виправлений код

Нижче наведено повністю виправлений варіант із використанням дерева файлів для прискорення деплою в рази:

```python
#!/usr/bin/env python3
"""
deploy_pages.py — створює GitHub repo, пушить файли з кореня проекту,
вмикає GitHub Pages і повертає посилання.
"""

import os
import sys
import base64
import json
import time
import pathlib
import requests

# ─── Config ────────────────────────────────────────────────────────────────────
GH_TOKEN = os.environ["GH_TOKEN"]
GH_USERNAME = os.environ.get("GH_USERNAME")
REPO_NAME = os.environ.get("PAGES_REPO", "funsDiia-pages")
BRANCH = os.environ.get("PAGES_BRANCH", "main")
DEPLOY_FILES = os.environ.get("DEPLOY_FILES", "")

HEADERS = {
    "Authorization": f"token {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
API = "[https://api.github.com](https://api.github.com)"


def gh(method, path, **kwargs):
    url = f"{API}{path}"
    resp = requests.request(method, url, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"[GH API] {method.upper()} {path} → {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
    return resp.json() if resp.text else {}


def get_username():
    global GH_USERNAME
    if GH_USERNAME:
        return GH_USERNAME
    data = gh("GET", "/user")
    GH_USERNAME = data["login"]
    return GH_USERNAME


def ensure_repo(username):
    try:
        repo = gh("GET", f"/repos/{username}/{REPO_NAME}")
        print(f"✅ Репо вже існує: {repo['html_url']}")
        return repo
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise

    repo = gh("POST", "/user/repos", json={
        "name": REPO_NAME,
        "description": "GitHub Pages deployment",
        "private": False,
        "auto_init": False,
    })
    print(f"🆕 Репо створено: {repo['html_url']}")
    time.sleep(2)
    return repo


def collect_files():
    root = pathlib.Path(".")
    skip = {".git", ".github", "__pycache__", ".env", "deploy_pages.py", "pages_url.txt"}

    if DEPLOY_FILES:
        targets = [root / f.strip() for f in DEPLOY_FILES.split(",") if f.strip()]
    else:
        targets = [p for p in root.iterdir() if p.name not in skip]

    files = {}
    for target in targets:
        if target.is_file():
            # .as_posix() гарантує кросплатформенні шляхи з "/"
            files[target.relative_to(root).as_posix()] = target
        elif target.is_dir():
            for f in target.rglob("*"):
                if f.is_file() and ".git" not in f.parts:
                    files[f.relative_to(root).as_posix()] = f

    print(f"📁 Файлів для деплою: {len(files)}")
    return files


def get_existing_tree_shas(username):
    """
    Отримує карту { relative_path: sha } за 1 запит замість окремого N-запитів.
    """
    try:
        tree_data = gh("GET", f"/repos/{username}/{REPO_NAME}/git/trees/{BRANCH}", params={"recursive": "1"})
        return {item["path"]: item["sha"] for item in tree_data.get("tree", []) if item["type"] == "blob"}
    except requests.HTTPError:
        # Репо або гілка ще не існує
        return {}


def push_files(username, files):
    existing_shas = get_existing_tree_shas(username)
    total = len(files)

    for i, (rel_path, abs_path) in enumerate(files.items(), 1):
        content = abs_path.read_bytes()
        b64 = base64.b64encode(content).decode()
        
        sha = existing_shas.get(rel_path)

        payload = {
            "message": f"deploy: {rel_path}",
            "content": b64,
        }

        # Якщо гілка існує — вказуємо її
        if existing_shas or sha:
            payload["branch"] = BRANCH

        if sha:
            payload["sha"] = sha

        gh("PUT", f"/repos/{username}/{REPO_NAME}/contents/{rel_path}", json=payload)
        print(f"  [{i}/{total}] ✔ {rel_path}")


def enable_pages(username):
    try:
        pages = gh("GET", f"/repos/{username}/{REPO_NAME}/pages")
        url = pages.get("html_url") or f"https://{username}.github.io/{REPO_NAME}/"
        print(f"📄 GitHub Pages вже активний: {url}")
        return url
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise

    # Пауза для того, щоб GitHub встиг згенерувати commit ref для Pages
    time.sleep(3)

    gh("POST", f"/repos/{username}/{REPO_NAME}/pages", json={
        "source": {"branch": BRANCH, "path": "/"}
    })
    url = f"https://{username}.github.io/{REPO_NAME}/"
    print(f"🚀 GitHub Pages увімкнено: {url}")
    return url


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

    with open("pages_url.txt", "w") as f:
        f.write(pages_url)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"pages_url={pages_url}\n")


if __name__ == "__main__":
    main()
