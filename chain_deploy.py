from __future__ import annotations

import base64
import dataclasses
import io
import logging
import os
import pathlib
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import qrcode
import requests

logger = logging.getLogger("chain_deploy")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class DeployError(Exception):
    """Базовий клас для помилок деплою."""
    pass


class GitHubAPIError(DeployError):
    """Помилка взаємодії з GitHub API."""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: str = ""):
        super().__init__(f"{message} (status={status_code}): {response_text[:200]}")
        self.status_code = status_code
        self.response_text = response_text


@dataclasses.dataclass
class DeployConfig:
    """Конфігурація середовища та параметрів деплою."""
    api_url: str = "https://api.github.com"
    branch: str = "main"
    folder1_dir: str = "1"
    folder2_dir: str = "2"
    qr_rel_path: str = "assets/q.png"
    index_rel_path: str = "index.html"
    
    # Credentials for Folder 2 (New Repositories)
    token_2: str = dataclasses.field(default_factory=lambda: os.getenv("GH_TOKEN_2", "").strip())
    username_2_override: Optional[str] = dataclasses.field(default_factory=lambda: os.getenv("GH_USERNAME_2"))
    repo_2_prefix: str = dataclasses.field(default_factory=lambda: os.getenv("PAGES_REPO_2", "site2").strip())

    # Credentials for Folder 1 (Main Repository)
    token_1: str = dataclasses.field(default_factory=lambda: os.getenv("PAGES_GH_TOKEN", "").strip())
    username_1_override: Optional[str] = dataclasses.field(default_factory=lambda: os.getenv("GH_USERNAME"))
    repo_1_name: str = dataclasses.field(default_factory=lambda: os.getenv("PAGES_REPO_1", "diia-main-pages").strip())

    max_retries: int = 3
    timeout: int = 30


class GitHubClient:
    """Клієнт для обробки запитів до GitHub REST/Git Trees API з підтримкою Retry."""
    
    def __init__(self, token: str, config: DeployConfig):
        if not token:
            raise DeployError("GitHub API token обов'язковий!")
        self.token = token
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })

    def request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.config.api_url}{path}"
        kwargs.setdefault("timeout", self.config.timeout)
        
        for attempt in range(1, self.config.max_retries + 1):
            try:
                resp = self.session.request(method, url, **kwargs)
                
                # Обробка Rate Limit або Server Error
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.config.max_retries:
                    sleep_time = attempt * 2
                    logger.warning("[GH Retry %d/%d] HTTP %s for %s %s. Waiting %ds...", 
                                   attempt, self.config.max_retries, resp.status_code, method, path, sleep_time)
                    time.sleep(sleep_time)
                    continue

                if not resp.ok:
                    logger.error("[GH Error] %s %s -> %s: %s", method, path, resp.status_code, resp.text[:300])
                    raise GitHubAPIError("GitHub API request failed", resp.status_code, resp.text)
                
                return resp.json() if resp.text else {}

            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt == self.config.max_retries:
                    raise DeployError(f"Мережева помилка після {self.config.max_retries} спроб: {e}") from e
                time.sleep(attempt * 2)

    def get_username(self, override: Optional[str] = None) -> str:
        if override and override.strip():
            return override.strip()
        return self.request("GET", "/user")["login"]


def _make_repo_name(prefix: str, order_id: Optional[str | int]) -> str:
    """Генерує унікальне і безпечне ім'я репозиторію."""
    clean_prefix = (prefix or "site2").strip().lower()
    clean_prefix = re.sub(r"[^a-z0-9_-]", "-", clean_prefix)

    uid = uuid.uuid4().hex[:6]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    if order_id:
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", str(order_id))[:30]
        name = f"{clean_prefix}-{safe_id}-{uid}"
    else:
        name = f"{clean_prefix}-{ts}-{uid}"

    return name[:100].lower()


def _collect_files(local_dir: str) -> Dict[str, pathlib.Path]:
    """Збирає всі файли з диска, ігноруючи .git та __pycache__."""
    root = pathlib.Path(local_dir)
    if not root.is_dir():
        raise DeployError(f"Папка '{local_dir}' не знайдена!")
    
    files = {}
    for f in root.rglob("*"):
        if f.is_file() and not any(part.startswith(".") or part == "__pycache__" for part in f.parts):
            rel_path = str(f.relative_to(root)).replace("\\", "/")
            files[rel_path] = f

    if not files:
        raise DeployError(f"Папка '{local_dir}' порожня!")
    return files


def _push_folder_atomerly(
    gh: GitHubClient,
    username: str,
    repo: str,
    files: Dict[str, pathlib.Path],
    overrides: Optional[Dict[str, bytes]] = None,
    branch: str = "main",
    commit_message: str = "deploy: batch update"
) -> None:
    """
    Завантажує всю папку ОДНИМ коммітом за допомогою Git Trees API (значно швидше ніж окремі PUT).
    """
    overrides = overrides or {}
    logger.info("Batch pushing %d files to %s/%s via Git Data API...", len(files), username, repo)

    # 1. Створюємо blobs для всіх файлів
    tree_items: List[Dict[str, Any]] = []
    for rel_path, abs_path in files.items():
        content = overrides.get(rel_path, abs_path.read_bytes())
        
        # Створення Blob
        blob_res = gh.request("POST", f"/repos/{username}/{repo}/git/blobs", json={
            "content": base64.b64encode(content).decode("utf-8"),
            "encoding": "base64"
        })
        
        tree_items.append({
            "path": rel_path,
            "mode": "100644",
            "type": "blob",
            "sha": blob_res["sha"]
        })

    # 2. Отримуємо останній комміт гілки (якщо є)
    parent_sha = None
    try:
        ref_res = gh.request("GET", f"/repos/{username}/{repo}/git/ref/heads/{branch}")
        parent_sha = ref_res["object"]["sha"]
    except GitHubAPIError as e:
        if e.status_code != 404:
            raise

    # 3. Створюємо дерево (Tree)
    tree_payload: Dict[str, Any] = {"tree": tree_items}
    if parent_sha:
        # Отримуємо tree sha батьківського комміту
        commit_info = gh.request("GET", f"/repos/{username}/{repo}/git/commits/{parent_sha}")
        tree_payload["base_tree"] = commit_info["tree"]["sha"]

    tree_res = gh.request("POST", f"/repos/{username}/{repo}/git/trees", json=tree_payload)
    new_tree_sha = tree_res["sha"]

    # 4. Створюємо комміт
    commit_payload: Dict[str, Any] = {
        "message": commit_message,
        "tree": new_tree_sha,
    }
    if parent_sha:
        commit_payload["parents"] = [parent_sha]

    commit_res = gh.request("POST", f"/repos/{username}/{repo}/git/commits", json=commit_payload)
    new_commit_sha = commit_res["sha"]

    # 5. Оновлюємо або створюємо посилання (Ref)
    if parent_sha:
        gh.request("PATCH", f"/repos/{username}/{repo}/git/refs/heads/{branch}", json={
            "sha": new_commit_sha,
            "force": True
        })
    else:
        gh.request("POST", f"/repos/{username}/{repo}/git/refs", json={
            "ref": f"refs/heads/{branch}",
            "sha": new_commit_sha
        })


def _create_new_repo_strict(gh: GitHubClient, username: str, repo_name: str) -> None:
    """Створює новий порожній репозиторій. Кидає виняток, якщо існує."""
    if not repo_name:
        raise DeployError("Назва репозиторію не може бути порожньою!")

    try:
        gh.request("GET", f"/repos/{username}/{repo_name}")
        raise DeployError(f"Репозиторій {username}/{repo_name} ВЖЕ ІСНУЄ!")
    except GitHubAPIError as e:
        if e.status_code != 404:
            raise

    logger.info("Creating fresh repository: %s/%s", username, repo_name)
    gh.request("POST", "/user/repos", json={
        "name": repo_name,
        "description": "Auto-generated order deploy",
        "private": False,
        "auto_init": True,  # Створює початковий комміт для ініціалізації гілки main
        "has_issues": False,
        "has_projects": False,
        "has_wiki": False,
    })
    time.sleep(2)  # Даємо GitHub час для ініціалізації репо


def _enable_pages(gh: GitHubClient, username: str, repo: str, branch: str = "main") -> str:
    """Вмикає GitHub Pages для репозиторію."""
    url = f"https://{username}.github.io/{repo}/"
    try:
        gh.request("POST", f"/repos/{username}/{repo}/pages", json={
            "source": {"branch": branch, "path": "/"}
        })
    except GitHubAPIError as e:
        if e.status_code == 409:
            logger.info("Pages already active for %s/%s", username, repo)
        else:
            raise
    return url


def get_rendered_index_content(index_path: pathlib.Path, values_data: Dict[str, Any]) -> bytes:
    """Підставляє шаблоновані значення у HTML без перезапису файлів на диску."""
    if not index_path.exists():
        logger.warning("%s не знайдено, пропуск підстановки", index_path)
        return b""

    content = index_path.read_text(encoding="utf-8")
    for key, val in values_data.items():
        content = content.replace(f"{{{{{key}}}}}", str(val if val is not None else ""))

    return content.encode("utf-8")


def deploy_folder2_for_order(
    config: DeployConfig,
    values_data: Optional[Dict[str, Any]] = None,
    order_id: Optional[str] = None,
) -> Tuple[str, str]:
    """Створює окремий репозиторій під замовлення та деплоїть туди Folder 2."""
    gh = GitHubClient(config.token_2, config)
    username = gh.get_username(config.username_2_override)
    repo_name = _make_repo_name(config.repo_2_prefix, order_id)

    logger.info("Order %s -> Generating repo: %s/%s", order_id, username, repo_name)

    overrides = {}
    if values_data:
        index_file = pathlib.Path(config.folder2_dir) / config.index_rel_path
        overrides[config.index_rel_path] = get_rendered_index_content(index_file, values_data)

    _create_new_repo_strict(gh, username, repo_name)
    
    files = _collect_files(config.folder2_dir)
    _push_folder_atomerly(
        gh, username, repo_name, files, 
        overrides=overrides, branch=config.branch, commit_message=f"deploy order: {order_id}"
    )

    url = _enable_pages(gh, username, repo_name, branch=config.branch)
    logger.info("Order %s deployed at %s", order_id, url)
    return url, repo_name


def generate_qr(target_url: str, save_path: pathlib.Path) -> bytes:
    """Генерує QR-код, повертає його байтами та записує за вказаним шляхом."""
    save_path.parent.mkdir(parents=True, exist_ok=True)
    
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )
    qr.add_data(target_url)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")
    
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    img_bytes = buf.getvalue()

    save_path.write_bytes(img_bytes)
    logger.info("QR code generated for %s -> saved at %s", target_url, save_path)
    return img_bytes


def deploy_folder1(config: DeployConfig, qr_bytes_override: Optional[bytes] = None) -> str:
    """Пушить Folder 1 в основний центральний репозиторій."""
    gh = GitHubClient(config.token_1, config)
    username = gh.get_username(config.username_1_override)
    repo = config.repo_1_name

    try:
        gh.request("GET", f"/repos/{username}/{repo}")
    except GitHubAPIError as e:
        if e.status_code == 404:
            logger.info("Main repo %s/%s missing. Creating...", username, repo)
            gh.request("POST", "/user/repos", json={"name": repo, "private": False, "auto_init": True})
            time.sleep(2)
        else:
            raise

    overrides = {}
    if qr_bytes_override:
        overrides[config.qr_rel_path] = qr_bytes_override

    files = _collect_files(config.folder1_dir)
    _push_folder_atomerly(
        gh, username, repo, files, 
        overrides=overrides, branch=config.branch, commit_message="update folder 1 assets & QR"
    )

    try:
        res = gh.request("GET", f"/repos/{username}/{repo}/pages")
        return res.get("html_url", f"https://{username}.github.io/{repo}/")
    except GitHubAPIError:
        return _enable_pages(gh, username, repo, branch=config.branch)


def run_full_chain(
    values_data: Optional[Dict[str, Any]] = None,
    order_id: Optional[str] = None,
    config: Optional[DeployConfig] = None
) -> Dict[str, Any]:
    """Головний оркестратор всього циклу деплою."""
    config = config or DeployConfig()

    logger.info("--- STARTING DEPLOY CHAIN FOR ORDER: %s ---", order_id)

    # 1. Деплой унікального сайту для замовлення (Folder 2)
    folder2_url, repo2_name = deploy_folder2_for_order(
        config=config,
        values_data=values_data,
        order_id=order_id
    )

    # 2. Генерація QR-коду, що веде на новий сайт
    qr_full_path = pathlib.Path(config.folder1_dir) / config.qr_rel_path
    qr_bytes = generate_qr(folder2_url, qr_full_path)

    # 3. Пуш головного сайту/сканера (Folder 1) із новим QR-кодом
    folder1_url = deploy_folder1(config=config, qr_bytes_override=qr_bytes)

    logger.info("--- DEPLOY CHAIN SUCCESSFUL ---")
    
    return {
        "folder2_url": folder2_url,
        "folder1_url": folder1_url,
        "qr_path": str(qr_full_path),
        "repo2_name": repo2_name,
        "order_id": order_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    # Приклад запуску:
    try:
        result = run_full_chain(
            values_data={"FULL_NAME": "Іван Іванов", "DOCUMENT_ID": "123456"},
            order_id="ORD-9921"
        )
        print("Результат деплою:", result)
    except DeployError as err:
        logger.error("Помилка під час виконання ланцюжка: %s", err)
