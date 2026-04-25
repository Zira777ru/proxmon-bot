import asyncio
import logging
import os
import re
import socket
import sqlite3
import ssl
import subprocess
import time
from datetime import datetime, timedelta, time as dtime

import requests
import urllib3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, CallbackContext

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PX_HOST             = os.getenv("PROXMOX_HOST", "192.168.0.50")
PX_NODE             = os.getenv("PROXMOX_NODE", "pve")
PX_USER             = os.getenv("PROXMOX_USER", "root@pam")
PX_TOKEN_NAME       = os.getenv("PROXMOX_TOKEN_NAME", "proxmon-bot")
PX_TOKEN_VALUE      = os.getenv("PROXMOX_TOKEN_VALUE", "")
TG_TOKEN            = os.getenv("TG_TOKEN", "")
ADMIN_ID            = int(os.getenv("ADMIN_TG_ID", "0"))
GEMINI_KEY          = os.getenv("GEMINI_KEY", "")
CHECK_INTERVAL      = int(os.getenv("CHECK_INTERVAL", "60"))
CPU_WARN            = int(os.getenv("CPU_WARN", "85"))
MEM_WARN            = int(os.getenv("MEM_WARN", "90"))
DISK_WARN           = int(os.getenv("DISK_WARN", "85"))
TEMP_WARN           = int(os.getenv("TEMP_WARN", "80"))
BACKUP_MAX_AGE_H    = int(os.getenv("BACKUP_MAX_AGE_HOURS", "25"))
CRASH_LOOP_MIN      = int(os.getenv("CRASH_LOOP_MIN_RESTARTS", "3"))
SSL_WARN_DAYS       = int(os.getenv("SSL_WARN_DAYS", "14"))
VOLUME_WARN_GB      = int(os.getenv("VOLUME_WARN_GB", "10"))
SUMMARY_HOUR        = int(os.getenv("SUMMARY_HOUR", "9"))
ALERT_COOLDOWN      = int(os.getenv("ALERT_COOLDOWN", "1800"))
DB_PATH             = os.getenv("DB_PATH", "/data/monitor.db")
BACKUP_WIN_START    = int(os.getenv("BACKUP_WINDOW_START", "2"))
BACKUP_WIN_END      = int(os.getenv("BACKUP_WINDOW_END", "4"))
WATCH_URLS_EXTRA    = [u.strip() for u in os.getenv("WATCH_URLS", "").split(",") if u.strip()]
COOLIFY_API         = os.getenv("COOLIFY_API_URL", "http://host.docker.internal:8000/api/v1")
COOLIFY_TOKEN       = os.getenv("COOLIFY_API_TOKEN", "")
WATCH_DOMAIN_FILTER = os.getenv("WATCH_DOMAIN_FILTER", "coscore.us")
GITHUB_TOKEN        = os.getenv("GITHUB_TOKEN", "")

PX_BASE    = f"https://{PX_HOST}:8006/api2/json"
PX_HEADERS = {"Authorization": f"PVEAPIToken={PX_USER}!{PX_TOKEN_NAME}={PX_TOKEN_VALUE}"}

# ── DB ────────────────────────────────────────────────────────────────────────
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS state   (key TEXT PRIMARY KEY, value TEXT, ts INTEGER);
        CREATE TABLE IF NOT EXISTS alerts  (key TEXT PRIMARY KEY, last_sent INTEGER);
        CREATE TABLE IF NOT EXISTS metrics (date TEXT, key TEXT, value REAL,
                                            PRIMARY KEY (date, key));
    """)
    con.commit()
    return con

def get_state(con, key, default=None):
    row = con.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

def set_state(con, key, value):
    con.execute("INSERT OR REPLACE INTO state VALUES(?,?,?)", (key, str(value), int(time.time())))
    con.commit()

def can_alert(con, key):
    row = con.execute("SELECT last_sent FROM alerts WHERE key=?", (key,)).fetchone()
    return not row or time.time() - row[0] > ALERT_COOLDOWN

def mark_alerted(con, key):
    con.execute("INSERT OR REPLACE INTO alerts VALUES(?,?)", (key, int(time.time())))
    con.commit()

def clear_alert(con, key):
    con.execute("DELETE FROM alerts WHERE key=?", (key,))
    con.commit()

def is_silenced(con):
    until = get_state(con, "silence_until", "0")
    return time.time() < float(until)

def in_backup_window():
    h = datetime.now().hour
    return BACKUP_WIN_START <= h < BACKUP_WIN_END

def store_metric(con, key, value):
    day = datetime.now().strftime("%Y-%m-%d")
    con.execute("INSERT OR REPLACE INTO metrics VALUES(?,?,?)", (day, key, value))
    con.commit()

def get_metric_history(con, key, days=7):
    rows = con.execute(
        "SELECT date, value FROM metrics WHERE key=? ORDER BY date DESC LIMIT ?",
        (key, days)
    ).fetchall()
    return list(reversed(rows))

# ── Gemini AI ─────────────────────────────────────────────────────────────────
def ai_fix_prompt(issue: str, context: str) -> str:
    if not GEMINI_KEY:
        return ""
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_KEY)
        result = genai.GenerativeModel("gemini-2.5-flash").generate_content(
            f"Ты — автоматический ассистент DevOps-бота. На домашнем сервере произошла проблема.\n"
            f"Проблема: {issue}\nКонтекст: {context}\n\n"
            f"Напиши ОДИН короткий промпт (1-3 предложения на русском языке), который владелец "
            f"скопирует и отправит ИИ-девопсу Claude чтобы тот немедленно исправил проблему. "
            f"Включи все технические детали. Выведи ТОЛЬКО текст промпта, без кавычек."
        )
        return result.text.strip()
    except Exception as e:
        log.error(f"Gemini: {e}")
        return ""

# ── Alert grouping ────────────────────────────────────────────────────────────
async def send_alerts(bot, alerts: list[dict]):
    if not alerts:
        return
    if len(alerts) == 1:
        a = alerts[0]
        fix = ai_fix_prompt(a["issue"], a["context"])
        msg = a["text"]
        if fix:
            msg += f"\n\n📋 <b>Промпт для Claude:</b>\n<code>{fix}</code>"
        await bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
    else:
        issue_sum = f"{len(alerts)} проблем: " + "; ".join(a["issue"] for a in alerts[:3])
        ctx_sum   = " | ".join(a["context"] for a in alerts[:3])
        fix = ai_fix_prompt(issue_sum, ctx_sum)
        body = "\n".join(a["text"] for a in alerts)
        msg  = f"🚨 <b>{len(alerts)} проблемы одновременно:</b>\n\n{body}"
        if fix:
            msg += f"\n\n📋 <b>Промпт для Claude:</b>\n<code>{fix}</code>"
        await bot.send_message(ADMIN_ID, msg, parse_mode="HTML")

# ── Proxmox API ───────────────────────────────────────────────────────────────
def px(path):
    try:
        r = requests.get(f"{PX_BASE}{path}", headers=PX_HEADERS, verify=False, timeout=10)
        return r.json().get("data")
    except Exception as e:
        log.error(f"PX API {path}: {e}")
        return None

def node_status(): return px(f"/nodes/{PX_NODE}/status")
def vms():         return px(f"/nodes/{PX_NODE}/qemu") or []
def lxc():         return px(f"/nodes/{PX_NODE}/lxc") or []
def storages():    return px(f"/nodes/{PX_NODE}/storage") or []
def backups():     return px("/cluster/backup") or []

def last_backup_age_hours(vmid):
    data = px(f"/nodes/{PX_NODE}/storage/backup-disk/content?content=backup&vmid={vmid}")
    if not data:
        return None
    entries = [e for e in data if e.get("vmid") == vmid]
    if not entries:
        return None
    latest = max(entries, key=lambda x: x.get("ctime", 0))
    return (time.time() - latest["ctime"]) / 3600

def backup_task_results():
    """Returns list of vzdump task results that finished within the last 2 check intervals."""
    tasks = px(f"/nodes/{PX_NODE}/tasks?typefilter=vzdump&limit=50") or []
    cutoff = time.time() - max(CHECK_INTERVAL * 2, 300)
    results = []
    for t in tasks:
        if not t.get("endtime") or not t.get("status"):
            continue
        if t["endtime"] < cutoff:
            continue
        upid = t.get("upid", "")
        # UPID format: UPID:node:pid:pstart:starttime:type:vmid:user:
        parts = upid.split(":")
        vmid = parts[6] if len(parts) > 6 and parts[6] else t.get("id", "?")
        results.append({
            "vmid":    vmid,
            "status":  t.get("status", ""),
            "endtime": t["endtime"],
            "upid":    upid,
        })
    return results

def pve_temperature():
    """Get max CPU temperature from PVE via SSH."""
    try:
        r = subprocess.run(
            ["ssh", "-i", "/root/.ssh/id_ed25519", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=5", f"root@{PX_HOST}",
             "cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null"],
            capture_output=True, text=True, timeout=8
        )
        temps = [int(t) // 1000 for t in r.stdout.split() if t.strip().isdigit() and len(t) >= 4]
        return max(temps) if temps else None
    except Exception as e:
        log.error(f"PVE temp: {e}")
        return None

# ── Docker ────────────────────────────────────────────────────────────────────
def docker_client():
    try:
        import docker
        return docker.from_env()
    except Exception as e:
        log.error(f"Docker client: {e}")
        return None

def docker_containers():
    c = docker_client()
    return c.containers.list(all=True) if c else []

def docker_volume_sizes():
    try:
        import docker
        df = docker.APIClient().df()
        vols = []
        for v in df.get("Volumes") or []:
            size = v.get("UsageData", {}).get("Size", -1)
            if size > 0:
                vols.append({"name": v["Name"], "size": size})
        return sorted(vols, key=lambda x: x["size"], reverse=True)
    except Exception as e:
        log.error(f"Volume sizes: {e}")
        return []

def _coolify_uuid_name_map() -> dict:
    """Returns {coolify_uuid: friendly_name} for all apps."""
    result = {}
    hdrs = {"Authorization": f"Bearer {COOLIFY_TOKEN}"}
    try:
        for app in requests.get(f"{COOLIFY_API}/applications", headers=hdrs, timeout=5).json():
            result[app["uuid"]] = app.get("name", app["uuid"])
    except Exception as e:
        log.error(f"UUID name map: {e}")
    return result


def _friendly_name(container_name: str, uuid_map: dict) -> str:
    """Convert ugly Coolify container name to human-readable."""
    for uuid, name in uuid_map.items():
        if uuid in container_name:
            return name
    # Strip trailing -{uuid}-{number} or -{uuid}
    import re
    cleaned = re.sub(r"-[a-z0-9]{16,}-\d+$", "", container_name)
    cleaned = re.sub(r"-[a-z0-9]{16,}$", "", cleaned)
    return cleaned


# ── Coolify discovery ─────────────────────────────────────────────────────────
_watch_cache: list[str] = []
_watch_ts: float = 0

def discover_watch_urls() -> list[str]:
    global _watch_cache, _watch_ts
    if time.time() - _watch_ts < 300 and _watch_cache:
        return _watch_cache
    urls: set[str] = set(WATCH_URLS_EXTRA)
    hdrs = {"Authorization": f"Bearer {COOLIFY_TOKEN}"}
    try:
        for app in requests.get(f"{COOLIFY_API}/applications", headers=hdrs, timeout=5).json():
            for part in (app.get("fqdn") or "").split(","):
                part = part.strip()
                if WATCH_DOMAIN_FILTER in part and "sslip.io" not in part:
                    urls.add(part if part.startswith("http") else "https://" + part)
    except Exception as e:
        log.error(f"Coolify apps: {e}")
    try:
        for svc in requests.get(f"{COOLIFY_API}/services", headers=hdrs, timeout=5).json():
            for fqdn in re.findall(r"COOLIFY_FQDN:\s*([^\s\n]+)", svc.get("docker_compose", "") or ""):
                if WATCH_DOMAIN_FILTER in fqdn and "sslip.io" not in fqdn:
                    urls.add("https://" + fqdn)
    except Exception as e:
        log.error(f"Coolify services: {e}")
    result = sorted(urls)
    if result:
        _watch_cache, _watch_ts = result, time.time()
        log.info(f"Watch URLs: {result}")
    return result

# ── GitHub update checks ──────────────────────────────────────────────────────
def _gh_latest_sha(repo: str, branch: str) -> str | None:
    """Return latest commit SHA for repo/branch via GitHub API."""
    if not GITHUB_TOKEN:
        return None
    try:
        r = requests.get(
            f"https://api.github.com/repos/{repo}/commits/{branch}",
            headers={"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.sha"},
            timeout=10,
        )
        return r.text.strip() if r.ok else None
    except Exception as e:
        log.warning(f"GitHub API {repo}: {e}")
        return None


def _coolify_apps_map() -> dict:
    """Returns {normalized_fqdn: {uuid, name, repo, branch}} for apps with git repos."""
    result = {}
    hdrs = {"Authorization": f"Bearer {COOLIFY_TOKEN}"}
    try:
        for app in requests.get(f"{COOLIFY_API}/applications", headers=hdrs, timeout=5).json():
            repo = app.get("git_repository", "")
            if not repo:
                continue
            # Strip https://github.com/ prefix if present
            repo = repo.replace("https://github.com/", "").replace("git@github.com:", "").rstrip(".git")
            for part in (app.get("fqdn") or "").split(","):
                part = part.strip().replace("https://", "").replace("http://", "").rstrip("/")
                if part:
                    result[part] = {
                        "uuid": app["uuid"],
                        "name": app.get("name", ""),
                        "repo": repo,
                        "branch": app.get("git_branch", "master"),
                    }
    except Exception as e:
        log.error(f"Coolify apps map: {e}")
    return result


def check_github_updates(con) -> dict:
    """Returns {fqdn: app_info} for apps that have new commits since last stored SHA."""
    if not GITHUB_TOKEN:
        return {}
    apps = _coolify_apps_map()
    updates = {}
    for fqdn, info in apps.items():
        sha_key = f"gh_sha:{info['uuid']}"
        latest = _gh_latest_sha(info["repo"], info["branch"])
        if not latest:
            continue
        stored = get_state(con, sha_key)
        if stored is None:
            # First time — store baseline, no update shown
            set_state(con, sha_key, latest)
        elif stored != latest:
            updates[fqdn] = {**info, "new_sha": latest}
    return updates


# ── SSL ───────────────────────────────────────────────────────────────────────
def ssl_days_left(hostname: str):
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((hostname, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=hostname) as s:
                exp = datetime.strptime(s.getpeercert()["notAfter"], "%b %d %H:%M:%S %Y %Z")
                return (exp - datetime.utcnow()).days
    except Exception as e:
        log.error(f"SSL {hostname}: {e}")
        return None

# ── Helpers ───────────────────────────────────────────────────────────────────
def pct(used, total): return used / total * 100 if total else 0
def fmt_gb(b):        return f"{b/1024**3:.1f}G"
def fmt_mb(b):        return f"{b/1024**2:.0f}M"
def uptime_str(s):
    d, rem = divmod(int(s), 86400)
    h, m   = divmod(rem // 60, 60)
    return f"{d}д {h}ч {m}м" if d else f"{h}ч {m}м"
def vm_icon(st): return "🟢" if st == "running" else "🔴"
def b(t):        return f"<b>{t}</b>"
def url_ok(url):
    try:
        return requests.get(url, timeout=10, allow_redirects=True).status_code < 500
    except:
        return False

# ── Status ────────────────────────────────────────────────────────────────────
def build_status(con=None):
    lines = [f"📊 {b('Proxmox Monitor')}  {datetime.now().strftime('%d.%m %H:%M')}"]

    node = node_status()
    if node:
        cpu  = node["cpu"] * 100
        mem  = pct(node["memory"]["used"], node["memory"]["total"])
        disk = pct(node["rootfs"]["used"], node["rootfs"]["total"])
        temp = pve_temperature()
        temp_s = f"  🌡 {temp}°C" if temp else ""
        lines += ["", f"🖥 {b('Node: ' + PX_NODE)}",
                  f"CPU {cpu:.1f}%  RAM {mem:.1f}%  Disk {disk:.1f}%{temp_s}",
                  f"Uptime: {uptime_str(node['uptime'])}"]
    else:
        lines += ["", f"🔴 {b('Proxmox недоступен!')}"]

    all_guests = sorted(vms() + lxc(), key=lambda x: x["vmid"])
    if all_guests:
        lines += ["", f"🖧 {b('VMs')}"]
        for g in all_guests:
            mem_s = f"{pct(g.get('mem',0), g.get('maxmem',1)):.0f}%" if g.get("maxmem") else "—"
            lines.append(f"{vm_icon(g['status'])} [{g['vmid']}] {g['name']}  RAM {mem_s}  CPU {g.get('cpu',0)*100:.1f}%")

    containers = docker_containers()
    uuid_map = _coolify_uuid_name_map()
    stopped = [c for c in containers if c.status != "running"]
    lines += ["", f"🐳 {b('Docker')} ({len(containers)-len(stopped)}/{len(containers)} running)"]
    for c in sorted(stopped, key=lambda x: x.name):
        lines.append(f"🔴 {_friendly_name(c.name, uuid_map)}")
    if not stopped:
        lines.append(f"🟢 Все контейнеры запущены")

    # Боты — Coolify-приложения без публичного домена
    hdrs = {"Authorization": f"Bearer {COOLIFY_TOKEN}"}
    try:
        bot_apps = [
            a for a in requests.get(f"{COOLIFY_API}/applications", headers=hdrs, timeout=5).json()
            if a.get("git_repository") and not any(
                WATCH_DOMAIN_FILTER in (p.strip())
                for p in (a.get("fqdn") or "").split(",")
                if "sslip.io" not in p
            )
        ]
        if bot_apps:
            lines += ["", f"🤖 {b('Боты')}"]
            for a in sorted(bot_apps, key=lambda x: x.get("name", "")):
                uuid = a["uuid"]
                # Найдём контейнер по UUID
                matched = [c for c in containers if uuid in c.name]
                status = matched[0].status if matched else "not found"
                icon = "🟢" if status == "running" else "🔴"
                lines.append(f"{icon} {a.get('name', uuid)}")
    except Exception as e:
        log.error(f"Bot apps: {e}")

    stor = storages()
    if stor:
        lines += ["", f"💾 {b('Storage')}"]
        for s in stor:
            if s.get("total", 0) > 0:
                p = pct(s["used"], s["total"])
                warn = "⚠️ " if p > DISK_WARN else ""
                lines.append(f"{warn}{s['storage']}: {p:.0f}%  {fmt_gb(s['used'])}/{fmt_gb(s['total'])}")

    # Top volumes
    vols = [v for v in docker_volume_sizes() if v["size"] > 1024**3][:5]
    if vols:
        lines += ["", f"📦 {b('Volumes (топ)')}"]
        for v in vols:
            warn = "⚠️ " if v["size"] > VOLUME_WARN_GB * 1024**3 else ""
            short = v["name"][-40:] if len(v["name"]) > 40 else v["name"]
            lines.append(f"{warn}{fmt_gb(v['size'])}  {short}")

    bkps = backups()
    if bkps:
        lines += ["", f"🗄 {b('Бэкапы')}"]
        for bk in bkps:
            nxt  = datetime.fromtimestamp(bk["next-run"]).strftime("%d.%m %H:%M") if bk.get("next-run") else "—"
            keep = bk.get("prune-backups", {}).get("keep-last", "?")
            lines.append(f"{'✅' if bk.get('enabled') else '⏸'} {bk['schedule']}  след: {nxt}  хранить: {keep}")
        age = last_backup_age_hours(100)
        if age is not None:
            lines.append(f"{'⚠️' if age > BACKUP_MAX_AGE_H else '✅'} Последний бэкап VM 100: {age:.0f}ч назад")

    gh_updates = check_github_updates(con) if con else {}

    watch = discover_watch_urls()
    if watch:
        lines += ["", f"🌐 {b('Сервисы')} ({len(watch)})"]
        for url in watch:
            ok   = url_ok(url)
            name = url.replace("https://","").replace("http://","").split("/")[0]
            has_update = name in gh_updates
            upd_mark = " ⬆️" if has_update else ""
            lines.append(f"{'🟢' if ok else '🔴'} {name}{upd_mark}")

    lines += ["", "─" * 20, "/logs — логи и контейнеры    /help — справка"]

    return "\n".join(lines)

# ── Weekly trend ──────────────────────────────────────────────────────────────
def build_weekly_trend(con) -> str:
    lines = [f"📈 {b('Еженедельный отчёт')}  {datetime.now().strftime('%d.%m.%Y')}"]

    for stor in storages():
        if stor.get("total", 0) == 0:
            continue
        history = get_metric_history(con, f"disk_{stor['storage']}", 7)
        if len(history) >= 2:
            oldest_val, newest_val = history[0][1], history[-1][1]
            delta = newest_val - oldest_val
            sign  = "+" if delta >= 0 else ""
            days_until_full = None
            if delta > 0:
                rate_per_day = delta / max(len(history) - 1, 1)
                free = stor["total"] / 1024**3 - newest_val
                if rate_per_day > 0:
                    days_until_full = int(free / rate_per_day)
            until_s = f"  (полный через ~{days_until_full}д)" if days_until_full and days_until_full < 60 else ""
            lines.append(f"💾 {stor['storage']}: {sign}{delta:.1f}G за неделю{until_s}")

    cpu_hist = get_metric_history(con, "cpu_avg", 7)
    if len(cpu_hist) >= 3:
        avg = sum(v for _, v in cpu_hist) / len(cpu_hist)
        lines.append(f"🖥 CPU среднее за неделю: {avg:.1f}%")

    vols = [v for v in docker_volume_sizes() if v["size"] > 1024**3][:3]
    if vols:
        lines.append(f"📦 Топ volumes: " + ", ".join(f"{fmt_gb(v['size'])} {v['name'].split('_')[-1][:20]}" for v in vols))

    return "\n".join(lines)

def record_daily_metrics(con):
    node = node_status()
    if node:
        store_metric(con, "cpu_avg", node["cpu"] * 100)
    for s in storages():
        if s.get("total", 0) > 0:
            store_metric(con, f"disk_{s['storage']}", s["used"] / 1024**3)

# ── Checks ────────────────────────────────────────────────────────────────────
async def run_checks(bot, con):
    if is_silenced(con):
        return

    backup_window = in_backup_window()
    alerts, recoveries = [], []

    def alert(key, text, issue, context):
        if can_alert(con, key):
            alerts.append({"key": key, "text": text, "issue": issue, "context": context})

    def recovery(text):
        recoveries.append(text)

    def threshold(key, name, value, thr, ctx="", suppress_in_backup=False):
        firing = get_state(con, f"{key}_f") == "1"
        if value > thr:
            set_state(con, f"{key}_f", "1")
            if not (suppress_in_backup and backup_window):
                alert(key, f"⚠️ {b(name)}: {value:.0f}% (порог {thr}%)", f"{name} = {value:.0f}%", ctx or name)
        else:
            if firing:
                recovery(f"✅ {b(name)} в норме: {value:.0f}%")
                clear_alert(con, key)
            set_state(con, f"{key}_f", "0")

    # ── Proxmox доступность
    node = node_status()
    px_key = "px_reachable"
    if not node:
        set_state(con, f"{px_key}_f", "1")
        alert(px_key, f"🔴 {b('Proxmox PVE недоступен!')}",
              "Proxmox PVE недоступен через API",
              f"IP: {PX_HOST}, порт 8006")
    else:
        if get_state(con, f"{px_key}_f") == "1":
            recovery(f"🟢 {b('Proxmox снова доступен')}")
        set_state(con, f"{px_key}_f", "0")
        clear_alert(con, px_key)

        threshold("node_cpu", "CPU (PVE)", node["cpu"]*100, CPU_WARN,
                  f"Proxmox node {PX_NODE}")
        threshold("node_mem", "RAM (PVE)",
                  pct(node["memory"]["used"], node["memory"]["total"]), MEM_WARN,
                  f"RAM: {fmt_gb(node['memory']['used'])}/{fmt_gb(node['memory']['total'])}")

    # ── Температура
    temp = pve_temperature()
    if temp is not None:
        t_key = "pve_temp"
        firing = get_state(con, f"{t_key}_f") == "1"
        if temp > TEMP_WARN:
            set_state(con, f"{t_key}_f", "1")
            alert(t_key, f"🌡 {b('Температура CPU PVE')}: {temp}°C (порог {TEMP_WARN}°C)",
                  f"CPU температура PVE = {temp}°C",
                  f"Intel i5-8500T на Proxmox PVE {PX_HOST}. TjMax=100°C. "
                  f"Проверь кулер и термопасту.")
        else:
            if firing:
                recovery(f"✅ {b('Температура')} в норме: {temp}°C")
                clear_alert(con, t_key)
            set_state(con, f"{t_key}_f", "0")

    # ── Диски
    for s in storages():
        if s.get("total", 0) > 0:
            p = pct(s["used"], s["total"])
            threshold(f"disk_{s['storage']}", f"Диск {s['storage']}", p, DISK_WARN,
                      f"Storage '{s['storage']}': {fmt_gb(s['used'])} из {fmt_gb(s['total'])}. "
                      f"{'Хранилище бэкапов.' if 'backup' in s['storage'] else ''}")

    # ── Бэкап: возраст
    for vmid in [v["vmid"] for v in vms()]:
        age = last_backup_age_hours(vmid)
        key = f"backup_age_{vmid}"
        firing = get_state(con, f"{key}_f") == "1"
        if age is None or age > BACKUP_MAX_AGE_H:
            set_state(con, f"{key}_f", "1")
            msg = (f"⚠️ {b(f'Нет бэкапов VM {vmid}')}" if age is None
                   else f"⚠️ {b(f'Старый бэкап VM {vmid}')}: {age:.0f}ч назад")
            alert(key, msg,
                  f"VM {vmid} не бэкапилась {'вообще' if age is None else f'{age:.0f}ч'}",
                  f"Proxmox backup-disk, VMID {vmid}, норма: каждые {BACKUP_MAX_AGE_H}ч")
        else:
            if firing:
                recovery(f"✅ {b(f'Бэкап VM {vmid}')} свежий: {age:.0f}ч назад")
                clear_alert(con, key)
            set_state(con, f"{key}_f", "0")

    # ── Бэкап: результат задачи (только задачи завершившиеся в последние 2*CHECK_INTERVAL сек)
    alerted_tasks = set((get_state(con, "alerted_backup_tasks") or "").split(","))
    new_alerted = set()
    for task in backup_task_results():
        upid = task["upid"]
        if task["status"] not in ("OK", "") and upid not in alerted_tasks:
            vmid = task["vmid"]
            new_alerted.add(upid)
            alert(f"backup_task_{upid[:20]}", f"❌ {b(f'Бэкап VM {vmid} завершился с ошибкой')}: {task['status']}",
                  f"Задача vzdump для VM {vmid} завершилась со статусом '{task['status']}'",
                  f"Proxmox task UPID: {upid}. Проверь детали в PVE UI → Tasks.")
    if new_alerted:
        combined = (alerted_tasks | new_alerted) - {""}
        # Храним только последние 50 UPID чтобы строка не росла бесконечно
        set_state(con, "alerted_backup_tasks", ",".join(list(combined)[-50:]))

    # ── Статус VM (подавляем в окне бэкапа — Proxmox freeze)
    for g in vms() + lxc():
        key  = f"vm_{g['vmid']}"
        prev = get_state(con, key)
        curr = g["status"]
        if prev and prev != curr:
            if curr != "running" and not backup_window:
                alert(f"{key}_chg", f"{vm_icon(curr)} {b(g['name'])} ({g['vmid']}): {prev} → {curr}",
                      f"VM '{g['name']}' изменила статус: {prev} → {curr}",
                      f"VMID {g['vmid']}, {'главная VM с Docker' if g['vmid']==100 else 'вторичная VM'}.")
            elif curr == "running":
                recovery(f"🟢 {b(g['name'])} ({g['vmid']}): снова запущена")
        set_state(con, key, curr)

    # ── Docker контейнеры (статус + crash loop)
    for c in docker_containers():
        cid  = c.id[:12]
        skey = f"docker_{cid}"
        prev = get_state(con, skey)
        curr = c.status
        if prev and prev != curr:
            if curr != "running" and not backup_window:
                alert(f"{skey}_chg", f"🔴 {b(c.name)}: {prev} → {curr}",
                      f"Docker контейнер '{c.name}' упал: {prev} → {curr}",
                      f"docker-core, image: {c.image.tags[0] if c.image.tags else 'unknown'}. "
                      f"Coolify API: http://localhost:8000/api/v1.")
            elif curr == "running":
                recovery(f"🟢 {b(c.name)}: снова запущен")
        set_state(con, skey, curr)

        if curr == "running":
            try:
                rc      = c.attrs.get("RestartCount", 0)
                rkey    = f"docker_rc_{cid}"
                prev_rc = int(get_state(con, rkey) or 0)
                if rc - prev_rc >= CRASH_LOOP_MIN:
                    alert(f"{rkey}_loop",
                          f"🔄 {b(c.name)}: crash loop ({rc-prev_rc} рестартов, всего {rc})",
                          f"Контейнер '{c.name}' в crash loop",
                          f"docker-core, {rc-prev_rc} рестартов. Image: {c.image.tags[0] if c.image.tags else '?'}")
                set_state(con, rkey, rc)
            except Exception as e:
                log.error(f"Crash loop {c.name}: {e}")

    # ── Docker volumes
    for v in docker_volume_sizes():
        if v["size"] > VOLUME_WARN_GB * 1024**3:
            key = f"vol_{v['name'][:30]}"
            threshold_gb = v["size"] / 1024**3
            if can_alert(con, key):
                short = v["name"][-50:]
                alert(key, f"📦 {b('Большой volume')}: {fmt_gb(v['size'])}  {short}",
                      f"Docker volume {v['name']} занимает {fmt_gb(v['size'])}",
                      f"Порог: {VOLUME_WARN_GB}GB. Проверь что данные нужны.")
                mark_alerted(con, key)

    # ── Внешние сервисы (double-check: 2 подряд неудачи → алерт)
    for url in discover_watch_urls():
        key     = f"url_{url}"
        firing  = get_state(con, f"{key}_f") == "1"
        pending = get_state(con, f"{key}_p") == "1"
        ok      = url_ok(url)
        name    = url.replace("https://","").replace("http://","").split("/")[0]
        if not ok:
            if pending:
                set_state(con, f"{key}_f", "1")
                set_state(con, f"{key}_p", "0")
                alert(key, f"🔴 {b(name)} недоступен",
                      f"Сервис {url} не отвечает",
                      f"Cloudflare tunnel или контейнер упал. URL: {url}")
            else:
                set_state(con, f"{key}_p", "1")
        else:
            set_state(con, f"{key}_p", "0")
            if firing:
                recovery(f"🟢 {b(name)}: снова доступен")
                clear_alert(con, key)
            set_state(con, f"{key}_f", "0")

    for a in alerts:
        mark_alerted(con, a["key"])
    await send_alerts(bot, alerts)
    for r in recoveries:
        await bot.send_message(ADMIN_ID, r, parse_mode="HTML")

async def run_ssl_checks(bot, con):
    if is_silenced(con):
        return
    for url in discover_watch_urls():
        hostname = url.replace("https://","").replace("http://","").split("/")[0]
        days = ssl_days_left(hostname)
        if days is None:
            continue
        key = f"ssl_{hostname}"
        firing = get_state(con, f"{key}_f") == "1"
        if days < SSL_WARN_DAYS:
            set_state(con, f"{key}_f", "1")
            if can_alert(con, key):
                fix = ai_fix_prompt(f"SSL {hostname} истекает через {days} дней",
                                    f"Traefik/Let's Encrypt должен обновлять автоматически.")
                msg = f"⚠️ {b('SSL: ' + hostname)}: истекает через {b(str(days) + ' дней')}"
                if fix:
                    msg += f"\n\n📋 <b>Промпт для Claude:</b>\n<code>{fix}</code>"
                await bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
                mark_alerted(con, key)
        else:
            if firing:
                await bot.send_message(ADMIN_ID, f"✅ {b('SSL ' + hostname)}: {days} дней", parse_mode="HTML")
                clear_alert(con, key)
            set_state(con, f"{key}_f", "0")

# ── Commands ──────────────────────────────────────────────────────────────────
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(build_status(ctx.bot_data.get("con")), parse_mode="HTML")

async def _send_logs_menu(send_fn, con):
    """Send the /logs interactive menu."""
    gh_updates = check_github_updates(con) if con else {}
    watch = discover_watch_urls()
    uuid_map = _coolify_uuid_name_map()
    containers = docker_containers()

    lines = [f"📋 {b('Логи и управление')}"]

    # Сервисы
    if watch:
        lines.append("")
        lines.append(f"🌐 {b('Сервисы')}")
    kb_rows = []
    if watch:
        svc_names = [url.replace("https://","").replace("http://","").split("/")[0] for url in watch]
        row = []
        for name in svc_names:
            has_update = name in gh_updates
            label = f"⬆️{name.split('.')[0][:10]}" if has_update else name.split(".")[0][:12]
            cb = f"deploy_update:{gh_updates[name]['uuid']}" if has_update else f"svc_logs:{name}"
            row.append(InlineKeyboardButton(label, callback_data=cb[:64]))
            if len(row) == 3:
                kb_rows.append(row)
                row = []
        if row:
            kb_rows.append(row)

    # Контейнеры
    lines += ["", f"🐳 {b('Контейнеры')}"]
    kb_rows.append([InlineKeyboardButton("🐳 Все контейнеры", callback_data="docker_list")])

    await send_fn("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="HTML")


async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await _send_logs_menu(update.message.reply_text, ctx.bot_data.get("con"))

async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        await update.message.reply_text("Использование: /restart <имя_контейнера>")
        return
    name  = ctx.args[0].lower()
    found = [c for c in docker_containers() if name in c.name.lower()]
    if not found:
        await update.message.reply_text(f"Контейнер '{name}' не найден.")
        return
    c = found[0]
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Да, перезапустить", callback_data=f"restart:{c.id[:12]}:{c.name}"),
        InlineKeyboardButton("❌ Отмена",            callback_data="cancel"),
    ]])
    await update.message.reply_text(
        f"Перезапустить контейнер {b(c.name)}?\nТекущий статус: {c.status}",
        reply_markup=kb, parse_mode="HTML")

async def cmd_reboot(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    vms = px(f"/nodes/{PX_NODE}/qemu") or []
    if not vms:
        await update.message.reply_text("❌ Не удалось получить список VM.")
        return
    # Если передан аргумент — ищем VM по имени или vmid
    if ctx.args:
        query_str = ctx.args[0].lower()
        vms = [v for v in vms if str(v.get("vmid","")) == query_str or query_str in v.get("name","").lower()]
        if not vms:
            await update.message.reply_text(f"VM '{ctx.args[0]}' не найдена.")
            return
        vm = vms[0]
        vmid, name, status = vm["vmid"], vm.get("name", str(vm["vmid"])), vm.get("status","?")
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да, перезагрузить", callback_data=f"reboot:{vmid}:{name}"),
            InlineKeyboardButton("❌ Отмена",             callback_data="cancel"),
        ]])
        await update.message.reply_text(
            f"Перезагрузить VM {b(name)} (VMID {vmid})?\nСтатус: {status}",
            reply_markup=kb, parse_mode="HTML")
    else:
        # Показываем список VM с кнопками
        running = [v for v in vms if v.get("status") == "running"]
        if not running:
            await update.message.reply_text("Нет запущенных VM.")
            return
        buttons = [[InlineKeyboardButton(
            f"{v.get('name', v['vmid'])} (VM {v['vmid']})",
            callback_data=f"reboot:{v['vmid']}:{v.get('name', v['vmid'])}"
        )] for v in running]
        buttons.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await update.message.reply_text(
            "Выбери VM для перезагрузки:",
            reply_markup=InlineKeyboardMarkup(buttons))

def _container_logs(name_hint: str, tail: int = 40, uuid_hint: str = "") -> str:
    try:
        client = docker_client()
        if not client:
            return "❌ Docker недоступен"
        all_c = client.containers.list(all=True)
        # Ищем по имени, затем по UUID Coolify
        matched = [c for c in all_c if name_hint.lower() in c.name.lower()]
        if not matched and uuid_hint:
            matched = [c for c in all_c if uuid_hint.lower() in c.name.lower()]
        if not matched:
            return f"❌ Контейнер с '{name_hint}' не найден"
        c = matched[0]
        logs = c.logs(tail=tail, timestamps=False).decode("utf-8", errors="replace")
        return f"📋 <b>{c.name}</b>\n<pre>{logs[-3500:]}</pre>"
    except Exception as e:
        return f"❌ Ошибка: {e}"


async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        await query.edit_message_text("❌ Отменено")
        return

    if query.data == "docker_list":
        try:
            client = docker_client()
            containers = client.containers.list(all=True) if client else []
            uuid_map = _coolify_uuid_name_map()
            running = [c for c in containers if c.status == "running"]
            stopped = [c for c in containers if c.status != "running"]
            lines = [f"🐳 <b>Docker контейнеры</b> ({len(running)}/{len(containers)})"]
            rows = []
            for c in sorted(running, key=lambda x: x.name):
                fname = _friendly_name(c.name, uuid_map)
                lines.append(f"🟢 {fname}")
                rows.append([InlineKeyboardButton(f"📋 {fname[:28]}", callback_data=f"ctr_logs:{c.id[:12]}")])
            for c in sorted(stopped, key=lambda x: x.name):
                fname = _friendly_name(c.name, uuid_map)
                lines.append(f"🔴 {fname}")
                rows.append([InlineKeyboardButton(f"📋 {fname[:28]}", callback_data=f"ctr_logs:{c.id[:12]}")])
            rows.append([InlineKeyboardButton("« Назад", callback_data="back_logs")])
            await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(rows), parse_mode="HTML")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")
        return

    if query.data.startswith("svc_logs:"):
        hostname = query.data.split(":", 1)[1]
        subdomain = hostname.split(".")[0]
        # Ищем UUID Coolify по hostname для поиска контейнера по UUID
        apps = _coolify_apps_map()
        uuid_hint = apps.get(hostname, {}).get("uuid", "")
        msg = _container_logs(subdomain, uuid_hint=uuid_hint)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data="back_logs")]])
        await query.edit_message_text(msg, reply_markup=kb, parse_mode="HTML")
        return

    if query.data.startswith("ctr_logs:"):
        cid = query.data.split(":", 1)[1]
        try:
            client = docker_client()
            c = client.containers.get(cid)
            logs = c.logs(tail=40, timestamps=False).decode("utf-8", errors="replace")
            msg = f"📋 <b>{c.name}</b>\n<pre>{logs[-3500:]}</pre>"
        except Exception as e:
            msg = f"❌ Ошибка: {e}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("« Список", callback_data="docker_list")]])
        await query.edit_message_text(msg, reply_markup=kb, parse_mode="HTML")
        return

    if query.data == "back_logs":
        await _send_logs_menu(query.edit_message_text, ctx.bot_data.get("con"))
        return

    if query.data.startswith("deploy_update:"):
        app_uuid = query.data.split(":", 1)[1]
        await query.edit_message_text(f"🚀 Запускаю деплой...", parse_mode="HTML")
        try:
            hdrs = {"Authorization": f"Bearer {COOLIFY_TOKEN}"}
            r = requests.post(f"{COOLIFY_API}/applications/{app_uuid}/start", headers=hdrs, timeout=10)
            deploy_uuid = r.json().get("deployment_uuid", "")
            # Обновляем stored SHA → latest (чтобы убрать ⬆️ сразу)
            con = ctx.bot_data.get("con")
            if con:
                apps = _coolify_apps_map()
                for fqdn, info in apps.items():
                    if info["uuid"] == app_uuid:
                        latest = _gh_latest_sha(info["repo"], info["branch"])
                        if latest:
                            set_state(con, f"gh_sha:{app_uuid}", latest)
                        break
            msg = f"✅ Деплой запущен\n<code>{deploy_uuid}</code>"
        except Exception as e:
            msg = f"❌ Ошибка деплоя: {e}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data="back_logs")]])
        await query.edit_message_text(msg, reply_markup=kb, parse_mode="HTML")
        return

    if query.data.startswith("restart:"):
        _, cid, cname = query.data.split(":", 2)
        try:
            client = docker_client()
            c = client.containers.get(cid) if client else None
            if not c:
                await query.edit_message_text(f"❌ Контейнер не найден")
                return
            c.restart(timeout=30)
            await query.edit_message_text(f"🔄 {b(cname)}: перезапущен", parse_mode="HTML")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")
    if query.data.startswith("reboot:"):
        _, vmid, name = query.data.split(":", 2)
        try:
            r = requests.post(f"{PX_BASE}/nodes/{PX_NODE}/qemu/{vmid}/status/reboot",
                              headers=PX_HEADERS, verify=False, timeout=10)
            if r.ok:
                await query.edit_message_text(f"🔄 VM {b(name)} перезагружается...", parse_mode="HTML")
            else:
                await query.edit_message_text(f"❌ Ошибка Proxmox API: {r.status_code} {r.text[:100]}")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")

async def cmd_silence(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    con = ctx.bot_data["con"]
    duration = 3600
    if ctx.args:
        arg = ctx.args[0].lower()
        try:
            duration = int(arg[:-1]) * (3600 if arg.endswith("h") else 60)
        except ValueError:
            await update.message.reply_text("Пример: /silence 2h или /silence 30m")
            return
    until = time.time() + duration
    set_state(con, "silence_until", str(until))
    await update.message.reply_text(
        f"🔕 Алерты отключены до {datetime.fromtimestamp(until).strftime('%H:%M')}")

async def cmd_unsilence(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    set_state(ctx.bot_data["con"], "silence_until", "0")
    await update.message.reply_text("🔔 Алерты включены")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        f"🤖 {b('Proxmox Monitor Bot')}\n\n"
        f"{b('Команды:')}\n"
        "/status — полный статус сервера\n"
        "/logs &lt;имя&gt; — последние 40 строк логов\n"
        "/restart &lt;имя&gt; — перезапустить Docker контейнер\n"
        "/reboot [vmid|имя] — перезагрузить VM (без аргумента — список)\n"
        "/silence 2h — тишина на 2ч (или 30m)\n"
        "/unsilence — включить алерты\n"
        "/help — эта справка\n\n"
        f"{b('Алерты (с промптом для Claude):')}\n"
        f"• CPU &gt;{CPU_WARN}% / RAM &gt;{MEM_WARN}% / Диск &gt;{DISK_WARN}%\n"
        f"• Температура CPU &gt;{TEMP_WARN}°C\n"
        f"• Бэкап старше {BACKUP_MAX_AGE_H}ч или завершился с ошибкой\n"
        f"• VM / Docker контейнер изменил статус\n"
        f"• Crash loop (≥{CRASH_LOOP_MIN} рестартов)\n"
        f"• Docker volume &gt;{VOLUME_WARN_GB}GB\n"
        f"• Внешний сервис недоступен (авто-список из Coolify)\n"
        f"• SSL сертификат &lt;{SSL_WARN_DAYS} дней\n\n"
        f"{b('Умное поведение:')}\n"
        f"• Группировка: много проблем = одно сообщение\n"
        f"• Авто-тишина {BACKUP_WIN_START}:00–{BACKUP_WIN_END}:00 (окно бэкапов)\n"
        f"• Ежедневный отчёт в {SUMMARY_HOUR}:00\n"
        f"• Еженедельный тренд по воскресеньям\n"
        f"• Cooldown алерта: {ALERT_COOLDOWN//60} мин",
        parse_mode="HTML")

# ── Jobs ──────────────────────────────────────────────────────────────────────
def make_check_job(con):
    async def job(ctx: CallbackContext):
        await run_checks(ctx.bot, con)
    return job

def make_daily_job(con):
    async def job(ctx: CallbackContext):
        record_daily_metrics(con)
        await ctx.bot.send_message(ADMIN_ID, build_status(con), parse_mode="HTML")
    return job

def make_ssl_job(con):
    async def job(ctx: CallbackContext):
        await run_ssl_checks(ctx.bot, con)
    return job

def make_weekly_job(con):
    async def job(ctx: CallbackContext):
        await ctx.bot.send_message(ADMIN_ID, build_weekly_trend(con), parse_mode="HTML")
    return job

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    con = init_db()
    app = Application.builder().token(TG_TOKEN).build()
    app.bot_data["con"] = con

    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("logs",      cmd_logs))
    app.add_handler(CommandHandler("restart",   cmd_restart))
    app.add_handler(CommandHandler("reboot",    cmd_reboot))
    app.add_handler(CommandHandler("silence",   cmd_silence))
    app.add_handler(CommandHandler("unsilence", cmd_unsilence))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CallbackQueryHandler(handle_callback))

    app.job_queue.run_repeating(make_check_job(con), interval=CHECK_INTERVAL, first=10)
    app.job_queue.run_daily(make_daily_job(con),  time=dtime(hour=SUMMARY_HOUR, minute=0))
    app.job_queue.run_daily(make_ssl_job(con),    time=dtime(hour=SUMMARY_HOUR, minute=5))
    app.job_queue.run_daily(make_weekly_job(con), time=dtime(hour=SUMMARY_HOUR, minute=10),
                            days=(6,))  # воскресенье

    await app.initialize()
    await app.bot.send_message(ADMIN_ID, f"🚀 {b('Proxmox Monitor v2')} запущен!", parse_mode="HTML")
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
