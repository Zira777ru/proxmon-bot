import asyncio
import logging
import os
import socket
import sqlite3
import ssl
import time
from datetime import datetime, timedelta, time as dtime

import requests
import urllib3
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackContext

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
SUMMARY_HOUR        = int(os.getenv("SUMMARY_HOUR", "9"))
ALERT_COOLDOWN      = int(os.getenv("ALERT_COOLDOWN", "1800"))
DB_PATH             = os.getenv("DB_PATH", "/data/monitor.db")
BACKUP_MAX_AGE_H    = int(os.getenv("BACKUP_MAX_AGE_HOURS", "25"))
CRASH_LOOP_MIN      = int(os.getenv("CRASH_LOOP_MIN_RESTARTS", "3"))
SSL_WARN_DAYS       = int(os.getenv("SSL_WARN_DAYS", "14"))
WATCH_URLS_EXTRA    = [u.strip() for u in os.getenv("WATCH_URLS", "").split(",") if u.strip()]
COOLIFY_API         = os.getenv("COOLIFY_API_URL", "http://host.docker.internal:8000/api/v1")
COOLIFY_TOKEN       = os.getenv("COOLIFY_API_TOKEN", "")
WATCH_DOMAIN_FILTER = os.getenv("WATCH_DOMAIN_FILTER", "coscore.us")

PX_BASE    = f"https://{PX_HOST}:8006/api2/json"
PX_HEADERS = {"Authorization": f"PVEAPIToken={PX_USER}!{PX_TOKEN_NAME}={PX_TOKEN_VALUE}"}

# ── Coolify auto-discovery ────────────────────────────────────────────────────
_watch_urls_cache: list[str] = []
_watch_urls_ts: float = 0
WATCH_CACHE_TTL = 300  # 5 min

def discover_watch_urls() -> list[str]:
    global _watch_urls_cache, _watch_urls_ts
    if time.time() - _watch_urls_ts < WATCH_CACHE_TTL and _watch_urls_cache:
        return _watch_urls_cache

    urls: set[str] = set(WATCH_URLS_EXTRA)
    hdrs = {"Authorization": f"Bearer {COOLIFY_TOKEN}"}

    try:
        # Applications
        r = requests.get(f"{COOLIFY_API}/applications", headers=hdrs, timeout=5)
        for app in r.json():
            fqdn = app.get("fqdn", "") or ""
            for part in fqdn.split(","):
                part = part.strip()
                if WATCH_DOMAIN_FILTER in part and "sslip.io" not in part:
                    if not part.startswith("http"):
                        part = "https://" + part
                    urls.add(part)
    except Exception as e:
        log.error(f"Coolify apps discovery: {e}")

    try:
        # Services — extract COOLIFY_FQDN from compose env
        import re
        r = requests.get(f"{COOLIFY_API}/services", headers=hdrs, timeout=5)
        for svc in r.json():
            compose = svc.get("docker_compose", "") or ""
            for fqdn in re.findall(r"COOLIFY_FQDN:\s*([^\s\n]+)", compose):
                if WATCH_DOMAIN_FILTER in fqdn and "sslip.io" not in fqdn:
                    urls.add("https://" + fqdn)
    except Exception as e:
        log.error(f"Coolify services discovery: {e}")

    result = sorted(urls)
    if result:
        _watch_urls_cache = result
        _watch_urls_ts = time.time()
        log.info(f"Discovered {len(result)} watch URLs: {result}")
    return result

# ── DB ────────────────────────────────────────────────────────────────────────
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS state  (key TEXT PRIMARY KEY, value TEXT, ts INTEGER);
        CREATE TABLE IF NOT EXISTS alerts (key TEXT PRIMARY KEY, last_sent INTEGER);
    """)
    con.commit()
    return con

def get_state(con, key):
    row = con.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return row[0] if row else None

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
    until = get_state(con, "silence_until")
    return until and time.time() < float(until)

# ── Gemini AI — только при алерте ────────────────────────────────────────────
def ai_fix_prompt(issue: str, context: str) -> str:
    if not GEMINI_KEY:
        return ""
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")
        result = model.generate_content(
            f"Ты — автоматический ассистент DevOps-бота. На домашнем сервере произошла проблема.\n"
            f"Проблема: {issue}\n"
            f"Контекст: {context}\n\n"
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
        issue_sum = f"{len(alerts)} проблем одновременно: " + "; ".join(a["issue"] for a in alerts[:4])
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
        log.error(f"Proxmox API {path}: {e}")
        return None

def node_status(): return px(f"/nodes/{PX_NODE}/status")
def vms():         return px(f"/nodes/{PX_NODE}/qemu") or []
def lxc():         return px(f"/nodes/{PX_NODE}/lxc") or []
def storages():    return px(f"/nodes/{PX_NODE}/storage") or []
def backups():     return px("/cluster/backup") or []

def last_backup_age_hours(vmid: int) -> float | None:
    data = px(f"/nodes/{PX_NODE}/storage/backup-disk/content?content=backup&vmid={vmid}")
    if not data:
        return None
    entries = [e for e in data if e.get("vmid") == vmid]
    if not entries:
        return None
    latest = max(entries, key=lambda x: x.get("ctime", 0))
    return (time.time() - latest["ctime"]) / 3600

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

# ── SSL cert check ────────────────────────────────────────────────────────────
def ssl_days_left(hostname: str) -> int | None:
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((hostname, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=hostname) as s:
                cert = s.getpeercert()
                exp = datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
                return (exp - datetime.utcnow()).days
    except Exception as e:
        log.error(f"SSL {hostname}: {e}")
        return None

# ── External URL check ────────────────────────────────────────────────────────
def url_ok(url: str) -> bool:
    try:
        r = requests.get(url, timeout=10, allow_redirects=True)
        return r.status_code < 500
    except:
        return False

# ── Helpers ───────────────────────────────────────────────────────────────────
def pct(used, total): return used / total * 100 if total else 0
def fmt_gb(b):        return f"{b/1024**3:.1f}G"
def uptime_str(s):
    d, rem = divmod(int(s), 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    return f"{d}д {h}ч {m}м" if d else f"{h}ч {m}м"
def vm_icon(st):  return "🟢" if st == "running" else "🔴"
def b(t):         return f"<b>{t}</b>"

# ── Status report ─────────────────────────────────────────────────────────────
def build_status():
    lines = [f"📊 {b('Proxmox Monitor')}  {datetime.now().strftime('%d.%m %H:%M')}"]

    node = node_status()
    if node:
        cpu  = node["cpu"] * 100
        mem  = pct(node["memory"]["used"], node["memory"]["total"])
        disk = pct(node["rootfs"]["used"], node["rootfs"]["total"])
        lines += ["", f"🖥 {b('Node: ' + PX_NODE)}",
                  f"CPU {cpu:.1f}%  RAM {mem:.1f}%  Disk {disk:.1f}%",
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
    running = [c for c in containers if c.status == "running"]
    stopped = [c for c in containers if c.status != "running"]
    if containers:
        lines += ["", f"🐳 {b('Docker')} ({len(running)}/{len(containers)} running)"]
        if stopped:
            for c in sorted(stopped, key=lambda x: x.name):
                lines.append(f"🔴 {c.name}")
        lines.append(f"🟢 {len(running)} контейнеров запущено")

    stor = storages()
    if stor:
        lines += ["", f"💾 {b('Storage')}"]
        for s in stor:
            if s.get("total", 0) > 0:
                p = pct(s["used"], s["total"])
                warn = "⚠️ " if p > DISK_WARN else ""
                lines.append(f"{warn}{s['storage']}: {p:.0f}%  {fmt_gb(s['used'])}/{fmt_gb(s['total'])}")

    bkps = backups()
    if bkps:
        lines += ["", f"🗄 {b('Бэкапы')}"]
        for bk in bkps:
            nxt  = datetime.fromtimestamp(bk["next-run"]).strftime("%d.%m %H:%M") if bk.get("next-run") else "—"
            keep = bk.get("prune-backups", {}).get("keep-last", "?")
            st   = "✅" if bk.get("enabled") else "⏸"
            lines.append(f"{st} {bk['schedule']}  след: {nxt}  хранить: {keep} шт")
        age = last_backup_age_hours(100)
        if age is not None:
            warn = "⚠️ " if age > BACKUP_MAX_AGE_H else "✅"
            lines.append(f"{warn} Последний бэкап VM 100: {age:.0f}ч назад")

    if discover_watch_urls():
        watch = discover_watch_urls()
        lines += ["", f"🌐 {b('Сервисы')} ({len(watch)})"]
        for url in watch:
            ok = url_ok(url)
            name = url.replace("https://", "").replace("http://", "").split("/")[0]
            lines.append(f"{'🟢' if ok else '🔴'} {name}")

    return "\n".join(lines)

# ── Check functions ───────────────────────────────────────────────────────────
async def run_checks(bot, con):
    if is_silenced(con):
        return

    alerts    = []
    recoveries = []

    def alert(key, text, issue, context):
        if can_alert(con, key):
            alerts.append({"key": key, "text": text, "issue": issue, "context": context})

    def recovery(text):
        recoveries.append(text)

    # ── Proxmox доступность
    node = node_status()
    px_key = "px_reachable"
    if not node:
        set_state(con, f"{px_key}_f", "1")
        alert(px_key,
              f"🔴 {b('Proxmox PVE недоступен!')}",
              "Proxmox PVE недоступен через API",
              f"IP: {PX_HOST}, порт 8006. Возможно завис или сеть упала.")
    else:
        if get_state(con, f"{px_key}_f") == "1":
            recovery(f"🟢 {b('Proxmox снова доступен')}")
        set_state(con, f"{px_key}_f", "0")
        clear_alert(con, px_key)

        cpu = node["cpu"] * 100
        mem = pct(node["memory"]["used"], node["memory"]["total"])

        for metric, val, thr, key in [
            ("CPU нагрузка (PVE)", cpu, CPU_WARN, "node_cpu"),
            ("RAM (PVE)",          mem, MEM_WARN, "node_mem"),
        ]:
            firing = get_state(con, f"{key}_f") == "1"
            if val > thr:
                set_state(con, f"{key}_f", "1")
                alert(key, f"⚠️ {b(metric)}: {val:.0f}% (порог {thr}%)",
                      f"{metric} = {val:.0f}%",
                      f"Proxmox node {PX_NODE}, порог {thr}%")
            else:
                if firing:
                    recovery(f"✅ {b(metric)} в норме: {val:.0f}%")
                    clear_alert(con, key)
                set_state(con, f"{key}_f", "0")

    # ── Диски
    for s in storages():
        if s.get("total", 0) > 0:
            p   = pct(s["used"], s["total"])
            key = f"disk_{s['storage']}"
            firing = get_state(con, f"{key}_f") == "1"
            if p > DISK_WARN:
                set_state(con, f"{key}_f", "1")
                alert(key, f"⚠️ {b('Диск ' + s['storage'])}: {p:.0f}%  {fmt_gb(s['used'])}/{fmt_gb(s['total'])}",
                      f"Диск {s['storage']} = {p:.0f}%",
                      f"Storage '{s['storage']}': {fmt_gb(s['used'])} из {fmt_gb(s['total'])}. "
                      f"{'Хранилище бэкапов — нехватка места не даст сохранить следующий бэкап.' if 'backup' in s['storage'] else ''}")
            else:
                if firing:
                    recovery(f"✅ {b('Диск ' + s['storage'])} в норме: {p:.0f}%")
                    clear_alert(con, key)
                set_state(con, f"{key}_f", "0")

    # ── Бэкапы
    for vmid in [v["vmid"] for v in vms()]:
        age = last_backup_age_hours(vmid)
        key = f"backup_age_{vmid}"
        firing = get_state(con, f"{key}_f") == "1"
        if age is None:
            set_state(con, f"{key}_f", "1")
            alert(key, f"⚠️ {b(f'Нет бэкапов VM {vmid}')}",
                  f"VM {vmid} не имеет ни одного бэкапа на backup-disk",
                  f"Proxmox PVE, backup-disk storage, VMID {vmid}")
        elif age > BACKUP_MAX_AGE_H:
            set_state(con, f"{key}_f", "1")
            alert(key, f"⚠️ {b(f'Старый бэкап VM {vmid}')}: {age:.0f}ч назад (порог {BACKUP_MAX_AGE_H}ч)",
                  f"VM {vmid} не бэкапилась {age:.0f} часов",
                  f"Последний бэкап VM {vmid} на backup-disk был {age:.0f}ч назад. "
                  f"Норма: каждые {BACKUP_MAX_AGE_H}ч. Возможно диск был полный или задача упала.")
        else:
            if firing:
                recovery(f"✅ {b(f'Бэкап VM {vmid}')} свежий: {age:.0f}ч назад")
                clear_alert(con, key)
            set_state(con, f"{key}_f", "0")

    # ── Статус VM
    for g in vms() + lxc():
        key  = f"vm_{g['vmid']}"
        prev = get_state(con, key)
        curr = g["status"]
        if prev and prev != curr:
            if curr != "running":
                alert(f"{key}_chg", f"{vm_icon(curr)} {b(g['name'])} ({g['vmid']}): {prev} → {curr}",
                      f"VM '{g['name']}' (VMID {g['vmid']}) остановилась: {prev} → {curr}",
                      f"Proxmox PVE 192.168.0.50, VMID {g['vmid']} ({g['name']}). "
                      f"{'Это главная VM — все Docker сервисы на ней.' if g['vmid'] == 100 else ''}")
            else:
                recovery(f"🟢 {b(g['name'])} ({g['vmid']}): снова запущена")
        set_state(con, key, curr)

    # ── Docker контейнеры (статус + crash loop)
    for c in docker_containers():
        cid  = c.id[:12]
        # Статус
        skey = f"docker_{cid}"
        prev = get_state(con, skey)
        curr = c.status
        if prev and prev != curr:
            if curr != "running":
                alert(f"{skey}_chg", f"🔴 {b(c.name)}: {prev} → {curr}",
                      f"Docker контейнер '{c.name}' упал: {prev} → {curr}",
                      f"docker-core, image: {c.image.tags[0] if c.image.tags else 'unknown'}. "
                      f"Управление через Coolify API http://localhost:8000/api/v1.")
            else:
                recovery(f"🟢 {b(c.name)}: снова запущен")
        set_state(con, skey, curr)

        # Crash loop — считаем рестарты
        if curr == "running":
            try:
                rc   = c.attrs.get("RestartCount", 0)
                rkey = f"docker_rc_{cid}"
                prev_rc = int(get_state(con, rkey) or 0)
                delta   = rc - prev_rc
                if delta >= CRASH_LOOP_MIN and can_alert(con, f"{rkey}_alert"):
                    alert(f"{rkey}_alert",
                          f"🔄 {b(c.name)}: crash loop ({delta} рестартов за цикл, всего {rc})",
                          f"Контейнер '{c.name}' в crash loop: {delta} рестартов за последний период",
                          f"docker-core, image: {c.image.tags[0] if c.image.tags else 'unknown'}. "
                          f"Проверь логи контейнера.")
                set_state(con, rkey, rc)
            except Exception as e:
                log.error(f"Crash loop check {c.name}: {e}")

    # ── Внешние сервисы (автодискавери из Coolify)
    for url in discover_watch_urls():
        key    = f"url_{url}"
        firing = get_state(con, f"{key}_f") == "1"
        ok     = url_ok(url)
        name   = url.replace("https://", "").split("/")[0]
        if not ok:
            set_state(con, f"{key}_f", "1")
            alert(key, f"🔴 {b(name)} недоступен: {url}",
                  f"Сервис {url} не отвечает (HTTP >= 500 или timeout)",
                  f"Внешний URL {url}. Cloudflare tunnel может быть упал или контейнер не отвечает.")
        else:
            if firing:
                recovery(f"🟢 {b(name)}: снова доступен")
                clear_alert(con, key)
            set_state(con, f"{key}_f", "0")

    # Отправляем алерты (сгруппированно) и восстановления
    for a in alerts:
        mark_alerted(con, a["key"])
    await send_alerts(bot, alerts)

    for r in recoveries:
        await bot.send_message(ADMIN_ID, r, parse_mode="HTML")

async def run_ssl_checks(bot, con):
    if is_silenced(con):
        return
    for url in discover_watch_urls():
        hostname = url.replace("https://", "").replace("http://", "").split("/")[0]
        days = ssl_days_left(hostname)
        key  = f"ssl_{hostname}"
        if days is None:
            continue
        firing = get_state(con, f"{key}_f") == "1"
        if days < SSL_WARN_DAYS:
            set_state(con, f"{key}_f", "1")
            if can_alert(con, key):
                fix = ai_fix_prompt(
                    f"SSL сертификат {hostname} истекает через {days} дней",
                    f"Домен {hostname}, осталось {days} дней. Traefik/Let's Encrypt должен обновлять автоматически."
                )
                msg = f"⚠️ {b('SSL: ' + hostname)}: истекает через {b(str(days) + ' дней')}"
                if fix:
                    msg += f"\n\n📋 <b>Промпт для Claude:</b>\n<code>{fix}</code>"
                await bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
                mark_alerted(con, key)
        else:
            if firing:
                await bot.send_message(ADMIN_ID, f"✅ {b('SSL ' + hostname)}: {days} дней до истечения", parse_mode="HTML")
                clear_alert(con, key)
            set_state(con, f"{key}_f", "0")

# ── Job wrappers ──────────────────────────────────────────────────────────────
def make_check_job(con):
    async def job(ctx: CallbackContext):
        await run_checks(ctx.bot, con)
    return job

def make_daily_job():
    async def job(ctx: CallbackContext):
        await ctx.bot.send_message(ADMIN_ID, build_status(), parse_mode="HTML")
    return job

def make_ssl_job(con):
    async def job(ctx: CallbackContext):
        await run_ssl_checks(ctx.bot, con)
    return job

# ── Commands ──────────────────────────────────────────────────────────────────
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(build_status(), parse_mode="HTML")

async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        await update.message.reply_text("Использование: /logs <имя_контейнера>")
        return
    name = ctx.args[0].lower()
    containers = docker_containers()
    found = [c for c in containers if name in c.name.lower()]
    if not found:
        await update.message.reply_text(f"Контейнер '{name}' не найден.")
        return
    c = found[0]
    try:
        logs = c.logs(tail=40).decode("utf-8", errors="replace").strip()
        if not logs:
            logs = "(логи пусты)"
        text = f"📋 {b(c.name)} — последние строки:\n\n<code>{logs[-3500:]}</code>"
        await update.message.reply_text(text, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def cmd_silence(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    con = ctx.bot_data["con"]
    duration = 3600
    if ctx.args:
        arg = ctx.args[0].lower()
        try:
            if arg.endswith("h"):
                duration = int(arg[:-1]) * 3600
            elif arg.endswith("m"):
                duration = int(arg[:-1]) * 60
            else:
                duration = int(arg) * 3600
        except ValueError:
            await update.message.reply_text("Пример: /silence 2h или /silence 30m")
            return
    until = time.time() + duration
    set_state(con, "silence_until", str(until))
    until_str = datetime.fromtimestamp(until).strftime("%H:%M")
    await update.message.reply_text(f"🔕 Алерты отключены до {until_str}")

async def cmd_unsilence(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    con = ctx.bot_data["con"]
    set_state(con, "silence_until", "0")
    await update.message.reply_text("🔔 Алерты снова включены")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = (
        f"🤖 {b('Proxmox Monitor Bot')}\n\n"
        f"{b('Команды:')}\n"
        "/status — полный статус сервера\n"
        "/logs &lt;имя&gt; — последние 40 строк логов контейнера\n"
        "/silence 2h — отключить алерты на 2 часа (или 30m)\n"
        "/unsilence — включить алерты обратно\n"
        "/help — эта справка\n\n"
        f"{b('Автоматические алерты:')}\n"
        f"• CPU &gt; {CPU_WARN}% или RAM &gt; {MEM_WARN}% на PVE\n"
        f"• Любой диск &gt; {DISK_WARN}%\n"
        f"• Бэкап старше {BACKUP_MAX_AGE_H}ч или отсутствует\n"
        f"• VM изменила статус\n"
        f"• Docker контейнер упал\n"
        f"• Crash loop (≥{CRASH_LOOP_MIN} рестартов)\n"
        f"• Внешний сервис недоступен\n"
        f"• SSL сертификат истекает &lt; {SSL_WARN_DAYS} дней\n"
        f"• Proxmox API недоступен\n\n"
        f"{b('Умное поведение:')}\n"
        "• Если несколько проблем сразу — одно сгруппированное сообщение\n"
        "• Каждый алерт содержит готовый промпт для Claude DevOps\n"
        f"• Повтор алерта не чаще раз в {ALERT_COOLDOWN//60} минут\n"
        f"• Ежедневный отчёт в {SUMMARY_HOUR}:00"
    )
    await update.message.reply_text(text, parse_mode="HTML")

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    con = init_db()
    app = Application.builder().token(TG_TOKEN).build()
    app.bot_data["con"] = con

    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("logs",      cmd_logs))
    app.add_handler(CommandHandler("silence",   cmd_silence))
    app.add_handler(CommandHandler("unsilence", cmd_unsilence))
    app.add_handler(CommandHandler("help",      cmd_help))

    app.job_queue.run_repeating(make_check_job(con), interval=CHECK_INTERVAL, first=10)
    app.job_queue.run_daily(make_daily_job(),         time=dtime(hour=SUMMARY_HOUR, minute=0))
    app.job_queue.run_daily(make_ssl_job(con),        time=dtime(hour=SUMMARY_HOUR, minute=5))

    await app.initialize()
    await app.bot.send_message(ADMIN_ID, f"🚀 {b('Proxmox Monitor')} запущен!", parse_mode="HTML")
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
