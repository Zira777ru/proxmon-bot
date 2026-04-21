import asyncio
import logging
import os
import sqlite3
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
PX_HOST        = os.getenv("PROXMOX_HOST", "192.168.0.50")
PX_NODE        = os.getenv("PROXMOX_NODE", "pve")
PX_USER        = os.getenv("PROXMOX_USER", "root@pam")
PX_TOKEN_NAME  = os.getenv("PROXMOX_TOKEN_NAME", "proxmon-bot")
PX_TOKEN_VALUE = os.getenv("PROXMOX_TOKEN_VALUE", "")
TG_TOKEN       = os.getenv("TG_TOKEN", "")
ADMIN_ID       = int(os.getenv("ADMIN_TG_ID", "0"))
GEMINI_KEY     = os.getenv("GEMINI_KEY", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
CPU_WARN       = int(os.getenv("CPU_WARN", "85"))
MEM_WARN       = int(os.getenv("MEM_WARN", "90"))
DISK_WARN      = int(os.getenv("DISK_WARN", "85"))
SUMMARY_HOUR   = int(os.getenv("SUMMARY_HOUR", "9"))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", "1800"))
DB_PATH        = os.getenv("DB_PATH", "/data/monitor.db")

PX_BASE    = f"https://{PX_HOST}:8006/api2/json"
PX_HEADERS = {"Authorization": f"PVEAPIToken={PX_USER}!{PX_TOKEN_NAME}={PX_TOKEN_VALUE}"}

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
            f"Напиши ОДИН короткий промпт (1-3 предложения на русском языке), который владелец сервера "
            f"скопирует и отправит ИИ-девопсу по имени Claude чтобы тот немедленно исправил проблему. "
            f"Промпт должен содержать все нужные технические детали. "
            f"Выведи ТОЛЬКО текст промпта, без кавычек и пояснений."
        )
        return result.text.strip()
    except Exception as e:
        log.error(f"Gemini: {e}")
        return ""

def fmt_alert(title: str, fix_prompt: str) -> str:
    msg = title
    if fix_prompt:
        msg += f"\n\n📋 <b>Промпт для Claude:</b>\n<code>{fix_prompt}</code>"
    return msg

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

# ── Docker ────────────────────────────────────────────────────────────────────
def docker_containers():
    try:
        import docker
        return docker.from_env().containers.list(all=True)
    except Exception as e:
        log.error(f"Docker: {e}")
        return []

# ── Helpers ───────────────────────────────────────────────────────────────────
def pct(used, total): return used / total * 100 if total else 0
def fmt_gb(b):        return f"{b/1024**3:.1f}G"
def uptime_str(s):
    d, rem = divmod(int(s), 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    return f"{d}д {h}ч {m}м" if d else f"{h}ч {m}м"
def vm_icon(st):  return "🟢" if st == "running" else "🔴"
def b(text):      return f"<b>{text}</b>"

# ── Status report ─────────────────────────────────────────────────────────────
def build_status():
    lines = [f"📊 {b('Proxmox Monitor')}  {datetime.now().strftime('%d.%m %H:%M')}"]

    node = node_status()
    if node:
        cpu  = node["cpu"] * 100
        mem  = pct(node["memory"]["used"], node["memory"]["total"])
        disk = pct(node["rootfs"]["used"], node["rootfs"]["total"])
        lines += [
            "",
            f"🖥 {b('Node: ' + PX_NODE)}",
            f"CPU {cpu:.1f}%  RAM {mem:.1f}%  Disk {disk:.1f}%",
            f"Uptime: {uptime_str(node['uptime'])}",
        ]
    else:
        lines += ["", f"🔴 {b('Proxmox недоступен!')}"]

    all_guests = sorted(vms() + lxc(), key=lambda x: x["vmid"])
    if all_guests:
        lines += ["", f"🖧 {b('VMs')}"]
        for g in all_guests:
            mem_s = f"{pct(g.get('mem',0), g.get('maxmem',1)):.0f}%" if g.get("maxmem") else "—"
            cpu_s = f"{g.get('cpu',0)*100:.1f}%"
            lines.append(f"{vm_icon(g['status'])} [{g['vmid']}] {g['name']}  RAM {mem_s}  CPU {cpu_s}")

    containers = docker_containers()
    if containers:
        lines += ["", f"🐳 {b('Docker')}"]
        for c in sorted(containers, key=lambda x: x.name):
            icon = "🟢" if c.status == "running" else "🔴"
            lines.append(f"{icon} {c.name}")

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
            nxt = datetime.fromtimestamp(bk["next-run"]).strftime("%d.%m %H:%M") if bk.get("next-run") else "—"
            st  = "✅" if bk.get("enabled") else "⏸"
            keep = bk.get("prune-backups", {}).get("keep-last", "?")
            lines.append(f"{st} {bk['schedule']}  след: {nxt}  хранить: {keep} шт")

    return "\n".join(lines)

# ── Alert checks ──────────────────────────────────────────────────────────────
async def check_threshold(bot, con, key, name, value, threshold, context=""):
    firing = get_state(con, f"{key}_f") == "1"
    if value > threshold:
        set_state(con, f"{key}_f", "1")
        if can_alert(con, key):
            fix = ai_fix_prompt(
                f"{name} = {value:.0f}% (порог {threshold}%)",
                context or f"Сервер: docker-core / Proxmox PVE"
            )
            await bot.send_message(
                ADMIN_ID,
                fmt_alert(f"⚠️ {b(name)}: {value:.0f}% (порог {threshold}%)", fix),
                parse_mode="HTML"
            )
            mark_alerted(con, key)
    else:
        if firing:
            await bot.send_message(ADMIN_ID, f"✅ {b(name)} в норме: {value:.0f}%", parse_mode="HTML")
            clear_alert(con, key)
        set_state(con, f"{key}_f", "0")

async def run_checks(bot, con):
    # Proxmox доступность
    node = node_status()
    px_key = "px_reachable"
    if not node:
        if can_alert(con, px_key):
            fix = ai_fix_prompt(
                "Proxmox PVE недоступен через API",
                "IP: 192.168.0.50, порт 8006. Возможно PVE завис или сеть недоступна."
            )
            await bot.send_message(ADMIN_ID, fmt_alert(f"🔴 {b('Proxmox недоступен!')}", fix), parse_mode="HTML")
            mark_alerted(con, px_key)
        return
    else:
        if get_state(con, f"{px_key}_f") == "1":
            await bot.send_message(ADMIN_ID, f"🟢 {b('Proxmox снова доступен')}", parse_mode="HTML")
        set_state(con, f"{px_key}_f", "0")
        clear_alert(con, px_key)

    # Ресурсы ноды
    await check_threshold(bot, con, "node_cpu", "CPU нагрузка (PVE)",
        node["cpu"] * 100, CPU_WARN,
        f"Proxmox node pve, текущий CPU: {node['cpu']*100:.1f}%")

    await check_threshold(bot, con, "node_mem", "RAM (PVE)",
        pct(node["memory"]["used"], node["memory"]["total"]), MEM_WARN,
        f"RAM used: {fmt_gb(node['memory']['used'])} / {fmt_gb(node['memory']['total'])}")

    # Диски
    for s in storages():
        if s.get("total", 0) > 0:
            p = pct(s["used"], s["total"])
            await check_threshold(bot, con, f"disk_{s['storage']}", f"Диск {s['storage']}", p, DISK_WARN,
                f"Storage '{s['storage']}': {fmt_gb(s['used'])} использовано из {fmt_gb(s['total'])}. "
                f"Это хранилище для {'бэкапов VM' if 'backup' in s['storage'] else 'данных'}.")

    # Статус VM
    for g in vms() + lxc():
        key  = f"vm_{g['vmid']}"
        prev = get_state(con, key)
        curr = g["status"]
        if prev and prev != curr:
            fix = ai_fix_prompt(
                f"VM '{g['name']}' (VMID {g['vmid']}) изменила статус: {prev} → {curr}",
                f"Proxmox PVE 192.168.0.50, VM {g['vmid']} ({g['name']}), "
                f"{'это главная VM с Docker и всеми сервисами' if g['vmid']==100 else 'вторичная VM'}."
            )
            await bot.send_message(
                ADMIN_ID,
                fmt_alert(f"{vm_icon(curr)} {b(g['name'])} ({g['vmid']}): {prev} → {curr}", fix),
                parse_mode="HTML"
            )
        set_state(con, key, curr)

    # Статус Docker контейнеров
    for c in docker_containers():
        key  = f"docker_{c.id[:12]}"
        prev = get_state(con, key)
        curr = c.status
        if prev and prev != curr and curr != "running":
            fix = ai_fix_prompt(
                f"Docker контейнер '{c.name}' упал: {prev} → {curr}",
                f"Контейнер на docker-core. Image: {c.image.tags[0] if c.image.tags else 'unknown'}. "
                f"Все сервисы управляются через Coolify (http://localhost:8000/api/v1)."
            )
            await bot.send_message(
                ADMIN_ID,
                fmt_alert(f"🔴 {b(c.name)}: {prev} → {curr}", fix),
                parse_mode="HTML"
            )
        elif prev and prev != curr and curr == "running":
            await bot.send_message(ADMIN_ID, f"🟢 {b(c.name)}: снова запущен", parse_mode="HTML")
        set_state(con, key, curr)

# ── PTB job wrappers ──────────────────────────────────────────────────────────
def make_check_job(con):
    async def job(ctx: CallbackContext):
        await run_checks(ctx.bot, con)
    return job

def make_daily_job():
    async def job(ctx: CallbackContext):
        await ctx.bot.send_message(ADMIN_ID, build_status(), parse_mode="HTML")
    return job

# ── Commands ──────────────────────────────────────────────────────────────────
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(build_status(), parse_mode="HTML")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        f"🤖 {b('Proxmox Monitor Bot')}\n\n"
        "/status — полный статус сервера\n"
        "/help — справка\n\n"
        f"Авто-уведомления + промпт для Claude если что-то сломается:\n"
        f"• CPU &gt; {CPU_WARN}% или RAM &gt; {MEM_WARN}%\n"
        f"• Диск &gt; {DISK_WARN}%\n"
        "• VM запустилась / остановилась\n"
        "• Docker контейнер упал\n"
        "• Proxmox недоступен\n"
        f"• Ежедневный отчёт в {SUMMARY_HOUR}:00",
        parse_mode="HTML"
    )

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    con = init_db()
    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("help",   cmd_help))

    app.job_queue.run_repeating(make_check_job(con), interval=CHECK_INTERVAL, first=10)
    app.job_queue.run_daily(make_daily_job(), time=dtime(hour=SUMMARY_HOUR, minute=0))

    await app.initialize()
    await app.bot.send_message(ADMIN_ID, f"🚀 {b('Proxmox Monitor')} запущен!", parse_mode="HTML")
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
