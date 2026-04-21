import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta

import requests
import urllib3
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler

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
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
CPU_WARN       = int(os.getenv("CPU_WARN", "85"))
MEM_WARN       = int(os.getenv("MEM_WARN", "90"))
DISK_WARN      = int(os.getenv("DISK_WARN", "85"))
SUMMARY_HOUR   = int(os.getenv("SUMMARY_HOUR", "9"))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", "1800"))  # 30 min
DB_PATH        = os.getenv("DB_PATH", "/data/monitor.db")

PX_BASE    = f"https://{PX_HOST}:8006/api2/json"
PX_HEADERS = {"Authorization": f"PVEAPIToken={PX_USER}!{PX_TOKEN_NAME}={PX_TOKEN_VALUE}"}

# ── DB ────────────────────────────────────────────────────────────────────────
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT, ts INTEGER);
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

# ── Proxmox API ───────────────────────────────────────────────────────────────
def px(path):
    try:
        r = requests.get(f"{PX_BASE}{path}", headers=PX_HEADERS, verify=False, timeout=10)
        return r.json().get("data")
    except Exception as e:
        log.error(f"Proxmox API {path}: {e}")
        return None

def node_status():  return px(f"/nodes/{PX_NODE}/status")
def vms():          return px(f"/nodes/{PX_NODE}/qemu") or []
def lxc():          return px(f"/nodes/{PX_NODE}/lxc") or []
def storages():     return px(f"/nodes/{PX_NODE}/storage") or []
def backups():      return px("/cluster/backup") or []

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
    h, m = divmod(s // 60, 60)
    d, h = divmod(h, 24)
    return f"{d}д {h}ч {m}м" if d else f"{h}ч {m}м"
def vm_icon(status):  return "🟢" if status == "running" else "🔴"

# ── Status report ─────────────────────────────────────────────────────────────
def build_status():
    lines = [f"📊 *Proxmox Monitor*  {datetime.now().strftime('%d.%m %H:%M')}"]

    node = node_status()
    if node:
        cpu  = node["cpu"] * 100
        mem  = pct(node["memory"]["used"], node["memory"]["total"])
        disk = pct(node["rootfs"]["used"], node["rootfs"]["total"])
        lines += [
            "",
            f"🖥 *Node: {PX_NODE}*",
            f"CPU {cpu:.1f}%  RAM {mem:.1f}%  Disk {disk:.1f}%",
            f"Uptime: {uptime_str(node['uptime'])}",
        ]
    else:
        lines += ["", "🔴 *Proxmox недоступен!*"]

    all_guests = sorted(vms() + lxc(), key=lambda x: x["vmid"])
    if all_guests:
        lines += ["", "🖧 *VMs*"]
        for g in all_guests:
            icon = vm_icon(g["status"])
            mem_pct = f"{pct(g.get('mem',0), g.get('maxmem',1)):.0f}%" if g.get("maxmem") else "—"
            cpu_pct = f"{g.get('cpu',0)*100:.1f}%"
            lines.append(f"{icon} [{g['vmid']}] {g['name']}  RAM {mem_pct}  CPU {cpu_pct}")

    containers = docker_containers()
    if containers:
        lines += ["", "🐳 *Docker*"]
        for c in sorted(containers, key=lambda x: x.name):
            icon = "🟢" if c.status == "running" else "🔴"
            lines.append(f"{icon} {c.name}")

    for s in storages():
        if not lines.__contains__("💾 *Storage*"):
            lines += ["", "💾 *Storage*"]
        if s.get("total", 0) > 0:
            p = pct(s["used"], s["total"])
            warn = "⚠️ " if p > DISK_WARN else ""
            lines.append(f"{warn}{s['storage']}: {p:.0f}%  {fmt_gb(s['used'])}/{fmt_gb(s['total'])}")

    bkps = backups()
    if bkps:
        lines += ["", "🗄 *Бэкапы*"]
        for b in bkps:
            nxt = datetime.fromtimestamp(b["next-run"]).strftime("%d.%m %H:%M") if b.get("next-run") else "—"
            status = "✅" if b.get("enabled") else "⏸"
            lines.append(f"{status} {b['schedule']}  следующий: {nxt}  хранить: {b.get('prune-backups',{}).get('keep-last','?')} шт")

    return "\n".join(lines)

# ── Alert checks ──────────────────────────────────────────────────────────────
async def check_threshold(bot, con, key, name, value, threshold):
    firing = get_state(con, f"{key}_f") == "1"
    if value > threshold:
        set_state(con, f"{key}_f", "1")
        if can_alert(con, key):
            await bot.send_message(ADMIN_ID, f"⚠️ *{name}*: {value:.0f}% (порог {threshold}%)", parse_mode="Markdown")
            mark_alerted(con, key)
    else:
        if firing:
            await bot.send_message(ADMIN_ID, f"✅ *{name}* в норме: {value:.0f}%", parse_mode="Markdown")
            clear_alert(con, key)
        set_state(con, f"{key}_f", "0")

async def run_checks(bot, con):
    # Proxmox node
    node = node_status()
    px_key = "px_reachable"
    if not node:
        if can_alert(con, px_key):
            await bot.send_message(ADMIN_ID, "🔴 *Proxmox недоступен!*", parse_mode="Markdown")
            mark_alerted(con, px_key)
        return
    else:
        if get_state(con, f"{px_key}_f") == "1":
            await bot.send_message(ADMIN_ID, "🟢 *Proxmox снова доступен*", parse_mode="Markdown")
            clear_alert(con, px_key)
        set_state(con, f"{px_key}_f", "0")

    await check_threshold(bot, con, "node_cpu", "CPU нагрузка (PVE)", node["cpu"]*100, CPU_WARN)
    await check_threshold(bot, con, "node_mem", "RAM (PVE)", pct(node["memory"]["used"], node["memory"]["total"]), MEM_WARN)

    # Storage
    for s in storages():
        if s.get("total", 0) > 0:
            await check_threshold(bot, con, f"disk_{s['storage']}", f"Диск {s['storage']}", pct(s["used"], s["total"]), DISK_WARN)

    # VM state changes
    for g in vms() + lxc():
        key = f"vm_{g['vmid']}"
        prev = get_state(con, key)
        curr = g["status"]
        if prev and prev != curr:
            icon = vm_icon(curr)
            await bot.send_message(ADMIN_ID, f"{icon} *{g['name']}* ({g['vmid']}): {prev} → {curr}", parse_mode="Markdown")
        set_state(con, key, curr)

    # Docker container changes
    for c in docker_containers():
        key = f"docker_{c.name}"
        prev = get_state(con, key)
        curr = c.status
        if prev and prev != curr:
            icon = "🟢" if curr == "running" else "🔴"
            await bot.send_message(ADMIN_ID, f"{icon} *{c.name}*: {prev} → {curr}", parse_mode="Markdown")
        set_state(con, key, curr)

# ── Commands ──────────────────────────────────────────────────────────────────
def admin_only(fn):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_ID:
            return
        await fn(update, ctx)
    return wrapper

@admin_only
async def cmd_status(update, ctx):
    await update.message.reply_text(build_status(), parse_mode="Markdown")

@admin_only
async def cmd_help(update, ctx):
    await update.message.reply_text(
        "🤖 *Proxmox Monitor Bot*\n\n"
        "/status — полный статус сервера\n"
        "/help — справка\n\n"
        "Автоматические уведомления:\n"
        "• CPU/RAM/диск выше порога\n"
        "• VM запустилась/остановилась\n"
        "• Docker контейнер упал/поднялся\n"
        "• Proxmox недоступен\n"
        f"• Ежедневный отчёт в {SUMMARY_HOUR}:00",
        parse_mode="Markdown"
    )

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    con = init_db()
    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("help", cmd_help))

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        lambda: asyncio.create_task(run_checks(app.bot, con)),
        "interval", seconds=CHECK_INTERVAL, id="checks"
    )
    scheduler.add_job(
        lambda: asyncio.create_task(app.bot.send_message(ADMIN_ID, build_status(), parse_mode="Markdown")),
        "cron", hour=SUMMARY_HOUR, minute=0, id="daily"
    )
    scheduler.start()

    await app.initialize()
    await app.bot.send_message(ADMIN_ID, "🚀 *Proxmox Monitor* запущен!", parse_mode="Markdown")
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
