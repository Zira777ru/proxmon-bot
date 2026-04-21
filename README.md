# 🤖 Proxmox Monitor Bot

Telegram-бот для мониторинга Proxmox-сервера и Docker-контейнеров. При проблеме вызывает Gemini AI и генерирует готовый промпт для Claude DevOps — ты просто копируешь и отправляешь.

## Возможности

### Мониторинг
| Что следит | Порог / условие |
|---|---|
| CPU ноды Proxmox | > 85% |
| RAM ноды Proxmox | > 90% |
| Диски (все storage) | > 85% |
| Возраст бэкапа VM | > 25 часов |
| Статус VM | любое изменение |
| Docker контейнеры | упал / поднялся |
| Crash loop контейнера | ≥ 3 рестарта за цикл |
| Внешние сервисы | HTTP ≥ 500 или timeout |
| SSL сертификаты | < 14 дней до истечения |
| Proxmox API | недоступен |

### Умное поведение
- **Группировка**: если одновременно несколько проблем — одно сообщение вместо спама
- **Cooldown**: повтор алерта не чаще раз в 30 минут
- **Gemini AI**: при каждом алерте генерирует готовый промпт для Claude DevOps
- **Восстановление**: уведомляет когда проблема устранена
- **Тишина**: `/silence 2h` отключает алерты на время обслуживания

## Команды

| Команда | Описание |
|---|---|
| `/status` | Полный статус: нода, VM, контейнеры, диски, бэкапы, сервисы |
| `/logs <имя>` | Последние 40 строк логов контейнера |
| `/silence 2h` | Отключить алерты на 2 часа (или `30m`) |
| `/unsilence` | Включить алерты обратно |
| `/help` | Справка |

## Установка через Coolify

1. Добавить репо `https://github.com/Zira777ru/proxmon-bot` как Docker Compose приложение
2. Настроить переменные окружения (см. ниже)
3. Deploy

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `TG_TOKEN` | — | Токен Telegram бота (от @BotFather) |
| `ADMIN_TG_ID` | — | Ваш Telegram ID |
| `GEMINI_KEY` | — | Google Gemini API ключ |
| `PROXMOX_HOST` | `192.168.0.50` | IP Proxmox PVE |
| `PROXMOX_NODE` | `pve` | Имя ноды |
| `PROXMOX_USER` | `root@pam` | Пользователь API |
| `PROXMOX_TOKEN_NAME` | `proxmon-bot` | Имя API токена |
| `PROXMOX_TOKEN_VALUE` | — | Значение API токена |
| `WATCH_URLS` | — | URL для проверки, через запятую |
| `CPU_WARN` | `85` | Порог CPU % |
| `MEM_WARN` | `90` | Порог RAM % |
| `DISK_WARN` | `85` | Порог диска % |
| `BACKUP_MAX_AGE_HOURS` | `25` | Макс. возраст бэкапа в часах |
| `CRASH_LOOP_MIN_RESTARTS` | `3` | Рестартов для crash loop алерта |
| `SSL_WARN_DAYS` | `14` | Дней до истечения SSL для алерта |
| `CHECK_INTERVAL` | `60` | Интервал проверки в секундах |
| `ALERT_COOLDOWN` | `1800` | Cooldown алерта в секундах |
| `SUMMARY_HOUR` | `9` | Час ежедневного отчёта |

## Создание Proxmox API токена

```bash
ssh root@<proxmox-ip>
pvesh create /access/users/root@pam/token/proxmon-bot --privsep 0
```

Скопируй `value` из ответа → в `PROXMOX_TOKEN_VALUE`.

## Архитектура

```
Proxmox PVE API ──┐
Docker Socket ────┼──► main.py ──► Gemini AI ──► Telegram
External URLs ────┘         │
                            └──► SQLite (state + alert cooldowns)
```

Бот запускается на `docker-core` (VM 100), мониторит Proxmox через REST API и Docker через socket.
