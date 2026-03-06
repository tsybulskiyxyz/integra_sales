# Integra Sales — деплой на сервер 24/7

Приложение и бот запускаются **одним процессом** (uvicorn). Nginx проксирует запросы на домен.

---

## 1. Подготовка на сервере

```bash
# Python и зависимости
sudo apt update
sudo apt install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx

# Перейти в папку проекта (путь может быть ~/integra_sales или ~/integra_sales/integra_sales)
cd ~/integra_sales
# или: cd ~/integra_sales/integra_sales

# Виртуальное окружение
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. Systemd — автозапуск 24/7

Создать файл сервиса:

```bash
sudo nano /etc/systemd/system/integra-sales.service
```

**Содержимое** (подставь свой путь и пользователя):

```ini
[Unit]
Description=Integra Sales (CRM + Telegram Bot)
After=network.target

[Service]
Type=simple
User=integra
WorkingDirectory=/home/integra/integra_sales
Environment="PATH=/home/integra/integra_sales/.venv/bin"
ExecStart=/home/integra/integra_sales/.venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

**Если проект в `~/integra_sales/integra_sales`:**
- `WorkingDirectory=/home/integra/integra_sales/integra_sales`
- `Environment="PATH=/home/integra/integra_sales/integra_sales/.venv/bin"`
- `ExecStart=/home/integra/integra_sales/integra_sales/.venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000`

Включить и запустить:

```bash
sudo systemctl daemon-reload
sudo systemctl enable integra-sales
sudo systemctl start integra-sales
sudo systemctl status integra-sales
```

---

## 3. Nginx + домен + HTTPS

### 3.1. DNS

В панели регистратора домена добавь **A-запись**:
- Имя: `@` (или `crm`, `app` — как хочешь)
- Значение: IP твоего сервера

### 3.2. Конфиг Nginx

```bash
sudo nano /etc/nginx/sites-available/integra-sales
```

**Содержимое** (замени `your-domain.com` на свой домен):

```nginx
server {
    listen 80;
    server_name your-domain.com www.your-domain.com;

    client_max_body_size 15M;
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Включить сайт:

```bash
sudo ln -s /etc/nginx/sites-available/integra-sales /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### 3.3. SSL (HTTPS)

```bash
sudo certbot --nginx -d your-domain.com -d www.your-domain.com
```

Certbot сам настроит HTTPS. Следовать подсказкам.

---

## 4. .env на сервере

Убедись, что в `.env` указаны:

```
APP_BASE_URL=https://your-domain.com
LOGIN_LINK_URL=https://your-domain.com
```

Остальные переменные — как в `.env.example`.

---

## 5. Файлы к задачам (PDF, фото) — настройка Nginx

Чтобы отправлять PDF и фото к задачам без ошибки «Сервер вернул неверный ответ»:

**1. Найди конфиг Nginx для домена:**
```bash
ls /etc/nginx/sites-available/
# Обычно: integra-sales, integrasales.ru или default
```

**2. Открой конфиг:**
```bash
sudo nano /etc/nginx/sites-available/integra-sales
# или тот файл, где твой домен integrasales.ru
```

**3. Добавь `client_max_body_size 15M;` в КАЖДЫЙ блок `server {`** — сразу после открывающей скобки:

```nginx
server {
    listen 443 ssl;
    server_name integrasales.ru;
    client_max_body_size 15M;   # ← обязательно
    # ... proxy_pass и т.д.
}

server {
    listen 80;
    server_name integrasales.ru;
    client_max_body_size 15M;   # ← и здесь
    # ...
}
```

**4. Проверь и перезагрузи:**
```bash
sudo nginx -t
sudo systemctl reload nginx
```

Без этого Nginx обрезает запросы > 1 МБ (фото и PDF часто больше).

---

## 6. Полезные команды

| Действие | Команда |
|----------|---------|
| Логи | `sudo journalctl -u integra-sales -f` |
| Перезапуск | `sudo systemctl restart integra-sales` |
| Статус | `sudo systemctl status integra-sales` |
| Обновление кода | `cd ~/integra_sales && git pull && sudo systemctl restart integra-sales` |

---

## 7. Проверка

- Сайт: `https://your-domain.com`
- Бот в Telegram должен отвечать на /start
- Логин через бота — по ссылке из /start
