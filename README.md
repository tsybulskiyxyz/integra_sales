# Integra CRM — Статистика холодных звонков

Связка Google Таблицы и локальной CRM для учёта холодных звонков с уведомлениями в Telegram.

## Установка

```bash
cd integra_script
pip install -r requirements.txt
```

## Настройка

1. Скопируй `.env.example` в `.env`:
   ```bash
   copy .env.example .env
   ```

2. **Google Sheets:**
   - Создай проект в [Google Cloud Console](https://console.cloud.google.com/)
   - Включи Google Sheets API
   - Создай Service Account (Credentials → Create Credentials → Service Account)
   - Скачай JSON-ключ и положи в `credentials/service_account.json`
   - Дай доступ к таблице: открой таблицу → Поделиться → добавь email из JSON (типа `...@...iam.gserviceaccount.com`) с правами «Читатель»

3. **Telegram:**
   - Создай бота через [@BotFather](https://t.me/BotFather)
   - Положи токен в `TELEGRAM_BOT_TOKEN`
   - Узнай свой Chat ID (напиши боту, затем `https://api.telegram.org/bot<TOKEN>/getUpdates`)
   - Укажи `TELEGRAM_CHAT_ID` в `.env`

4. В `.env` укажи ссылку на Google таблицу в `GOOGLE_SHEET_URL`

## Запуск

```bash
python main.py
```

Открой http://localhost:8000

## Цвета в таблице

- **Красный** — услуга не нужна
- **Зелёный** — целевой клиент, идут переговоры
- **Оранжевый** — не дозвонился
- **Фиолетовый** — закрытая сделка
- Пустые строки между рядами = один рабочий день

## Функции

- Синхронизация с Google Таблицей
- Статистика: % целевых от дозвонов, % закрытых от целевых, кол-во рабочих дней
- Комментарии и напоминания к контактам (хранятся локально в SQLite)
- Отправка напоминаний в Telegram
- Контакты с ролями: исполнитель, сметчица, инженер
- Отправка задач в Telegram выбранной роли
