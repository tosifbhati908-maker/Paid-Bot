═══════════════════════════════════════════════════════════════
  TELEGRAM BOARD HOSTED — Multi-Customer Subscription Service
═══════════════════════════════════════════════════════════════

REQUIRED ENVIRONMENT VARIABLES
───────────────────────────────
Set all of these in Railway → Project → Variables:

  API_ID            Your Telegram API ID (integer)
                    Get from https://my.telegram.org

  API_HASH          Your Telegram API hash (string)
                    Get from https://my.telegram.org

  BOT_TOKEN         BotFather token for the bot
                    Create bot at https://t.me/BotFather

  SESSION_STRING    Telethon user-session string
                    Generate with make_session.py (see below)

  ADMIN_ID          Your Telegram numeric user ID (integer, REQUIRED)
                    Find yours via https://t.me/userinfobot

  DATABASE_URL      PostgreSQL connection URL
                    Railway sets this automatically when you add
                    a PostgreSQL plugin to the project.


GENERATING SESSION_STRING (one-time, on your local machine or Termux)
──────────────────────────────────────────────────────────────────────
  1. Install dependencies locally:
       pip install telethon

  2. Export your credentials:
       export API_ID=12345
       export API_HASH=your_api_hash

  3. Run:
       python make_session.py

  4. Complete the phone number / OTP verification in the terminal.

  5. Copy the printed SESSION_STRING into Railway's environment variables.

  Important:
  - The Telethon account must have permission to send messages in every
    group/channel that customers add.
  - Keep SESSION_STRING secret — it grants full account access.


RAILWAY DEPLOYMENT STEPS
─────────────────────────
  1. Create a new Railway project.

  2. Add a PostgreSQL plugin:
       Dashboard → New → Database → PostgreSQL
     Railway injects DATABASE_URL automatically.

  3. Connect your GitHub repo (or use Railway CLI to push):
       railway login
       railway link
       railway up

  4. Set all environment variables listed above under
       Settings → Variables.

  5. Railway auto-detects requirements.txt and installs dependencies.

  6. Set the Start Command (Settings → Deploy):
       python bot.py

  7. Deploy. The bot starts, creates the schema, and is ready.


FILES
─────
  bot.py            Main application
  make_session.py   One-time session generator (run locally)
  requirements.txt  Python dependencies
  board.json        Reference only; live data is in PostgreSQL
  README.txt        This file


SUBSCRIPTION PLANS
──────────────────
  7 Days  — ₹49
  15 Days — ₹89
  30 Days — ₹149

  All plans: 20 groups max, 20-minute interval, 1,300 msgs/day (rolling 24h).


PAYMENT FLOW (MANUAL, ADMIN APPROVAL ONLY)
───────────────────────────────────────────
  Customer selects plan → sees UPI ID + QR → pays → submits UTR +
  screenshot → admin notified immediately → admin taps Approve/Reject →
  customer notified → subscription activates.

  Subscriptions are NEVER activated automatically.
  A new approval extends from the current expiry date if one exists.


ADMIN CONTROLS
──────────────
  • /start as admin → Admin Board menu
  • Customers list — view, block, unblock, extend subscription
  • Pending Payments — approve or reject each with one tap
  • Payment History — last 20 across all customers
  • Settings — change UPI ID, upload QR image, upload tutorial video


EXAMPLE VIDEO
─────────────
  Customers can watch the tutorial video BEFORE buying.
  No subscription required to view it.
  Upload/replace it anytime via Admin Board → Settings → Upload Video.


SECURITY NOTES
──────────────
  - ADMIN_ID is mandatory; omitting it will crash on startup.
  - SESSION_STRING grants full Telegram account access — treat like a password.
  - BOT_TOKEN is stored only in environment variables, never in code.
  - Duplicate UTR submissions are rejected at the database level.
  - Each customer's data is fully isolated; cross-customer queries are impossible
    through the bot interface.
  - Blocked customers are immediately stopped and cannot restart the scheduler.


LIMITS & BEHAVIOUR
──────────────────
  MAX_GROUPS       = 20 per customer
  MSG_INTERVAL_SEC = 1200 (20 minutes, fixed)
  DAILY_MSG_LIMIT  = 1300 per customer, rolling 24-hour window
                     (not midnight-reset; oldest send drops off after 24h)

  When the rolling limit is reached, sending pauses automatically and
  resumes as capacity opens up. The customer receives one notification.

  Telethon FloodWait errors are handled gracefully (sleep up to 300s).
  Groups where the account lacks send permission are logged and skipped.
