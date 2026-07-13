import datetime
import json
import logging
import os
import smtplib
import ssl
import threading
import time
import uuid
import zoneinfo
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape as html_escape

logger = logging.getLogger(__name__)

# Derived from the deployed folder name, using the same transform as
# Dispatcharr's plugin loader, so the plugin works whatever the folder is called.
_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
PLUGIN_KEY = os.path.basename(_PLUGIN_DIR).replace(" ", "_").lower()

STATE_PATH = os.path.join(_PLUGIN_DIR, "state.json")

# v1.0.0 scheduled checks through Celery Beat, but Dispatcharr imports plugin
# modules after the worker has already built its task dispatch table, so
# beat-triggered plugin tasks always fail as unregistered. Scheduling now runs
# on an in-process thread instead; the old task name is kept only so upgrades
# can delete the orphaned PeriodicTask row.
LEGACY_PERIODIC_TASK_NAME = f"plugin-{PLUGIN_KEY}-check"
SCHEDULER_LOCK_FILE = os.path.join(_PLUGIN_DIR, "scheduler.pid")
SCHEDULER_RELOAD_FLAG = os.path.join(_PLUGIN_DIR, "scheduler_reload.flag")
SCHEDULER_STOP_FLAG = os.path.join(_PLUGIN_DIR, "scheduler_stop.flag")
# Next run is persisted as a wall-clock timestamp so a newly elected host
# carries on the existing schedule rather than starting it again.
SCHEDULER_NEXT_RUN_FILE = os.path.join(_PLUGIN_DIR, "scheduler_next_run.json")
# daphne can win the lock but never runs the loop, so keep it out of the
# election. Matched against /proc/self/cmdline.
SCHEDULER_INELIGIBLE_HOST_MARKERS = ("daphne", "dispatcharr.asgi")
SCHEDULER_POLL_SECONDS = 30
# The loop only wakes every SCHEDULER_POLL_SECONDS, so shorter intervals
# can't fire any faster anyway.
MIN_INTERVAL_HOURS = SCHEDULER_POLL_SECONDS / 3600

# Logo shipped with the standard Docker image; emails fall back to a plain
# text wordmark if it's missing.
LOGO_PATH = "/app/frontend/dist/logo.png"
BRAND_DARK = "#1a1b1e"

DEFAULTS = {
    "check_interval_hours": 24,
    "warning_days": "30,14,7,3,1",
    "notify_email": "",
    "smtp_host": "",
    "smtp_port": 587,
    "smtp_security": "starttls",
    "smtp_username": "",
    "smtp_password": "",
    "smtp_from_email": "",
}


# ---------------------------------------------------------------------------
# Settings / state helpers
# ---------------------------------------------------------------------------

def _load_settings():
    from apps.plugins.models import PluginConfig

    try:
        cfg = PluginConfig.objects.get(key=PLUGIN_KEY)
        settings = dict(cfg.settings or {})
    except PluginConfig.DoesNotExist:
        settings = {}
    return {**DEFAULTS, **settings}


def _dispatcharr_tzinfo():
    """Dispatcharr's configured display timezone. Scheduling and threshold
    maths stay in UTC; this is only used when formatting for display."""
    try:
        from core.models import CoreSettings

        return zoneinfo.ZoneInfo(CoreSettings.get_system_time_zone() or "UTC")
    except Exception:
        return datetime.timezone.utc


def _format_local(dt, fmt="%Y-%m-%d %H:%M %Z"):
    try:
        return dt.astimezone(_dispatcharr_tzinfo()).strftime(fmt)
    except Exception:
        return dt.strftime(fmt)


def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def _save_json(path, data):
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        os.replace(tmp_path, path)
    except OSError as e:
        logger.warning("%s: failed to persist %s: %s", PLUGIN_KEY, path, e)


def _load_state():
    data = _load_json(STATE_PATH, {})
    return data if isinstance(data, dict) else {}


def _save_state(state):
    _save_json(STATE_PATH, state)


def _parse_thresholds(raw):
    values = set()
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            value = int(float(part))
        except ValueError:
            continue
        if value >= 0:
            values.add(value)
    if not values:
        values = {int(day) for day in DEFAULTS["warning_days"].split(",")}
    return sorted(values, reverse=True)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

_LOGO_CACHE = {"loaded": False, "bytes": None}


def _load_logo_bytes():
    """Read the Dispatcharr logo once per worker process and cache the result."""
    if not _LOGO_CACHE["loaded"]:
        try:
            with open(LOGO_PATH, "rb") as fh:
                _LOGO_CACHE["bytes"] = fh.read()
        except OSError:
            _LOGO_CACHE["bytes"] = None
        _LOGO_CACHE["loaded"] = True
    return _LOGO_CACHE["bytes"]


def _resolve_smtp_params(settings):
    host = (settings.get("smtp_host") or "").strip()
    if not host:
        raise ValueError("SMTP host is not configured")

    try:
        port = int(settings.get("smtp_port") or 587)
    except (TypeError, ValueError):
        port = 587

    security = settings.get("smtp_security") or "starttls"
    username = (settings.get("smtp_username") or "").strip()
    password = settings.get("smtp_password") or ""
    from_addr = (settings.get("smtp_from_email") or username or "").strip()

    recipients = [
        addr.strip()
        for addr in (settings.get("notify_email") or "").split(",")
        if addr.strip()
    ]
    if not recipients:
        raise ValueError("No notification email address configured")
    if not from_addr:
        raise ValueError("No From address available (set SMTP Username or From Address)")

    return {
        "host": host,
        "port": port,
        "security": security,
        "username": username,
        "password": password,
        "from_addr": from_addr,
        "recipients": recipients,
    }


def _render_email_html(heading, status_label, accent_color, message_html, detail_rows):
    logo_bytes = _load_logo_bytes()
    if logo_bytes:
        brand_html = (
            '<img src="cid:dispatcharr-logo" alt="Dispatcharr" height="28" '
            'style="display:block;border:0;outline:none;text-decoration:none;">'
        )
    else:
        brand_html = (
            f'<span style="font-size:18px;font-weight:700;color:#ffffff;'
            f'letter-spacing:0.5px;">DISPATCHARR</span>'
        )

    rows_html = "".join(
        f'''<tr>
              <td style="padding:8px 0;border-top:1px solid #e9ecef;color:#868e96;
                         font-size:13px;width:150px;vertical-align:top;">{html_escape(label)}</td>
              <td style="padding:8px 0;border-top:1px solid #e9ecef;color:#1a1b1e;
                         font-size:14px;font-weight:600;vertical-align:top;">{html_escape(value)}</td>
            </tr>'''
        for label, value in detail_rows
    )

    return f'''<!doctype html>
<html>
  <body style="margin:0;padding:0;background-color:#f1f3f5;
               font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
           style="background-color:#f1f3f5;padding:24px 0;">
      <tr>
        <td align="center">
          <table role="presentation" width="600" cellpadding="0" cellspacing="0"
                 style="background-color:#ffffff;border-radius:8px;overflow:hidden;
                        box-shadow:0 1px 3px rgba(0,0,0,0.12);">
            <tr>
              <td style="background-color:{BRAND_DARK};padding:18px 32px;">{brand_html}</td>
            </tr>
            <tr>
              <td style="height:4px;line-height:4px;font-size:0;background-color:{accent_color};">&nbsp;</td>
            </tr>
            <tr>
              <td style="padding:32px;">
                <h1 style="margin:0 0 6px 0;font-size:21px;color:#1a1b1e;">{html_escape(heading)}</h1>
                <p style="margin:0 0 20px 0;font-size:12px;font-weight:700;letter-spacing:0.6px;
                          text-transform:uppercase;color:{accent_color};">{html_escape(status_label)}</p>
                <p style="margin:0 0 24px 0;font-size:14px;line-height:1.6;color:#495057;">{message_html}</p>
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0">{rows_html}</table>
              </td>
            </tr>
            <tr>
              <td style="padding:16px 32px;background-color:#f8f9fa;border-top:1px solid #e9ecef;">
                <p style="margin:0;font-size:12px;color:#adb5bd;">
                  Sent by the M3U Expiration Notifier plugin for Dispatcharr.
                </p>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>'''


def _render_email_text(heading, status_label, message_text, detail_rows):
    lines = [f"{heading} - {status_label}", ""]
    lines.append(message_text)
    lines.append("")
    for label, value in detail_rows:
        lines.append(f"{label}: {value}")
    lines.append("")
    lines.append("Sent by the M3U Expiration Notifier plugin for Dispatcharr.")
    return "\n".join(lines)


def _build_email_message(subject, from_addr, recipients, html_body, text_body):
    msg_root = MIMEMultipart("related")
    msg_root["Subject"] = subject
    msg_root["From"] = from_addr
    msg_root["To"] = ", ".join(recipients)

    msg_alt = MIMEMultipart("alternative")
    msg_alt.attach(MIMEText(text_body, "plain", "utf-8"))
    msg_alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg_root.attach(msg_alt)

    logo_bytes = _load_logo_bytes()
    if logo_bytes:
        img = MIMEImage(logo_bytes, _subtype="png")
        img.add_header("Content-ID", "<dispatcharr-logo>")
        img.add_header("Content-Disposition", "inline")
        msg_root.attach(img)

    return msg_root


def _deliver_message(params, msg=None):
    """Connect, log in, and send msg. With msg=None this just proves the
    connection and credentials work (the Validate SMTP Connection action).
    smtplib issues EHLO itself before starttls/login/sendmail as needed."""
    if params["security"] == "ssl":
        server = smtplib.SMTP_SSL(
            params["host"], params["port"], timeout=30, context=ssl.create_default_context()
        )
    else:
        server = smtplib.SMTP(params["host"], params["port"], timeout=30)
    with server:
        if params["security"] == "starttls":
            server.starttls(context=ssl.create_default_context())
        if params["username"]:
            server.login(params["username"], params["password"])
        if msg is not None:
            server.sendmail(params["from_addr"], params["recipients"], msg.as_string())


def _validate_smtp_connection(settings):
    """Returns True if a login was performed, False if no username is set."""
    params = _resolve_smtp_params(settings)
    _deliver_message(params)
    return bool(params["username"])


def _send_styled_email(settings, subject, heading, status_label, accent_color, message_html, message_text, detail_rows):
    params = _resolve_smtp_params(settings)
    html_body = _render_email_html(heading, status_label, accent_color, message_html, detail_rows)
    text_body = _render_email_text(heading, status_label, message_text, detail_rows)
    msg = _build_email_message(subject, params["from_addr"], params["recipients"], html_body, text_body)
    _deliver_message(params, msg)


def _relative_days_label(days_left):
    if days_left <= 0:
        overdue = int(round(abs(days_left)))
        if overdue == 0:
            return "today"
        if overdue == 1:
            return "1 day ago"
        return f"{overdue} days ago"
    days = int(days_left)
    if days == 0:
        return "today"
    if days == 1:
        return "in 1 day"
    return f"in {days} days"


def _send_expiration_email(profile, exp_date, days_left, settings):
    account_name = profile.m3u_account.name
    profile_name = profile.name
    expires_str = _format_local(exp_date)
    expired = days_left <= 0

    subject = (
        f"[Dispatcharr] M3U account expired: {account_name}"
        if expired
        else f"[Dispatcharr] M3U account expiring soon: {account_name}"
    )
    status_label = "Expired" if expired else "Expiring Soon"
    accent_color = "#e03131" if expired else "#f08c00"

    verb = "has expired" if expired else "is expiring soon"
    message_text = f'The M3U account "{account_name}" (profile "{profile_name}") {verb}.'
    message_html = (
        f'The M3U account <strong>{html_escape(account_name)}</strong> '
        f'(profile <strong>{html_escape(profile_name)}</strong>) {verb}.'
    )

    detail_rows = [
        ("M3U Account", account_name),
        ("Profile", profile_name),
        ("Expiration Date", expires_str),
        ("Time Remaining", _relative_days_label(days_left)),
    ]

    _send_styled_email(
        settings, subject, account_name, status_label, accent_color,
        message_html, message_text, detail_rows,
    )


# ---------------------------------------------------------------------------
# Core check logic
# ---------------------------------------------------------------------------

def _process_profile(profile, thresholds, now, state):
    exp_date = profile.exp_date
    key = str(profile.id)
    entry = state.get(key) or {}

    if entry.get("exp_date") != exp_date.isoformat():
        # Expiration date changed (e.g. renewal) - start tracking afresh.
        entry = {"exp_date": exp_date.isoformat(), "last_notified_days": None}

    days_left = (exp_date - now).total_seconds() / 86400.0
    last_notified = entry.get("last_notified_days")

    if days_left <= 0:
        crossed = 0
    else:
        candidates = [threshold for threshold in thresholds if days_left <= threshold]
        crossed = min(candidates) if candidates else None

    should_notify = crossed is not None and (last_notified is None or crossed < last_notified)

    state[key] = entry
    return should_notify, days_left, crossed


def _sync_delivery_error_notification(errors):
    """Raise an in-app warning when a check fails to deliver emails (usually a
    broken SMTP config), and clear it again once a check completes cleanly.
    Same notification pattern Dispatcharr core uses for its own expiry checks."""
    try:
        from core.models import SystemNotification
        from core.utils import send_notification_dismissed, send_websocket_notification

        key = f"{PLUGIN_KEY}-delivery-error"
        if errors:
            notification, _created = SystemNotification.objects.update_or_create(
                notification_key=key,
                defaults={
                    "notification_type": SystemNotification.NotificationType.WARNING,
                    "priority": SystemNotification.Priority.HIGH,
                    "title": "M3U Expiration Notifier: email delivery failing",
                    "message": (
                        f"{len(errors)} profile notification(s) could not be delivered "
                        f"on the last check. First error: {errors[0]}. Run 'Validate "
                        "SMTP Connection' under the plugin's Actions tab to check your "
                        "settings."
                    ),
                    "action_data": {"errors": errors[:10]},
                    "is_active": True,
                    "admin_only": True,
                },
            )
            send_websocket_notification(notification)
        else:
            deleted_count, _ = SystemNotification.objects.filter(notification_key=key).delete()
            if deleted_count:
                send_notification_dismissed(key)
    except Exception:
        logger.warning("Failed to sync delivery-error notification for %s", PLUGIN_KEY, exc_info=True)


def _do_check(settings):
    from django.utils import timezone
    from apps.m3u.models import M3UAccountProfile

    thresholds = _parse_thresholds(settings.get("warning_days"))
    now = timezone.now()
    state = _load_state()

    profiles = M3UAccountProfile.objects.filter(
        m3u_account__is_active=True,
        is_active=True,
        exp_date__isnull=False,
    ).select_related("m3u_account")

    checked = 0
    notified = 0
    errors = []

    for profile in profiles:
        checked += 1
        try:
            should_notify, days_left, crossed = _process_profile(profile, thresholds, now, state)
            if should_notify:
                _send_expiration_email(profile, profile.exp_date, days_left, settings)
                state[str(profile.id)]["last_notified_days"] = crossed
                notified += 1
        except Exception as e:
            logger.exception(
                "Error checking expiration for profile '%s' on account '%s'",
                profile.name,
                profile.m3u_account.name,
            )
            errors.append(f"{profile.m3u_account.name}/{profile.name}: {e}")

    _save_state(state)
    _sync_delivery_error_notification(errors)

    message = f"Checked {checked} profile(s); sent {notified} notification(s)."
    if errors:
        message += " Errors: " + "; ".join(errors)

    return {"status": "error" if errors else "ok", "message": message}


# ---------------------------------------------------------------------------
# Scheduling
#
# One process claims SCHEDULER_LOCK_FILE and runs a background thread that
# calls _do_check() on the configured interval - no Celery involved. The
# reload/stop flag files let other processes signal the elected host, since
# Dispatcharr runs several processes with no shared memory.
# ---------------------------------------------------------------------------

_scheduler_thread = None
_scheduler_stop_event = threading.Event()
_scheduler_lifecycle_lock = threading.RLock()
_scheduler_init_lock = threading.Lock()
_scheduler_initialized = False
# PID numbers repeat across container restarts, so the lock also stores this
# per-process token - without it a process could mistake a dead predecessor's
# lock under its own PID for its own and never re-elect.
_SCHEDULER_PROCESS_TOKEN = uuid.uuid4().hex


def _remove_file(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _read_own_cmdline():
    try:
        with open("/proc/self/cmdline", "rb") as fh:
            return fh.read().decode("utf-8", "replace").replace("\x00", " ").lower()
    except OSError:
        return ""


def _is_ineligible_scheduler_host():
    cmdline = _read_own_cmdline()
    return any(marker in cmdline for marker in SCHEDULER_INELIGIBLE_HOST_MARKERS)


def _pid_alive(pid):
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        # Exists but not signalable by us (e.g. different uid) - treat as alive.
        return True
    return True


def _read_scheduler_lock():
    try:
        with open(SCHEDULER_LOCK_FILE, "r", encoding="utf-8") as fh:
            raw = (fh.read() or "").strip()
    except OSError:
        return 0, ""
    pid_str, _, token = raw.partition(":")
    try:
        return int(pid_str), token
    except ValueError:
        return 0, ""


def _write_scheduler_lock(fh):
    fh.write(f"{os.getpid()}:{_SCHEDULER_PROCESS_TOKEN}")


def _we_hold_scheduler_lock():
    holder_pid, holder_token = _read_scheduler_lock()
    return holder_pid == os.getpid() and holder_token == _SCHEDULER_PROCESS_TOKEN


def _acquire_scheduler_lock():
    """Return True iff this process should host the scheduler thread."""
    if _is_ineligible_scheduler_host():
        return False

    try:
        fd = os.open(SCHEDULER_LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as fh:
            _write_scheduler_lock(fh)
        return True
    except FileExistsError:
        pass

    holder_pid, holder_token = _read_scheduler_lock()

    # Our own PID but a different token means a previous incarnation of this
    # PID left the lock behind - reclaim it without a liveness check.
    stale_pid_reuse = holder_pid == os.getpid() and holder_token != _SCHEDULER_PROCESS_TOKEN

    if holder_pid and not stale_pid_reuse and _pid_alive(holder_pid):
        return False

    try:
        with open(SCHEDULER_LOCK_FILE, "w", encoding="utf-8") as fh:
            _write_scheduler_lock(fh)
        return True
    except OSError:
        return False


def _release_scheduler_lock():
    if _we_hold_scheduler_lock():
        _remove_file(SCHEDULER_LOCK_FILE)


def _write_scheduler_flag(path):
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(str(time.time()))
    except OSError:
        logger.warning("Could not write %s for %s", os.path.basename(path), PLUGIN_KEY)


def _configured_interval_hours(settings):
    try:
        hours = float(settings.get("check_interval_hours") or DEFAULTS["check_interval_hours"])
    except (TypeError, ValueError):
        hours = DEFAULTS["check_interval_hours"]
    return max(MIN_INTERVAL_HOURS, hours)


def _load_scheduler_next_run():
    data = _load_json(SCHEDULER_NEXT_RUN_FILE, {})
    try:
        return float(data.get("next_run"))
    except (TypeError, ValueError):
        return None


def _save_scheduler_next_run(next_run):
    _save_json(SCHEDULER_NEXT_RUN_FILE, {"next_run": next_run})


def _scheduler_loop():
    logger.info("%s: elected as scheduler host (pid %s)", PLUGIN_KEY, os.getpid())
    # Wall-clock time so the schedule survives re-election and container
    # restarts. No persisted value means first run - fire straight away.
    next_run = _load_scheduler_next_run()
    if next_run is None:
        next_run = time.time()

    while not _scheduler_stop_event.is_set():
        try:
            if threading.current_thread() is not _scheduler_thread:
                logger.info("%s: scheduler loop superseded by a newer thread, exiting", PLUGIN_KEY)
                return

            if not _we_hold_scheduler_lock():
                logger.warning("%s: no longer holds the scheduler lock, exiting", PLUGIN_KEY)
                return

            if os.path.exists(SCHEDULER_STOP_FLAG):
                _remove_file(SCHEDULER_STOP_FLAG)
                logger.info("%s: stop requested, exiting scheduler loop", PLUGIN_KEY)
                _release_scheduler_lock()
                return

            reload_requested = os.path.exists(SCHEDULER_RELOAD_FLAG)
            if reload_requested:
                _remove_file(SCHEDULER_RELOAD_FLAG)

            settings = _load_settings()
            interval_hours = _configured_interval_hours(settings)
            if reload_requested:
                # Apply Schedule was clicked - restart the countdown from now.
                next_run = time.time() + interval_hours * 3600
                _save_scheduler_next_run(next_run)

            if time.time() >= next_run:
                # This thread bypasses PluginManager, which normally closes DB
                # connections after each action, so close them ourselves.
                from django.db import close_old_connections

                try:
                    result = _do_check(settings)
                    logger.info("%s: %s", PLUGIN_KEY, result.get("message"))
                except Exception:
                    logger.exception("%s: scheduled check failed", PLUGIN_KEY)
                finally:
                    close_old_connections()
                next_run = time.time() + interval_hours * 3600
                _save_scheduler_next_run(next_run)

        except Exception:
            logger.exception("%s: scheduler loop error", PLUGIN_KEY)

        _scheduler_stop_event.wait(SCHEDULER_POLL_SECONDS)

    logger.info("%s: scheduler stopped (pid %s)", PLUGIN_KEY, os.getpid())


def _start_scheduler_thread():
    global _scheduler_thread

    with _scheduler_lifecycle_lock:
        _stop_scheduler_thread()
        _remove_file(SCHEDULER_STOP_FLAG)
        _scheduler_stop_event.clear()
        _scheduler_thread = threading.Thread(
            target=_scheduler_loop, name=f"{PLUGIN_KEY}-scheduler", daemon=True,
        )
        _scheduler_thread.start()


def _stop_scheduler_thread():
    global _scheduler_thread

    with _scheduler_lifecycle_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            _scheduler_stop_event.set()
            _scheduler_thread.join(timeout=5)
        _scheduler_thread = None


def _init_scheduler():
    global _scheduler_initialized

    with _scheduler_init_lock:
        if _scheduler_initialized:
            return
        _scheduler_initialized = True
        try:
            if _acquire_scheduler_lock():
                _start_scheduler_thread()
            else:
                logger.debug("%s: another process already hosts the scheduler", PLUGIN_KEY)
        except Exception:
            logger.exception("%s: failed to initialise scheduler", PLUGIN_KEY)


_legacy_cleanup_done = False
_legacy_cleanup_lock = threading.Lock()


def _cleanup_legacy_periodic_task():
    """Delete the PeriodicTask row left behind by v1.0.0's Celery Beat
    scheduling. Runs once per process - delete_periodic_task logs a warning
    when the row is missing, which on a fresh install it always is."""
    global _legacy_cleanup_done

    with _legacy_cleanup_lock:
        if _legacy_cleanup_done:
            return
        _legacy_cleanup_done = True
        try:
            from core.scheduling import delete_periodic_task

            delete_periodic_task(LEGACY_PERIODIC_TASK_NAME)
        except Exception:
            pass


def _reset_state():
    _save_state({})
    return {"status": "ok", "message": "Notification history cleared. The next check will re-evaluate every profile against the current thresholds."}


def _format_hours(hours):
    return str(int(hours)) if hours == int(hours) else f"{hours:g}"


def _apply_schedule(settings):
    interval_hours = _configured_interval_hours(settings)

    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        # We already host the scheduler - tell it to pick up the new interval.
        _write_scheduler_flag(SCHEDULER_RELOAD_FLAG)
    elif _acquire_scheduler_lock():
        # No live host found - start hosting here.
        _start_scheduler_thread()
    else:
        # Another live process holds the lock - signal it to reload.
        _write_scheduler_flag(SCHEDULER_RELOAD_FLAG)

    return {"status": "ok", "message": f"Background checks scheduled every {_format_hours(interval_hours)} hour(s)."}


def _scheduler_status():
    next_run = _load_scheduler_next_run()
    if next_run is None:
        return {"status": "ok", "message": "Next check: not yet scheduled"}
    next_run_dt = datetime.datetime.fromtimestamp(next_run, tz=datetime.timezone.utc)
    return {"status": "ok", "message": f"Next check: {_format_local(next_run_dt)}"}


# ---------------------------------------------------------------------------
# Plugin class
# ---------------------------------------------------------------------------

class Plugin:
    name = "M3U Expiration Notifier"
    version = "1.0.0"
    description = "Checks your M3U account expiration dates on a schedule and emails you before (and when) they expire."
    author = "banderson"
    help_url = ""

    fields = [
        {"id": "info_schedule", "type": "info", "label": "Check Schedule"},
        {
            "id": "check_interval_hours",
            "type": "number",
            "label": "Check Interval (hours)",
            "default": 24,
            "min": 0.01,
            "help_text": (
                "How often to check M3U account expiration dates, in hours. "
                "Default is 24 (once per day). Decimals are accepted (e.g. 0.0167 "
                "≈ 1 minute) for testing; the practical minimum is ~30 seconds. "
                "After changing this, run the "
                "'Apply Schedule' action for the new interval to take effect immediately."
            ),
        },
        {"id": "info_thresholds", "type": "info", "label": "Notification Thresholds"},
        {
            "id": "warning_days",
            "type": "string",
            "label": "Warn X Days Before Expiration",
            "default": "30,14,7,3,1",
            "placeholder": "30,14,7,3,1",
            "help_text": (
                "Comma-separated list of day thresholds. An email is sent the first "
                "time a check finds an account within one of these windows of "
                "expiring. An email is also always sent once when an account has "
                "fully expired."
            ),
        },
        {"id": "info_email", "type": "info", "label": "Email Notification"},
        {
            "id": "notify_email",
            "type": "string",
            "label": "Notify Email Address(es)",
            "placeholder": "you@example.com, other@example.com",
            "help_text": "Comma-separated list of recipient email addresses.",
        },
        {"id": "info_smtp", "type": "info", "label": "SMTP Server Settings"},
        {"id": "smtp_host", "type": "string", "label": "SMTP Host", "placeholder": "smtp.gmail.com"},
        {"id": "smtp_port", "type": "number", "label": "SMTP Port", "default": 587},
        {
            "id": "smtp_security",
            "type": "select",
            "label": "Connection Security",
            "default": "starttls",
            "options": [
                {"value": "starttls", "label": "STARTTLS"},
                {"value": "ssl", "label": "SSL/TLS"},
                {"value": "none", "label": "None"},
            ],
        },
        {"id": "smtp_username", "type": "string", "label": "SMTP Username"},
        {
            "id": "smtp_password",
            "type": "string",
            "label": "SMTP Password",
            "input_type": "password",
        },
        {
            "id": "smtp_from_email",
            "type": "string",
            "label": "From Address",
            "placeholder": "Defaults to SMTP username",
            "help_text": "Optional. Leave blank to use the SMTP username as the From address.",
        },
        {
            "id": "info_smtp_verify",
            "type": "info",
            "label": "Verify Your Settings",
            "help_text": (
                "These settings aren't checked automatically. After entering them, "
                "switch to the Actions tab and run 'Validate SMTP Connection' (quick, "
                "no email sent) or 'Send Test Email' (sends a real email) to confirm "
                "they work."
            ),
        },
    ]

    actions = [
        {
            "id": "check_now",
            "label": "Check Expirations Now",
            "description": (
                "Immediately check all M3U account expiration dates and send email "
                "notifications for any that are due, based on current settings."
            ),
            "button_label": "Check Now",
            "button_variant": "filled",
            "button_color": "blue",
        },
        {
            "id": "validate_smtp",
            "label": "Validate SMTP Connection",
            "description": (
                "Connects to your SMTP server and logs in (if a username is set) "
                "without sending any email. A quick way to confirm your settings "
                "before sending a real test email."
            ),
            "button_label": "Validate Connection",
            "button_variant": "outline",
        },
        {
            "id": "send_test_email",
            "label": "Send Test Email",
            "description": (
                "Sends a real test email to your configured recipient address(es) "
                "using the current SMTP settings. Confirms delivery end-to-end: "
                "your mail provider accepts and delivers the message, not just "
                "that the connection works."
            ),
            "button_label": "Send Test Email",
            "button_variant": "outline",
        },
        {
            "id": "apply_schedule",
            "label": "Apply Schedule",
            "description": (
                "Re-creates the periodic background check using the current Check "
                "Interval setting. Run this after changing the interval."
            ),
            "button_label": "Apply Schedule",
            "button_variant": "outline",
        },
        {
            "id": "scheduler_status",
            "label": "Scheduler Status",
            "description": (
                "Shows when the next background expiration check is due to run, "
                "based on the check interval and the time of the last completed "
                "check."
            ),
            "button_label": "Scheduler Status",
            "button_variant": "outline",
        },
        {
            "id": "reset_state",
            "label": "Reset Notification State",
            "description": (
                "Clears the record of which thresholds have already been notified "
                "for each profile. Use this after changing thresholds if an account "
                "isn't getting a new email because a since-removed threshold was "
                "already notified for it."
            ),
            "button_label": "Reset Notification State",
            "button_variant": "outline",
            "button_color": "red",
            "confirm": {
                "required": True,
                "title": "Reset Notification State?",
                "message": (
                    "This will clear all notification history. The next check will "
                    "re-send emails for any profile currently within a configured "
                    "threshold."
                ),
            },
        },
    ]

    def __init__(self):
        _cleanup_legacy_periodic_task()
        _init_scheduler()

    def run(self, action, params, context):
        settings = context["settings"]

        if action == "check_now":
            try:
                return _do_check(settings)
            except Exception as e:
                logger.exception("Manual check failed for %s", PLUGIN_KEY)
                return {"status": "error", "message": f"Check failed: {e}"}

        if action == "validate_smtp":
            try:
                logged_in = _validate_smtp_connection(settings)
                outcome = (
                    "Connected and logged in successfully."
                    if logged_in
                    else "Connected successfully (no username set, so login was skipped)."
                )
                return {"status": "ok", "message": f"{outcome} No email was sent."}
            except Exception as e:
                logger.exception("SMTP validation failed for %s", PLUGIN_KEY)
                return {"status": "error", "message": f"SMTP validation failed: {e}"}

        if action == "send_test_email":
            try:
                from django.utils import timezone

                message_text = (
                    "This is a test email from the M3U Expiration Notifier plugin. "
                    "If you received this, your SMTP settings are working correctly."
                )
                _send_styled_email(
                    settings,
                    "[Dispatcharr] M3U Expiration Notifier: Test Email",
                    "Test Email",
                    "SMTP Configuration OK",
                    "#2f9e44",
                    message_text,
                    message_text,
                    [("Sent At", _format_local(timezone.now()))],
                )
                return {"status": "ok", "message": "Test email sent successfully."}
            except Exception as e:
                logger.exception("Failed to send test email for %s", PLUGIN_KEY)
                return {"status": "error", "message": f"Failed to send test email: {e}"}

        if action == "apply_schedule":
            try:
                return _apply_schedule(settings)
            except Exception as e:
                logger.exception("Failed to apply schedule for %s", PLUGIN_KEY)
                return {"status": "error", "message": f"Failed to apply schedule: {e}"}

        if action == "scheduler_status":
            try:
                return _scheduler_status()
            except Exception as e:
                logger.exception("Failed to get scheduler status for %s", PLUGIN_KEY)
                return {"status": "error", "message": f"Failed to get scheduler status: {e}"}

        if action == "reset_state":
            try:
                return _reset_state()
            except Exception as e:
                logger.exception("Failed to reset notification state for %s", PLUGIN_KEY)
                return {"status": "error", "message": f"Failed to reset notification state: {e}"}

        return {"status": "error", "message": f"Unknown action '{action}'"}

    def stop(self, context):
        _stop_scheduler_thread()
        _release_scheduler_lock()
        # A different process may be the actual host - signal it to stop too.
        _write_scheduler_flag(SCHEDULER_STOP_FLAG)
        return {"status": "ok"}
