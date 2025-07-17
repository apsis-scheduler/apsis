"""
Sending email by SMTP.

The outgoing server configuration is represented by `smtp_cfg`, a configuration
dictionary with the following fields and defaults:

    {
        "host": "localhost",
        "port": 0,
        "ssl": False,
        "auth": None,
    }

If `port` is 0, the default port for the appropriate protocol is used.  `ssl`
may by false, true, or `"starttls"`.  `auth` is none for no auth, or a
`{username, password}` dict.
"""

from email.mime.text import MIMEText
import logging
import os
import pwd
import smtplib
import socket

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


def get_default_sender():
    """
    Determines the sender / to address for outgoing emails.
    """
    try:
        return os.environ["EMAIL"]
    except KeyError:
        pass
    else:
        # Guess.
        # Not sure if euid is the right one to use here.
        user = pwd.getpwuid(os.geteuid()).pw_name
        host = socket.getfqdn()
        return f"{user}@{host}"


def send_message(to, msg, *, from_=None, smtp_cfg={}):
    """
    Sends a message.

    :param to:
      A sequence of recipient email addresses.
    :param msg:
      `email.mime` message object to send.
    :param from_:
      The sender address, or none to determine automatically.
    """
    host = smtp_cfg.get("host", "localhost")
    port = smtp_cfg.get("port", 0)
    ssl = smtp_cfg.get("ssl", False)
    auth = smtp_cfg.get("auth", None)

    log.info(f"sending email: {from_} to {', '.join(to)}")
    SMTP = smtplib.SMTP_SSL if ssl is True else smtplib.SMTP
    with SMTP(host, port) as smtp:
        if ssl == "starttls":
            smtp.starttls()
        if auth is not None:
            smtp.login(auth["username"], auth["password"])
        smtp.send_message(msg, from_, to)
    log.debug("email sent successfully")


def send_html(to, subject, html, *, from_=None, smtp_cfg={}):
    """
    Sends an HTML message.

    :param to:
      A sequence of recipient email addresses.
    :param from_:
      The sender address, or none to determine automatically.
    """
    msg = MIMEText(html, "html")
    msg["Subject"] = subject
    msg["From"] = from_
    msg["To"] = ", ".join(to)  # FIXME: quote

    send_message(to, msg, from_=from_, smtp_cfg=smtp_cfg)
