import smtplib
import mimetypes
import os
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from package.logging_helper import logger, log_exception
from package.config_loader import config
import time


@log_exception
def send_email_by_mode(recepient, emailcc, attachment_paths, subject, mode="auto", file_type="files", content=None, content_type="html"):
    global config
    if not isinstance(recepient, list):
        recepient = [recepient]
    if not isinstance(emailcc, list):
        emailcc = [emailcc]
    if not isinstance(attachment_paths, list):
        attachment_paths = [attachment_paths]

    emailfrom = config["emails"][mode]["user_name"]
    emailto = recepient + emailcc
    username = config["emails"][mode]["user_name"]
    password = config["emails"][mode]["password"]

    msg = MIMEMultipart()
    msg["From"] = emailfrom
    msg["To"] = ",".join(recepient)
    msg["Cc"] = ",".join(emailcc)
    msg["Subject"] = subject

    if content is not None:
        msg.attach(MIMEText(content, content_type))

    if file_type == "files":
        for file_path in attachment_paths:
            ctype, encoding = mimetypes.guess_type(file_path)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"
            maintype, subtype = ctype.split("/", 1)
            try:
                with open(file_path, "rb") as f:
                    part = MIMEBase(maintype, subtype)
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", "attachment", filename=os.path.basename(file_path))
                    # print os.path.basename(file_path)
                    logger.info(f"success: upload the file {file_path}")
                    msg.attach(part)
            except IOError:
                logger.exception(f"error: Can't open the file {file_path}")
                raise Exception(f"error: Can't open the file {file_path}")
    elif file_type == "zip":
        with open(attachment_paths, "rb") as fin:
            data = fin.read()

            part = MIMEBase("application", "octet-stream")
            part.set_payload(data)
            encoders.encode_base64(part)

            part.add_header("Content-Disposition", 'attachment; filename="%s"' % attachment_paths)
            msg.attach(part)

    if mode in ["opl"]:
        server = smtplib.SMTP("smtpout.secureserver.net", 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()

    elif mode in ["pug"]:
        smtp_server = "puprime-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    elif mode in ["vfx"]:
        smtp_server = "vantagemarkets-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    elif mode in ["au"]:
        smtp_server = "vantagemarkets-com-au.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    elif mode in ["vt"]:
        smtp_server = "vtmarkets-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    elif mode in ["iv"]:
        smtp_server = "startrader-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    else:
        # 原先登入帳密寄信方式
        server = smtplib.SMTP("smtp.office365.com", 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
    time.sleep(10)
