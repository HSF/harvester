import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def encode_user_data(user_data):
    attached_message = MIMEMultipart()
    message = MIMEText(user_data, "cloud-config", sys.getdefaultencoding())
    message.add_header("Content-Disposition", 'attachment; filename="%s"' % ("cs-cloud-init.yaml"))
    attached_message.attach(message)

    return attached_message
