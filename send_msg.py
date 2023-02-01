from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename
import smtplib
import sys

path='/media/james/Database/pair_raw/'
SUBJECT = sys.argv[1]
with open(path+'config.json','r', encoding = 'utf8') as config:
    dic = json.load(config)
#SUBJECT = '[Pair Data] WARNING'
content = MIMEMultipart()  #建立MIMEMultipart物件
content["subject"] = SUBJECT  #郵件標題
content["from"] = dic["send_msg_from_email"]  #寄件者
content["to"] = dic["send_msg_to_email"] #收件者

if 'WARNING' in SUBJECT:
    content.attach(MIMEText("Pair data might have something wrong. Please see the attachment."))  #郵件內容
else:
    content.attach(MIMEText("Pair data has inserted successfully. For more information please see the attachment."))

with open(path+'log/pair_output.log', "rb") as fil:
    part = MIMEApplication(fil.read(),Name=basename('pair_output.log'))
# After the file is closed
part['Content-Disposition'] = 'attachment; filename="%s"' % basename('pair_output.log')
content.attach(part)

with open(path+'log/pair_error.log', "rb") as fil:
    part = MIMEApplication(fil.read(),Name=basename('pair_error.log'))
# After the file is closed
part['Content-Disposition'] = 'attachment; filename="%s"' % basename('pair_error.log')
content.attach(part)

    
with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:  # 設定SMTP伺服器
    try:
        smtp.ehlo()  # 驗證SMTP伺服器
        smtp.starttls()  # 建立加密傳輸
        smtp.login(dic["send_msg_from_email"], dic["send_msg_email_tocken"])  # 登入寄件者gmail
        smtp.send_message(content)  # 寄送郵件
        print("Complete!")
    except Exception as e:
        print("Error message: ", e)
