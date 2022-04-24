#consumer program to trigger a email to reviewer for the unreviewed data
#program updates the mailed records as sent to review

from urllib.request import Request, urlopen
import socket
import json
import base64
from confluent_kafka import Consumer, OFFSET_BEGINNING
import sys
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pymongo

#Pymongo config

myclient = pymongo.MongoClient("mongodb+srv://admin:testAdmin@cluster0.k5ld4.mongodb.net/SkinConditionIdentification?retryWrites=true&w=majority")
#db = client.test
mydb = myclient["SkinConditionIdentification"]
mycol = mydb["UserInformation"]

#

#function to send the image and results to the reviewer
def email_details(imagefile, predicted_result):
    subject = "New Skin condition image available for review "
    body = "Dear Dr. \n A new image is available for review. Attached are the image and results predicted. \n predicted_result \n{} \n Thanks".format(predicted_result)
    sender_email = "dse60002022@gmail.com"
    receiver_email = "dse60002022@gmail.com"

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message["Bcc"] = receiver_email  # Recommended for mass emails

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    filename = imagefile  # In same directory as script

    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login("dse60002022@gmail.com", "Waynedse@2022")
        server.sendmail(sender_email, receiver_email, text)
    return("success")

if __name__ == '__main__':
    config = {'bootstrap.servers': 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
              'security.protocol': 'SASL_SSL',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': '75JYP7BH76CYFKC7',
              'sasl.password': 'rRErWkw8ZmMR/A00IuODq0CXvCk/NYGB5rXaO9oc93162oWfCZ4f0kD8fE+Q9SJC',
              'group.id': 'python_example_group_1',
              'auto.offset.reset': 'earliest'}
    # Create Consumer instance
    consumer = Consumer(config)
    # Subscribe to topic
    topic = "new_user"
    consumer.subscribe([topic])
    # Poll for new messages from pythonKafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                messageread = json.loads(msg.value())
                encoded_image = messageread["image"]
                image_name=messageread["image_name"]
                if image_name=="" or image_name=='None':
                    image_name="test_image.jpg"
                print(image_name)
                decodeit = open(image_name, 'wb')
                decodeit.write(base64.b64decode((encoded_image)))
                decodeit.close()
                # add the call for result set here and load the string into result in json format#
                #defaulted to test result 1 for testing.
                result=messageread["result"]
                email_details(image_name,result)
                review = {"review": 'Y'}
                messageread.update(review)

                #update the review status in mongo
                mycol.find_one_and_update({"_id":messageread["_id"]},{"$set": {"review": "Y"}}, upsert=True)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()