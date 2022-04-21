FROM python:3.9-alpine
#WORKDIR C:\Users\isida\PycharmProjects\FinalProjecttest2
WORKDIR /code
ENV FLASK_APP main.py
#ENV FLASK_RUN_HOST 0.0.0.0
RUN apk add --no-cache gcc musl-dev linux-headers
#RUN pip install confluent_kafka
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .
CMD ["flask", "run"]
