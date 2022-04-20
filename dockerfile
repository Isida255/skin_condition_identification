FROM python:3.9

WORKDIR C:\Users\isida\PycharmProjects\FinalProjecttest2
COPY main.py .
COPY requirements.txt .
COPY upload.html .
COPY spark_clouddb_updated.py .

#RUN pip install flask_restful Flask Api Resource requests jsonify

RUN python -m pip install -r requirements.txt
CMD ["python", "./main.py", "./spark_clouddb_updated.py"]