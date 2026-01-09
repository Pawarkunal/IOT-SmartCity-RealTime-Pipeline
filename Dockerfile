FROM apache/spark:3.5.1

USER root

RUN pip install python-dotenv

USER spark
