FROM apache/spark:3.5.7-python3

USER root

RUN mkdir -p /opt/spark-app \
    /tmp/ivy \
    /tmp/spark-checkpoints \
    && chown -R spark:spark /opt/spark-app \
    /tmp/ivy \
    /tmp/spark-checkpoints \
    && chmod -R 775 /tmp/ivy \
    /tmp/spark-checkpoints

WORKDIR /opt/spark-app

COPY schemas.py .
COPY transformations.py .
COPY streaming_job.py .

USER spark