FROM eclipse-mosquitto:2

USER root

RUN apk add --no-cache python3 py3-pip

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/requirements.txt

COPY mosquitto.conf /mosquitto/config/mosquitto.conf
COPY entrypoint.sh /entrypoint.sh
COPY bridge.py /app/bridge.py
COPY credsync.py /app/credsync.py

RUN chmod +x /entrypoint.sh

EXPOSE 1883

CMD ["/entrypoint.sh"]
