FROM python:3.12.1-slim-bookworm

RUN #apt-get update && apt-get install -y git gcc

RUN groupadd styx \
    && useradd -m -d /usr/local/styx -g styx styx

USER styx

COPY --chown=styx:styx coordinator/requirements.txt /var/local/styx/
COPY --chown=styx:styx styx-package /var/local/styx-package/

ENV PATH="/usr/local/styx/.local/bin:${PATH}"

RUN pip install --upgrade pip \
    && pip install --user -r /var/local/styx/requirements.txt \
    && pip install --user ./var/local/styx-package/

WORKDIR /usr/local/styx

COPY --chown=styx:styx coordinator coordinator

COPY --chown=styx:styx coordinator/start-coordinator.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-coordinator.sh

ENV PYTHONPATH /usr/local/styx

CMD ["/usr/local/bin/start-coordinator.sh"]

EXPOSE 8888