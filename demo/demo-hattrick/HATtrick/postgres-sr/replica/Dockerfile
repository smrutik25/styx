FROM postgres:14.5

COPY recovery.sh /usr/local/bin/recovery.sh
RUN chmod +x /usr/local/bin/recovery.sh

ENTRYPOINT ["/usr/local/bin/recovery.sh"]