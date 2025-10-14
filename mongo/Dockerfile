FROM mongo:7
COPY init-sharding.sh /usr/local/bin/init-sharding.sh
RUN chmod +x /usr/local/bin/init-sharding.sh
ENTRYPOINT ["/usr/local/bin/init-sharding.sh"] 