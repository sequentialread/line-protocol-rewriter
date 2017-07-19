FROM debian

COPY line-protocol-rewriter config.json ./

RUN chmod +x line-protocol-rewriter && ls -lah /

EXPOSE 8086

CMD ./line-protocol-rewriter
