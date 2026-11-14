FROM python:3.13-alpine

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY hogs_server.py check.py ./

USER 10001
EXPOSE 47800
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD ["python", "check.py"]
CMD ["python", "hogs_server.py"]
