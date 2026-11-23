FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml constraints.txt README.md ./
COPY src ./src
RUN pip install --no-cache-dir -c constraints.txt ".[redis]"
RUN useradd --create-home broker
USER broker
EXPOSE 8765
# The secret comes from the environment (SIGBROKER_SECRET); nothing is baked into the image.
ENTRYPOINT ["sigbroker"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8765", "--backend", "redis://redis:6379/0"]
