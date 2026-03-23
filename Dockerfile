FROM python:3.11-slim

WORKDIR /app

# scipy / numpy 需要 gcc
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 先只复制 requirements 利用 layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# 复制项目代码
COPY . .

# SQLite 数据目录
RUN mkdir -p /app/data

EXPOSE 8082

COPY deploy/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
