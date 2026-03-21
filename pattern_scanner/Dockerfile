FROM python:3.11-slim

WORKDIR /app

# 系统依赖（scipy 需要 libgomp）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# 先复制依赖文件，利用 layer 缓存
COPY pattern_scanner/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 复制源码
COPY pattern_scanner/ /app/pattern_scanner/

# 非 root 用户运行
RUN useradd -m scanner
USER scanner

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# 默认启动调度器；可通过 CMD 覆盖
CMD ["python", "-m", "pattern_scanner.scheduler"]
