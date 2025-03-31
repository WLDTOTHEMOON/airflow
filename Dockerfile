# docker build -t my_airflow .
FROM apache/airflow:2.10.5

# 切换到 root 用户
USER root

# 如果 sources.list 不存在，则手动创建
RUN [ -f /etc/apt/sources.list ] || echo "deb http://deb.debian.org/debian bookworm main" > /etc/apt/sources.list

# 更新 APT 并安装依赖
RUN apt-get update --fix-missing && \
    apt-get install -y \
    gcc g++ build-essential \
    python3-dev python3-pip python3-setuptools \
    libpq-dev libffi-dev libssl-dev \
    --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 切换回 airflow 用户
USER airflow

# 复制 requirements.txt 并安装 Python 依赖
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
