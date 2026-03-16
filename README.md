# crypto

一个最小可运行的 OKX 行情采集工程，包含两条接入方案：

- `WebSocket`：实时订阅，延迟更低
- `ccxt REST`：轮询拉取，部署更稳，适合 WebSocket 被拦的云服务器

## 目录

```text
.
├── deploy
│   └── systemd
│       ├── crypto-okx-rest.service
│       └── crypto-okx-ws.service
├── requirements.txt
├── scripts
│   ├── okx_ccxt_poll.py
│   └── okx_ws_test.py
└── src
    ├── okx_rest
    │   ├── __init__.py
    │   └── client.py
    └── okx_ws
        ├── __init__.py
        └── client.py
```

## 功能

- 连接 OKX 公共 WebSocket
- 通过 `ccxt` 轮询 OKX 公共 REST 行情
- 订阅 `tickers`、`trades`、`books5`、`books`
- 轮询 `ticker`、`order-book`、`trades`
- 空闲时发送文本 `ping`，收到 `pong` 后继续保持连接
- 连接断开后自动重连
- 可通过参数切换交易对、频道、轮询类型

## 推荐方案

如果你的云服务器连接 `wss://ws.okx.com:8443` 会被重置，优先使用 `ccxt REST` 轮询。它走 HTTPS，通常比原生 WebSocket 更容易部署成功。

## 本地运行

先安装依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 1. ccxt REST 轮询

```bash
python3 scripts/okx_ccxt_poll.py --symbol BTC/USDT --kind ticker --hostname www.okx.com --interval 2 --max-polls 5
```

也可以拉盘口或成交：

```bash
python3 scripts/okx_ccxt_poll.py --symbol BTC/USDT --kind order-book --limit 5 --max-polls 3 --pretty
python3 scripts/okx_ccxt_poll.py --symbol BTC/USDT --kind trades --limit 10 --max-polls 3
```

### 2. WebSocket 测试

```bash
python3 scripts/okx_ws_test.py --symbol BTC-USDT --channel tickers --max-messages 5
```

如果你只是临时测试 `books5`：

```bash
python3 scripts/okx_ws_test.py --symbol BTC-USDT --channel books5 --max-messages 3 --pretty
```

## 常用参数

### ccxt REST

```bash
python3 scripts/okx_ccxt_poll.py --help
```

- `--symbol`：`ccxt` 统一交易对，例如 `BTC/USDT`
- `--kind`：支持 `ticker`、`order-book`、`trades`
- `--hostname`：REST 主机名，可选 `www.okx.com`、`us.okx.com`、`eea.okx.com`
- `--interval`：轮询间隔秒数
- `--limit`：盘口档位数或成交条数
- `--max-polls 0`：持续运行，不自动退出，适合部署
- `--retry-delay`：请求失败后重试等待秒数

### WebSocket

```bash
python3 scripts/okx_ws_test.py --help
```

- `--symbol`：订阅的 `instId`，例如 `BTC-USDT`
- `--channel`：支持 `tickers`、`trades`、`books5`、`books`
- `--demo`：切换到 OKX 模拟盘公共 WebSocket
- `--url`：手动指定 WebSocket 地址
- `--max-messages 0`：持续运行，不自动退出，适合部署
- `--heartbeat-interval`：空闲多少秒后发送文本 `ping`
- `--reconnect-delay`：断开后多久重连

## Ubuntu 部署

推荐优先部署 `ccxt REST`：

```bash
cd ~/workspace/crypto
git pull
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 scripts/okx_ccxt_poll.py --symbol BTC/USDT --kind ticker --hostname www.okx.com --max-polls 5
```

如果输出正常，再部署成 `systemd` 服务：

```bash
sudo cp deploy/systemd/crypto-okx-rest.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-okx-rest
sudo systemctl status crypto-okx-rest
journalctl -u crypto-okx-rest -f
```

如果 `www.okx.com` 也连不上，可以先手工切换主机名再试：

```bash
python3 scripts/okx_ccxt_poll.py --symbol BTC/USDT --kind ticker --hostname us.okx.com --max-polls 3
python3 scripts/okx_ccxt_poll.py --symbol BTC/USDT --kind ticker --hostname eea.okx.com --max-polls 3
```

如果后续你换了网络环境，再考虑启用 WebSocket 服务：

```bash
sudo cp deploy/systemd/crypto-okx-ws.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-okx-ws
```

## 推送代码

```bash
git add .
git commit -m "Add OKX market data client"
git push
```
