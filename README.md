# crypto

一个最小可运行的 OKX WebSocket 测试工程，用来验证公共行情数据订阅、心跳和断线重连逻辑。

## 目录

```text
.
├── requirements.txt
├── scripts
│   └── okx_ws_test.py
└── src
    └── okx_ws
        ├── __init__.py
        └── client.py
```

## 功能

- 连接 OKX 公共 WebSocket
- 订阅 `tickers`、`trades`、`books5`、`books`
- 空闲时发送文本 `ping`，收到 `pong` 后继续保持连接
- 连接断开后自动重连
- 可通过参数切换交易对、频道、正式环境和模拟环境

## 本地运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 scripts/okx_ws_test.py --symbol BTC-USDT --channel tickers --max-messages 5
```

如果你只是临时测试，也可以直接运行：

```bash
python3 scripts/okx_ws_test.py --symbol BTC-USDT --channel books5 --max-messages 3 --pretty
```

## 常用参数

```bash
python3 scripts/okx_ws_test.py --help
```

- `--symbol`：订阅的 `instId`，例如 `BTC-USDT`
- `--channel`：支持 `tickers`、`trades`、`books5`、`books`
- `--demo`：切换到 OKX 模拟盘公共 WebSocket
- `--url`：手动指定 WebSocket 地址
- `--max-messages`：收到多少条数据后退出
- `--max-messages 0`：持续运行，不自动退出，适合部署
- `--heartbeat-interval`：空闲多少秒后发送文本 `ping`
- `--reconnect-delay`：断开后多久重连
- `--pretty`：格式化打印 JSON

## Ubuntu 部署

先做一次手工验证：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 scripts/okx_ws_test.py --symbol BTC-USDT --channel tickers --max-messages 5
```

如果要长期运行，建议使用 `systemd`：

```bash
sudo cp deploy/systemd/crypto-okx-ws.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-okx-ws
sudo systemctl status crypto-okx-ws
journalctl -u crypto-okx-ws -f
```

## 推送代码

```bash
git add .
git commit -m "Add OKX websocket test client"
git push
```
