# crypto

一个最小可运行的多交易所行情采集工程，当前包含三类能力：

- `WebSocket`：实时订阅，延迟更低
- `ccxt REST`：轮询拉取，部署更稳，适合 WebSocket 被拦的云服务器
- `跨交易所盘口监控`：同时订阅 Binance 与 OKX，发现可套利价差时输出告警

## 目录

```text
.
├── deploy
│   └── systemd
│       ├── crypto-binance-rest.service
│       ├── crypto-cross-spread.service
│       ├── crypto-okx-rest.service
│       └── crypto-okx-ws.service
├── requirements.txt
├── scripts
│   ├── binance_rest_poll.py
│   ├── binance_ws_test.py
│   ├── cross_exchange_spread.py
│   ├── okx_ccxt_poll.py
│   └── okx_ws_test.py
└── src
    ├── arbitrage
    │   ├── __init__.py
    │   └── monitor.py
    ├── binance_rest
    │   ├── __init__.py
    │   └── client.py
    ├── binance_ws
    │   ├── __init__.py
    │   └── client.py
    ├── okx_rest
    │   ├── __init__.py
    │   └── client.py
    └── okx_ws
        ├── __init__.py
        └── client.py
```

## 功能

- 连接 OKX 公共 WebSocket
- 连接 Binance 公共 WebSocket `bookTicker`
- 通过 `ccxt` 轮询 OKX 公共 REST 行情
- 订阅 `tickers`、`trades`、`books5`、`books`
- 通过 Binance `bookTicker` 和 OKX `books5` 计算双边顶级盘口价差
- 按手续费、净价差和盘口数量过滤告警
- 命中后输出 JSON，可选推送到 webhook
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

### 3. Binance REST 测试

如果你要验证服务器是否能直接访问 Binance 公共行情接口，可以运行：

```bash
python3 scripts/binance_rest_poll.py --symbol BTC/USDT --kind ticker --max-polls 3
```

也可以测试盘口和成交：

```bash
python3 scripts/binance_rest_poll.py --symbol BTC/USDT --kind order-book --limit 5 --max-polls 3 --pretty
python3 scripts/binance_rest_poll.py --symbol BTC/USDT --kind trades --limit 10 --max-polls 3
```

### 4. Binance WebSocket 测试

如果你要验证服务器是否能实时拿到 Binance 顶级盘口：

```bash
python3 scripts/binance_ws_test.py --symbol BTCUSDT --max-messages 5
```

### 5. 跨交易所盘口价差监控

同时订阅 Binance `bookTicker` 和 OKX `books5`，当两个交易所的顶级盘口满足净价差阈值时输出告警：

```bash
python3 scripts/cross_exchange_spread.py --symbol BTC-USDT --binance-fee-bps 10 --okx-fee-bps 10 --min-net-bps 2 --min-size 0.001
```

如果你想把告警推到 webhook：

```bash
python3 scripts/cross_exchange_spread.py --symbol BTC-USDT --webhook-url https://example.com/webhook
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

### 跨交易所监控

```bash
python3 scripts/cross_exchange_spread.py --help
```

- `--symbol`：OKX 风格交易对，例如 `BTC-USDT`
- `--binance-fee-bps` / `--okx-fee-bps`：两边假设的 taker 手续费
- `--min-net-bps`：扣除手续费后，最小净价差阈值
- `--min-size`：要求盘口可成交数量至少达到这个值
- `--max-quote-age`：超过这个时效的报价不参与判断
- `--alert-cooldown`：同一方向两次告警之间至少间隔多少秒
- `--webhook-url`：命中后以 JSON POST 到你的告警入口

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

## 新服务器从 GitHub 部署

当前仓库远程地址使用 SSH：

```bash
git@github.com:castiel0921/crypto.git
```

推荐在海外服务器上配置 GitHub SSH key，然后直接拉代码部署。

### 1. 在服务器上生成 SSH key

```bash
ssh-keygen -t ed25519 -C "crypto-deploy" -f ~/.ssh/id_ed25519
cat ~/.ssh/id_ed25519.pub
```

把输出的公钥添加到 GitHub 仓库的 Deploy keys，至少勾选读取权限。

### 2. 测试服务器是否能访问 GitHub

```bash
ssh -T git@github.com
```

### 3. 克隆仓库并安装依赖

```bash
mkdir -p ~/workspace
cd ~/workspace
git clone git@github.com:castiel0921/crypto.git
cd crypto
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 4. 手工验证脚本

先验证 REST 轮询：

```bash
.venv/bin/python scripts/okx_ccxt_poll.py --symbol BTC/USDT --kind ticker --hostname www.okx.com --max-polls 3
```

如果网络允许，再验证 WebSocket：

```bash
.venv/bin/python scripts/okx_ws_test.py --symbol BTC-USDT --channel tickers --max-messages 3
```

### 5. 配置 systemd 服务

如果你的项目目录不是 `/home/ubuntu/workspace/crypto`，先把 service 文件里的路径替换成实际路径，例如 `/home/ubuntu/projects/crypto`。

部署 REST 服务：

```bash
sudo cp deploy/systemd/crypto-okx-rest.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-okx-rest
sudo systemctl status crypto-okx-rest
```

部署 Binance REST 服务：

```bash
sudo cp deploy/systemd/crypto-binance-rest.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-binance-rest
sudo systemctl status crypto-binance-rest
```

部署跨交易所盘口监控服务：

```bash
sudo cp deploy/systemd/crypto-cross-spread.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-cross-spread
sudo systemctl status crypto-cross-spread
```

部署 WebSocket 服务：

```bash
sudo cp deploy/systemd/crypto-okx-ws.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-okx-ws
sudo systemctl status crypto-okx-ws
```

### 6. 后续更新代码并重启服务

仓库已经提供一个更新脚本：

```bash
chmod +x deploy/update.sh
./deploy/update.sh crypto-okx-rest
```

如果你当前部署的是 WebSocket 服务：

```bash
./deploy/update.sh crypto-okx-ws
```

如果只想拉代码和安装依赖，不重启服务：

```bash
./deploy/update.sh none
```

注意：脚本内部使用 `sudo -n systemctl`，因此服务器上的部署用户需要具备无交互的 `systemctl` 权限，否则脚本会失败。

## 推送代码

```bash
git add .
git commit -m "Add OKX market data client"
git push
```
