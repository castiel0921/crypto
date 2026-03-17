const MARKET_LABELS = {
  spot: "现货",
  usdt_perp: "U本位永续",
  coin_perp: "币本位永续",
};

const LARK_STATUS_MAP = {
  ok: "正常",
  error: "异常",
  idle: "空闲",
  disabled: "未启用",
};

const CONNECTION_STATUS_MAP = {
  Live: "已连接",
  Error: "连接异常",
  Connecting: "连接中",
};

const elements = {
  connection: document.getElementById("connection-pill"),
  startedAt: document.getElementById("started-at"),
  symbols: document.getElementById("metric-symbols"),
  alerts: document.getElementById("metric-opportunities"),
  lark: document.getElementById("metric-lark"),
  subscribers: document.getElementById("metric-subscribers"),
  marketTabs: document.getElementById("market-tabs"),
  spreadBody: document.getElementById("spread-body"),
  spreadEmpty: document.getElementById("spread-empty"),
  opportunities: document.getElementById("opportunities"),
};

let currentMarketFilter = "all";
let latestState = null;

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatCompact(value, digits = 3) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits });
}

function formatSig(value, sigFigs = 4) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  const n = Number(value);
  if (n === 0) return "0";
  const abs = Math.abs(n);
  if (abs >= 1) {
    const intDigits = Math.floor(Math.log10(abs)) + 1;
    const decimals = Math.max(0, sigFigs - intDigits);
    return n.toFixed(decimals);
  }
  // For values < 1, count leading zeros after decimal point
  const leadingZeros = -Math.floor(Math.log10(abs)) - 1;
  return n.toFixed(leadingZeros + sigFigs);
}

function baseFromSymbol(symbol) {
  return symbol.split("-")[0];
}

function renderMarketTabs(marketTypes) {
  const existing = elements.marketTabs.querySelectorAll("[data-market]");
  const existingKeys = new Set();
  existing.forEach((btn) => existingKeys.add(btn.dataset.market));

  (marketTypes || []).forEach((mt) => {
    if (!existingKeys.has(mt)) {
      const btn = document.createElement("button");
      btn.className = "tab-btn";
      btn.dataset.market = mt;
      btn.textContent = MARKET_LABELS[mt] || mt;
      btn.addEventListener("click", () => {
        elements.marketTabs.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        currentMarketFilter = mt;
        renderSpreads(latestState?.topSpreads);
        renderOpportunities(latestState?.recentOpportunities);
      });
      elements.marketTabs.appendChild(btn);
    }
  });

  // "all" tab handler
  const allBtn = elements.marketTabs.querySelector('[data-market="all"]');
  if (allBtn && !allBtn._bound) {
    allBtn._bound = true;
    allBtn.addEventListener("click", () => {
      elements.marketTabs.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
      allBtn.classList.add("active");
      currentMarketFilter = "all";
      renderSpreads(latestState?.topSpreads);
      renderOpportunities(latestState?.recentOpportunities);
    });
  }
}

function renderSpreads(spreads) {
  const filtered =
    currentMarketFilter === "all"
      ? spreads || []
      : (spreads || []).filter((s) => s.marketType === currentMarketFilter);

  elements.spreadBody.innerHTML = "";

  if (!filtered.length) {
    elements.spreadEmpty.style.display = "block";
    return;
  }
  elements.spreadEmpty.style.display = "none";

  filtered.forEach((s) => {
    const tr = document.createElement("tr");
    const base = baseFromSymbol(s.symbol);
    tr.innerHTML = `
      <td><strong>${s.symbol}</strong></td>
      <td>${MARKET_LABELS[s.marketType] || s.marketType}</td>
      <td>买入 ${s.buyExchange.toUpperCase()} / 卖出 ${s.sellExchange.toUpperCase()}</td>
      <td class="${s.netBps >= 0 ? "positive" : "negative"}">${formatSig(s.netBps)}</td>
      <td>${formatSig(s.grossSpread)}</td>
      <td>${formatSig(s.buyPrice)}</td>
      <td>${formatSig(s.sellPrice)}</td>
      <td>${formatSig(s.executableSize)} ${base}</td>
      <td>${formatSig(s.notional)}</td>
      <td class="${s.netBps >= 0 ? "positive" : "negative"}">${formatSig(s.netBps / 10000 * 100)} USDT</td>
    `;
    elements.spreadBody.appendChild(tr);
  });
}

function renderOpportunities(opportunities) {
  elements.opportunities.innerHTML = "";
  const filtered =
    currentMarketFilter === "all"
      ? opportunities || []
      : (opportunities || []).filter((o) => o.market_type === currentMarketFilter);

  if (!filtered.length) {
    elements.opportunities.innerHTML = '<div class="empty-state">暂无满足筛选条件的套利机会</div>';
    return;
  }

  filtered.forEach((item) => {
    const base = baseFromSymbol(item.symbol);
    const card = document.createElement("article");
    card.className = "opportunity-card";

    const badgeClass = item.net_bps >= 0 ? "positive" : "negative";
    const marketLabel = MARKET_LABELS[item.market_type] || item.market_type;

    card.innerHTML = `
      <div class="opportunity-head">
        <div>
          <h3>买入 ${item.buy_exchange.toUpperCase()} → 卖出 ${item.sell_exchange.toUpperCase()}</h3>
          <p class="opportunity-time">${item.observed_at} · ${item.symbol} · ${marketLabel}</p>
        </div>
        <span class="opportunity-badge ${badgeClass}">${formatSig(item.net_bps)} bps</span>
      </div>
      <div class="opportunity-stats">
        <div class="stat-chip"><span>买价</span><strong>${formatSig(item.buy_price)} USDT</strong></div>
        <div class="stat-chip"><span>卖价</span><strong>${formatSig(item.sell_price)} USDT</strong></div>
        <div class="stat-chip"><span>毛价差</span><strong>${formatSig(item.gross_spread)} USDT</strong></div>
        <div class="stat-chip"><span>可成交量</span><strong>${formatSig(item.executable_size)} ${base}</strong></div>
        <div class="stat-chip"><span>手续费</span><strong>${formatSig(item.fee_bps)} bps</strong></div>
        <div class="stat-chip"><span>100U利润</span><strong>${formatSig(item.net_bps / 10000 * 100)} USDT</strong></div>
      </div>
    `;
    elements.opportunities.appendChild(card);
  });
}

function renderState(state) {
  latestState = state;

  elements.startedAt.textContent = `启动于 ${state.startedAt || "-"}`;
  elements.symbols.textContent = state.stats?.activeSymbols ?? 0;
  elements.alerts.textContent = state.stats?.totalOpportunities ?? 0;
  elements.subscribers.textContent = state.stats?.subscriberCount ?? 0;

  const rawLarkStatus = state.delivery?.lark?.lastStatus || "disabled";
  elements.lark.textContent = LARK_STATUS_MAP[rawLarkStatus] || rawLarkStatus;

  renderMarketTabs(state.marketTypes);
  renderSpreads(state.topSpreads);
  renderOpportunities(state.recentOpportunities);
}

function setConnectionStatus(status, detail = "") {
  elements.connection.className = "status-pill";
  const label = CONNECTION_STATUS_MAP[status] || status;
  elements.connection.textContent = detail ? `${label}: ${detail}` : label;
  if (status === "Live") {
    elements.connection.classList.add("ok");
  } else if (status === "Error") {
    elements.connection.classList.add("error");
  }
}

async function bootstrap() {
  try {
    const response = await fetch("/api/state");
    const state = await response.json();
    renderState(state);
    setConnectionStatus("Live", "HTTP");
  } catch (error) {
    setConnectionStatus("Error", "HTTP");
    console.error(error);
  }

  const stream = new EventSource("/api/events");
  stream.addEventListener("snapshot", (event) => {
    renderState(JSON.parse(event.data));
    setConnectionStatus("Live", "SSE");
  });
  stream.onerror = () => {
    setConnectionStatus("Error", "SSE 重连中");
  };
}

bootstrap();
