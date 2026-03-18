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
  moversBody: document.getElementById("movers-body"),
  moversEmpty: document.getElementById("movers-empty"),
  oiBody: document.getElementById("oi-body"),
  oiEmpty: document.getElementById("oi-empty"),
  oiDailyChart: document.getElementById("oi-daily-chart"),
  oiExchangeTabs: document.getElementById("oi-exchange-tabs"),
  etfBtcChart: document.getElementById("etf-btc-chart"),
  etfEthChart: document.getElementById("etf-eth-chart"),
  fundingBody: document.getElementById("funding-body"),
  fundingEmpty: document.getElementById("funding-empty"),
  opportunities: document.getElementById("opportunities"),
};

let currentMarketFilter = "all";
let latestState = null;
let oiDailyChartInstance = null;
let oiExchangeFilter = "all";
const etfBtcHolder = { chart: null };
const etfEthHolder = { chart: null };

const CHART_COLORS = [
  "#5eb0ff", "#2dd4a3", "#ff6b7a", "#ffd666", "#b388ff",
  "#4fc3f7", "#81c784", "#ff8a65", "#ce93d8", "#90a4ae",
  "#64ffda", "#ffab91", "#80cbc4", "#ef9a9a", "#a5d6a7",
  "#fff59d", "#f48fb1", "#80deea", "#c5e1a5", "#bcaaa4",
];

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

function formatUSD(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  const n = Number(value);
  if (n >= 1e8) return "$" + (n / 1e8).toFixed(2) + "亿";
  if (n >= 1e4) return "$" + (n / 1e4).toFixed(2) + "万";
  return "$" + n.toFixed(2);
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
        renderOpenInterest(latestState?.openInterest);
        renderMovers(latestState?.priceMovers);
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
      renderOpenInterest(latestState?.openInterest);
      renderMovers(latestState?.priceMovers);
      renderSpreads(latestState?.topSpreads);
      renderOpportunities(latestState?.recentOpportunities);
    });
  }
}

function renderOpenInterest(data) {
  const filtered =
    currentMarketFilter === "all"
      ? data || []
      : (data || []).filter((d) => d.marketType === currentMarketFilter);

  elements.oiBody.innerHTML = "";

  if (!filtered.length) {
    elements.oiEmpty.style.display = "block";
    return;
  }
  elements.oiEmpty.style.display = "none";

  filtered.forEach((item) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><strong>${item.symbol}</strong></td>
      <td>${MARKET_LABELS[item.marketType] || item.marketType}</td>
      <td>${formatUSD(item.binanceOI)}</td>
      <td>${formatUSD(item.okxOI)}</td>
      <td><strong>${formatUSD(item.totalOI)}</strong></td>
    `;
    elements.oiBody.appendChild(tr);
  });

  renderOIDailyChart(filtered);
}

function renderOIDailyChart(data) {
  const withDaily = (data || []).filter((d) => d.dailyHistory && d.dailyHistory.length >= 2);
  if (!withDaily.length) return;

  // Collect all unique dates and build aligned data
  const allDatesSet = new Set();
  withDaily.forEach((item) => {
    item.dailyHistory.forEach((h) => {
      // Normalize to date string (YYYY-MM-DD)
      const d = typeof h.t === "string" ? h.t.slice(0, 10) : new Date(h.t).toISOString().slice(0, 10);
      allDatesSet.add(d);
    });
  });
  const labels = Array.from(allDatesSet).sort();

  const valueKey = oiExchangeFilter === "binance" ? "bn" : oiExchangeFilter === "okx" ? "okx" : "v";

  const datasets = withDaily.map((item, i) => {
    const color = CHART_COLORS[i % CHART_COLORS.length];
    // Build a map of date -> value for this symbol
    const dateMap = {};
    item.dailyHistory.forEach((h) => {
      const d = typeof h.t === "string" ? h.t.slice(0, 10) : new Date(h.t).toISOString().slice(0, 10);
      dateMap[d] = h[valueKey] !== undefined ? h[valueKey] : h.v;
    });
    return {
      label: item.symbol.replace("-USDT-SWAP", "").replace("-USD-SWAP", ""),
      data: labels.map((d) => dateMap[d] || 0),
      backgroundColor: color + "CC",
      borderColor: color,
      borderWidth: 1,
      borderSkipped: false,
    };
  });

  if (oiDailyChartInstance) {
    oiDailyChartInstance.data.labels = labels;
    oiDailyChartInstance.data.datasets = datasets;
    oiDailyChartInstance.update("none");
    return;
  }

  oiDailyChartInstance = new Chart(elements.oiDailyChart, {
    type: "bar",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          position: "top",
          labels: {
            color: "#94a3b8",
            font: { size: 11 },
            boxWidth: 12,
            padding: 8,
          },
        },
        tooltip: {
          callbacks: {
            title: (items) => items[0]?.label || "",
            label: (ctx) => `${ctx.dataset.label}: ${formatUSD(ctx.parsed.y)}`,
            footer: (items) => {
              const total = items.reduce((sum, item) => sum + item.parsed.y, 0);
              return `合计: ${formatUSD(total)}`;
            },
          },
        },
      },
      scales: {
        x: {
          stacked: true,
          ticks: { color: "#64748b", maxTicksLimit: 15 },
          grid: { color: "rgba(100,116,139,0.15)" },
        },
        y: {
          stacked: true,
          ticks: {
            color: "#64748b",
            callback: (v) => formatUSD(v),
          },
          grid: { color: "rgba(100,116,139,0.15)" },
        },
      },
    },
  });
}

function renderETFChart(records, canvasEl, holder) {
  if (!records || !records.length) return;

  const labels = records.map((r) => r.date);
  const values = records.map((r) => r.totalNetInflow);
  const colors = values.map((v) => (v >= 0 ? "rgba(45,212,163,0.8)" : "rgba(255,107,122,0.8)"));
  const borders = values.map((v) => (v >= 0 ? "#2dd4a3" : "#ff6b7a"));

  const dataset = {
    label: "Daily Net Inflow",
    data: values,
    backgroundColor: colors,
    borderColor: borders,
    borderWidth: 1,
    borderSkipped: false,
  };

  if (holder.chart) {
    holder.chart.data.labels = labels;
    holder.chart.data.datasets = [dataset];
    holder.chart.update("none");
    return;
  }

  holder.chart = new Chart(canvasEl, {
    type: "bar",
    data: { labels, datasets: [dataset] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => items[0]?.label || "",
            label: (ctx) => {
              const v = ctx.parsed.y;
              const sign = v >= 0 ? "+" : "";
              return `净流入: ${sign}${formatUSD(Math.abs(v))}`;
            },
          },
        },
      },
      scales: {
        x: {
          ticks: { color: "#64748b", maxTicksLimit: 15 },
          grid: { color: "rgba(100,116,139,0.15)" },
        },
        y: {
          ticks: {
            color: "#64748b",
            callback: (v) => {
              const sign = v >= 0 ? "" : "-";
              return sign + formatUSD(Math.abs(v));
            },
          },
          grid: { color: "rgba(100,116,139,0.15)" },
        },
      },
    },
  });
}

function fmtRate(rate) {
  if (rate === null || rate === undefined) return "-";
  const pct = rate * 100;
  const sign = pct >= 0 ? "+" : "";
  return `${sign}${pct.toFixed(4)}%`;
}

function renderFundingRates(data) {
  elements.fundingBody.innerHTML = "";
  if (!data || !data.length) {
    elements.fundingEmpty.style.display = "block";
    return;
  }
  elements.fundingEmpty.style.display = "none";

  data.forEach((item) => {
    const bnRate = item.binanceRate;
    const okxRate = item.okxRate;
    const spread = item.spread;
    const annual = item.annualizedSpread;
    const nextMs = item.nextFundingMs;

    const bnCls = bnRate === null ? "" : bnRate > 0 ? "positive-rate" : bnRate < 0 ? "negative-rate" : "";
    const okxCls = okxRate === null ? "" : okxRate > 0 ? "positive-rate" : okxRate < 0 ? "negative-rate" : "";

    // Highlight row if annualized spread > 20%
    const rowCls = annual !== null && annual > 0.20 ? "arb-highlight" : "";

    // Format next funding countdown
    let nextStr = "-";
    if (nextMs) {
      const diff = Math.max(0, nextMs - Date.now());
      const h = Math.floor(diff / 3600000);
      const m = Math.floor((diff % 3600000) / 60000);
      nextStr = `${h}h ${m}m`;
    }

    const spreadCls = spread === null ? "" : Math.abs(spread) > 0.0005 ? "arb-spread" : "";

    const tr = document.createElement("tr");
    if (rowCls) tr.classList.add(rowCls);
    tr.innerHTML = `
      <td><strong>${item.symbol}</strong></td>
      <td class="${bnCls}">${fmtRate(bnRate)}</td>
      <td class="${okxCls}">${fmtRate(okxRate)}</td>
      <td class="${spreadCls}">${spread !== null ? fmtRate(spread) : "-"}</td>
      <td class="${spreadCls}">${annual !== null ? (annual * 100).toFixed(1) + "%" : "-"}</td>
      <td>${nextStr}</td>
    `;
    elements.fundingBody.appendChild(tr);
  });
}

function renderMovers(movers) {
  const filtered =
    currentMarketFilter === "all"
      ? movers || []
      : (movers || []).filter((m) => m.marketType === currentMarketFilter);

  elements.moversBody.innerHTML = "";

  if (!filtered.length) {
    elements.moversEmpty.style.display = "block";
    return;
  }
  elements.moversEmpty.style.display = "none";

  filtered.forEach((m) => {
    const tr = document.createElement("tr");
    const sign = m.changePct >= 0 ? "+" : "";
    const cls = m.changePct >= 0 ? "positive" : "negative";
    tr.innerHTML = `
      <td><strong>${m.symbol}</strong></td>
      <td>${MARKET_LABELS[m.marketType] || m.marketType}</td>
      <td>${formatSig(m.price)}</td>
      <td class="${cls}"><strong>${sign}${m.changePct.toFixed(2)}%</strong></td>
    `;
    elements.moversBody.appendChild(tr);
  });
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
  renderOpenInterest(state.openInterest);
  renderETFChart(state.etfHistory?.["us-btc-spot"], elements.etfBtcChart, etfBtcHolder);
  renderETFChart(state.etfHistory?.["us-eth-spot"], elements.etfEthChart, etfEthHolder);
  renderFundingRates(state.fundingRates);
  renderMovers(state.priceMovers);
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

// OI exchange filter tabs
elements.oiExchangeTabs.querySelectorAll("[data-exchange]").forEach((btn) => {
  btn.addEventListener("click", () => {
    elements.oiExchangeTabs.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    oiExchangeFilter = btn.dataset.exchange;
    renderOpenInterest(latestState?.openInterest);
  });
});

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
