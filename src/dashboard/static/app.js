const quoteTemplate = document.getElementById("quote-template");
const spreadTemplate = document.getElementById("spread-template");
const opportunityTemplate = document.getElementById("opportunity-template");

const elements = {
  connection: document.getElementById("connection-pill"),
  symbol: document.getElementById("symbol-label"),
  startedAt: document.getElementById("started-at"),
  alerts: document.getElementById("metric-opportunities"),
  subscribers: document.getElementById("metric-subscribers"),
  lark: document.getElementById("metric-lark"),
  threshold: document.getElementById("metric-threshold"),
  quotes: document.getElementById("quotes"),
  spreads: document.getElementById("spreads"),
  opportunities: document.getElementById("opportunities"),
};

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatCompact(value, digits = 3) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
  });
}

function renderEmpty(container, text) {
  container.innerHTML = `<div class="empty-state">${text}</div>`;
}

function renderQuotes(quotes) {
  const entries = Object.values(quotes || {});
  elements.quotes.innerHTML = "";
  if (!entries.length) {
    renderEmpty(elements.quotes, "Waiting for quotes...");
    return;
  }

  entries.forEach((quote) => {
    const node = quoteTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector("h3").textContent = quote.exchange.toUpperCase();
    node.querySelector(".quote-time").textContent = quote.updatedAt || "-";
    node.querySelector(".quote-bid").textContent = formatNumber(quote.bidPrice, 2);
    node.querySelector(".quote-bid-size").textContent = `${formatCompact(quote.bidSize, 6)} BTC`;
    node.querySelector(".quote-ask").textContent = formatNumber(quote.askPrice, 2);
    node.querySelector(".quote-ask-size").textContent = `${formatCompact(quote.askSize, 6)} BTC`;
    elements.quotes.appendChild(node);
  });
}

function renderSpreads(spreads) {
  elements.spreads.innerHTML = "";
  if (!spreads || !spreads.length) {
    renderEmpty(elements.spreads, "Waiting for both exchanges...");
    return;
  }

  spreads.forEach((spread) => {
    const node = spreadTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector("h3").textContent = `Buy ${spread.buyExchange.toUpperCase()} / Sell ${spread.sellExchange.toUpperCase()}`;
    const badge = node.querySelector(".spread-badge");
    badge.textContent = spread.meetsThreshold ? "Threshold hit" : "Below threshold";
    badge.classList.add(spread.meetsThreshold ? "positive" : "negative");
    const net = node.querySelector(".spread-net");
    net.textContent = `${formatCompact(spread.netBps, 3)} bps`;
    net.classList.add(spread.netBps >= 0 ? "positive" : "negative");
    node.querySelector(".spread-gross").textContent = `${formatNumber(spread.grossSpread, 2)} USDT`;
    node.querySelector(".spread-size").textContent = `${formatCompact(spread.executableSize, 6)} BTC`;
    elements.spreads.appendChild(node);
  });
}

function renderOpportunities(opportunities) {
  elements.opportunities.innerHTML = "";
  if (!opportunities || !opportunities.length) {
    renderEmpty(elements.opportunities, "No opportunity has passed the current filter yet.");
    return;
  }

  opportunities.forEach((item) => {
    const node = opportunityTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector("h3").textContent = `${item.buy_exchange.toUpperCase()} -> ${item.sell_exchange.toUpperCase()}`;
    node.querySelector(".opportunity-time").textContent = item.observed_at;
    const badge = node.querySelector(".opportunity-badge");
    badge.textContent = `${formatCompact(item.net_bps, 3)} bps`;
    badge.classList.add(item.net_bps >= 0 ? "positive" : "negative");

    const stats = [
      ["Buy", `${formatNumber(item.buy_price, 2)} USDT`],
      ["Sell", `${formatNumber(item.sell_price, 2)} USDT`],
      ["Gross spread", `${formatNumber(item.gross_spread, 2)} USDT`],
      ["Executable size", `${formatCompact(item.executable_size, 6)} BTC`],
      ["Fees", `${formatCompact(item.fee_bps, 2)} bps`],
      ["Symbol", item.symbol],
    ];

    const statsNode = node.querySelector(".opportunity-stats");
    stats.forEach(([label, value]) => {
      const chip = document.createElement("div");
      chip.className = "stat-chip";
      chip.innerHTML = `<span>${label}</span><strong>${value}</strong>`;
      statsNode.appendChild(chip);
    });

    elements.opportunities.appendChild(node);
  });
}

function renderState(state) {
  elements.symbol.textContent = state.symbol || "-";
  elements.startedAt.textContent = `Started ${state.startedAt || "-"}`;
  elements.alerts.textContent = state.stats?.totalOpportunities ?? 0;
  elements.subscribers.textContent = state.stats?.subscriberCount ?? 0;
  elements.lark.textContent = state.delivery?.lark?.lastStatus || "disabled";
  elements.threshold.textContent = `${formatCompact(state.config?.minNetBps ?? 0, 2)} bps / ${formatCompact(state.config?.minSize ?? 0, 6)} BTC`;

  renderQuotes(state.quotes);
  renderSpreads(state.currentSpreads);
  renderOpportunities(state.recentOpportunities);
}

function setConnectionStatus(status, detail = "") {
  elements.connection.textContent = detail ? `${status}: ${detail}` : status;
  elements.connection.className = "status-pill";
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
    setConnectionStatus("Error", "SSE reconnecting");
  };
}

bootstrap();
