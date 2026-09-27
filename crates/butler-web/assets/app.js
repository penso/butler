// butler dashboard: live counts over SSE, uPlot charts, small page helpers.
// Plain script, no build step: loaded after uplot.min.js on every page.
(function () {
  "use strict";

  var base = document.body.dataset.base || "";
  var LIVE_POINTS = 300; // 5 minutes at one snapshot per second

  function css(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function withAlpha(color, alpha) {
    var hex = color.replace("#", "");
    if (hex.length === 6) {
      var n = parseInt(hex, 16);
      return "rgba(" + (n >> 16) + "," + ((n >> 8) & 255) + "," + (n & 255) + "," + alpha + ")";
    }
    return color;
  }

  function formatCount(n) {
    return Math.round(n).toLocaleString("en-US");
  }

  function formatRate(n) {
    return n >= 100 ? formatCount(n) : n.toFixed(1);
  }

  function span(s) {
    if (s < 60) return s + "s";
    if (s < 3600) return Math.floor(s / 60) + "m";
    if (s < 86400) return Math.floor(s / 3600) + "h";
    return Math.floor(s / 86400) + "d";
  }

  function ago(ms) {
    return span(Math.max(0, Math.floor((Date.now() - ms) / 1000))) + " ago";
  }

  function until(ms) {
    var s = Math.floor((ms - Date.now()) / 1000);
    return s > 0 ? "in " + span(s) : "due now";
  }

  // Theme: dark by default, remembered per browser.
  var themeButton = document.querySelector("[data-theme-toggle]");
  if (themeButton) {
    themeButton.addEventListener("click", function () {
      var next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      document.documentElement.dataset.theme = next;
      try {
        localStorage.setItem("butler-theme", next);
      } catch (e) {}
      window.dispatchEvent(new Event("butler:theme"));
    });
  }

  // Destructive actions ask first.
  document.querySelectorAll("form[data-confirm]").forEach(function (form) {
    form.addEventListener("submit", function (event) {
      if (!window.confirm(form.dataset.confirm)) event.preventDefault();
    });
  });

  // Relative times stay current.
  function refreshTimes() {
    document.querySelectorAll("[data-ms]").forEach(function (el) {
      el.textContent = ago(Number(el.dataset.ms));
    });
    document.querySelectorAll("[data-until-ms]").forEach(function (el) {
      el.textContent = until(Number(el.dataset.untilMs));
    });
  }
  setInterval(refreshTimes, 5000);

  // Exact times, in the viewer's zone, on hover.
  document.querySelectorAll("[data-local-ms]").forEach(function (el) {
    el.title = new Date(Number(el.dataset.localMs)).toLocaleString();
  });

  // Charts -----------------------------------------------------------------

  function makeChart(el, series, data) {
    var muted = css("--muted");
    var grid = css("--grid");
    var axis = {
      stroke: muted,
      grid: { stroke: grid, width: 1 },
      ticks: { stroke: grid, width: 1 },
      font: "11px " + css("--font-sans"),
    };
    var chart = new uPlot(
      {
        width: el.clientWidth,
        height: 220,
        padding: [10, 10, 0, 0],
        cursor: { points: { size: 7 } },
        scales: { x: { time: true }, y: { range: function (u, min, max) { return [0, Math.max(1, max * 1.15)]; } } },
        axes: [axis, Object.assign({}, axis, { size: 52 })],
        series: [{}].concat(
          series.map(function (s) {
            var color = css(s.color);
            return {
              label: s.label,
              stroke: color,
              width: 2,
              fill: s.fill ? withAlpha(color, 0.14) : undefined,
              points: { show: false },
              value: s.value,
            };
          })
        ),
      },
      data,
      el
    );
    new ResizeObserver(function () {
      chart.setSize({ width: el.clientWidth, height: 220 });
    }).observe(el);
    return chart;
  }

  // Live: one point per SSE snapshot, rates from counter deltas.
  var liveEl = document.getElementById("live-chart");
  var liveData = null;
  var liveChart = null;
  var last = null;

  function drawLive() {
    if (!liveEl) return;
    liveEl.replaceChildren();
    liveChart = makeChart(
      liveEl,
      [
        { label: "processed/s", color: "--accent", fill: true, value: function (u, v) { return v == null ? "-" : formatRate(v); } },
        { label: "failed/s", color: "--error", fill: true, value: function (u, v) { return v == null ? "-" : formatRate(v); } },
      ],
      liveData
    );
  }

  if (liveEl) {
    var now = Math.floor(Date.now() / 1000);
    var xs = [];
    var zeros = [];
    for (var i = LIVE_POINTS - 1; i >= 0; i--) {
      xs.push(now - i);
      zeros.push(0);
    }
    liveData = [xs, zeros.slice(), zeros.slice()];
    var initial = document.getElementById("initial-snapshot");
    if (initial) {
      try {
        last = JSON.parse(initial.textContent);
      } catch (e) {}
    }
    drawLive();
  }

  function pushLive(snapshot) {
    if (!liveData || !last || !last.ts_ms) return;
    var seconds = (snapshot.ts_ms - last.ts_ms) / 1000;
    if (seconds <= 0) return;
    var processed = Math.max(0, snapshot.processed_total - last.processed_total) / seconds;
    var failed = Math.max(0, snapshot.failed_total - last.failed_total) / seconds;
    liveData[0].push(Math.floor(snapshot.ts_ms / 1000));
    liveData[1].push(processed);
    liveData[2].push(failed);
    if (liveData[0].length > LIVE_POINTS) {
      liveData.forEach(function (column) {
        column.shift();
      });
    }
    liveChart.setData(liveData);
    document.querySelectorAll("[data-rate]").forEach(function (el) {
      el.textContent = formatRate(el.dataset.rate === "failed" ? failed : processed);
    });
  }

  // History and duration: per-minute series from the JSON API.
  var historyEl = document.getElementById("history-chart");
  var durationEl = document.getElementById("duration-chart");
  var range = 1440;
  var history = null;

  function drawHistory() {
    if (!history || !historyEl) return;
    var xs = history.minutes.map(function (m) {
      return m * 60;
    });
    historyEl.replaceChildren();
    makeChart(
      historyEl,
      [
        { label: "processed", color: "--accent", fill: true },
        { label: "failed", color: "--error", fill: true },
      ],
      [xs, history.processed, history.failed]
    );
    if (durationEl) {
      durationEl.replaceChildren();
      makeChart(
        durationEl,
        [
          { label: "avg ms", color: "--info", fill: true },
          { label: "max ms", color: "--warn" },
        ],
        [xs, history.avg_ms, history.max_ms]
      );
    }
  }

  function loadHistory() {
    if (!historyEl) return;
    fetch(base + "/api/metrics?minutes=" + range)
      .then(function (response) {
        return response.json();
      })
      .then(function (series) {
        history = series;
        drawHistory();
      })
      .catch(function () {});
  }

  document.querySelectorAll("[data-range]").forEach(function (button) {
    button.addEventListener("click", function () {
      range = Number(button.dataset.range);
      document.querySelectorAll("[data-range]").forEach(function (other) {
        other.classList.toggle("active", other === button);
      });
      loadHistory();
    });
  });

  if (historyEl) {
    loadHistory();
    setInterval(loadHistory, 60000);
  }

  window.addEventListener("butler:theme", function () {
    drawLive();
    drawHistory();
  });

  // Live counts over server-sent events --------------------------------------

  var dot = document.querySelector("[data-live-dot]");
  var label = document.querySelector("[data-live-label]");

  function setLive(state, text) {
    if (dot) dot.style.background = css(state === "live" ? "--accent" : state === "error" ? "--error" : "--warn");
    if (label) label.textContent = text;
  }

  function apply(snapshot) {
    ["processed_total", "failed_total", "processing", "pending", "scheduled", "dead", "workers_alive"].forEach(function (key) {
      document.querySelectorAll('[data-stat="' + key + '"]').forEach(function (el) {
        el.textContent = formatCount(snapshot[key] || 0);
      });
    });
    (snapshot.queues || []).forEach(function (entry) {
      document.querySelectorAll('[data-queue-pending="' + CSS.escape(entry[0]) + '"]').forEach(function (el) {
        el.textContent = formatCount(entry[1]);
      });
    });
  }

  if (window.EventSource) {
    var source = new EventSource(base + "/events");
    source.addEventListener("open", function () {
      setLive("live", "live");
    });
    source.addEventListener("error", function () {
      setLive("reconnecting", "reconnecting");
    });
    source.addEventListener("stats", function (event) {
      var snapshot = JSON.parse(event.data);
      if (!snapshot.ts_ms) return;
      if (snapshot.error) {
        setLive("error", "backend error");
      } else {
        setLive("live", "live");
      }
      apply(snapshot);
      pushLive(snapshot);
      last = snapshot;
    });
  }
})();
