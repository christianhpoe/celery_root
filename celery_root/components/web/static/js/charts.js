// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

// Chart rendering helpers.
(function () {
  const palette = ["#4C6FFF", "#4CC9F0", "#F7C59F", "#F28482", "#84A59D", "#F6BD60"];

  function runOrQueue(callback) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", callback, { once: true });
    } else {
      callback();
    }
  }

  function getCanvas(canvasId) {
    return document.getElementById(canvasId);
  }

  function drawThroughputChart(canvasId, series) {
    const safeSeries = Array.isArray(series) ? series : [];
    runOrQueue(() => {
      const canvas = getCanvas(canvasId);
      if (!canvas || typeof window.Chart !== "function") {
        return;
      }
      const ctx = canvas.getContext("2d");
      const labels = safeSeries.map((point) => point.label);
      const data = safeSeries.map((point) => point.count);
      new Chart(ctx, {
        type: "line",
        data: {
          labels,
          datasets: [
            {
              label: "Tasks",
              data,
              borderColor: "#4C6FFF",
              borderWidth: 2,
              tension: 0.25,
              fill: false,
            },
          ],
        },
        options: {
          maintainAspectRatio: false,
          responsive: true,
          scales: {
            x: {
              grid: {
                display: false,
              },
            },
            y: {
              beginAtZero: true,
            },
          },
          plugins: {
            legend: {
              display: false,
            },
          },
        },
      });
    });
  }

  function drawStateDonut(canvasId, series) {
    const safeSeries = Array.isArray(series) ? series : [];
    runOrQueue(() => {
      const canvas = getCanvas(canvasId);
      if (!canvas || typeof window.Chart !== "function") {
        return;
      }
      const ctx = canvas.getContext("2d");
      const labels = safeSeries.map((state) => state.label);
      const data = safeSeries.map((state) => state.count);
      const colors = safeSeries.map((_, index) => palette[index % palette.length]);
      new Chart(ctx, {
        type: "doughnut",
        data: {
          labels,
          datasets: [
            {
              data,
              backgroundColor: colors,
              borderWidth: 0,
            },
          ],
        },
        options: {
          maintainAspectRatio: false,
          plugins: {
            legend: {
              position: "bottom",
            },
          },
        },
      });
    });
  }

  window.CeleryCharts = {
    drawThroughputChart,
    drawStateDonut,
  };
})();
