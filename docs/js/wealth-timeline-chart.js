class WealthTimelineChart {
  constructor(canvasId, chartData) {
    this.canvasId = canvasId;
    this.chartData = chartData;
    this.chart = null;
    this.showInflation = false;
    this.originalDatasetConfigs = null;
    this.animationPlayed = false;
    this.isAnimating = false;

    if (typeof Chart !== "undefined") {
      this.init();
      this.setupScrollAnimation();
      this.setupControls();
    }
  }

  init() {
    const canvas = document.getElementById(this.canvasId);
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    const datasets = this.prepareDatasets();
    this.originalDatasetConfigs = datasets.map((dataset) => ({ ...dataset }));

    this.chart = new Chart(ctx, {
      type: "line",
      data: { datasets },
      options: this.getChartOptions(),
    });

    this.setupResizeHandler();
    this.updateInfo();
  }

  prepareDatasets() {
    const datasets = [];
    const isMobile = window.innerWidth <= 768;
    const isSmallMobile = window.innerWidth <= 480;

    const dataPoints = this.chartData.data.map((point) => ({
      x: new Date(point.x),
      y: point.y,
    }));

    datasets.push({
      label: "Total Wealth",
      data: dataPoints,
      borderColor: "#8b2635",
      backgroundColor: "#8b2635",
      borderWidth: 0,
      fill: false,
      pointRadius: isMobile ? (isSmallMobile ? 2 : 2.5) : 3,
      pointHoverRadius: isMobile ? (isSmallMobile ? 3 : 4) : 5,
      pointBorderWidth: 0,
      showLine: false,
      order: 2,
      animation: false,
    });

    if (this.chartData.trendLine?.length) {
      const trendData = this.chartData.trendLine.map((point) => ({
        x: new Date(point.x),
        y: point.y,
      }));

      datasets.push({
        label: "Exponential Trend",
        data: trendData,
        borderColor: "#e74c3c",
        backgroundColor: "transparent",
        borderWidth: isMobile ? (isSmallMobile ? 2 : 2.5) : 3,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 0,
        tension: 0.4,
        order: 1,
        animation: false,
      });
    }

    return datasets;
  }

  getChartOptions() {
    const isMobile = window.innerWidth <= 768;
    const isSmallMobile = window.innerWidth <= 480;

    return {
      responsive: true,
      maintainAspectRatio: false,
      devicePixelRatio: window.devicePixelRatio || 1,
      interaction: { intersect: false, mode: "index" },
      layout: {
        padding: {
          left: isMobile ? 5 : 10,
          right: isMobile ? 5 : 10,
          top: isMobile ? 5 : 10,
          bottom: isMobile ? 5 : 10,
        },
      },
      plugins: {
        legend: {
          display: true,
          position: isMobile ? "bottom" : "top",
          labels: {
            color: "#f8f9fa",
            font: {
              family: "Inter, sans-serif",
              size: isMobile ? (isSmallMobile ? 11 : 12) : 14,
            },
            boxWidth: isMobile ? 8 : 12,
          },
        },
        tooltip: {
          backgroundColor: "rgba(45, 45, 45, 0.95)",
          titleColor: "#f8f9fa",
          bodyColor: "#adb5bd",
          callbacks: {
            title: (context) =>
              new Date(context[0].parsed.x).toLocaleDateString("en-US", {
                year: "numeric",
                month: isMobile ? "short" : "long",
                day: "numeric",
              }),
            label: (context) => {
              const value = context.parsed.y;
              const label = context.dataset.label.split(" ")[0];
              return `${label}: $${value.toFixed(1)}T`;
            },
          },
        },
      },
      scales: {
        x: {
          type: "time",
          time: {
            tooltipFormat: isMobile ? "MMM dd, yy" : "MMM dd, yyyy",
            displayFormats: {
              day: "MMM dd",
              week: "MMM dd",
              month: isMobile ? "MMM yy" : "MMM yyyy",
              year: "yyyy",
            },
          },
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: {
            color: "#adb5bd",
            font: {
              family: "Inter, sans-serif",
              size: isMobile ? (isSmallMobile ? 9 : 10) : 11,
            },
            maxRotation: isMobile ? 45 : 0,
            maxTicksLimit: isMobile ? (isSmallMobile ? 4 : 6) : 8,
          },
        },
        y: {
          beginAtZero: false,
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: {
            color: "#adb5bd",
            font: {
              family: "JetBrains Mono, monospace",
              size: isMobile ? (isSmallMobile ? 9 : 10) : 11,
            },
            maxTicksLimit: isMobile ? (isSmallMobile ? 4 : 5) : 6,
            callback: (value) => `$${value.toFixed(1)}T`,
          },
        },
      },
    };
  }

  setupResizeHandler() {
    let resizeTimeout;
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimeout);
      resizeTimeout = setTimeout(() => {
        if (this.chart) {
          this.chart.options = this.getChartOptions();
          this.chart.update("none");
          this.updateInfo();
        }
      }, 250);
    });
  }

  setupScrollAnimation() {
    const chartContainer = document.querySelector(".chart-container");
    if (!chartContainer) return;

    new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (
            entry.isIntersecting &&
            !this.animationPlayed &&
            !this.isAnimating
          ) {
            this.animateChart();
            this.animationPlayed = true;
          }
        });
      },
      { threshold: 0.5 },
    ).observe(chartContainer);
  }

  animateChart() {
    if (!this.chart || this.isAnimating) return;
    this.isAnimating = true;

    const datasets = this.chart.data.datasets;
    const originalData = [...datasets[0].data];
    const originalTrend = datasets[1] ? [...datasets[1].data] : [];

    datasets[0].data = [];
    if (datasets[1]) datasets[1].data = [];
    this.chart.update("none");

    let pointIndex = 0;
    const pointInterval = setInterval(() => {
      if (pointIndex < originalData.length) {
        datasets[0].data.push(originalData[pointIndex]);
        this.chart.update("none");
        pointIndex++;
      } else {
        clearInterval(pointInterval);
        if (datasets[1] && originalTrend.length) {
          this.animateTrendLine(datasets[1], originalTrend);
        } else {
          this.isAnimating = false;
          this.updateInfo();
        }
      }
    }, 5);
  }

  animateTrendLine(trendDataset, trendData) {
    const duration = 800;
    const steps = 40;
    let currentStep = 0;

    const interval = setInterval(() => {
      if (currentStep <= steps) {
        const progress = currentStep / steps;
        trendDataset.data = trendData.slice(
          0,
          Math.floor(trendData.length * progress),
        );
        this.chart.update("none");
        currentStep++;
      } else {
        clearInterval(interval);
        this.isAnimating = false;
        this.updateInfo();
      }
    }, duration / steps);
  }

  setupControls() {
    const chartSection = document.querySelector(".chart-section");
    if (!chartSection) return;

    if (!chartSection.querySelector(".chart-controls")) {
      chartSection.insertAdjacentHTML(
        "beforeend",
        `
        <div class="chart-controls">
          <button class="chart-btn active" data-view="nominal">Nominal Values</button>
          <button class="chart-btn" data-view="inflation" ${!this.chartData.inflationData ? "disabled" : ""}>Inflation Adjusted</button>
        </div>
      `,
      );
    }

    chartSection.querySelectorAll(".chart-btn[data-view]").forEach((btn) => {
      btn.addEventListener("click", (e) => this.handleControlClick(e));
    });
  }

  handleControlClick(e) {
    const btn = e.target;
    const view = btn.getAttribute("data-view");
    if (!view || btn.disabled) return;

    const newShowInflation = view === "inflation";
    if (newShowInflation !== this.showInflation) {
      this.showInflation = newShowInflation;
      this.updateChartData();
      this.updateInfo();
    }

    document.querySelectorAll(".chart-btn[data-view]").forEach((b) => {
      b.classList.toggle("active", b === btn);
    });
  }

  updateChartData() {
    if (!this.chart || !this.originalDatasetConfigs) return;

    let dataToUse = this.chartData.data;
    let trendToUse = this.chartData.trendLine;
    let labelSuffix = "";

    if (this.showInflation && this.chartData.inflationData) {
      dataToUse = this.chartData.inflationData.data;
      labelSuffix = ` (${this.chartData.inflationData.inflationType} Adjusted)`;
      if (this.chartData.inflationTrendLine) {
        trendToUse = this.chartData.inflationTrendLine;
      }
    }

    const isMobile = window.innerWidth <= 768;
    const isSmallMobile = window.innerWidth <= 480;

    this.chart.data.datasets[0] = {
      ...this.originalDatasetConfigs[0],
      data: dataToUse.map((p) => ({ x: new Date(p.x), y: p.y })),
      label: "Total Wealth" + labelSuffix,
      pointRadius: isMobile ? (isSmallMobile ? 2 : 2.5) : 3,
    };

    if (this.originalDatasetConfigs[1] && this.chart.data.datasets[1]) {
      this.chart.data.datasets[1] = {
        ...this.originalDatasetConfigs[1],
        data: trendToUse.map((p) => ({ x: new Date(p.x), y: p.y })),
        label: "Exponential Trend" + labelSuffix,
        borderWidth: isMobile ? (isSmallMobile ? 2 : 2.5) : 3,
      };
    }

    this.chart.update("none");
  }

  updateInfo() {
    const infoArea = document.querySelector(".chart-info");
    if (!infoArea || !this.chartData?.summary) return;

    let summary = this.chartData.summary;
    let inflationNote = "";
    let growthRate = this.chartData.fitParams?.annualGrowthRate || 0;
    let rSquared = this.chartData.fitParams?.r_squared || 0;

    if (
      this.showInflation &&
      this.chartData.inflationData &&
      this.chartData.inflationSummary
    ) {
      inflationNote = ` (${this.chartData.inflationData.inflationType} adjusted)`;
      summary = this.chartData.inflationSummary;
      if (this.chartData.inflationFitParams) {
        growthRate = this.chartData.inflationFitParams.annualGrowthRate;
        rSquared = this.chartData.inflationFitParams.r_squared;
      }
    }

    infoArea.innerHTML = `
      <strong>${summary.dataPoints || "N/A"} data points</strong> from ${summary.timespan || "N/A"}<br>
      Growth${inflationNote}: $${summary.startValue?.toFixed(1) || "N/A"}T → $${summary.endValue?.toFixed(1) || "N/A"}T 
      (+${summary.totalIncrease?.toFixed(1) || "N/A"}%)<br>
      <span style="color: var(--red-light)">Exponential growth rate: ${growthRate?.toFixed(1) || "N/A"}% per year (R² = ${rSquared?.toFixed(3) || "N/A"})</span>
    `;
  }

  destroy() {
    if (this.chart) this.chart.destroy();
  }
}

// Auto-initialize
document.addEventListener("DOMContentLoaded", () => {
  if (
    typeof window.wealthTimelineData !== "undefined" &&
    typeof Chart !== "undefined"
  ) {
    window.wealthChart = new WealthTimelineChart(
      "wealth-timeline-chart",
      window.wealthTimelineData,
    );
  }
});
