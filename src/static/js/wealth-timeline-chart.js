/**
 * Wealth Timeline Chart
 * Enhanced with exponential trend line, animations, and inflation toggle
 */

class WealthTimelineChart {
  constructor(canvasId, chartData) {
    this.canvasId = canvasId;
    this.chartData = chartData;
    this.chart = null;
    this.showInflation = false;
    this.inflationType = "cpi_u";
    this.animationPlayed = false;
    this.isAnimating = false;

    if (typeof Chart !== "undefined") {
      this.init();
      this.setupScrollAnimation();
      this.setupControls();
    } else {
      console.error("Chart.js is not loaded");
    }
  }

  init() {
    const canvas = document.getElementById(this.canvasId);
    if (!canvas) {
      console.error(`Canvas with id '${this.canvasId}' not found`);
      return;
    }

    const ctx = canvas.getContext("2d");

    // Prepare datasets
    const datasets = this.prepareDatasets();

    // Chart configuration
    const config = {
      type: "line",
      data: { datasets },
      options: this.getChartOptions(),
    };

    this.chart = new Chart(ctx, config);
    this.updateInfo();
  }

  prepareDatasets() {
    const datasets = [];

    // Data points dataset
    const dataPoints = this.chartData.data.map((point) => ({
      x: new Date(point.x),
      y: point.y,
    }));

    datasets.push({
      label: "Total Wealth",
      data: dataPoints,
      borderColor: "#8b2635", // Dark red for line
      backgroundColor: "#8b2635", // Dark red for points
      borderWidth: 0,
      fill: false,
      pointRadius: 3,
      pointHoverRadius: 5,
      pointBorderWidth: 0,
      showLine: false, // Only show points
      order: 2,
      animation: false, // We'll handle animation ourselves
    });

    // Trend line dataset
    if (this.chartData.trendLine && this.chartData.trendLine.length > 0) {
      const trendData = this.chartData.trendLine.map((point) => ({
        x: new Date(point.x),
        y: point.y,
      }));

      datasets.push({
        label: "Exponential Trend",
        data: trendData,
        borderColor: "#e74c3c", // Vivid red for trend
        backgroundColor: "transparent",
        borderWidth: 3,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 0,
        tension: 0.4, // Smooth curve
        order: 1,
        animation: false,
      });
    }

    return datasets;
  }

  getChartOptions() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        intersect: false,
        mode: "index",
      },
      plugins: {
        legend: {
          display: true,
          position: "top",
          labels: {
            color: "#f8f9fa",
            font: {
              family: "Inter, sans-serif",
              size: 14,
              weight: "500",
            },
            usePointStyle: true,
            padding: 20,
          },
        },
        tooltip: {
          backgroundColor: "rgba(45, 45, 45, 0.95)",
          titleColor: "#f8f9fa",
          bodyColor: "#adb5bd",
          borderColor: "#3a3a3a",
          borderWidth: 1,
          cornerRadius: 4,
          padding: 12,
          titleFont: {
            family: "Inter, sans-serif",
            size: 14,
            weight: "600",
          },
          bodyFont: {
            family: "JetBrains Mono, monospace",
            size: 13,
          },
          callbacks: {
            title: (context) => {
              const date = new Date(context[0].parsed.x);
              return date.toLocaleDateString("en-US", {
                year: "numeric",
                month: "long",
                day: "numeric",
              });
            },
            label: (context) => {
              const value = context.parsed.y;
              const label = context.dataset.label;
              return `${label}: $${value.toFixed(1)} trillion`;
            },
          },
        },
      },
      scales: {
        x: {
          type: "time",
          time: {
            tooltipFormat: "MMM dd, yyyy",
            displayFormats: {
              day: "MMM dd",
              week: "MMM dd",
              month: "MMM yyyy",
              quarter: "MMM yyyy",
              year: "yyyy",
            },
          },
          grid: {
            color: "rgba(255, 255, 255, 0.05)",
            drawOnChartArea: true,
          },
          ticks: {
            color: "#adb5bd",
            font: {
              family: "Inter, sans-serif",
              size: 11,
            },
          },
        },
        y: {
          beginAtZero: false,
          grid: {
            color: "rgba(255, 255, 255, 0.05)",
            drawOnChartArea: true,
          },
          ticks: {
            color: "#adb5bd",
            font: {
              family: "JetBrains Mono, monospace",
              size: 11,
            },
            callback: (value) => {
              return `$${value.toFixed(1)}T`;
            },
          },
        },
      },
    };
  }

  setupScrollAnimation() {
    const chartContainer = document.querySelector(".chart-container");
    if (!chartContainer) return;

    const observer = new IntersectionObserver(
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
    );

    observer.observe(chartContainer);
  }

  animateChart() {
    if (!this.chart || this.isAnimating) return;

    this.isAnimating = true;
    const datasets = this.chart.data.datasets;
    const dataPoints = datasets[0].data;
    const trendLine = datasets[1] ? datasets[1].data : [];

    // Store original data
    const originalData = dataPoints.slice();
    const originalTrend = trendLine.slice();

    // Clear data initially
    datasets[0].data = [];
    if (datasets[1]) datasets[1].data = [];
    this.chart.update("none");

    // Animate data points appearing
    let pointIndex = 0;
    const pointInterval = setInterval(() => {
      if (pointIndex < originalData.length) {
        datasets[0].data.push(originalData[pointIndex]);
        this.chart.update("none");
        pointIndex++;
      } else {
        clearInterval(pointInterval);

        // Animate trend line after points
        if (datasets[1] && originalTrend.length > 0) {
          this.animateTrendLine(datasets[1], originalTrend);
        } else {
          this.isAnimating = false;
        }
      }
    }, this.chartData.animation?.pointDelay || 10);
  }

  animateTrendLine(trendDataset, trendData) {
    const duration = this.chartData.animation?.trendLineSpeed || 1500;
    const steps = 50;
    const stepDuration = duration / steps;
    let currentStep = 0;

    const interval = setInterval(() => {
      if (currentStep <= steps) {
        const progress = currentStep / steps;
        const pointCount = Math.floor(trendData.length * progress);
        trendDataset.data = trendData.slice(0, pointCount);
        this.chart.update("none");
        currentStep++;
      } else {
        clearInterval(interval);
        this.isAnimating = false;
      }
    }, stepDuration);
  }

  setupControls() {
    // Add control buttons after the chart
    const chartSection = document.querySelector(".chart-section");
    if (!chartSection) return;

    const controlsHtml = `
      <div class="chart-controls">
        <button class="chart-btn active" data-view="nominal">Nominal Values</button>
        <button class="chart-btn" data-view="inflation">Inflation Adjusted</button>
        <button class="chart-btn" data-action="replay">Replay Animation</button>
      </div>
    `;

    // Insert controls if not already present
    if (!chartSection.querySelector(".chart-controls")) {
      chartSection.insertAdjacentHTML("beforeend", controlsHtml);
    }

    // Add event listeners
    const controls = chartSection.querySelectorAll(".chart-btn");
    controls.forEach((btn) => {
      btn.addEventListener("click", (e) => this.handleControlClick(e));
    });
  }

  handleControlClick(e) {
    const btn = e.target;
    const view = btn.getAttribute("data-view");
    const action = btn.getAttribute("data-action");

    if (view) {
      // Toggle inflation view
      this.showInflation = view === "inflation";
      this.updateChartData();

      // Update button states
      document.querySelectorAll(".chart-btn[data-view]").forEach((b) => {
        b.classList.toggle("active", b === btn);
      });
    } else if (action === "replay") {
      // Replay animation
      this.animationPlayed = false;
      this.animateChart();
    }
  }

  updateChartData() {
    if (!this.chart) return;

    let dataToUse = this.chartData.data;

    // Use inflation-adjusted data if available and selected
    if (this.showInflation && this.chartData.inflationData) {
      dataToUse = this.chartData.inflationData.data;
      this.chart.options.scales.y.title = {
        display: true,
        text: `Wealth (Trillions USD, ${this.chartData.inflationData.inflationType} Adjusted)`,
        color: "#adb5bd",
      };
    } else {
      this.chart.options.scales.y.title = {
        display: true,
        text: "Wealth (Trillions USD)",
        color: "#adb5bd",
      };
    }

    // Update data points
    const processedData = dataToUse.map((point) => ({
      x: new Date(point.x),
      y: point.y,
    }));

    this.chart.data.datasets[0].data = processedData;

    // Note: Trend line remains the same (nominal) for simplicity
    // You could calculate a separate inflation-adjusted trend if needed

    this.chart.update();
  }

  updateInfo() {
    const infoArea = document.querySelector(".chart-info");
    if (infoArea && this.chartData.summary) {
      const summary = this.chartData.summary;
      infoArea.innerHTML = `
        <strong>${summary.dataPoints} data points</strong> from ${summary.timespan}<br>
        Growth: $${summary.startValue.toFixed(1)}T → $${summary.endValue.toFixed(1)}T 
        (+${summary.totalIncrease.toFixed(1)}%)<br>
        <span style="color: var(--red-light)">Exponential growth rate: ${summary.exponentialGrowthRate.toFixed(1)}% per year</span>
      `;
    }
  }

  destroy() {
    if (this.chart) {
      this.chart.destroy();
      this.chart = null;
    }
  }
}

// Global initialization
window.initWealthTimelineChart = function (canvasId, chartData) {
  return new WealthTimelineChart(canvasId, chartData);
};

// Auto-initialize
document.addEventListener("DOMContentLoaded", function () {
  setTimeout(function () {
    if (
      typeof window.wealthTimelineData !== "undefined" &&
      typeof Chart !== "undefined"
    ) {
      try {
        window.wealthChart = window.initWealthTimelineChart(
          "wealth-timeline-chart",
          window.wealthTimelineData,
        );
        console.log("✅ Enhanced wealth timeline chart initialized");
      } catch (error) {
        console.error("❌ Failed to initialize chart:", error);
      }
    }
  }, 100);
});
