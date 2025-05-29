/**
 * Wealth Timeline Chart
 * Simple Chart.js implementation for total wealth evolution
 */

class WealthTimelineChart {
  constructor(canvasId, chartData) {
    this.canvasId = canvasId;
    this.chartData = chartData;
    this.chart = null;

    // Wait for Chart.js to be available
    if (typeof Chart !== "undefined") {
      this.init();
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

    // Debug: Check the data format
    console.log("Chart data structure:", this.chartData);
    console.log("Data points:", this.chartData.data.length);

    // Convert string dates to Date objects
    const processedData = this.chartData.data.map((point) => ({
      x: new Date(point.x),
      y: point.y,
    }));

    // Chart.js configuration
    const config = {
      type: "line",
      data: {
        datasets: [
          {
            label: "Total Billionaire Wealth",
            data: processedData,
            borderColor: "#e74c3c",
            backgroundColor: "rgba(231, 76, 60, 0.1)",
            borderWidth: 2,
            fill: true,
            tension: 0.1,
            pointRadius: 0,
            pointHoverRadius: 6,
            pointHoverBackgroundColor: "#e74c3c",
            pointHoverBorderColor: "#ffffff",
            pointHoverBorderWidth: 2,
          },
        ],
      },
      options: this.getChartOptions(),
    };

    console.log(
      "Chart config data sample:",
      config.data.datasets[0].data.slice(0, 3),
    );

    try {
      this.chart = new Chart(ctx, config);
      this.updateInfo();
      console.log("✅ Chart created successfully");
    } catch (error) {
      console.error("❌ Error creating chart:", error);
      throw error;
    }
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
          },
        },
        tooltip: {
          backgroundColor: "rgba(45, 45, 45, 0.95)",
          titleColor: "#f8f9fa",
          bodyColor: "#adb5bd",
          borderColor: "#3a3a3a",
          borderWidth: 1,
          cornerRadius: 4,
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
              const timestamp = context[0].parsed.x;
              const date = new Date(timestamp);
              return date.toLocaleDateString("en-US", {
                year: "numeric",
                month: "long",
                day: "numeric",
              });
            },
            label: (context) => {
              const value = context.parsed.y;
              return `Total Wealth: $${value.toFixed(1)} trillion`;
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
            color: "rgba(255, 255, 255, 0.1)",
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
            color: "rgba(255, 255, 255, 0.1)",
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

  updateInfo() {
    // Update chart description if info area exists
    const infoArea = document.querySelector(".chart-info");
    if (infoArea && this.chartData.summary) {
      const summary = this.chartData.summary;
      infoArea.innerHTML = `
                <strong>${summary.dataPoints} data points</strong> from ${summary.timespan}<br>
                Growth: $${summary.startValue.toFixed(1)}T → $${summary.endValue.toFixed(1)}T 
                (+${summary.totalIncrease.toFixed(1)}%)
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

// Global function to initialize wealth timeline chart
window.initWealthTimelineChart = function (canvasId, chartData) {
  return new WealthTimelineChart(canvasId, chartData);
};

// Auto-initialize if chart data is available
document.addEventListener("DOMContentLoaded", function () {
  // Small delay to ensure Chart.js is fully loaded
  setTimeout(function () {
    if (
      typeof window.wealthTimelineData !== "undefined" &&
      typeof Chart !== "undefined"
    ) {
      console.log("Initializing wealth timeline chart...");
      try {
        window.wealthChart = window.initWealthTimelineChart(
          "wealth-timeline-chart",
          window.wealthTimelineData,
        );
        console.log("✅ Wealth timeline chart initialized successfully");
      } catch (error) {
        console.error("❌ Failed to initialize wealth timeline chart:", error);
      }
    } else {
      if (typeof Chart === "undefined") {
        console.error("❌ Chart.js is not loaded");
      }
      if (typeof window.wealthTimelineData === "undefined") {
        console.error("❌ Chart data is not available");
      }
    }
  }, 100);
});
