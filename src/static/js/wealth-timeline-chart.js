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
    
    // Store original dataset configurations to prevent Chart.js from modifying them
    this.originalDatasetConfigs = null;

    // Debug chart data structure
    console.log("🔍 Chart data structure:");
    console.log("   inflationData:", !!chartData.inflationData);
    console.log("   inflationSummary:", !!chartData.inflationSummary);
    console.log("   inflationFitParams:", !!chartData.inflationFitParams);
    
    if (chartData.inflationSummary) {
      console.log("   inflationSummary values:", {
        start: chartData.inflationSummary.startValue,
        end: chartData.inflationSummary.endValue,
        increase: chartData.inflationSummary.totalIncrease
      });
    }

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
    
    // Store original dataset configurations before Chart.js can modify them
    this.originalDatasetConfigs = datasets.map(dataset => ({...dataset}));

    // Chart configuration
    const config = {
      type: "line",
      data: { datasets },
      options: this.getChartOptions(),
    };

    this.chart = new Chart(ctx, config);
    
    // Initial call to updateInfo() - this might be overriding our changes later!
    console.log("🎬 Initial updateInfo() call during chart init");
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
              const inflationSuffix = this.showInflation && this.chartData.inflationData 
                ? ` (${this.chartData.inflationData.inflationType} Adjusted)` 
                : "";
              return `${label}${inflationSuffix}: $${value.toFixed(1)} trillion`;
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
          // Never show y-axis title - respecting design choice
          title: {
            display: false,
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

    // Animate data points appearing - faster animation
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
    }, 5); // Reduced from 10 to 5 for faster animation
  }

  animateTrendLine(trendDataset, trendData) {
    const duration = 800; // Reduced from 1500 to 800 for faster animation
    const steps = 40; // Reduced from 50 to 40
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
        // IMPORTANT: Call updateInfo() after animation completes
        console.log("🎬 Animation completed, calling updateInfo()");
        this.updateInfo();
      }
    }, stepDuration);
  }

  setupControls() {
    // Add control buttons after the chart - removed replay button
    const chartSection = document.querySelector(".chart-section");
    if (!chartSection) return;

    const controlsHtml = `
      <div class="chart-controls">
        <button class="chart-btn active" data-view="nominal">Nominal Values</button>
        <button class="chart-btn" data-view="inflation" ${!this.chartData.inflationData ? 'disabled' : ''}>Inflation Adjusted</button>
      </div>
    `;

    // Insert controls if not already present
    if (!chartSection.querySelector(".chart-controls")) {
      chartSection.insertAdjacentHTML("beforeend", controlsHtml);
    }

    // Add event listeners
    const controls = chartSection.querySelectorAll(".chart-btn[data-view]");
    controls.forEach((btn) => {
      btn.addEventListener("click", (e) => this.handleControlClick(e));
    });

    // Log inflation data availability for debugging
    console.log("Inflation data available:", !!this.chartData.inflationData);
    if (this.chartData.inflationData) {
      console.log("Inflation data type:", this.chartData.inflationData.inflationType);
      console.log("Inflation data points:", this.chartData.inflationData.data?.length || 0);
    }
  }

  handleControlClick(e) {
    const btn = e.target;
    const view = btn.getAttribute("data-view");

    if (view && !btn.disabled) {
      // Toggle inflation view
      const newShowInflation = view === "inflation";
      
      console.log(`🔄 Switching from ${this.showInflation ? 'inflation' : 'nominal'} to ${newShowInflation ? 'inflation' : 'nominal'} mode`);
      
      // Only update if there's actually a change
      if (newShowInflation !== this.showInflation) {
        this.showInflation = newShowInflation;
        console.log("🔄 About to call updateChartData()");
        this.updateChartData();
        
        // Force update info immediately after chart data update
        console.log("🔄 About to call updateInfo()");
        try {
          this.updateInfo();
          console.log("✅ updateInfo() completed successfully");
        } catch (error) {
          console.error("❌ updateInfo() failed:", error);
        }
      }

      // Update button states
      document.querySelectorAll(".chart-btn[data-view]").forEach((b) => {
        b.classList.toggle("active", b === btn);
      });
    }
  }

  updateChartData() {
    if (!this.chart || !this.originalDatasetConfigs) return;

    let dataToUse = this.chartData.data;
    let trendToUse = this.chartData.trendLine;
    let labelSuffix = "";

    // Use inflation-adjusted data if available and selected
    if (this.showInflation && this.chartData.inflationData) {
      dataToUse = this.chartData.inflationData.data;
      labelSuffix = ` (${this.chartData.inflationData.inflationType} Adjusted)`;
      
      // Use inflation-adjusted trend line if available
      if (this.chartData.inflationTrendLine) {
        trendToUse = this.chartData.inflationTrendLine;
        console.log("Using inflation-adjusted trend line");
      } else {
        console.log("Inflation trend line not available, using nominal trend");
      }
      
      console.log("Switching to inflation-adjusted data:", dataToUse.length, "points");
    } else {
      console.log("Using nominal data:", dataToUse.length, "points");
    }

    // Process new data
    const processedData = dataToUse.map((point) => ({
      x: new Date(point.x),
      y: point.y,
    }));

    // Process trend line data
    const processedTrendData = trendToUse ? trendToUse.map((point) => ({
      x: new Date(point.x),
      y: point.y,
    })) : [];

    // Completely restore original dataset configurations
    // This prevents Chart.js internal modifications from persisting
    this.chart.data.datasets[0] = {
      ...this.originalDatasetConfigs[0],
      data: processedData,
      label: "Total Wealth" + labelSuffix,
    };

    // Restore trend line dataset if it exists
    if (this.originalDatasetConfigs[1] && this.chart.data.datasets[1]) {
      this.chart.data.datasets[1] = {
        ...this.originalDatasetConfigs[1],
        data: processedTrendData,
        label: "Exponential Trend" + labelSuffix,
      };
    }

    // Force chart update with no animation to prevent styling changes
    this.chart.update('none');
    console.log("✅ Chart data updated successfully");
  }

  updateInfo() {
    // UNIQUE SIGNATURE TO ENSURE WE'RE IN THE RIGHT FUNCTION
    console.log("🔥🔥🔥 ENTERING UPDATED updateInfo() METHOD 🔥🔥🔥");
    
    const timestamp = new Date().getTime();
    console.log(`🔍 updateInfo() ENTRY [${timestamp}] - showInflation:`, this.showInflation);
    
    // Check all required properties exist
    console.log("📋 Property check:");
    console.log("  this.chartData exists:", !!this.chartData);
    console.log("  this.chartData.summary exists:", !!this.chartData?.summary);
    console.log("  this.chartData.inflationSummary exists:", !!this.chartData?.inflationSummary);
    console.log("  this.showInflation:", this.showInflation);
    
    try {
      const infoArea = document.querySelector(".chart-info");
      if (!infoArea) {
        console.error("❌ .chart-info element not found");
        return;
      }
      
      if (!this.chartData || !this.chartData.summary) {
        console.error("❌ chartData or summary not available");
        console.log("chartData:", this.chartData);
        return;
      }
      
      console.log(`✅ Basic checks passed [${timestamp}], proceeding with update`);
      
      let summary = this.chartData.summary;
      let inflationNote = "";
      let growthRate = this.chartData.fitParams?.annualGrowthRate || 0;
      let rSquared = this.chartData.fitParams?.r_squared || 0;
      
      console.log(`📊 Starting with nominal summary: $${summary.startValue?.toFixed(1)}T → $${summary.endValue?.toFixed(1)}T (+${summary.totalIncrease?.toFixed(1)}%)`);
      
      // Use inflation-adjusted metrics if available and selected
      if (this.showInflation) {
        console.log(`🎯 showInflation is TRUE, checking for inflation data...`);
        console.log("  inflationData exists:", !!this.chartData.inflationData);
        console.log("  inflationSummary exists:", !!this.chartData.inflationSummary);
        
        if (this.chartData.inflationData && this.chartData.inflationSummary) {
          console.log(`🔥 SWITCHING TO INFLATION MODE! 🔥`);
          
          const inflationData = this.chartData.inflationData;
          inflationNote = ` (${inflationData.inflationType} adjusted to ${inflationData.baseDate || 'latest'} dollars)`;
          
          // Switch to inflation summary
          const originalSummary = summary;
          summary = this.chartData.inflationSummary;
          
          console.log(`📊 CRITICAL SUMMARY SWITCH:`);
          console.log("  ORIGINAL:", `$${originalSummary.startValue?.toFixed(1)}T → $${originalSummary.endValue?.toFixed(1)}T (+${originalSummary.totalIncrease?.toFixed(1)}%)`);
          console.log("  INFLATION:", `$${summary.startValue?.toFixed(1)}T → $${summary.endValue?.toFixed(1)}T (+${summary.totalIncrease?.toFixed(1)}%)`);
          
          // Use inflation growth rate if available
          if (this.chartData.inflationFitParams) {
            const oldGrowthRate = growthRate;
            growthRate = this.chartData.inflationFitParams.annualGrowthRate;
            rSquared = this.chartData.inflationFitParams.r_squared;
            console.log(`📈 Growth rate change: ${oldGrowthRate?.toFixed(1)}% → ${growthRate?.toFixed(1)}%`);
          }
        } else {
          console.log(`❌ showInflation=true but missing data:`, {
            inflationData: !!this.chartData.inflationData,
            inflationSummary: !!this.chartData.inflationSummary
          });
        }
      } else {
        console.log(`📊 NOMINAL MODE (showInflation=false)`);
      }
      
      const newHTML = `
        <strong>${summary.dataPoints || 'N/A'} data points</strong> from ${summary.timespan || 'N/A'}<br>
        Growth${inflationNote}: $${summary.startValue?.toFixed(1) || 'N/A'}T → $${summary.endValue?.toFixed(1) || 'N/A'}T 
        (+${summary.totalIncrease?.toFixed(1) || 'N/A'}%)<br>
        <span style="color: var(--red-light)">Exponential growth rate: ${growthRate?.toFixed(1) || 'N/A'}% per year (R² = ${rSquared?.toFixed(3) || 'N/A'})</span>
        ${this.showInflation ? '<br><span style="color: var(--text-muted); font-size: 0.875rem;">Growth rate calculated using inflation-adjusted values</span>' : ''}
      `;
      
      console.log(`📝 Setting HTML [${timestamp}]:`);
      console.log("📝 New HTML snippet:", newHTML.substring(0, 150) + "...");
      
      infoArea.innerHTML = newHTML;
      
      // Immediate verification
      const immediateCheck = infoArea.innerHTML;
      console.log(`🔍 Immediate verification: content updated =`, immediateCheck.includes(summary.startValue?.toFixed(1) || 'XXX'));
      
      console.log(`✅ updateInfo() completed successfully [${timestamp}]`);
      
    } catch (error) {
      console.error("❌ updateInfo() CRITICAL ERROR:", error);
      console.error("Stack:", error.stack);
    }
    
    console.log("🔥🔥🔥 EXITING UPDATED updateInfo() METHOD 🔥🔥🔥");
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
