// static/js/charts.js - Chart JavaScript
// Placeholder for chart functionality - will integrate with your chosen library

/**
 * Initialize dashboard charts
 */
function initializeDashboard(data) {
  console.log("📊 Initializing dashboard with data:", data);

  // Initialize main wealth timeline chart
  initializeWealthTimelineChart(data);

  // Initialize chart controls
  initializeChartControls();

  // Update metric cards if needed
  updateMetricCards(data);
}

/**
 * Initialize the main wealth timeline chart
 */
function initializeWealthTimelineChart(data) {
  const chartContainer = document.getElementById("wealth-timeline-chart");

  if (!chartContainer) return;

  // Placeholder implementation - replace with your chosen chart library
  chartContainer.innerHTML = `
        <div class="chart-placeholder-content">
            <div class="chart-loading">
                <div class="loading-spinner"></div>
                <p>Loading wealth timeline chart...</p>
                <div class="chart-preview-data">
                    <div class="preview-metric">
                        <span class="preview-label">Current Total:</span>
                        <span class="preview-value">${window.RedFlags.formatCurrency(data.totalWealth * 1000000000000)}</span>
                    </div>
                    <div class="preview-metric">
                        <span class="preview-label">Growth Rate:</span>
                        <span class="preview-value">+${data.growthRate.toFixed(1)}%</span>
                    </div>
                    <div class="preview-metric">
                        <span class="preview-label">Billionaires:</span>
                        <span class="preview-value">${data.billionaireCount.toLocaleString()}</span>
                    </div>
                </div>
            </div>
        </div>
    `;

  // Simulate loading delay
  setTimeout(() => {
    renderWealthChart(chartContainer, data);
  }, 1000);
}

/**
 * Render the actual wealth chart (placeholder)
 */
function renderWealthChart(container, data) {
  // This is where you'll integrate with your chosen chart library
  // For now, showing a styled placeholder

  container.innerHTML = `
        <div class="chart-rendered">
            <svg width="100%" height="100%" viewBox="0 0 800 400" class="wealth-chart-svg">
                <!-- Background grid -->
                <defs>
                    <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
                        <path d="M 40 0 L 0 0 0 40" fill="none" stroke="var(--bg-tertiary)" stroke-width="1" opacity="0.3"/>
                    </pattern>
                </defs>
                <rect width="100%" height="100%" fill="url(#grid)" />
                
                <!-- Mock trend line -->
                <path d="M 50 350 Q 200 300 400 200 T 750 100" 
                      fill="none" 
                      stroke="var(--red-primary)" 
                      stroke-width="3" 
                      opacity="0.8"/>
                
                <!-- Data points -->
                <circle cx="50" cy="350" r="4" fill="var(--red-primary)"/>
                <circle cx="200" cy="280" r="4" fill="var(--red-primary)"/>
                <circle cx="400" cy="200" r="4" fill="var(--red-primary)"/>
                <circle cx="600" cy="150" r="4" fill="var(--red-primary)"/>
                <circle cx="750" cy="100" r="6" fill="var(--red-light)"/>
                
                <!-- Labels -->
                <text x="50" y="380" text-anchor="middle" fill="var(--text-muted)" font-size="12">2020</text>
                <text x="200" y="380" text-anchor="middle" fill="var(--text-muted)" font-size="12">2021</text>
                <text x="400" y="380" text-anchor="middle" fill="var(--text-muted)" font-size="12">2022</text>
                <text x="600" y="380" text-anchor="middle" fill="var(--text-muted)" font-size="12">2023</text>
                <text x="750" y="380" text-anchor="middle" fill="var(--text-muted)" font-size="12">2024</text>
                
                <!-- Y-axis labels -->
                <text x="30" y="360" text-anchor="end" fill="var(--text-muted)" font-size="12">$8T</text>
                <text x="30" y="280" text-anchor="end" fill="var(--text-muted)" font-size="12">$10T</text>
                <text x="30" y="200" text-anchor="end" fill="var(--text-muted)" font-size="12">$12T</text>
                <text x="30" y="120" text-anchor="end" fill="var(--text-muted)" font-size="12">$14T</text>
                
                <!-- Current value indicator -->
                <text x="750" y="90" text-anchor="middle" fill="var(--red-light)" font-size="14" font-weight="bold">
                    ${window.RedFlags.formatCurrency(data.totalWealth * 1000000000000)}
                </text>
            </svg>
        </div>
    `;
}

/**
 * Initialize chart control buttons
 */
function initializeChartControls() {
  const chartButtons = document.querySelectorAll(".chart-btn");

  chartButtons.forEach((button) => {
    button.addEventListener("click", function () {
      // Remove active class from all buttons
      chartButtons.forEach((btn) => btn.classList.remove("active"));

      // Add active class to clicked button
      this.classList.add("active");

      // Get the period
      const period = this.dataset.period;

      // Update chart based on period
      updateChartPeriod(period);
    });
  });
}

/**
 * Update chart based on selected time period
 */
function updateChartPeriod(period) {
  console.log(`📅 Updating chart for period: ${period}`);

  // This is where you'd update your chart library with new data
  // For now, just log the change

  const chartContainer = document.getElementById("wealth-timeline-chart");
  if (chartContainer) {
    // Add loading state
    chartContainer.classList.add("loading");

    setTimeout(() => {
      chartContainer.classList.remove("loading");
      console.log(`✅ Chart updated for ${period}`);
    }, 500);
  }
}

/**
 * Update metric cards with new data
 */
function updateMetricCards(data) {
  const updates = [
    {
      selector: ".metric-card:nth-child(1) .metric-value",
      value: window.RedFlags.formatCurrency(data.totalWealth * 1000000000000),
    },
    {
      selector: ".metric-card:nth-child(2) .metric-value",
      value: data.billionaireCount.toLocaleString(),
    },
    {
      selector: ".metric-card:nth-child(3) .metric-value",
      value:
        window.RedFlags.formatCurrency(data.averageWealth * 1000000000) + "B",
    },
    {
      selector: ".metric-card:nth-child(4) .metric-value",
      value: data.doublingTime.toFixed(1) + " years",
    },
  ];

  updates.forEach((update) => {
    const element = document.querySelector(update.selector);
    if (element && element.textContent !== update.value) {
      element.textContent = update.value;
    }
  });
}

/**
 * Initialize growth rate chart (placeholder)
 */
function initializeGrowthRateChart() {
  const chartContainer = document.getElementById("growth-rate-chart");

  if (!chartContainer) return;

  chartContainer.innerHTML = `
        <div class="chart-rendered">
            <div class="growth-chart-placeholder">
                <div class="growth-bars">
                    <div class="growth-bar" style="height: 60%; background: var(--red-dark);" title="2020: 5.2%"></div>
                    <div class="growth-bar" style="height: 80%; background: var(--red-secondary);" title="2021: 7.8%"></div>
                    <div class="growth-bar" style="height: 45%; background: var(--red-secondary);" title="2022: 4.1%"></div>
                    <div class="growth-bar" style="height: 90%; background: var(--red-primary);" title="2023: 9.2%"></div>
                    <div class="growth-bar" style="height: 100%; background: var(--red-light);" title="2024: 11.5%"></div>
                </div>
                <div class="growth-labels">
                    <span>2020</span>
                    <span>2021</span>
                    <span>2022</span>
                    <span>2023</span>
                    <span>2024</span>
                </div>
            </div>
        </div>
    `;
}

// Add CSS for chart components
const chartStyles = document.createElement("style");
chartStyles.textContent = `
    .chart-placeholder-content {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 100%;
        color: var(--text-muted);
    }
    
    .chart-loading {
        text-align: center;
    }
    
    .loading-spinner {
        width: 40px;
        height: 40px;
        border: 3px solid var(--bg-tertiary);
        border-top: 3px solid var(--red-primary);
        border-radius: 50%;
        animation: spin 1s linear infinite;
        margin: 0 auto 1rem;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    .chart-preview-data {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
        margin-top: 1rem;
        font-family: var(--font-mono);
        font-size: 0.875rem;
    }
    
    .preview-metric {
        display: flex;
        justify-content: space-between;
        min-width: 200px;
    }
    
    .preview-value {
        color: var(--red-primary);
        font-weight: bold;
    }
    
    .chart-rendered {
        height: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    
    .wealth-chart-svg {
        background-color: var(--bg-primary);
        border-radius: 4px;
    }
    
    .chart-container.loading {
        opacity: 0.6;
        pointer-events: none;
    }
    
    .growth-chart-placeholder {
        width: 100%;
        height: 300px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }
    
    .growth-bars {
        display: flex;
        align-items: end;
        gap: 1rem;
        height: 200px;
        margin-bottom: 1rem;
    }
    
    .growth-bar {
        width: 40px;
        min-height: 20px;
        border-radius: 4px 4px 0 0;
        cursor: pointer;
        transition: all 0.3s ease;
    }
    
    .growth-bar:hover {
        opacity: 0.8;
        transform: translateY(-2px);
    }
    
    .growth-labels {
        display: flex;
        gap: 1rem;
        font-family: var(--font-mono);
        font-size: 0.875rem;
        color: var(--text-muted);
    }
    
    .growth-labels span {
        width: 40px;
        text-align: center;
    }
`;
document.head.appendChild(chartStyles);

// Initialize growth rate chart when DOM is ready
document.addEventListener("DOMContentLoaded", function () {
  initializeGrowthRateChart();
});
