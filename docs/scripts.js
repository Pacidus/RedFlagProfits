// MathJax configuration
window.MathJax = {
    tex: {
        inlineMath: [['$', '$'], ['\\(', '\\)']],
        displayMath: [['$$', '$$'], ['\\[', '\\]']]
    },
    options: {
        skipHtmlTags: ['script', 'noscript', 'style', 'textarea', 'pre']
    }
};

// Fetch plot data
async function fetchPlotData() {
    const resp1 = await fetch('figures/Billionaires.json');
    const resp2 = await fetch('figures/Millionaires.json');
    const data1 = await resp1.json();
    const data2 = await resp2.json();
    return [data1, data2];
}

// Render plots with data decoding
async function renderPlot() {
    const [plotData1, plotData2] = await fetchPlotData();

    // Decode binary data for Billionaires.json
    plotData1.data.forEach(trace => {
        if (trace.y?.bdata) {
            const decoded = atob(trace.y.bdata);
            const buffer = new Uint8Array(decoded.length);
            for (let i = 0; i < decoded.length; i++) {
                buffer[i] = decoded.charCodeAt(i);
            }
            trace.y = new Float64Array(buffer.buffer);
        }
    });

    // Decode binary data for Millionaires.json
    plotData2.data.forEach(trace => {
        if (trace.y?.bdata) {
            const decoded = atob(trace.y.bdata);
            const buffer = new Uint8Array(decoded.length);
            for (let i = 0; i < decoded.length; i++) {
                buffer[i] = decoded.charCodeAt(i);
            }
            trace.y = new Float64Array(buffer.buffer);
        }
    });

    Plotly.newPlot('figure1', plotData1.data, plotData1.layout);
    Plotly.newPlot('figure2', plotData2.data, plotData2.layout);
}

// Initialize visualization
renderPlot();
