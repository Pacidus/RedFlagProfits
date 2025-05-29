/* static/css/components.css - Component Styles */

/* Header Component */
.header {
    background-color: var(--bg-secondary);
    border-bottom: 1px solid var(--bg-tertiary);
    padding: var(--space-lg) 0;
    position: sticky;
    top: 0;
    z-index: 100;
    backdrop-filter: blur(10px);
}

.header-content {
    display: flex;
    align-items: center;
    justify-content: space-between;
}

.logo {
    display: flex;
    align-items: center;
    gap: var(--space-md);
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--text-primary);
    text-decoration: none;
    transition: var(--transition);
}

.logo:hover {
    color: var(--red-light);
}

.flag-icon {
    width: 32px;
    height: 24px;
}

.nav {
    display: flex;
    gap: var(--space-xl);
}

.nav-link {
    color: var(--text-secondary);
    text-decoration: none;
    font-weight: 500;
    transition: var(--transition);
    position: relative;
}

.nav-link:hover {
    color: var(--red-primary);
}

.nav-link.active::after {
    content: '';
    position: absolute;
    bottom: -8px;
    left: 0;
    right: 0;
    height: 2px;
    background-color: var(--red-primary);
}

.nav-toggle {
    display: none;
    flex-direction: column;
    gap: 4px;
    background: none;
    border: none;
    cursor: pointer;
    padding: var(--space-sm);
}

.nav-toggle span {
    width: 24px;
    height: 2px;
    background-color: var(--text-primary);
    transition: var(--transition);
}

/* Footer Component */
.footer {
    background-color: var(--bg-secondary);
    padding: var(--space-xxl) 0 var(--space-xl);
    margin-top: var(--space-xxl);
    border-top: 1px solid var(--bg-tertiary);
}

.footer-content {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: var(--space-xl);
    margin-bottom: var(--space-xl);
}

.footer-section h4 {
    color: var(--red-primary);
    margin-bottom: var(--space-md);
}

.footer-section ul {
    list-style: none;
}

.footer-section ul li {
    color: var(--text-secondary);
    margin-bottom: var(--space-xs);
    font-size: 0.875rem;
}

.footer-bottom {
    text-align: center;
    padding-top: var(--space-lg);
    border-top: 1px solid var(--bg-tertiary);
    color: var(--text-muted);
    font-size: 0.875rem;
}

/* Hero Section */
.hero {
    background: linear-gradient(135deg, var(--bg-primary) 0%, var(--bg-secondary) 100%);
    padding: var(--space-xxl) 0;
}

.hero-title {
    text-align: center;
    margin-bottom: var(--space-xxl);
}

.hero-subtitle {
    font-size: 1.25rem;
    color: var(--text-secondary);
    margin-bottom: var(--space-sm);
    font-weight: 400;
}

/* Metrics Grid */
.metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: var(--space-lg);
    margin-bottom: var(--space-xxl);
}

.metric-card {
    background-color: var(--bg-secondary);
