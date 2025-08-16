# Grafana Integration for Crypto Data Pipeline

This directory contains the Grafana provisioning configuration for the Crypto Data Pipeline project. The setup includes multiple dashboards for monitoring market data, pipeline performance, AI analytics, sentiment analysis, and liquidity metrics.

## Dashboards Overview

### 1. Market Overview

- **Purpose**: Real-time monitoring of cryptocurrency market conditions
- **Key Metrics**:
  - Total market capitalization
  - 24-hour trading volume
  - BTC dominance percentage
  - Top gainers/losers
  - Price history charts for top cryptocurrencies
  - Comprehensive table of top 10 cryptocurrencies

### 2. Pipeline Monitoring

- **Purpose**: Track the health and performance of the ETL pipeline
- **Key Metrics**:
  - Pipeline status (running/stopped)
  - Success rate percentage
  - Average execution duration
  - Total records processed
  - Performance trends over time
  - Detailed table of recent pipeline runs

### 3. AI Analytics

- **Purpose**: Monitor AI prediction performance and accuracy
- **Key Metrics**:
  - AI Confidence Index (average)
  - Prediction accuracy (MAPE)
  - Count of high-confidence predictions
  - Prediction deviation alerts
  - Confidence index trends over time
  - Accuracy trends over time
  - Detailed table of latest AI predictions

### 4. Sentiment Analysis

- **Purpose**: Track market sentiment from social media and news sources
- **Key Metrics**:
  - Overall sentiment score
  - Bullish vs bearish sentiment counts
  - Sentiment volatility measurements
  - Sentiment score trends over time
  - Sentiment distribution visualization
  - Detailed table of latest sentiment analysis

### 5. Liquidity Metrics

- **Purpose**: Monitor market liquidity and order book depth
- **Key Metrics**:
  - Average liquidity index
  - Count of healthy liquidity pairs
  - Average spread percentage
  - Liquidity drop alerts
  - Liquidity index trends over time
  - Spread percentage trends over time
  - Detailed table of latest liquidity metrics

## Accessing Grafana

1. Start the services using Docker Compose:

   ```bash
   docker-compose up -d
   ```

2. Access Grafana at: <http://localhost:3001>
   - **Username**: admin
   - **Password**: admin123

## Configuration Details

### Data Source

- **Type**: PostgreSQL
- **Host**: postgres
- **Port**: 5432
- **Database**: crypto_warehouse
- **User**: postgres
- **Password**: crypto_password_123

### Provisioning Structure

```
grafana/
├── provisioning/
│   ├── datasources/
│   │   └── datasource.yml
│   └── dashboards/
│       ├── dashboard.yml
│       ├── market_overview.json
│       ├── pipeline_monitoring.json
│       ├── ai_analytics.json
│       ├── sentiment_analysis.json
│       └── liquidity_metrics.json
└── README.md
```

## Customizing Dashboards

### Adding New Panels

1. Open Grafana in your browser
2. Navigate to the desired dashboard
3. Click "Add panel"
4. Configure the query using PostgreSQL syntax
5. Customize visualization options

### Modifying Existing Dashboards

1. Open Grafana in your browser
2. Navigate to the desired dashboard
3. Click "Settings" (gear icon)
4. Select "Save As" to create a copy
5. Make your modifications
6. Save the dashboard

### Creating New Dashboards

1. Open Grafana in your browser
2. Click "Create" → "Dashboard"
3. Add panels with your desired metrics
4. Save the dashboard
5. Export as JSON and add to the provisioning directory

## Alerting

Grafana alerting is configured to monitor key metrics:

### Market Alerts

- Price movement thresholds
- Volume spike detection
- Market cap ranking changes

### Pipeline Alerts

- Pipeline failure detection
- Performance degradation alerts
- Data freshness monitoring

### AI Analytics Alerts

- Prediction deviation monitoring
- Confidence score thresholds
- Model performance degradation

### Sentiment Alerts

- Sentiment trend changes
- Extreme sentiment readings
- Sentiment volatility spikes

### Liquidity Alerts

- Liquidity drop detection
- Spread widening alerts
- Order book depth anomalies

## Performance Considerations

### Database Indexes

Ensure the following indexes exist for optimal performance:

```sql
CREATE INDEX IF NOT EXISTS idx_mart_crypto_prices_time ON marts.mart_crypto_latest_prices (last_updated);
CREATE INDEX IF NOT EXISTS idx_mart_pipeline_monitoring_date ON marts.mart_pipeline_monitoring (run_date);
CREATE INDEX IF NOT EXISTS idx_mart_ai_predictions_date ON marts.mart_ai_predictions (prediction_date);
CREATE INDEX IF NOT EXISTS idx_mart_sentiment_timestamp ON marts.mart_sentiment_analysis (timestamp);
CREATE INDEX IF NOT EXISTS idx_mart_liquidity_timestamp ON marts.mart_liquidity_metrics (timestamp);
```

### Query Optimization

- Use time-based filters to limit data scope
- Aggregate data at the database level when possible
- Avoid SELECT * queries
- Use appropriate LIMIT clauses

## Troubleshooting

### Common Issues

1. **Dashboard not loading**
   - Check PostgreSQL connection in Grafana datasource settings
   - Verify database credentials
   - Ensure PostgreSQL service is running

2. **No data in panels**
   - Verify data exists in the database tables
   - Check query syntax in panel settings
   - Ensure time range filters are appropriate

3. **Performance issues**
   - Add database indexes for frequently queried columns
   - Optimize SQL queries in panels
   - Reduce refresh frequency for heavy queries

### Logs and Monitoring

- Check Grafana logs: `docker-compose logs grafana`
- Check PostgreSQL logs: `docker-compose logs postgres`
- Monitor system resources: CPU, memory, disk usage

## Security Considerations

### Authentication

- Default credentials should be changed immediately
- Use strong passwords for all accounts
- Enable two-factor authentication when possible

### Network Security

- Restrict access to Grafana port (3001) in production
- Use HTTPS in production environments
- Implement proper firewall rules

### Data Security

- Regular database backups
- Secure database connections with SSL
- Limit database user permissions to minimum required

## Future Enhancements

### Planned Improvements

1. **Advanced Alerting**: Machine learning-based anomaly detection
2. **Mobile Optimization**: Responsive design for all dashboards
3. **Historical Analysis**: Backtesting capabilities for AI models
4. **Correlation Analysis**: Cross-asset correlation matrices
5. **Risk Metrics**: VaR, stress testing, and scenario analysis

### Integration Opportunities

1. **External Data Sources**: Integration with additional market data providers
2. **Trading Platform Integration**: Direct trading signals from dashboards
3. **Machine Learning Models**: Advanced predictive analytics
4. **Natural Language Processing**: Enhanced sentiment analysis
5. **Blockchain Analytics**: On-chain data integration

This Grafana setup provides a comprehensive monitoring and analytics solution for the Crypto Data Pipeline, enabling real-time insights into market conditions, pipeline performance, and AI-driven predictions.
