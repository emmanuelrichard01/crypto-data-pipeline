import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
from monitoring.health_monitor import PipelineHealthMonitor

@pytest.fixture
def mock_loader():
    """Create a mock loader for testing"""
    loader = Mock()
    session = Mock()
    loader.get_session.return_value.__enter__.return_value = session
    return loader, session

def test_health_monitor_healthy(mock_loader):
    """Test health monitor with healthy system"""
    loader, session = mock_loader
    
    # Mock database responses
    session.execute.return_value.fetchall.return_value = [
        Mock(stage="extract", status="success", count=5),
        Mock(stage="load", status="success", count=5)
    ]
    
    latest_time = datetime.utcnow() - timedelta(minutes=30)
    session.execute.return_value.fetchone.side_effect = [
        Mock(latest_extraction=latest_time, total_records=1000),
        Mock(total_records=50, valid_prices=50, avg_price=45000.0)
    ]
    
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    
    assert health["status"] == "healthy"
    assert "pipeline_runs" in health
    assert "data_freshness" in health
    assert "data_quality" in health

def test_health_monitor_unhealthy_stale_data(mock_loader):
    """Test health monitor with stale data"""
    loader, session = mock_loader
    
    # Mock database responses
    session.execute.return_value.fetchall.return_value = [
        Mock(stage="extract", status="success", count=5)
    ]
    
    # Stale data (older than 2 hours)
    latest_time = datetime.utcnow() - timedelta(hours=3)
    session.execute.return_value.fetchone.side_effect = [
        Mock(latest_extraction=latest_time, total_records=0),
        Mock(total_records=0, valid_prices=0, avg_price=0.0)
    ]
    
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    
    assert health["status"] == "unhealthy"
    assert "issues" in health
    assert "Data is stale" in health["issues"][0]

def test_health_monitor_no_pipeline_runs(mock_loader):
    """Test health monitor with no pipeline run data"""
    loader, session = mock_loader
    
    # Mock database responses
    session.execute.return_value.fetchall.return_value = []
    
    latest_time = datetime.utcnow() - timedelta(minutes=30)
    session.execute.return_value.fetchone.side_effect = [
        Mock(latest_extraction=latest_time, total_records=1000),
        Mock(total_records=50, valid_prices=50, avg_price=45000.0)
    ]
    
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    
    assert health["status"] == "warning"
    assert "issues" in health
    assert "No pipeline run data" in health["issues"][0]

def test_health_monitor_database_error(mock_loader):
    """Test health monitor with database error"""
    loader, session = mock_loader
    
    # Mock database error
    session.execute.side_effect = Exception("Database connection failed")
    
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    
    assert health["status"] == "error"
    assert "error" in health