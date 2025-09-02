from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock

import pytest

from monitoring.health_monitor import PipelineHealthMonitor


@pytest.fixture
def mock_loader():
    loader = MagicMock()
    session = MagicMock()
    loader.get_session.return_value.__enter__.return_value = session
    return loader, session


def test_health_monitor_healthy(mock_loader):
    loader, session = mock_loader
    session.execute.return_value.fetchall.return_value = [
        Mock(stage="extract", status="success", count=5),
        Mock(stage="load", status="success", count=5),
    ]
    latest_time = datetime.utcnow() - timedelta(minutes=30)
    session.execute.return_value.fetchone.side_effect = [
        Mock(latest_extraction=latest_time, total_records=1000),
        Mock(total_records=50, valid_prices=50, avg_price=45000.0),
    ]
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    assert health["status"] == "healthy"


def test_health_monitor_unhealthy_stale_data(mock_loader):
    loader, session = mock_loader
    session.execute.return_value.fetchall.return_value = [
        Mock(stage="extract", status="success", count=5)
    ]
    latest_time = datetime.utcnow() - timedelta(hours=3)
    session.execute.return_value.fetchone.side_effect = [
        Mock(latest_extraction=latest_time, total_records=0),
        Mock(total_records=0, valid_prices=0, avg_price=0.0),
    ]
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    assert health["status"] == "unhealthy"


def test_health_monitor_no_pipeline_runs(mock_loader):
    loader, session = mock_loader
    session.execute.return_value.fetchall.return_value = []
    latest_time = datetime.utcnow() - timedelta(minutes=30)
    session.execute.return_value.fetchone.side_effect = [
        Mock(latest_extraction=latest_time, total_records=1000),
        Mock(total_records=50, valid_prices=50, avg_price=45000.0),
    ]
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    assert health["status"] == "warning"


def test_health_monitor_database_error(mock_loader):
    loader, session = mock_loader
    session.execute.side_effect = Exception("Database connection failed")
    monitor = PipelineHealthMonitor(loader)
    health = monitor.check_pipeline_health()
    assert health["status"] == "error"
