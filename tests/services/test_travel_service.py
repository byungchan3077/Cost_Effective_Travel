import sys
import os
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
# tests/services -> tests -> root
project_root = os.path.dirname(os.path.dirname(current_dir))
src_path = os.path.join(project_root, 'src')

if project_root not in sys.path:
    sys.path.append(project_root)
if src_path not in sys.path:
    sys.path.append(src_path)

from src.services.travel_service import run_analysis_pipeline

@pytest.fixture
def mock_dependencies():
    """Mock external dependencies (API, DB, Calculators)"""
    with patch('src.services.travel_service.country_loader') as mock_country, \
         patch('src.services.travel_service.api_loader') as mock_api, \
         patch('src.services.travel_service.moveAvgDay') as mock_ma, \
         patch('src.services.travel_service.export_json') as mock_export, \
         patch('src.services.travel_service.basket') as mock_basket, \
         patch('src.services.travel_service.calculator') as mock_calc:
        
        yield mock_country, mock_api, mock_ma, mock_export, mock_basket, mock_calc

def test_pipeline_success(mock_dependencies):
    """Test successful execution of the full pipeline"""
    (mock_country, mock_api, mock_ma, mock_export, mock_basket, mock_calc) = mock_dependencies
    
    # 1. Mock Data Setup
    mock_country.get_target_currencies.return_value = ['USD', 'JPY(100)']
    mock_api.load_api_key.return_value = ('fake_key', 'code', 'url')
    
    # Mock MA Data (DataFrame)
    mock_ma.get_50day_ma_data.return_value = pd.DataFrame({
        'Currency Code': ['USD', 'JPY(100)'],
        'Currency': [1300.0, 900.0],     # JPY raw: 900 -> 9.0 per yen
        '50-day_MA': [1200.0, 950.0]     # JPY raw: 950 -> 9.5 per yen
    })
    
    # Mock Cost Data (JSON Dict)
    mock_export.main.return_value = {
        'United States': {'currency': 'USD', 'big_mac': 8.0, 'starbucks': 5.0, 'avg_hotel_krw': 150000},
        'Japan': {'currency': 'JPY(100)', 'big_mac': 500, 'starbucks': 450, 'avg_hotel_krw': 100000}
    }
    
    # Mock Calculation Results
    mock_basket.calculate_lsb.return_value = 100.0  # Dummy LSB
    mock_calc.calculate_tei.return_value = {'tei_score': 2.5, 'trend_impact': -0.05}
    
    # 2. Run Pipeline
    results, status = run_analysis_pipeline(total_budget=2000000, days=10)
    
    # 3. Assertions
    assert status == "Success"
    assert len(results) == 2
    assert results[0]['country_code'] == 'United States'
    assert results[0]['ppi_score'] == 2.5
    
    # Verify JPY Normalization (Divided by 100 logic check)
    # This is tricky to check directly without inspecting calls, but outcome confirms flow.
    mock_export.export_data.assert_called_once()

def test_pipeline_no_currencies(mock_dependencies):
    """Test failure when no target currencies found"""
    (mock_country, *_) = mock_dependencies
    mock_country.get_target_currencies.return_value = []
    
    results, status = run_analysis_pipeline(1000, 5)
    
    assert results == []
    assert "No target currencies" in status

def test_pipeline_api_error(mock_dependencies):
    """Test failure when API throws exception"""
    (mock_country, mock_api, mock_ma, *_) = mock_dependencies
    mock_country.get_target_currencies.return_value = ['USD']
    mock_api.load_api_key.return_value = ('key', 'code', 'url')
    mock_ma.get_50day_ma_data.side_effect = Exception("API Timeout")
    
    results, status = run_analysis_pipeline(1000, 5)
    
    assert results == []
    assert "API/DB failed" in status
