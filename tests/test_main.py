import sys
import os
import pytest
from unittest.mock import patch
from io import StringIO

# Add project root
current_dir = os.path.dirname(os.path.abspath(__file__))
# tests -> root
project_root = os.path.dirname(current_dir)
src_path = os.path.join(project_root, 'src')

if project_root not in sys.path:
    sys.path.append(project_root)
if src_path not in sys.path:
    sys.path.append(src_path)

# Import main (assuming main.py is in src or root)
# If main.py is in root, import might need adjustment based on file structure
from src.main import main 

@patch('src.main.run_analysis_pipeline')
def test_main_success(mock_pipeline, capsys):
    """Test main execution with valid arguments and successful results"""
    # 1. Setup Mock
    mock_results = [
        {'country_code': 'Japan', 'ppi_score': 1.5, 'currency_code': 'JPY(100)'},
        {'country_code': 'USA', 'ppi_score': 0.8, 'currency_code': 'USD'}
    ]
    mock_pipeline.return_value = (mock_results, "Success")
    
    # 2. Simulate CLI Arguments
    test_args = ["main.py", "--budget", "1000000", "--days", "5"]
    with patch.object(sys, 'argv', test_args):
        main()
        
    # 3. Capture Output
    captured = capsys.readouterr()
    output = captured.out
    
    # 4. Verify Output
    assert "Purchasing Power Index (PPI)" in output
    assert "Japan" in output
    assert "1.50" in output
    assert "🤑 PLENTY" in output  # Score 1.5 -> PLENTY
    assert "USA" in output
    assert "⚠️ TIGHT" in output   # Score 0.8 -> TIGHT

@patch('src.main.run_analysis_pipeline')
def test_main_failure(mock_pipeline, capsys):
    """Test main execution when pipeline fails"""
    # 1. Setup Mock Error
    mock_pipeline.return_value = ([], "Error: Database Connection Failed")
    
    # 2. Simulate CLI Arguments
    test_args = ["main.py", "--budget", "1000000", "--days", "5"]
    with patch.object(sys, 'argv', test_args):
        main()
        
    # 3. Verify Error Output
    captured = capsys.readouterr()
    assert "!!! ANALYSIS FAILED !!!" in captured.out
    assert "Database Connection Failed" in captured.out

def test_main_invalid_args(capsys):
    """Test input validation (negative budget)"""
    test_args = ["main.py", "--budget", "-100", "--days", "5"]
    
    with patch.object(sys, 'argv', test_args):
        # Expect SystemExit due to validation check
        with pytest.raises(SystemExit):
            main()
    
    captured = capsys.readouterr()
    assert "!!! ANALYSIS FAILED !!!" in captured.out
    assert "positive values" in captured.out
