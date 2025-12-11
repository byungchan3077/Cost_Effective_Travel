import os
import sys
import pandas as pd
import requests_mock
import pytest
from datetime import datetime, timedelta

# Add project root path to allow import of moveAvgDay
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import the module under test and its constants
from src.api.moveAvgDay import setup_database, load_db_data, save_db_data, get_50day_ma_data
from src.api.moveAvgDay import DB_DIR, DB_FILE_PREFIX, DAYS_TO_FETCH, MIN_PERIODS

# Define temporary settings for testing
TEST_DB_DIR = os.path.join(os.path.dirname(__file__), 'test_database')
TEST_API_KEY = "TEST_KEY_123"
TEST_CURRENCY = "USD"
TEST_FILE_PATH = os.path.join(TEST_DB_DIR, f"{DB_FILE_PREFIX}{TEST_CURRENCY}.csv")

# =============================================================================
# Mock Setup and Data Generation
# =============================================================================

@pytest.fixture(scope="module", autouse=True)
def setup_teardown_db():
    """Fixture to set up a temporary test DB directory and clean it up."""
    
    # Setup: Create temporary folder
    os.makedirs(TEST_DB_DIR, exist_ok=True)

    # Override the global setup_database function to use the temporary test path
    def setup_database_mock(currency_code):
        os.makedirs(TEST_DB_DIR, exist_ok=True)
        return os.path.join(TEST_DB_DIR, f"{DB_FILE_PREFIX}{currency_code}.csv")
    
    global setup_database 
    setup_database = setup_database_mock

    yield
    
    # Teardown: Remove temporary folder and its contents
    import shutil
    if os.path.exists(TEST_DB_DIR):
        shutil.rmtree(TEST_DB_DIR)

def create_mock_api_response(currency_code, date_str, rate, result_code=1):
    """Generates mock JSON data for a specific date and currency API response."""
    return [{
        "result": result_code,
        "cur_unit": currency_code,
        "deal_bas_r": f"{rate:,.2f}",  # Format like "1,300.50"
        "ttb": "1290.00",
        "tts": "1310.00",
        "yyyymmdd": date_str
    }]

# =============================================================================
# Individual DB Management Function Tests
# =============================================================================

def test_setup_database():
    """Verifies that the database setup function correctly creates the directory and returns the path."""
    path = setup_database(TEST_CURRENCY)
    assert path == TEST_FILE_PATH
    assert os.path.isdir(TEST_DB_DIR)

def test_save_and_load_db_data():
    """Verifies data saving, file existence, loading, and correct sorting."""
    data = {
        'Date': ['20251201', '20251202', '20251203'],
        'Currency Code': [TEST_CURRENCY, TEST_CURRENCY, TEST_CURRENCY],
        'Currency': [1300.0, 1310.0, 1320.0]
    }
    df = pd.DataFrame(data)
    
    # Test save
    save_db_data(df, TEST_FILE_PATH)
    assert os.path.exists(TEST_FILE_PATH)
    
    # Test load
    loaded_df = load_db_data(TEST_FILE_PATH)
    assert not loaded_df.empty
    assert len(loaded_df) == 3
    # load_db_data sorts descending (latest date on top)
    assert loaded_df['Date'].iloc[0] == '20251203' 
    assert loaded_df['Currency'].iloc[0] == 1320.0
    
    # Test loading a non-existent file returns empty DataFrame
    os.remove(TEST_FILE_PATH)
    empty_df = load_db_data(TEST_FILE_PATH)
    assert empty_df.empty

# =============================================================================
# Core Function: get_50day_ma_data Test
# =============================================================================

@pytest.fixture
def mock_target_currencies(monkeypatch):
    """Mocks get_target_currencies to return only the test currency (USD)."""
    def mock_get_target_currencies():
        return [TEST_CURRENCY]
    
    # Apply the mock to the imported function reference within moveAvgDay
    monkeypatch.setattr('src.api.moveAvgDay.get_target_currencies', mock_get_target_currencies)

def test_get_50day_ma_data_full_process(requests_mock, mock_target_currencies):
    """Tests the full data lifecycle: loading, fetching, calculating MA, and saving."""
    
    # 1. Initial DB State: Assume 45 days of data exist (Need 5 more days, since DAYS_TO_FETCH = 50)
    existing_days = 45 
    needed_new_days = DAYS_TO_FETCH - existing_days # Should be 5
    
    # Generate 45 days of historical data
    base_date = datetime.now() - timedelta(days=70) # Set a baseline far enough in the past
    existing_data = []
    for i in range(1, existing_days + 1):
        date_str = (base_date + timedelta(days=i)).strftime('%Y%m%d')
        existing_data.append({
            'Date': date_str, 
            'Currency Code': TEST_CURRENCY, 
            'Currency': 1000.0 + i, 
            '50-day_MA': float('nan') # MA is NaN for old data here
        })
    df_existing = pd.DataFrame(existing_data)
    save_db_data(df_existing, TEST_FILE_PATH)
    
    # 2. API Mocking: Generate the 5 required days (latest data)
    latest_rate = 1500.0 
    api_base_url = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON" 
    
    mock_dates = []
    
    # Mocking for the 5 newest days needed (fetch_optimized_data works backward from today)
    for i in range(1, needed_new_days + 1):
        date_obj = datetime.now() - timedelta(days=i * 2) # Use non-sequential days to mimic business days
        date_str = date_obj.strftime('%Y%m%d')
        rate = latest_rate + i
        
        # Mock the specific API URL call
        requests_mock.get(
            f"{api_base_url}?authkey={TEST_API_KEY}&searchdate={date_str}&data=AP01", 
            json=create_mock_api_response(TEST_CURRENCY, date_str, rate),
            status_code=200
        )
        mock_dates.append((date_str, rate))

    # 3. Execute the test function
    result_df = get_50day_ma_data(TEST_API_KEY)

    # 4. Verify the final returned DataFrame (result_df)
    assert not result_df.empty
    assert len(result_df) == 1 # Only one currency was processed
    
    result_row = result_df.iloc[0]
    
    # - Verify Current Rate ('Currency') is the newest rate fetched
    latest_currency_rate = mock_dates[0][1]
    assert result_row['Currency Code'] == TEST_CURRENCY
    assert result_row['Currency'] == latest_currency_rate 
    
    # - Verify 50-day MA calculation (check within a reasonable range)
    expected_ma_min = 1000  # Lowest historical rate
    expected_ma_max = latest_rate + needed_new_days # Highest fetched rate
    
    # The average of 50 days should fall between the min and max rates
    assert result_row['50-day_MA'] > expected_ma_min
    assert result_row['50-day_MA'] < expected_ma_max

    # 5. Verify the final DB file state
    final_df = load_db_data(TEST_FILE_PATH)
    assert len(final_df) == DAYS_TO_FETCH # Must contain 50 days of data
    
    # Verify the MA column was saved and has non-NaN values for recent data
    assert '50-day_MA' in final_df.columns
    assert pd.notna(final_df['50-day_MA'].max())