import sys
import os
# tests/logic/test_foo.py -> tests/logic -> tests -> root
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))



from src.logic.basket import calculate_lsb
from src.logic.calculator import calculate_tei


def test_full_logic_flow():
    """
    Integration Test: Raw Prices -> LSB -> TEI Score
    """
    # 1. Prepare Data (Scenario: Tokyo)
    # 500 JPY Meal, 400 JPY Drink, 6000 JPY Hotel
    # 1 JPY = 9.0 KRW (Undervalued vs 10.0 Avg)
    
    # Step 1: Calculate LSB
    lsb_jpy = calculate_lsb(500, 400, 6000)
    assert lsb_jpy == 8300.0  # (1500 + 800 + 6000)

    # Step 2: Calculate TEI
    result = calculate_tei(
        budget=1000000,       # 1M KRW
        duration=5,           # 200k Daily
        local_daily_cost=lsb_jpy,
        current_rate=9.0,     # Current 9.0
        ma_rate=10.0          # Avg 10.0
    )

    # Verification
    # r = -0.1
    # Adj Rate = 9.0 * 0.9 = 8.1
    # Local Cost KRW = 8300 * 8.1 = 67,230
    # PPI = 200,000 / 67,230 = 2.97
    assert result["tei_score"] == 2.97
    assert result["is_undervalued"] is True
