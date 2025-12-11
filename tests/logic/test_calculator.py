import sys
import os
# tests/logic/test_foo.py -> tests/logic -> tests -> root
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))



from src.logic.calculator import calculate_tei, calculate_trend_factor


def test_calculate_trend_factor():
    # Undervalued: (850 - 1000) / 1000 = -0.15
    assert calculate_trend_factor(850, 1000) == -0.15
    # Overvalued: (1100 - 1000) / 1000 = 0.10
    assert calculate_trend_factor(1100, 1000) == 0.10


def test_calculate_tei_valid():
    # Standard scenario without trend impact
    # PPI = 200,000 / 100,000 = 2.0
    result = calculate_tei(1000000, 5, 100, 1000, 1000)
    assert result["tei_score"] == 2.0
    assert result["is_undervalued"] is False


def test_calculate_tei_trend():
    # Currency 10% cheaper (r = -0.1)
    # Adjusted Rate = 900 -> Local Cost = 90,000 -> PPI = 2.22
    result = calculate_tei(1000000, 5, 100, 900, 1000)
    assert result["tei_score"] == 2.47
    assert result["is_undervalued"] is True


def test_calculate_tei_invalid():
    result = calculate_tei(1000000, 0, 100, 1000, 1000)
    assert result["tei_score"] == 0.0
