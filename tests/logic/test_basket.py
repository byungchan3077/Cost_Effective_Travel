import sys
import os
# tests/logic/test_foo.py -> tests/logic -> tests -> root
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))



from src.logic.basket import calculate_lsb


def test_calculate_lsb_valid():
    # (3 * 5000) + (2 * 4000) + 50000 = 73000
    assert calculate_lsb(5000, 4000, 50000) == 73000.0


def test_calculate_lsb_negative():
    assert calculate_lsb(-100, 4000, 50000) == 0.0


def test_calculate_lsb_zero():
    assert calculate_lsb(0, 0, 0) == 0.0
