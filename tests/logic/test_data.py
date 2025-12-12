import sys
import os
import json
import pandas as pd
import pytest

# ---------- 경로 설정 ----------
# 현재 파일 tests/logic/test_data.py 기준
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "src", "data")
sys.path.insert(0, DATA_DIR)
# --------------------------------

import export_json  # ✔ 실제 파일 이름과 맞춰야 함!






def test_clean_number():
    assert export_json.clean_number("1,000") == 1000.0
    assert export_json.clean_number(" 123 ") == 123.0
    assert export_json.clean_number("abc") == 0
    assert export_json.clean_number(3.14) == 3.14


def test_main_creates_correct_result_json(tmp_path, monkeypatch):
    hotel_df = pd.DataFrame({
        "Country": ["Japan", "Japan ", "France"],
        "Avg_price": ["10,000", "20,000", "80,000"]
    })
    hotel_path = tmp_path / "hotel_price_index.csv"
    hotel_df.to_csv(hotel_path, index=False)

    starbucks_df = pd.DataFrame({
        "Country": ["Japan", "France"],
        "Avg_price": ["5", "45"]
    })
    starbucks_path = tmp_path / "starbucks_drink_index.csv"
    starbucks_df.to_csv(starbucks_path, index=False)

    bigmac_df = pd.DataFrame({
        "Country": ["Japan", "Euro area"],
        "local_price": ["4.2", "4.0"]
    })
    bigmac_path = tmp_path / "big_mac_index.csv"
    bigmac_df.to_csv(bigmac_path, index=False)

    # export_data 내부의 script_dir을 테스트용 tmp_path로 바꿉니다
    monkeypatch.setattr(export_json, "script_dir", str(tmp_path))

    export_json.main()

    output_path = tmp_path / "result.json"
    assert output_path.exists()

    with output_path.open(encoding="utf-8") as f:
        result = json.load(f)

    assert "Japan" in result
    assert "France" in result

    jp = result["Japan"]
    assert jp["currency"] == "JPY(100)"
    assert jp["avg_hotel_krw"] == 15000
    assert jp["starbucks"] == 5.0
    assert jp["big_mac"] == 4.2

    fr = result["France"]
    assert fr["currency"] == "EUR"
    assert fr["avg_hotel_krw"] == 80000
    assert fr["starbucks"] == 45.0
    assert fr["big_mac"] == 4.0

