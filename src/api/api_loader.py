import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime

# --- 1. 설정 및 상수 정의 ---
load_dotenv()
API_KEY = os.getenv("EXIM_API_KEY")

BASE_URL = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"
SERVICE_CODE = "AP01" 
TIMEOUT_SECONDS = 10 

if not API_KEY:
    raise ValueError("❌ API 키가 .env 파일에 설정되지 않았습니다. EXIM_API_KEY 변수를 확인하세요.")

def load_api_key():
    """API 키와 기본 URL, 서비스 코드를 반환합니다."""
    return API_KEY, BASE_URL, SERVICE_CODE

def print_data_format(api_key, base_url, service_code):
    """
    API 데이터 형식을 확인하기 위해 최근 영업일 데이터를 불러와 출력합니다.
    """
    # 데이터 형식 확인을 위해 임시로 최근 영업일인 20251205를 사용합니다.
    date = "20251205" 
    
    params = {
        "authkey": api_key,
        "searchdate": date,
        "data": service_code
    }
    
    print(f"\n--- 🔍 API 데이터 형식 확인: [{date}] ---")
    
    try:
        response = requests.get(base_url, params=params, timeout=TIMEOUT_SECONDS)
        response.raise_for_status() 

        raw_json = response.json()
        
        if raw_json and raw_json[0].get('result') == 4:
            print("❌ API 오류: 일일 제한 횟수가 마감되었거나 데이터가 존재하지 않습니다.")
            return

        print(f"✅ API 응답 상태: {response.status_code} (성공)")
        
        # 데이터 형식 확인 (처음 1개 항목 출력)
        print("\n### 데이터 형식 (필수 컬럼 확인) ###")
        first_item = raw_json[0] if raw_json else {}
        print(json.dumps(first_item, indent=4, ensure_ascii=False))
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API 요청 실패: {e}.")

if __name__ == "__main__":
    API_KEY, BASE_URL, SERVICE_CODE = load_api_key()
    print_data_format(API_KEY, BASE_URL, SERVICE_CODE)