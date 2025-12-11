import os
import sys
import requests
import pandas as pd
import time
import math
from datetime import datetime, timedelta

# **--- 1. 프로젝트 루트 경로 추가 (상대 경로 임포트 문제 해결) ---**
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# 상대 경로를 사용하여 필요한 모듈 임포트
# NOTE: country_loader의 함수명이 get_target_currencies로 수정되었다고 가정
from src.api.api_loader import load_api_key, SERVICE_CODE, BASE_URL
from src.api.country_loader import get_target_currencies 

# --- 1. 설정 및 상수 정의 ---
DAYS_TO_FETCH = 10
DB_DIR = os.path.join(os.path.dirname(__file__), 'database') 
DB_FILE_PREFIX = 'exchange_data_'
MIN_PERIODS = 10 

# --- 2. DB 및 데이터 관리 함수 (복원된 이전 함수들) ---

def setup_database(currency_code):
    """DB 폴더를 생성하고 파일 경로를 반환합니다."""
    os.makedirs(DB_DIR, exist_ok=True)
    return os.path.join(DB_DIR, f"{DB_FILE_PREFIX}{currency_code}.csv")

def load_db_data(file_path):
    """기존 DB 데이터를 불러옵니다. 파일이 없거나 오류 발생 시 빈 DataFrame을 반환합니다."""
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, index_col=0, parse_dates=['날짜'])
            df['날짜'] = df['날짜'].dt.strftime('%Y%m%d') 
            df = df.sort_values(by='날짜', ascending=False)
            return df
        except Exception as e:
            print(f"⚠️ {os.path.basename(file_path)} 불러오기 오류: {e}. 새 데이터프레임을 생성합니다.")
            return pd.DataFrame()
    return pd.DataFrame()

def save_db_data(df, file_path):
    """데이터프레임을 CSV 파일로 저장합니다."""
    if not df.empty:
        df = df.drop_duplicates(subset=['날짜'], keep='first')
        df = df.sort_values(by='날짜', ascending=True)
        # 저장 전 '날짜'를 문자열로 변환 (load_db_data와 일관성을 위해)
        df['날짜'] = pd.to_datetime(df['날짜']).dt.strftime('%Y%m%d')
        df.to_csv(file_path, index=True, encoding='utf-8')
        print(f"✅ DB 저장 완료: {os.path.basename(file_path)}, 총 {len(df)}일치 데이터.")
    else:
        print(f"❌ 저장할 데이터가 없어 {os.path.basename(file_path)}에 저장하지 않습니다.")

# --- 3. 최적화된 데이터 수집 함수 (복원된 이전 함수) ---
# NOTE: BASE_URL, SERVICE_CODE 상수는 api_loader에서 임포트되어 전역에서 사용 가능함
def fetch_optimized_data(api_key, currency_code, existing_dates, days_needed):
    """기존 DB 데이터에 없는, 필요한 날짜의 데이터만 API 호출로 가져옵니다."""
    new_data = []
    fetched_count = 0
    MAX_ITERATIONS = days_needed * 2 + 7 
    
    # BASE_URL과 SERVICE_CODE는 상위 모듈에서 임포트된 전역 상수입니다.

    print(f"🔍 [{currency_code}] 신규 데이터 확보 시작 (필요한 영업일 수: {days_needed})")

    for i in range(1, MAX_ITERATIONS + 1):
        search_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        
        if fetched_count >= days_needed: break
        if search_date in existing_dates: continue

        params = {"authkey": api_key, "searchdate": search_date, "data": SERVICE_CODE}
        
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status() 
            day_data = response.json()

            if day_data and day_data[0].get('result') == 4:
                print("❌ API 제한 횟수 마감.")
                break 

            if day_data and not day_data[0].get('result') == 4:
                for item in day_data:
                    cur_unit = item.get('cur_unit')
                    deal_bas_r_str = item.get('deal_bas_r')

                    if cur_unit == currency_code and deal_bas_r_str:
                        numeric_rate = float(deal_bas_r_str.replace(',', ''))
                        new_data.append({'날짜': search_date, '통화코드': cur_unit, '현재환율': numeric_rate})
                        fetched_count += 1
                        print(f"  > [{search_date}] 신규 데이터 수집 완료. (추가 확보: {fetched_count}/{days_needed}일)")
                        break 
        
        except requests.exceptions.RequestException as e:
            print(f"❌ [{search_date}] API 요청 오류 발생: {e}. 반복 중단.")
            break 
            
        time.sleep(0.1) 

    return pd.DataFrame(new_data)


# --- 4. 메인 분석 함수 (외부 참조용) ---

def get_50day_ma_data(api_key):
    """
    환율 데이터를 수집 및 갱신하고, 50일 이동평균(MA)을 계산한 DataFrame을 반환합니다.
    R값 계산 로직은 포함하지 않습니다.
    """
    
    TARGET_CURRENCIES = get_target_currencies() # 통화 코드 목록 로드
    all_ma_results = []
    
    for currency_code in TARGET_CURRENCIES:
        file_path = setup_database(currency_code)
        existing_df = load_db_data(file_path) # 이전 데이터 로드 (문자열 '날짜')
        
        existing_dates = set(existing_df['날짜'].unique()) if not existing_df.empty else set()
        current_data_count = len(existing_df)
        needed_days = DAYS_TO_FETCH - current_data_count
        
        updated_df = existing_df.copy()

        # 1. 데이터 수집 및 갱신 (최적화)
        if needed_days > 0:
            new_df = fetch_optimized_data(api_key, currency_code, existing_dates, needed_days)
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            print(f"✅ [{currency_code}] DB에 충분한 데이터({current_data_count}일) 존재. API 호출 건너뜃니다.")

        
        # 2. 이동평균 계산
        if len(updated_df) >= MIN_PERIODS:
            
            # MA 계산을 위해 날짜를 datetime 객체로 변환하고 오름차순 정렬
            updated_df['날짜'] = pd.to_datetime(updated_df['날짜'])
            updated_df = updated_df.sort_values(by='날짜', ascending=True).reset_index(drop=True)
            
            # 50일 MA 계산
            updated_df['50일_MA'] = updated_df['현재환율'].rolling(window=DAYS_TO_FETCH, min_periods=MIN_PERIODS).mean()
            
            # 3. 데이터베이스 저장 (다음 실행을 위해 갱신)
            # (save_db_data 내부에서 날짜를 다시 문자열로 변환하여 저장)
            save_db_data(updated_df, file_path)

            # 4. 반환을 위한 데이터 준비 (R값 계산에 필요한 최종 데이터)
            latest_ma_data = updated_df.iloc[-1]
            
            # 필요한 컬럼만 추출하여 리스트에 추가 (날짜는 문자열로 변환)
            all_ma_results.append({
                '통화코드': currency_code,
                '날짜': latest_ma_data['날짜'].strftime('%Y%m%d'),
                '현재환율': latest_ma_data['현재환율'],
                '50일_MA': latest_ma_data['50일_MA']
            })

        else:
            print(f"⚠️ [{currency_code}] 데이터가 최소 {MIN_PERIODS}일 미만이라 이동평균 계산 불가. ({len(updated_df)}일)")

    return pd.DataFrame(all_ma_results)

if __name__ == "__main__":
    API_KEY, _, _ = load_api_key()
    
    print("50일 이동평균 데이터 수집 및 갱신을 시작합니다...")
    result_df = get_50day_ma_data(API_KEY)
    
    if not result_df.empty:
        print("\n[최종 50일 이동평균 데이터 (R값 계산용)]")
        # R값은 계산하지 않고, 필요한 데이터만 출력
        print(result_df[['통화코드', '날짜', '현재환율', '50일_MA']].to_markdown(index=False, floatfmt=".2f"))
    else:
        print("수집된 데이터가 없습니다.")