import os
import sys

# src/api 폴더에서 data/index.py에 접근하기 위해 프로젝트 루트 경로를 추가
# (프로젝트 루트가 src의 상위 폴더와 동일하다고 가정)
try:
    # 현재 파일의 경로에서 두 번 상위 폴더로 이동 (src/api -> src -> root)
    # 실제 실행 환경에 따라 이 경로 설정이 달라질 수 있습니다.
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from data.index import country_map 
except ImportError:
    # data/index.py가 로드되지 않을 경우를 위한 임시 데이터
    country_map = {
        "미국": "USD", 
        "일본": "JPY(100)", 
        "유럽연합": "EUR", 
        "중국": "CNY",
        "호주": "AUD"
    }
    print("⚠️ data/index.py에서 country_map을 찾을 수 없어 임시 통화 코드를 사용합니다.")

def get_target_currencies():
    """
    country_map에서 value(통화 코드)를 추출하여 중복 없는 리스트로 반환합니다.
    """
    # country_map의 value(통화 코드)만 추출하여 중복 제거
    target_currencies = list(set(country_map.values()))
    
    print(f"✅ Country_loader: 총 {len(target_currencies)}개의 통화 코드 로드 완료.")
    
    return target_currencies

if __name__ == "__main__":
    get_target_currencies()