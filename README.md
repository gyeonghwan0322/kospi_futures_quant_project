# KOSPI Futures Quantitative Trading Project (Sushi)

## 📋 프로젝트 개요
한국투자증권 API를 활용한 KOSPI 선물 정량 거래 시스템입니다. (Sushi 버전)

## 🏗️ 현재 구조 문제점 및 개선 제안

### 현재 구조
```
kospi_futures_quant_project/
├── config/           # 설정 파일들
├── database/         # DB 관리 모듈
├── data/             # 피처 및 API 클라이언트 모듈 (문제: 잘못된 위치)
├── src/              # 비어있음
├── notebooks/        # Jupyter notebooks
├── results/          # 결과 파일들
└── run_data_collector.py  # 메인 실행 스크립트
```

### 🚨 주요 문제점
1. **Import 경로 불일치**: `sushi` 패키지를 import하지만 실제 폴더 구조와 맞지 않음
2. **모듈 배치 혼란**: 핵심 기능이 `data/` 폴더에 위치
3. **패키지 구조 부재**: 적절한 Python 패키지 구조가 없음

### 💡 권장 개선 구조
```
kospi_futures_quant_project/
├── sushi/                     # 메인 패키지
│   ├── __init__.py
│   ├── feature/               # 피처 관련 모듈
│   │   ├── __init__.py
│   │   ├── abstract_feature.py
│   │   ├── api_client.py
│   │   ├── feature_manager.py
│   │   └── features/          # 개별 피처들
│   │       ├── __init__.py
│   │       ├── domestic_futures_minute.py
│   │       └── investor_buy.py
│   ├── database/              # 데이터베이스 모듈
│   │   ├── __init__.py
│   │   └── db_manager.py
│   └── utils/                 # 유틸리티 함수들
│       └── __init__.py
├── config/                    # 설정 파일들
├── data/                      # 데이터 파일들 (CSV 등)
├── notebooks/                 # 분석 노트북
├── tests/                     # 테스트 코드
├── scripts/                   # 실행 스크립트들
│   └── run_data_collector.py
├── requirements.txt
└── setup.py
```

## 🚀 설치 및 실행

### 1. 환경 설정
```bash
# 가상환경 생성
python -m venv .venv

# 가상환경 활성화 (Windows)
.venv\Scripts\activate

# 가상환경 활성화 (Linux/Mac)
source .venv/bin/activate

# 의존성 설치
pip install -r requirements.txt
```

### 2. 설정 파일 확인
- `config/api_config.yaml`: 한국투자증권 API 키 설정
- `config/db_config.yaml`: PostgreSQL 데이터베이스 설정
- `config/features.yaml`: 피처 설정
- `config/params.yaml`: 매개변수 설정

### 3. 실행
```bash
# 전체 피처 수집
python run_data_collector.py

# 특정 피처만 수집
python run_data_collector.py -f "domestic_futures_minute,investor_buy"

# 테스트 모드 (DB 저장 없이)
python run_data_collector.py --test

# 스케줄된 피처만 실행
python run_data_collector.py --scheduled

# 또는 설치 후 명령어 사용
sushi-collect --test
```

## 📦 주요 의존성
- **pandas**: 데이터 분석 및 조작
- **psycopg2**: PostgreSQL 데이터베이스 연결
- **requests**: HTTP API 호출
- **PyYAML**: 설정 파일 관리
- **pandas-ta**: 기술 분석 지표 (선택사항)

## ⚠️ 즉시 개선 필요 사항
1. 패키지 구조 재정리 (`sushi` 폴더 생성 및 모듈 이동)
2. Import 경로 수정 (`ignacio` → `sushi`)
3. `__init__.py` 파일 추가
4. 테스트 코드 작성
5. 적절한 `setup.py` 파일 생성

