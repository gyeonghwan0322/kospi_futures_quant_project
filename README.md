# 🚀 KOSPI Futures Quantitative Trading System

> **KOSPI200 선물 및 옵션 데이터를 활용한 체계적인 퀀트 트레이딩 시스템**  
> 실시간 데이터 수집부터 백테스팅까지 End-to-End 퀀트 전략 개발 플랫폼

## 📋 프로젝트 개요

본 프로젝트는 **한국투자증권 OpenAPI**를 활용하여 KOSPI200 선물, VKOSPI 선물, 위클리 옵션 등의 금융 데이터를 체계적으로 수집하고, 이를 기반으로 퀀트 트레이딩 전략을 개발하는 **종합적인 금융 데이터 분석 시스템**입니다.

## ✨ 주요 특징

### 🔄 **지능형 데이터 수집 시스템**
- **다중 데이터 소스**: KOSPI200 선물, VKOSPI 선물, 위클리 옵션, 투자자 매매동향
- **유연한 스케줄링**: YAML 설정 기반 시간별/일별 자동 수집
- **에러 복구**: API 한도, 네트워크 오류 등에 대한 robust한 처리
- **중복 데이터 방지**: 날짜 기반 지능형 중복 제거 알고리즘

### ⚙️ **설정 기반 아키텍처**
- **YAML 설정 관리**: 코드 수정 없이 설정 파일만으로 데이터 수집 대상/주기 변경
- **동적 스키마 처리**: 콜/풋 옵션 등 상품별 서로 다른 API 응답 스키마 자동 처리
- **심볼 타입 자동 인식**: 정규식 패턴 매칭을 통한 종목 코드 분류 및 파라미터 자동 생성

### 🛡️ **Enterprise급 안정성**
- **계층화된 로깅**: 수집 상태, 오류, 성능 지표의 체계적 모니터링
- **데이터 검증**: 수집된 데이터의 무결성 및 일관성 자동 검증
- **Graceful Error Handling**: 부분 실패 시에도 시스템 전체 중단 방지

## 🏗️ 시스템 아키텍처

```
📦 KOSPI Futures Quant System
│
├── 🔌 Data Collection Layer
│   ├── Korean Investment API Client
│   ├── Multi-Symbol Data Fetcher  
│   ├── Schema-Aware Parser
│   └── Intelligent Deduplication
│
├── ⚙️ Configuration Management
│   ├── YAML-based Settings
│   ├── Dynamic Parameter Generation
│   ├── Symbol Pattern Recognition
│   └── API Endpoint Mapping
│
├── 📊 Data Processing Pipeline
│   ├── Real-time Data Normalization
│   ├── Historical Data Aggregation
│   ├── Missing Data Interpolation
│   └── Quality Assurance Checks
│
├── 🧠 Strategy Development
│   ├── Feature Engineering Framework
│   ├── Signal Generation Engine
│   ├── Risk Management Module
│   └── Portfolio Optimization
│
└── 📈 Backtesting & Analysis
    ├── Historical Performance Testing
    ├── Risk Metrics Calculation
    ├── Visualization & Reporting
    └── Strategy Comparison
```

## 📁 프로젝트 구조

```
kospi_futures_quant_project/
├── 📁 config/                     # 설정 관리
│   ├── api_config.yaml            # API 엔드포인트, TR ID 매핑
│   ├── params.yaml                # 수집 파라미터 설정
│   └── features.yaml              # 피처 및 스케줄 정의
│
├── 📁 data/                       # 데이터 저장소
│   ├── domestic_futures/          # 국내 선물 데이터
│   ├── domestic_options/          # 국내 옵션 데이터
│   ├── market_data/               # 시장 지표 데이터
│   └── reference/                 # 참조 데이터 (종목 코드 등)
│
├── 📁 src/                        # 핵심 소스 코드
│   ├── data_collection/           # 데이터 수집 모듈
│   │   ├── domestic_futures_price.py      # 선물 일별 시세
│   │   ├── domestic_futures_minute.py     # 선물 분봉 데이터
│   │   ├── domestic_weekly_options_price.py # 위클리 옵션
│   │   ├── investor_buy.py               # 투자자 매매동향
│   │   └── run_data_collector.py         # 수집 실행 엔진
│   │
│   ├── feature_engineering/       # 피처 엔지니어링
│   │   ├── abstract_feature.py    # 피처 기본 클래스
│   │   └── feature_manager.py     # 피처 관리자
│   │
│   ├── utils/                     # 공통 유틸리티
│   │   ├── api_config_manager.py  # 설정 관리자
│   │   └── logging_config.py      # 로깅 설정
│   │
│   ├── modeling/                  # 모델링 (개발 예정)
│   ├── backtesting/              # 백테스팅 (개발 예정)
│   └── visualization/            # 시각화 (개발 예정)
│
├── 📁 logs/                       # 로그 파일
├── 📁 notebooks/                  # 분석 노트북
├── 📁 tests/                      # 테스트 코드
└── 📁 results/                    # 분석 결과
```

## 🚀 핵심 기능 상세

### 1. 🔄 자동화된 데이터 수집

```python
# 스케줄 기반 자동 수집 (features.yaml)
domestic_futures_price:
  inquiry: true
  inquiry_time_list: ['090500', '120000', '150000', '180000']
  code_list: ['101V06', '101W03', '101W06', '101W09']
```

### 2. 🧠 지능형 심볼 인식

```python
# 정규식 패턴으로 자동 분류 (api_config.yaml)
symbol_patterns:
  futures:
    futures_kospi200: "^10[1-9][A-Z][0-9]{2}$"    # 101V06, 101W03
    futures_continuous: "^101000$"                 # 연속선물
  options:
    call_weekly: "^2[0-9A-F]{2}[A-Z]{2}W[0-9]{3}$"    # 209DXW320
    put_weekly: "^3[0-9A-F]{2}[A-Z]{2}W[0-9]{3}$"     # 309DYW320
```

### 3. 📊 스키마 기반 데이터 처리

```python
# 콜/풋 옵션 스키마 차이 자동 처리
data_schemas:
  call_options: ['stck_bsop_date', 'futs_prpr', 'futs_oprc', ...]
  put_options: ['futs_prpr', 'futs_oprc', 'futs_hgpr', ...]  # 날짜 컬럼 없음
```

## 📊 수집 데이터 현황

| 데이터 유형 | 수집 주기 | 종목 수 | 상태 |
|------------|----------|---------|------|
| **KOSPI200 선물 일별** | 4회/일 | 5개 | ✅ 정상 |
| **KOSPI200 연속선물** | 4회/일 | 1개 | ✅ 정상 |
| **VKOSPI 선물** | 4회/일 | 2개 | ✅ 정상 |
| **위클리 옵션 시세** | 4회/일 | 15개 | ✅ 정상 |
| **투자자 매매동향** | 실시간 | 5개 시장 | ✅ 정상 |
| **옵션 전광판** | 4회/일 | 9개 만기월 | ✅ 정상 |

## 🛠️ 기술 스택

### **Backend & Core**
- ![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white) **Python 3.8+**
- ![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white) **pandas** - 데이터 처리 및 분석
- ![NumPy](https://img.shields.io/badge/NumPy-013243?style=flat-square&logo=numpy&logoColor=white) **NumPy** - 수치 연산
- ![PyYAML](https://img.shields.io/badge/PyYAML-FF6B6B?style=flat-square) **PyYAML** - 설정 관리

### **Data & API**
- ![REST API](https://img.shields.io/badge/REST_API-02569B?style=flat-square&logo=rest&logoColor=white) **한국투자증권 OpenAPI**
- ![JSON](https://img.shields.io/badge/JSON-000000?style=flat-square&logo=json&logoColor=white) **JSON** - 데이터 교환 형식
- ![CSV](https://img.shields.io/badge/CSV-217346?style=flat-square&logo=microsoft-excel&logoColor=white) **CSV** - 데이터 저장 형식

### **Development & Tools**
- ![Git](https://img.shields.io/badge/Git-F05032?style=flat-square&logo=git&logoColor=white) **Git** - 버전 관리
- ![Logging](https://img.shields.io/badge/Logging-4B8BBE?style=flat-square) **Python Logging** - 시스템 모니터링
- ![Regex](https://img.shields.io/badge/RegEx-FF6B35?style=flat-square) **정규식** - 패턴 매칭

