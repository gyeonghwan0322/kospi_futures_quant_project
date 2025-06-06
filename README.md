kospi_futures_quant_project/
├── 📁 config/                     ✅ 설정 파일들
│   ├── api_config.yaml
│   ├── params.yaml 
│   └── features.yaml
├── 📁 data/                       ✅ 데이터 관련
│   ├── raw/                       (빈 폴더 - 원본 데이터용)
│   ├── processed/                 (빈 폴더 - 전처리된 데이터용)
│   └── reference/                 ✅ 참조 데이터
│       ├── fo_idx_code_mts.csv
│       └── fo_com_code.csv
├── 📁 src/                        ✅ 소스 코드 (구 sushi 패키지)
│   ├── data_collection/           ✅ 1단계: 데이터 수집
│   │   ├── __init__.py
│   │   ├── api_client.py
│   │   ├── domestic_futures_price.py
│   │   ├── domestic_futures_minute.py
│   │   └── run_data_collector.py
│   ├── feature_engineering/       ✅ 2-3단계: 피처 엔지니어링
│   │   ├── __init__.py
│   │   ├── abstract_feature.py
│   │   └── feature_manager.py
│   ├── modeling/                  ✅ 3-4단계: 모델링 및 레이블링 (준비됨)
│   ├── backtesting/               ✅ 5-6단계: 백테스팅
│   │   ├── __init__.py
│   │   └── investor_buy.py
│   └── utils/                     ✅ 공통 유틸리티 (준비됨)
├── 📁 notebooks/                  ✅ 주피터 노트북용 (준비됨)
├── 📁 tests/                      ✅ 테스트 코드용 (준비됨)
├── 📁 docs/                       ✅ 문서
│   └── Roadmap.md
└── 📁 results/                    ✅ 결과물 저장용 (준비됨)
    ├── models/
    ├── plots/
    └── reports/