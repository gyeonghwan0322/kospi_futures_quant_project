# 증분 데이터 수집 가이드

## 개요

모든 피처 클래스에서 증분 데이터 수집을 쉽게 사용할 수 있도록 `Feature` 추상 클래스에 공통 메서드들을 추가했습니다. 이를 통해 API 호출량을 크게 줄이고 데이터 수집 효율성을 높일 수 있습니다.

## 주요 기능

### 1. 동적 날짜 범위 계산
- 기존 데이터의 마지막 날짜를 확인하여 필요한 구간만 조회
- 최신 상태인 경우 API 호출을 건너뜀
- 설정 가능한 최대 백데이트 기간 (기본값: 90일)

### 2. 증분 데이터 저장
- 기존 CSV 파일과 새 데이터를 자동으로 병합
- 중복 데이터 제거 및 정렬
- 자동 백업 생성
- 메타데이터 관리

### 3. 표준화된 API 호출 패턴
- 날짜 범위 분할 (API 100건 제한 대응)
- 파라미터 빌딩
- 오류 처리
- 로깅

## 사용 방법

### 기본 패턴 (권장)

가장 간단한 방법은 `perform_incremental_inquiry` 메서드를 사용하는 것입니다:

```python
def _perform_inquiry(self, clock: str):
    # 증분 업데이트를 사용한 데이터 조회
    collected_data = self.perform_incremental_inquiry(
        clock=clock,
        api_name=self.API_NAME,
        tr_id=self.get_tr_id(self.API_NAME),
        date_param_names=("FID_INPUT_DATE_1", "FID_INPUT_DATE_2"),
        code_param_name="FID_INPUT_ISCD",
        max_days_per_request=90,
        pagination_delay_sec=0.2
    )

    # 수집된 데이터 저장
    if collected_data:
        save_results = self.save_incremental_data(
            data_dict=collected_data,
            date_column="stck_bsop_date",
            backup_enabled=True
        )
        
        # 메모리에도 저장
        for code, data in collected_data.items():
            self.futures_data[code] = data
```

### 고급 패턴 (커스터마이징)

더 세밀한 제어가 필요한 경우:

```python
def _perform_inquiry(self, clock: str):
    # 1. 동적 날짜 범위 계산
    date_ranges_per_code = self.get_dynamic_date_range(
        max_days_back=60,  # 커스텀 백데이트 기간
        force_full_update=False
    )
    
    collected_data = {}
    
    for code in self.code_list:
        start_date, end_date = date_ranges_per_code.get(
            code, (self.start_date, self.end_date)
        )
        
        # 업데이트 불필요한 경우 건너뛰기
        if start_date > end_date:
            continue
            
        # 커스텀 API 호출 로직
        # ... 
        
        collected_data[code] = data
    
    # 2. 증분 저장
    if collected_data:
        self.save_incremental_data(
            data_dict=collected_data,
            date_column="trade_date",  # 커스텀 날짜 컬럼
            time_column="trade_time",  # 시간 컬럼 추가
            backup_enabled=True
        )
```

## 클래스별 구현 예시

### DomesticFuturesPrice (이미 적용됨)

```python
class DomesticFuturesPrice(Feature):
    def _get_additional_api_params(self) -> Dict[str, str]:
        """클래스별 고유 파라미터"""
        return {
            "FID_COND_MRKT_DIV_CODE": self.market_code,
            "FID_PERIOD_DIV_CODE": self.period_code,
        }

    def _perform_inquiry(self, clock: str):
        collected_data = self.perform_incremental_inquiry(
            clock=clock,
            api_name=self.API_NAME,
            tr_id=self.get_tr_id(self.API_NAME),
            date_param_names=("FID_INPUT_DATE_1", "FID_INPUT_DATE_2"),
            code_param_name="FID_INPUT_ISCD",
            max_days_per_request=self.max_days_per_request,
            pagination_delay_sec=self.pagination_delay_sec
        )
        
        if collected_data:
            self.save_incremental_data(
                data_dict=collected_data,
                date_column="stck_bsop_date"
            )
```

### 새로운 클래스에 적용하기

```python
class YourNewFeature(Feature):
    def _get_additional_api_params(self) -> Dict[str, str]:
        """클래스별 고유 파라미터 정의"""
        return {
            "YOUR_PARAM_1": self.param1,
            "YOUR_PARAM_2": self.param2,
        }

    def _perform_inquiry(self, clock: str):
        collected_data = self.perform_incremental_inquiry(
            clock=clock,
            api_name="Your API Name",
            tr_id="YOUR_TR_ID",
            date_param_names=("START_DATE_PARAM", "END_DATE_PARAM"),
            code_param_name="CODE_PARAM",
            max_days_per_request=90,
            pagination_delay_sec=0.2
        )
        
        if collected_data:
            self.save_incremental_data(
                data_dict=collected_data,
                date_column="your_date_column"
            )
```

## 이점

1. **API 호출량 절약**: 전체 데이터 대신 증분 데이터만 조회
2. **빠른 업데이트**: 최신 상태 확인으로 불필요한 호출 방지  
3. **안전한 저장**: 자동 백업 및 롤백 기능
4. **일관된 인터페이스**: 모든 피처에서 동일한 패턴 사용
5. **유연한 설정**: 클래스별 커스터마이징 가능

## 메타데이터 구조

```
data/
└── schema_name/
    └── feature_name/
        ├── code1.csv
        ├── code2.csv
        └── .metadata/
            ├── last_update_code1.json
            ├── last_update_code2.json
            ├── update_history_code1.json
            └── update_history_code2.json
```

## 설정 옵션

| 메서드 | 파라미터 | 설명 | 기본값 |
|--------|----------|------|--------|
| `get_dynamic_date_range` | `max_days_back` | 최대 백데이트 기간 | 90일 |
| | `force_full_update` | 강제 전체 업데이트 | False |
| `save_incremental_data` | `date_column` | 날짜 컬럼명 | "stck_bsop_date" |
| | `time_column` | 시간 컬럼명 | None |
| | `backup_enabled` | 백업 생성 여부 | True |
| `perform_incremental_inquiry` | `max_days_per_request` | 요청당 최대 일수 | 90일 |
| | `pagination_delay_sec` | 요청 간 지연 시간 | 0.2초 |

## 문제 해결

### 메타데이터 재설정
```python
# 특정 코드의 메타데이터 삭제 (다음 실행시 전체 수집)
import os
os.remove("data/schema_name/feature_name/.metadata/last_update_code.json")
```

### 강제 전체 업데이트
```python
def _perform_inquiry(self, clock: str):
    collected_data = self.perform_incremental_inquiry(
        # ... 다른 파라미터들
    )
    
    # 또는 동적 날짜 범위에서 강제 설정
    date_ranges = self.get_dynamic_date_range(force_full_update=True)
``` 