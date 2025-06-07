# 📈 **위클리 옵션 코드 매핑 가이드**

## 🎯 **개요**

이 문서는 KOSPI200 위클리 옵션의 종목코드 매핑 시스템과 시간이 지나면서 새로운 위클리 옵션 코드를 관리하는 방법을 설명합니다.

## 📊 **위클리 옵션 구조**

### **기본 정보**
- **위클리 옵션**: 일반 옵션보다 짧은 만기를 가진 옵션
- **만료일**: 월요일(L 타입) 또는 목요일(N 타입)
- **종목코드 형태**: `시리즈코드` + `W` + `행사가`

### **타입별 분류**
| 타입 | 만료일 | 종목코드 패턴 | 설명 |
|------|--------|---------------|------|
| **L 타입** | 월요일 | `209DXW320` (콜), `209DYW320` (풋) | 2506W1 (2025년 6월 1주차) |
| **N 타입** | 목요일 | `2AF97W320` | 2506W2 (2025년 6월 2주차) |

## 🔧 **코드 매핑 시스템**

### **1. YAML 설정 기반 관리**

```yaml
# config/features.yaml
domestic_weekly_options_price:
  code_list:
    # L 타입 (월요일 만료, 2506W1)
    - "209DXW320"  # 콜 320
    - "209DXW325"  # 콜 325
    - "209DYW320"  # 풋 320
    - "209DYW325"  # 풋 325
    # N 타입 (목요일 만료, 2506W2)
    - "2AF97W320"  # 320
    - "2AF97W325"  # 325
```

### **2. CSV 기반 업데이트**

```python
# 최신 거래 종목 CSV에서 자동 업데이트
feature = DomesticWeeklyOptionsPrice(...)
updated_codes = feature.update_weekly_codes_from_csv("fo_idx_code_mts.csv")
print(f"업데이트된 위클리 옵션: {len(updated_codes)}개")
```

## 📅 **시간별 관리 전략**

### **Phase 1: 현재 (2025년 6월)**
```yaml
# config/features.yaml
domestic_weekly_options_price:
  code_list:
    # L 타입 (월요일 만료, 2506W1)
    - "209DXW320"
    - "209DXW325"
    - "209DXW330"
    # N 타입 (목요일 만료, 2506W2)  
    - "2AF97W320"
    - "2AF97W325"
    - "2AF97W330"
```

### **Phase 2: 향후 확장 (2025년 7월~)**
```python
# 새로운 위클리 옵션 상장 시 자동 감지 및 추가
def auto_update_weekly_codes():
    """신규 위클리 옵션 자동 감지 및 설정 업데이트"""
    
    # 1. KIS API에서 최신 종목 정보 조회
    latest_symbols = get_latest_option_symbols()
    
    # 2. 위클리 옵션 필터링
    weekly_symbols = filter_weekly_options(latest_symbols)
    
    # 3. 설정 파일 자동 업데이트
    update_features_config(weekly_symbols)
    
    # 4. 로그 및 알림
    notify_new_weekly_options(weekly_symbols)
```

## 🔄 **자동화 워크플로우**

### **일일 체크 시스템**
```python
# cron job 또는 스케줄러에 등록
# 매일 09:00에 실행
def daily_weekly_options_check():
    """위클리 옵션 일일 체크 및 업데이트"""
    
    try:
        # 1. 현재 설정된 위클리 옵션 확인
        current_codes = load_current_weekly_codes()
        
        # 2. 거래소 최신 정보와 비교
        latest_codes = fetch_latest_weekly_codes()
        
        # 3. 신규/만료 종목 감지
        new_codes = set(latest_codes) - set(current_codes)
        expired_codes = set(current_codes) - set(latest_codes)
        
        # 4. 자동 업데이트 또는 알림
        if new_codes or expired_codes:
            update_weekly_codes_config(latest_codes)
            send_notification(new_codes, expired_codes)
            
    except Exception as e:
        log_error(f"위클리 옵션 일일 체크 실패: {e}")
```

## 📋 **실무 관리 가이드**

### **1. 신규 위클리 옵션 상장 시**

#### **Step 1: 종목 발견**
```bash
# KIS 종목 정보 API 호출로 신규 위클리 옵션 확인
python -c "
from src.data_collection.domestic_weekly_options_price import DomesticWeeklyOptionsPrice
feature = DomesticWeeklyOptionsPrice(...)
new_codes = feature.update_weekly_codes_from_csv('latest_fo_idx_code_mts.csv')
print('신규 위클리 옵션:', new_codes)
"
```

#### **Step 2: 설정 업데이트**
```yaml
# config/features.yaml 수정
domestic_weekly_options_price:
  code_list:
    # 기존 코드들...
    # 새로 추가된 코드들
    - "새로운코드1"
    - "새로운코드2"
```

#### **Step 3: 테스트 실행**
```bash
# 새 코드로 데이터 수집 테스트
python src/data_collection/run_data_collector.py \
  --features domestic_weekly_options_price \
  --test
```

### **2. 만료 위클리 옵션 정리**

#### **자동 정리 스크립트**
```python
def cleanup_expired_weekly_options():
    """만료된 위클리 옵션 자동 정리"""
    
    current_date = datetime.now()
    
    # 1. 현재 설정된 코드들 확인
    codes = load_weekly_codes_from_config()
    
    # 2. 각 코드의 만료일 계산
    active_codes = []
    for code in codes:
        expiry_date = calculate_weekly_option_expiry(code)
        if expiry_date > current_date:
            active_codes.append(code)
    
    # 3. 활성 코드만 설정 파일에 저장
    update_config_with_active_codes(active_codes)
    
    print(f"정리 완료: {len(codes)} -> {len(active_codes)}개 코드")
```

## 🚨 **주의사항 및 팁**

### **코드 패턴 변경 대응**
```python
# 거래소에서 코드 패턴이 변경될 수 있으므로 유연한 설계 필요
def detect_weekly_pattern_changes():
    """위클리 옵션 코드 패턴 변경 감지"""
    
    # 1. 알려진 패턴과 실제 거래 종목 비교
    known_patterns = ["209DXW", "209DYW", "2AF97W"]
    actual_codes = get_current_weekly_codes_from_exchange()
    
    # 2. 새로운 패턴 감지
    new_patterns = []
    for code in actual_codes:
        if not any(code.startswith(pattern) for pattern in known_patterns):
            new_patterns.append(extract_pattern(code))
    
    # 3. 알림 및 수동 검토 요청
    if new_patterns:
        alert_pattern_changes(new_patterns)
```

### **데이터 품질 모니터링**
```python
def monitor_weekly_options_data_quality():
    """위클리 옵션 데이터 품질 모니터링"""
    
    # 1. 거래량이 0인 종목 체크
    zero_volume_codes = check_zero_volume_options()
    
    # 2. 시세 데이터 이상치 체크
    price_anomalies = detect_price_anomalies()
    
    # 3. API 응답 실패율 체크
    api_failure_rate = calculate_api_failure_rate()
    
    # 4. 종합 리포트 생성
    generate_quality_report(zero_volume_codes, price_anomalies, api_failure_rate)
```

## 📞 **지원 및 문의**

위클리 옵션 코드 매핑과 관련된 문의사항이나 이슈가 있으시면:

1. **코드 이슈**: GitHub Issues에 보고
2. **긴급 장애**: 실시간 모니터링 알림 확인
3. **새로운 패턴**: 수동 검토 후 시스템 업데이트

---
*이 가이드는 지속적으로 업데이트됩니다. 위클리 옵션 시장의 변화에 따라 매핑 규칙이 변경될 수 있습니다.* 