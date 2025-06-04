# 🍣 Sushi 퀀트 프로젝트 시작 가이드

## 🚀 **즉시 시작하기**

### 1. **환경 설정 (5분)**
```bash
# 가상환경 활성화
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# 한번에 모든 설정 완료
make quant-start
```

이 명령어 하나로 다음이 모두 실행됩니다:
- ✅ 프로젝트 구조 마이그레이션
- ✅ 개발 환경 설정
- ✅ 첫 번째 태스크 파일 생성
- ✅ 데이터 수집 테스트

### 2. **오늘의 작업 시작**
```bash
# 오늘의 태스크 파일 열기
make today-task

# 또는 직접 편집
code docs/daily_tasks/2024/01-january/day_01.md
```

## 📋 **1일차 태스크 체크리스트**

현재 Sushi 프로젝트는 이미 기본 구조가 완료되어 있으므로, 바로 퀀트 분석을 위한 확장 작업을 시작할 수 있습니다:

### ✅ **완료된 작업** (기존 Sushi 프로젝트)
- [x] 한국투자증권 API 연동
- [x] 기본 데이터 수집 시스템
- [x] PostgreSQL 데이터베이스 연동
- [x] 프로젝트 구조 및 패키지화

### 🔥 **오늘 해야 할 핵심 작업**

#### 1. **프로젝트 구조 마이그레이션 (1시간)**
```bash
# 백업 생성 후 실행
python migrate_structure.py
make install-dev
make collect-test
```

#### 2. **추가 데이터 피처 개발 (4시간)**
- [ ] KOSPI 200 지수 일별 데이터 피처 생성
- [ ] 옵션 데이터 피처 개발 (변동성 분석용)
- [ ] 투자자별 매매동향 데이터 확장
- [ ] 분봉 데이터 수집 로직 개선

#### 3. **데이터 파이프라인 개선 (2시간)**
- [ ] 데이터 품질 검증 로직 추가
- [ ] 결측치 자동 처리 시스템
- [ ] 이상치 탐지 및 알림 시스템

#### 4. **분석 환경 설정 (1시간)**
- [ ] Jupyter Lab 설정 및 템플릿 생성
- [ ] 기본 시각화 대시보드 구축
- [ ] 퀀트 분석 노트북 템플릿

## 🛠️ **개발 도구 활용**

### **Makefile 명령어 전체 목록**
```bash
make help              # 사용 가능한 명령어 확인
make collect           # 데이터 수집
make collect-test      # 테스트 모드 데이터 수집
make format            # 코드 포맷팅
make lint              # 코드 검사
make jupyter           # Jupyter Lab 실행
make new-task          # 새로운 일일 태스크 생성
make progress          # 진행률 확인
```

### **태스크 관리 시스템**
```bash
# 새로운 태스크 파일 생성
make new-task

# 오늘의 태스크 열기
make today-task

# 진행률 확인
make progress
```

## 📊 **데이터 수집 현황**

### **현재 사용 가능한 피처들**
1. **domestic_futures_minute**: KOSPI 선물 분봉 데이터
2. **investor_buy**: 투자자별 매매동향
3. **domestic_options_open_interest**: 옵션 미결제약정
4. **overseas_futures_minute**: 해외선물 분봉 데이터

### **데이터 테스트**
```bash
# 모든 피처 테스트
make collect-test

# 특정 피처만 테스트
python scripts/run_data_collector.py -f "domestic_futures_minute" --test
```

## 🎯 **성공을 위한 팁**

### **1. 효율적인 작업 순서**
1. 🏗️ **구조 먼저**: 마이그레이션부터 완료
2. 🔍 **테스트 우선**: 새로운 기능 개발 전 기존 시스템 검증
3. 📝 **문서화**: 각 단계마다 학습 내용 기록
4. 🔄 **점진적 개발**: 한 번에 하나씩 피처 추가

### **2. 시간 관리**
- ⏰ **포모도로 기법**: 25분 집중 + 5분 휴식
- 📊 **진행률 추적**: 매시간 태스크 상태 업데이트
- 🎯 **우선순위**: HIGH → MEDIUM → LOW 순서

### **3. 문제 해결**
```bash
# 문제 발생 시 로그 확인
tail -f logs/data_collector_$(date +%Y%m%d).log

# 환경 재설정
make clean
make dev-setup
```

## 📚 **참고 자료**

### **프로젝트 문서**
- `docs/PROJECT_ROADMAP.md`: 전체 프로젝트 로드맵
- `docs/daily_tasks/README.md`: 태스크 관리 시스템 가이드
- `README.md`: 프로젝트 개요 및 설정

### **설정 파일들**
- `config/api_config.yaml`: 한국투자증권 API 설정
- `config/features.yaml`: 피처 설정
- `config/params.yaml`: 매개변수 설정
- `config/db_config.yaml`: 데이터베이스 설정

### **MCP 서버 활용**
- `mcp_config.json`: Cursor/Claude에서 사용할 MCP 서버 설정
- Task Manager MCP: 프로젝트 태스크 관리 자동화

## 🚨 **주의사항**

1. **백업**: 중요한 작업 전에는 반드시 백업
2. **API 키**: `config/api_config.yaml`의 실제 키는 보안 유지
3. **데이터베이스**: PostgreSQL 서비스 실행 상태 확인
4. **로그 모니터링**: 데이터 수집 중 에러 발생 여부 확인

## 🎉 **첫 날 목표 달성 기준**

- ✅ 프로젝트 구조 마이그레이션 완료
- ✅ 최소 2개 이상의 새로운 피처 개발
- ✅ 데이터 품질 검증 시스템 구축
- ✅ Jupyter 분석 환경 설정
- ✅ 내일 계획 수립

---

**화이팅! 성공적인 퀀트 프로젝트의 시작입니다!** 🍣🚀

질문이나 문제가 있으면 언제든지 태스크 파일에 기록하고 다음 단계에서 해결하세요. 