# 📅 Sushi 프로젝트 일일 태스크 관리 시스템

## 🎯 **목적**
매일의 작업을 체계적으로 관리하고 진행 상황을 추적하여 프로젝트 목표 달성을 지원합니다.

## 📁 **파일 구조**
```
docs/daily_tasks/
├── README.md              # 이 파일 (사용법 안내)
├── templates/             # 템플릿 파일들
│   ├── daily_task_template.md
│   └── weekly_review_template.md
├── 2024/                  # 년도별 폴더
│   ├── 01-january/        # 월별 폴더
│   │   ├── day_01.md      # 일별 태스크 파일
│   │   ├── day_02.md
│   │   └── ...
│   └── weekly_reviews/    # 주간 리뷰
│       ├── week_01.md
│       └── ...
└── progress_tracker.md    # 전체 진행 상황 추적
```

## 🚀 **사용법**

### 1. **새로운 날짜 태스크 파일 생성**
```bash
# Makefile 명령어 사용
make new-task

# 또는 수동으로
cp docs/daily_tasks/templates/daily_task_template.md docs/daily_tasks/2024/01-january/day_$(date +%d).md
```

### 2. **태스크 상태 관리**
- ⏳ `TODO`: 계획된 작업
- 🔄 `IN_PROGRESS`: 진행 중인 작업  
- ✅ `COMPLETED`: 완료된 작업
- ❌ `BLOCKED`: 블로킹된 작업
- 🔄 `DEFERRED`: 연기된 작업

### 3. **우선순위 시스템**
- 🔥 `HIGH`: 반드시 오늘 완료해야 하는 작업
- 📊 `MEDIUM`: 중요하지만 조금 유연한 작업
- 💡 `LOW`: 시간이 남을 때 하는 작업

## 📊 **진행률 추적**
각 일별 파일에서 작업 완료율을 계산하고 `progress_tracker.md`에 기록합니다.

## 🔗 **관련 도구**
- **MCP Server**: 프로젝트 관리 자동화
- **GitHub Issues**: 코드 관련 태스크 연동
- **Makefile**: 자동화된 태스크 생성 