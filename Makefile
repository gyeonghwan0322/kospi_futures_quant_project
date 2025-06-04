# Sushi Futures Quantitative Trading Project Makefile

.PHONY: help install install-dev test lint format clean migrate structure new-task today-task progress

# 기본 타겟
help: ## 사용 가능한 명령어 표시
	@echo "Sushi 프로젝트 Makefile 명령어:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## 기본 의존성 설치
	pip install -r requirements.txt
	pip install -e .

install-dev: ## 개발 의존성 포함 설치
	pip install -r requirements.txt
	pip install -e ".[dev,analysis,advanced]"

test: ## 테스트 실행
	pytest

test-verbose: ## 상세한 테스트 실행
	pytest -v

test-coverage: ## 테스트 커버리지 포함 실행
	pytest --cov=sushi --cov-report=html --cov-report=term

lint: ## 코드 스타일 검사
	flake8 sushi scripts tests
	mypy sushi scripts

format: ## 코드 포맷팅
	black sushi scripts tests

format-check: ## 코드 포맷팅 검사만
	black --check sushi scripts tests

clean: ## 임시 파일 정리
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build dist htmlcov .coverage .pytest_cache .mypy_cache

migrate: ## 프로젝트 구조 마이그레이션 실행
	python migrate_structure.py

structure: ## 프로젝트 구조 표시
	tree -I '__pycache__|*.pyc|.git|.venv|node_modules'

# 데이터 수집 관련
collect: ## 전체 피처 데이터 수집
	python scripts/run_data_collector.py

collect-test: ## 테스트 모드로 데이터 수집
	python scripts/run_data_collector.py --test

collect-scheduled: ## 스케줄된 피처만 수집
	python scripts/run_data_collector.py --scheduled

# 태스크 관리
new-task: ## 새로운 일일 태스크 파일 생성
	@echo "📅 새로운 태스크 파일을 생성합니다..."
	@mkdir -p docs/daily_tasks/2024/01-january
	@DAY=$$(date +%d) && \
	DATE=$$(date +%Y-%m-%d) && \
	cp docs/daily_tasks/templates/daily_task_template.md docs/daily_tasks/2024/01-january/day_$$DAY.md && \
	sed -i "s/{DAY_NUMBER}/$$DAY/g" docs/daily_tasks/2024/01-january/day_$$DAY.md && \
	sed -i "s/{DATE}/$$DATE/g" docs/daily_tasks/2024/01-january/day_$$DAY.md && \
	echo "✅ docs/daily_tasks/2024/01-january/day_$$DAY.md 파일이 생성되었습니다!"

today-task: ## 오늘의 태스크 파일 열기
	@DAY=$$(date +%d) && \
	if [ -f docs/daily_tasks/2024/01-january/day_$$DAY.md ]; then \
		echo "📖 오늘의 태스크 파일을 엽니다..."; \
		code docs/daily_tasks/2024/01-january/day_$$DAY.md; \
	else \
		echo "❌ 오늘의 태스크 파일이 없습니다. 'make new-task'로 생성하세요."; \
	fi

progress: ## 프로젝트 전체 진행률 확인
	@echo "📊 Sushi 프로젝트 진행률"
	@echo "=========================="
	@if [ -f docs/daily_tasks/progress_tracker.md ]; then \
		tail -n 10 docs/daily_tasks/progress_tracker.md; \
	else \
		echo "진행률 추적 파일이 없습니다."; \
	fi

# 환경 설정
setup-env: ## 가상환경 설정 가이드 표시
	@echo "가상환경 설정:"
	@echo "1. python -m venv .venv"
	@echo "2. Windows: .venv\\Scripts\\activate"
	@echo "   Linux/Mac: source .venv/bin/activate"
	@echo "3. make install-dev"

# 개발 도구
jupyter: ## Jupyter Lab 실행
	jupyter lab

docs: ## 문서 생성 (향후 추가)
	@echo "문서 생성 기능은 향후 추가 예정입니다."

build: ## 패키지 빌드
	python -m build

publish: ## PyPI에 패키지 게시 (테스트)
	python -m twine upload --repository testpypi dist/*

# 데이터베이스 관련
db-migrate: ## 데이터베이스 마이그레이션 (향후 추가)
	@echo "데이터베이스 마이그레이션 기능은 향후 추가 예정입니다."

# 보안 체크
security-check: ## 보안 취약점 검사
	pip-audit

# 전체 CI 파이프라인
ci: clean lint test ## CI 파이프라인 (린트 + 테스트)

# 개발 환경 초기화
dev-setup: clean install-dev ## 개발 환경 초기 설정

# 퀀트 프로젝트 시작
quant-start: ## 퀀트 프로젝트 시작 (1일차 설정)
	@echo "🍣 Sushi 퀀트 프로젝트를 시작합니다!"
	@echo "1. 프로젝트 구조 마이그레이션..."
	@make migrate
	@echo "2. 개발 환경 설정..."
	@make install-dev
	@echo "3. 오늘의 태스크 파일 생성..."
	@make new-task
	@echo "4. 데이터 수집 테스트..."
	@make collect-test
	@echo "🎯 모든 설정이 완료되었습니다! 'make today-task'로 시작하세요." 