#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
프로젝트 구조 재정리 스크립트

현재 구조를 권장 구조로 변경합니다. (Sushi 패키지)
"""

import os
import shutil
import sys


def create_directory_structure():
    """새로운 디렉토리 구조 생성"""
    directories = [
        "sushi",
        "sushi/feature",
        "sushi/feature/features",
        "sushi/database",
        "sushi/utils",
        "scripts",
        "tests",
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        # __init__.py 파일 생성
        init_file = os.path.join(directory, "__init__.py")
        if not os.path.exists(init_file):
            with open(init_file, "w", encoding="utf-8") as f:
                f.write(
                    '"""{}"""\n'.format(directory.replace("/", ".").replace("\\", "."))
                )


def move_files():
    """파일들을 새로운 위치로 이동"""
    file_moves = [
        # 데이터베이스 관련
        ("database/db_manager.py", "sushi/database/db_manager.py"),
        # 피처 관련
        ("data/abstract_feature.py", "sushi/feature/abstract_feature.py"),
        ("data/api_client.py", "sushi/feature/api_client.py"),
        ("data/feature_manager.py", "sushi/feature/feature_manager.py"),
        (
            "data/domestic_futures_minute.py",
            "sushi/feature/features/domestic_futures_minute.py",
        ),
        ("data/investor_buy.py", "sushi/feature/features/investor_buy.py"),
        # 메인 스크립트
        ("run_data_collector.py", "scripts/run_data_collector.py"),
    ]

    for source, destination in file_moves:
        if os.path.exists(source):
            print(f"Moving {source} -> {destination}")
            shutil.move(source, destination)
        else:
            print(f"Warning: {source} not found")


def update_imports_in_file(file_path, old_imports, new_imports):
    """파일의 import 문을 업데이트"""
    if not os.path.exists(file_path):
        return

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    for old, new in zip(old_imports, new_imports):
        content = content.replace(old, new)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)


def update_imports():
    """모든 파일의 import 문 업데이트"""
    updates = [
        (
            "scripts/run_data_collector.py",
            [
                "from ignacio.feature.feature_manager import FeatureManager",
                "from ignacio.database.db_manager import DBManager",
            ],
            [
                "from sushi.feature.feature_manager import FeatureManager",
                "from sushi.database.db_manager import DBManager",
            ],
        ),
        (
            "sushi/feature/feature_manager.py",
            [
                "from ignacio.feature.abstract_feature import Feature",
                "from ignacio.feature.api_client import APIClient",
            ],
            [
                "from sushi.feature.abstract_feature import Feature",
                "from sushi.feature.api_client import APIClient",
            ],
        ),
        (
            "sushi/feature/features/domestic_futures_minute.py",
            [
                "from ignacio.feature.abstract_feature import Feature",
                "from ignacio.feature.api_client import APIClient",
            ],
            [
                "from sushi.feature.abstract_feature import Feature",
                "from sushi.feature.api_client import APIClient",
            ],
        ),
        (
            "sushi/feature/features/investor_buy.py",
            [
                "from ignacio.feature.api_client import APIClient",
                "from ignacio.feature.abstract_feature import Feature",
            ],
            [
                "from sushi.feature.api_client import APIClient",
                "from sushi.feature.abstract_feature import Feature",
            ],
        ),
    ]

    for file_path, old_imports, new_imports in updates:
        print(f"Updating imports in {file_path}")
        update_imports_in_file(file_path, old_imports, new_imports)


def clean_old_directories():
    """기존 빈 디렉토리 정리"""
    old_dirs = ["data", "database"]
    for directory in old_dirs:
        if os.path.exists(directory) and not os.listdir(directory):
            print(f"Removing empty directory: {directory}")
            os.rmdir(directory)


def main():
    """메인 함수"""
    print("🍣 Sushi 프로젝트 구조 재정리를 시작합니다...")

    # 백업 생성 권장
    backup_answer = input("계속하기 전에 프로젝트를 백업하셨나요? (y/N): ")
    if backup_answer.lower() != "y":
        print("먼저 프로젝트를 백업해주세요!")
        sys.exit(1)

    # 1. 디렉토리 구조 생성
    print("📁 새로운 Sushi 디렉토리 구조 생성 중...")
    create_directory_structure()

    # 2. 파일 이동
    print("📦 파일들을 새로운 위치로 이동 중...")
    move_files()

    # 3. Import 문 업데이트 (ignacio -> sushi)
    print("🔄 Import 문을 Sushi 패키지로 업데이트 중...")
    update_imports()

    # 4. 기존 빈 디렉토리 정리
    print("🧹 기존 빈 디렉토리 정리 중...")
    clean_old_directories()

    print("✅ Sushi 프로젝트 구조 재정리가 완료되었습니다! 🍣")
    print("\n다음 단계:")
    print(
        "1. 가상환경 활성화: .venv\\Scripts\\activate (Windows) 또는 source .venv/bin/activate (Linux/Mac)"
    )
    print("2. 의존성 설치: pip install -r requirements.txt")
    print("3. 개발 모드로 설치: pip install -e .")
    print("4. 테스트 실행: python scripts/run_data_collector.py --test")
    print("5. 또는 명령어 사용: sushi-collect --test")


if __name__ == "__main__":
    main()
