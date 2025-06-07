#!/usr/bin/env python3
"""
기존 CSV 파일들에 대한 메타데이터 초기화 스크립트

이 스크립트는 현재 존재하는 모든 CSV 파일에 대해 메타데이터를 생성합니다.
Phase 1 구현의 일부로, 증분 업데이트를 위한 기반을 마련합니다.
"""

import sys
import os
from pathlib import Path
import logging
from typing import List, Tuple

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.metadata_manager import MetadataManager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def find_csv_files(data_dir: Path) -> List[Tuple[str, str, Path]]:
    """
    데이터 디렉토리에서 모든 CSV 파일을 찾습니다.

    Args:
        data_dir (Path): 데이터 디렉토리 경로

    Returns:
        List[Tuple[str, str, Path]]: (feature_path, code, csv_path) 리스트
    """
    csv_files = []

    for csv_path in data_dir.rglob("*.csv"):
        # 메타데이터 디렉토리는 제외
        if ".metadata" in str(csv_path):
            continue

        # 상대 경로 계산
        try:
            relative_to_data = csv_path.relative_to(data_dir)
            feature_path = str(relative_to_data.parent)
            code = csv_path.stem

            csv_files.append((feature_path, code, csv_path))

        except ValueError:
            logger.warning(f"CSV 파일 경로 계산 실패: {csv_path}")
            continue

    return csv_files


def detect_date_column(csv_path: Path) -> str:
    """
    CSV 파일에서 날짜 컬럼을 자동 감지합니다.

    Args:
        csv_path (Path): CSV 파일 경로

    Returns:
        str: 감지된 날짜 컬럼명 또는 기본값
    """
    try:
        import pandas as pd

        df = pd.read_csv(csv_path, nrows=5)  # 처음 5행만 읽기

        # 가능한 날짜 컬럼명들
        date_columns = [
            "trade_date",
            "stnd_dt",
            "date",
            "trading_date",
            "stck_bsop_date",
            "bsns_date",
        ]

        for col in date_columns:
            if col in df.columns:
                return col

        # 날짜 패턴을 가진 컬럼 찾기
        for col in df.columns:
            if "date" in col.lower() or "dt" in col.lower():
                return col

        # 기본값
        return "trade_date"

    except Exception as e:
        logger.warning(f"날짜 컬럼 감지 실패: {e}")
        return "trade_date"


def determine_feature_name(feature_path: str) -> str:
    """
    경로에서 피처명을 결정합니다.

    Args:
        feature_path (str): 피처 경로

    Returns:
        str: 피처명
    """
    # 경로의 마지막 부분을 피처명으로 사용
    return Path(feature_path).name


def main():
    """메인 실행 함수"""
    logger.info("🚀 메타데이터 초기화 시작")

    # 데이터 디렉토리 설정
    data_dir = project_root / "data"
    if not data_dir.exists():
        logger.error(f"데이터 디렉토리가 존재하지 않습니다: {data_dir}")
        return

    # MetadataManager 초기화
    metadata_manager = MetadataManager(str(data_dir))

    # CSV 파일 찾기
    logger.info("📂 CSV 파일 검색 중...")
    csv_files = find_csv_files(data_dir)

    if not csv_files:
        logger.warning("CSV 파일을 찾을 수 없습니다.")
        return

    logger.info(f"📊 {len(csv_files)}개의 CSV 파일을 발견했습니다.")

    # 각 CSV 파일에 대해 메타데이터 생성
    success_count = 0
    total_count = len(csv_files)

    for feature_path, code, csv_path in csv_files:
        try:
            logger.info(f"🔍 처리 중: {feature_path}/{code}.csv")

            # 피처명 결정
            feature_name = determine_feature_name(feature_path)

            # 날짜 컬럼 감지
            date_column = detect_date_column(csv_path)

            # 메타데이터 생성
            update_info = metadata_manager.create_update_info(
                feature_name=feature_name,
                code=code,
                csv_path=csv_path,
                date_column=date_column,
            )

            # 메타데이터 저장
            if metadata_manager.save_last_update_info(feature_path, code, update_info):
                # 히스토리에 추가
                metadata_manager.add_to_history(feature_path, code, update_info)

                success_count += 1
                logger.info(f"✅ 메타데이터 생성 완료: {feature_path}/{code}")

                # 생성된 정보 요약 출력
                date_range = update_info.get("date_range", {})
                logger.info(
                    f"   📅 데이터 범위: {date_range.get('start')} ~ {date_range.get('end')}"
                )
                logger.info(f"   📈 총 레코드: {update_info.get('total_records')}개")

            else:
                logger.error(f"❌ 메타데이터 저장 실패: {feature_path}/{code}")

        except Exception as e:
            logger.error(f"❌ 메타데이터 생성 오류 ({feature_path}/{code}): {e}")

    # 결과 요약
    logger.info("=" * 50)
    logger.info(f"🏁 메타데이터 초기화 완료")
    logger.info(f"✅ 성공: {success_count}/{total_count}개")

    if success_count < total_count:
        logger.warning(f"⚠️  실패: {total_count - success_count}개")

    # 생성된 메타데이터 디렉토리 정보
    metadata_dirs = []
    for root, dirs, files in os.walk(data_dir):
        if ".metadata" in dirs:
            metadata_dirs.append(Path(root) / ".metadata")

    logger.info(f"📁 메타데이터 디렉토리: {len(metadata_dirs)}개 생성")
    for md_dir in metadata_dirs:
        file_count = len(list(md_dir.glob("*.json")))
        logger.info(f"   {md_dir.relative_to(data_dir)}: {file_count}개 파일")


if __name__ == "__main__":
    main()
