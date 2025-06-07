"""
증분 데이터 수집을 위한 유틸리티 함수들

이 모듈은 다양한 피처 클래스에서 사용할 수 있는
증분 업데이트 관련 공통 함수들을 제공합니다.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Union
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


def save_feature_to_csv_incremental(
    data_dict: Dict[str, pd.DataFrame],
    base_path: Path,
    feature_name: str,
    metadata_manager,
    incremental_mode: bool = True,
    date_column: str = "trade_date",
    time_column: Optional[str] = None,
    backup_enabled: bool = True,
) -> Dict[str, Any]:
    """
    피처 데이터를 증분 모드로 CSV에 저장

    Args:
        data_dict: {code: dataframe} 형태의 데이터
        base_path: 저장할 기본 경로
        feature_name: 피처명
        metadata_manager: MetadataManager 인스턴스
        incremental_mode: 증분 모드 여부
        date_column: 날짜 컬럼명
        time_column: 시간 컬럼명 (선택)
        backup_enabled: 백업 생성 여부

    Returns:
        Dict: 저장 결과 및 통계
    """
    results = {
        "success_count": 0,
        "error_count": 0,
        "total_files": len(data_dict),
        "saved_files": [],
        "errors": [],
        "stats": {
            "total_new_records": 0,
            "total_existing_records": 0,
            "total_merged_records": 0,
        },
    }

    # 저장 디렉토리 생성
    save_dir = base_path / feature_name
    save_dir.mkdir(parents=True, exist_ok=True)

    for code, df in data_dict.items():
        try:
            # CSV 파일 경로
            csv_path = save_dir / f"{code}.csv"

            if not incremental_mode:
                # 전체 덮어쓰기 모드
                df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                results["saved_files"].append(str(csv_path))
                results["success_count"] += 1
                results["stats"]["total_new_records"] += len(df)

                # 메타데이터 업데이트
                metadata_manager.update_metadata_incremental(
                    feature_name, code, csv_path, len(df), ("", ""), date_column
                )

                logger.info(
                    f"✅ {feature_name}/{code}: 전체 덮어쓰기 저장 완료 ({len(df)}건)"
                )
                continue

            # 증분 모드 처리
            backup_path = None

            try:
                # 1. 기존 데이터 백업 (선택사항)
                if backup_enabled and csv_path.exists():
                    backup_path = metadata_manager.backup_csv_file(csv_path)

                # 2. 기존 데이터와 새 데이터 합치기
                existing_df = pd.DataFrame()
                if csv_path.exists():
                    existing_df = pd.read_csv(csv_path)

                merged_df = metadata_manager.merge_csv_data(
                    csv_path, df, date_column, time_column
                )

                # 3. 데이터 검증
                validation_result = metadata_manager.validate_merged_data(
                    existing_df, df, merged_df, date_column
                )

                # 검증 실패시 경고 로그
                if not validation_result["is_valid"]:
                    logger.error(f"데이터 검증 실패: {feature_name}/{code}")
                    for error in validation_result["errors"]:
                        logger.error(f"  - {error}")

                if validation_result["warnings"]:
                    for warning in validation_result["warnings"]:
                        logger.warning(f"  - {warning}")

                # 4. 합쳐진 데이터 저장
                merged_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

                # 5. 메타데이터 업데이트
                new_records = len(df)
                date_range = ("", "")  # 실제 구현에서는 적절한 날짜 범위 설정

                metadata_manager.update_metadata_incremental(
                    feature_name, code, csv_path, new_records, date_range, date_column
                )

                # 6. 통계 업데이트
                stats = validation_result["stats"]
                results["stats"]["total_new_records"] += stats["new_records"]
                results["stats"]["total_existing_records"] += stats["old_records"]
                results["stats"]["total_merged_records"] += stats["merged_records"]

                results["saved_files"].append(str(csv_path))
                results["success_count"] += 1

                logger.info(
                    f"✅ {feature_name}/{code}: 증분 저장 완료 "
                    f"(기존: {stats['old_records']}건, "
                    f"신규: {stats['new_records']}건, "
                    f"최종: {stats['merged_records']}건)"
                )

                # 백업 파일 정리 (성공시)
                if backup_path and backup_path.exists():
                    backup_path.unlink()

            except Exception as save_error:
                # 저장 중 오류 발생시 롤백
                if backup_path:
                    logger.warning(f"저장 오류로 롤백 시도: {feature_name}/{code}")
                    metadata_manager.rollback_from_backup(csv_path, backup_path)

                raise save_error

        except Exception as e:
            error_msg = f"{feature_name}/{code} 저장 실패: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            results["error_count"] += 1

    return results


def get_dynamic_date_range(
    metadata_manager,
    feature_path: str,
    codes: list,
    default_start: str = "20240101",
    default_end: Optional[str] = None,
    max_days_back: int = 90,
) -> Dict[str, Tuple[str, str]]:
    """
    각 코드별로 동적 날짜 범위 계산

    Args:
        metadata_manager: MetadataManager 인스턴스
        feature_path: 피처 경로
        codes: 처리할 코드 리스트
        default_start: 기본 시작일
        default_end: 기본 종료일 (None이면 오늘)
        max_days_back: 최대 며칠 전까지 허용할지

    Returns:
        Dict: {code: (start_date, end_date)} 형태
    """
    if default_end is None:
        default_end = datetime.now().strftime("%Y%m%d")

    date_ranges = {}

    for code in codes:
        try:
            start_date, end_date = metadata_manager.calculate_incremental_range(
                feature_path, code, max_days_back
            )

            if start_date is None:
                # 전체 수집 필요
                date_ranges[code] = (default_start, default_end)
                logger.info(
                    f"전체 수집: {feature_path}/{code} ({default_start}~{default_end})"
                )
            else:
                # 증분 수집
                date_ranges[code] = (start_date, end_date)
                if start_date <= end_date:
                    logger.info(
                        f"증분 수집: {feature_path}/{code} ({start_date}~{end_date})"
                    )
                else:
                    logger.info(f"수집 불필요: {feature_path}/{code} (최신 상태)")

        except Exception as e:
            logger.error(f"날짜 범위 계산 오류 {feature_path}/{code}: {e}")
            # 오류시 기본 범위 사용
            date_ranges[code] = (default_start, default_end)

    return date_ranges


def should_update_data(
    metadata_manager, feature_path: str, code: str, max_age_hours: int = 24
) -> bool:
    """
    데이터 업데이트가 필요한지 판단

    Args:
        metadata_manager: MetadataManager 인스턴스
        feature_path: 피처 경로
        code: 코드
        max_age_hours: 최대 데이터 유효 시간 (시간)

    Returns:
        bool: 업데이트 필요 여부
    """
    try:
        last_info = metadata_manager.load_last_update_info(feature_path, code)

        if not last_info:
            return True  # 메타데이터 없으면 업데이트 필요

        last_update = last_info.get("last_update_timestamp")
        if not last_update:
            return True

        # 마지막 업데이트 시간 확인
        last_update_dt = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
        hours_passed = (
            datetime.now() - last_update_dt.replace(tzinfo=None)
        ).total_seconds() / 3600

        if hours_passed > max_age_hours:
            logger.info(
                f"데이터 갱신 필요: {feature_path}/{code} ({hours_passed:.1f}시간 경과)"
            )
            return True
        else:
            logger.info(
                f"데이터 최신 상태: {feature_path}/{code} ({hours_passed:.1f}시간 경과)"
            )
            return False

    except Exception as e:
        logger.error(f"업데이트 필요성 판단 오류: {e}")
        return True  # 오류시 안전하게 업데이트 실행


def log_incremental_summary(results: Dict[str, Any]) -> None:
    """
    증분 업데이트 결과 요약 로깅

    Args:
        results: save_feature_to_csv_incremental 결과
    """
    stats = results["stats"]

    logger.info("=" * 60)
    logger.info("📊 증분 업데이트 요약")
    logger.info("=" * 60)
    logger.info(f"📁 처리 파일: {results['total_files']}개")
    logger.info(f"✅ 성공: {results['success_count']}개")
    logger.info(f"❌ 실패: {results['error_count']}개")
    logger.info(f"📈 기존 레코드: {stats['total_existing_records']:,}건")
    logger.info(f"🆕 신규 레코드: {stats['total_new_records']:,}건")
    logger.info(f"🔄 최종 레코드: {stats['total_merged_records']:,}건")

    if results["errors"]:
        logger.error("❌ 오류 목록:")
        for error in results["errors"]:
            logger.error(f"  - {error}")

    logger.info("=" * 60)
