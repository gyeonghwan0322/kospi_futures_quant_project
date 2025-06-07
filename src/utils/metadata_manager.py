"""
메타데이터 관리 모듈

CSV 파일의 업데이트 정보, 데이터 범위, 무결성 등을 관리합니다.
증분 업데이트를 위한 핵심 모듈입니다.
"""

import json
import os
import hashlib
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class MetadataManager:
    """
    CSV 파일의 메타데이터를 관리하는 클래스

    - 마지막 업데이트 정보 추적
    - 데이터 범위 관리
    - 무결성 검증
    - 증분 업데이트 지원
    """

    def __init__(self, base_data_dir: str = "data"):
        """
        MetadataManager 초기화

        Args:
            base_data_dir (str): 데이터 기본 디렉토리 경로
        """
        self.base_data_dir = Path(base_data_dir)
        self.metadata_dir_name = ".metadata"

    def get_metadata_dir(self, feature_path: str) -> Path:
        """
        특정 피처의 메타데이터 디렉토리 경로 반환

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로

        Returns:
            Path: 메타데이터 디렉토리 경로
        """
        feature_dir = self.base_data_dir / feature_path
        return feature_dir / self.metadata_dir_name

    def create_metadata_dir(self, feature_path: str) -> None:
        """
        메타데이터 디렉토리 생성

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
        """
        metadata_dir = self.get_metadata_dir(feature_path)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"메타데이터 디렉토리 생성: {metadata_dir}")

    def get_last_update_path(self, feature_path: str, code: str) -> Path:
        """
        마지막 업데이트 정보 파일 경로 반환

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드

        Returns:
            Path: last_update.json 파일 경로
        """
        metadata_dir = self.get_metadata_dir(feature_path)
        return metadata_dir / f"last_update_{code}.json"

    def get_history_path(self, feature_path: str, code: str) -> Path:
        """
        업데이트 히스토리 파일 경로 반환

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드

        Returns:
            Path: update_history.json 파일 경로
        """
        metadata_dir = self.get_metadata_dir(feature_path)
        return metadata_dir / f"update_history_{code}.json"

    def calculate_file_hash(self, file_path: Path) -> str:
        """
        파일의 SHA256 해시 계산

        Args:
            file_path (Path): 파일 경로

        Returns:
            str: SHA256 해시값
        """
        if not file_path.exists():
            return ""

        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def get_csv_date_range(
        self, csv_path: Path, date_column: str = "trade_date"
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        CSV 파일에서 날짜 범위 추출

        Args:
            csv_path (Path): CSV 파일 경로
            date_column (str): 날짜 컬럼명

        Returns:
            Tuple[Optional[str], Optional[str]]: (시작일, 종료일) YYYYMMDD 형식
        """
        try:
            if not csv_path.exists():
                return None, None

            df = pd.read_csv(csv_path)
            if date_column not in df.columns or df.empty:
                return None, None

            # 날짜 컬럼을 문자열로 변환하고 정렬
            dates = df[date_column].dropna().astype(str).unique()
            dates = sorted([d for d in dates if len(d) >= 8 and d.isdigit()])

            if not dates:
                return None, None

            return dates[0], dates[-1]

        except Exception as e:
            logger.error(f"CSV 날짜 범위 추출 오류: {e}")
            return None, None

    def load_last_update_info(
        self, feature_path: str, code: str
    ) -> Optional[Dict[str, Any]]:
        """
        마지막 업데이트 정보 로드

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드

        Returns:
            Optional[Dict[str, Any]]: 마지막 업데이트 정보 또는 None
        """
        update_file = self.get_last_update_path(feature_path, code)

        if not update_file.exists():
            return None

        try:
            with open(update_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"메타데이터 로드 오류: {e}")
            return None

    def save_last_update_info(
        self, feature_path: str, code: str, update_info: Dict[str, Any]
    ) -> bool:
        """
        마지막 업데이트 정보 저장

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드
            update_info (Dict[str, Any]): 업데이트 정보

        Returns:
            bool: 저장 성공 여부
        """
        try:
            # 메타데이터 디렉토리 생성
            self.create_metadata_dir(feature_path)

            update_file = self.get_last_update_path(feature_path, code)

            with open(update_file, "w", encoding="utf-8") as f:
                json.dump(update_info, f, indent=2, ensure_ascii=False)

            logger.info(f"메타데이터 저장 완료: {update_file}")
            return True

        except Exception as e:
            logger.error(f"메타데이터 저장 오류: {e}")
            return False

    def create_update_info(
        self,
        feature_name: str,
        code: str,
        csv_path: Path,
        date_column: str = "trade_date",
    ) -> Dict[str, Any]:
        """
        CSV 파일 기반으로 업데이트 정보 생성

        Args:
            feature_name (str): 피처명
            code (str): 종목/데이터 코드
            csv_path (Path): CSV 파일 경로
            date_column (str): 날짜 컬럼명

        Returns:
            Dict[str, Any]: 업데이트 정보
        """
        # 현재 시간
        now = datetime.now()

        # CSV 파일 정보
        total_records = 0
        start_date, end_date = None, None

        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                total_records = len(df)
                start_date, end_date = self.get_csv_date_range(csv_path, date_column)
            except Exception as e:
                logger.error(f"CSV 파일 분석 오류: {e}")

        update_info = {
            "feature_name": feature_name,
            "code": code,
            "last_update_date": now.strftime("%Y%m%d"),
            "last_update_time": now.strftime("%H%M%S"),
            "last_update_timestamp": now.isoformat(),
            "total_records": total_records,
            "date_range": {"start": start_date, "end": end_date},
            "data_hash": self.calculate_file_hash(csv_path),
            "api_version": "v1",
            "collection_mode": "full",  # 첫 수집은 전체
            "last_error": None,
            "retry_count": 0,
            "csv_path": str(csv_path),
            "metadata_version": "1.0",
        }

        return update_info

    def add_to_history(
        self, feature_path: str, code: str, update_info: Dict[str, Any]
    ) -> bool:
        """
        업데이트 히스토리에 정보 추가

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드
            update_info (Dict[str, Any]): 업데이트 정보

        Returns:
            bool: 추가 성공 여부
        """
        try:
            history_file = self.get_history_path(feature_path, code)

            # 기존 히스토리 로드
            history = []
            if history_file.exists():
                with open(history_file, "r", encoding="utf-8") as f:
                    history = json.load(f)

            # 새 업데이트 정보 추가
            history.append(
                {
                    "timestamp": update_info.get("last_update_timestamp"),
                    "date": update_info.get("last_update_date"),
                    "time": update_info.get("last_update_time"),
                    "records": update_info.get("total_records"),
                    "mode": update_info.get("collection_mode"),
                    "date_range": update_info.get("date_range"),
                }
            )

            # 최근 50개만 유지
            history = history[-50:]

            with open(history_file, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            logger.error(f"히스토리 추가 오류: {e}")
            return False

    def get_next_update_date(self, feature_path: str, code: str) -> Optional[str]:
        """
        다음 증분 업데이트를 위한 시작 날짜 계산

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드

        Returns:
            Optional[str]: 다음 업데이트 시작 날짜 (YYYYMMDD) 또는 None
        """
        last_info = self.load_last_update_info(feature_path, code)

        if not last_info:
            return None

        date_range = last_info.get("date_range", {})
        end_date = date_range.get("end")

        if not end_date:
            return None

        try:
            # 마지막 날짜의 다음 날부터 시작
            last_date = datetime.strptime(end_date, "%Y%m%d")
            next_date = last_date + timedelta(days=1)
            return next_date.strftime("%Y%m%d")

        except Exception as e:
            logger.error(f"다음 업데이트 날짜 계산 오류: {e}")
            return None

    def calculate_incremental_range(
        self, feature_path: str, code: str, max_days_back: int = 90
    ) -> Tuple[Optional[str], str]:
        """
        증분 업데이트를 위한 날짜 범위 계산

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드
            max_days_back (int): 최대 몇일 전까지 데이터를 가져올지 (기본 90일)

        Returns:
            Tuple[Optional[str], str]: (시작일, 종료일) YYYYMMDD 형식
                                      시작일이 None이면 전체 수집 필요
        """
        # 현재 날짜 (종료일)
        end_date = datetime.now().strftime("%Y%m%d")

        # 마지막 업데이트 정보 확인
        last_info = self.load_last_update_info(feature_path, code)

        if not last_info:
            # 메타데이터가 없으면 전체 수집
            logger.info(f"메타데이터 없음. 전체 수집 필요: {feature_path}/{code}")
            return None, end_date

        # 마지막 데이터 날짜 확인
        date_range = last_info.get("date_range", {})
        last_end_date = date_range.get("end")

        if not last_end_date:
            # 날짜 범위 정보가 없으면 전체 수집
            logger.info(f"날짜 범위 정보 없음. 전체 수집 필요: {feature_path}/{code}")
            return None, end_date

        try:
            # 마지막 날짜의 다음 날부터 수집
            last_date = datetime.strptime(last_end_date, "%Y%m%d")
            start_date = last_date + timedelta(days=1)

            # 너무 오래된 데이터는 전체 수집으로 처리
            days_diff = (datetime.now() - last_date).days
            if days_diff > max_days_back:
                logger.warning(
                    f"마지막 업데이트가 {days_diff}일 전. 전체 수집 실행: {feature_path}/{code}"
                )
                return None, end_date

            start_date_str = start_date.strftime("%Y%m%d")

            # 시작일이 종료일보다 미래면 업데이트 불필요
            if start_date_str > end_date:
                logger.info(f"업데이트 불필요 (최신 상태): {feature_path}/{code}")
                return start_date_str, end_date  # 동일한 날짜 반환으로 빈 범위 표시

            logger.info(
                f"증분 업데이트 범위: {start_date_str} ~ {end_date} ({feature_path}/{code})"
            )
            return start_date_str, end_date

        except Exception as e:
            logger.error(f"증분 범위 계산 오류: {e}")
            return None, end_date

    def merge_csv_data(
        self,
        existing_csv_path: Path,
        new_df: pd.DataFrame,
        date_column: str = "trade_date",
        time_column: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        기존 CSV 데이터와 새 데이터를 합치고 중복 제거

        Args:
            existing_csv_path (Path): 기존 CSV 파일 경로
            new_df (pd.DataFrame): 새로 수집된 데이터
            date_column (str): 날짜 컬럼명
            time_column (Optional[str]): 시간 컬럼명 (있을 경우)

        Returns:
            pd.DataFrame: 합쳐진 데이터프레임 (중복 제거 및 정렬 완료)
        """
        try:
            # 기존 데이터 로드
            if existing_csv_path.exists():
                existing_df = pd.read_csv(existing_csv_path)
                logger.info(f"기존 데이터 로드: {len(existing_df)}건")
            else:
                existing_df = pd.DataFrame()
                logger.info("기존 파일 없음. 새로 생성")

            # 새 데이터가 비어있으면 기존 데이터만 반환
            if new_df.empty:
                logger.warning("새 데이터가 비어있음")
                return existing_df

            logger.info(f"새 데이터: {len(new_df)}건")

            # 데이터 합치기
            if existing_df.empty:
                merged_df = new_df.copy()
            else:
                merged_df = pd.concat([existing_df, new_df], ignore_index=True)

            # 중복 제거 키 설정
            if time_column and time_column in merged_df.columns:
                # 날짜 + 시간 기준으로 중복 제거
                dedup_columns = [date_column, time_column]
            else:
                # 날짜만으로 중복 제거
                dedup_columns = [date_column]

            # 중복 제거 (같은 키의 경우 마지막 데이터 유지)
            if date_column in merged_df.columns:
                before_count = len(merged_df)
                merged_df = merged_df.drop_duplicates(subset=dedup_columns, keep="last")
                after_count = len(merged_df)

                if before_count > after_count:
                    logger.info(
                        f"중복 제거: {before_count}건 → {after_count}건 ({before_count - after_count}건 제거)"
                    )

                # 날짜순 정렬 (문자열로 변환 후 정렬)
                try:
                    for col in dedup_columns:
                        if col in merged_df.columns:
                            merged_df[col] = merged_df[col].astype(str)
                    merged_df = merged_df.sort_values(by=dedup_columns).reset_index(
                        drop=True
                    )
                except Exception as sort_error:
                    logger.warning(f"정렬 중 오류 발생: {sort_error}")
                    # 정렬 실패해도 계속 진행
            else:
                logger.warning(f"날짜 컬럼 '{date_column}'이 없어서 중복 제거 생략")

            logger.info(f"최종 합쳐진 데이터: {len(merged_df)}건")
            return merged_df

        except Exception as e:
            logger.error(f"데이터 합치기 오류: {e}")
            # 오류 발생시 새 데이터만 반환
            return new_df.copy() if not new_df.empty else pd.DataFrame()

    def backup_csv_file(self, csv_path: Path) -> Optional[Path]:
        """
        CSV 파일 백업 생성

        Args:
            csv_path (Path): 백업할 CSV 파일 경로

        Returns:
            Optional[Path]: 백업 파일 경로 또는 None
        """
        try:
            if not csv_path.exists():
                return None

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = csv_path.parent / f"{csv_path.stem}_backup_{timestamp}.csv"

            import shutil

            shutil.copy2(csv_path, backup_path)

            logger.info(f"백업 생성: {backup_path}")
            return backup_path

        except Exception as e:
            logger.error(f"백업 생성 오류: {e}")
            return None

    def rollback_from_backup(self, csv_path: Path, backup_path: Path) -> bool:
        """
        백업에서 원본 파일로 롤백

        Args:
            csv_path (Path): 원본 CSV 파일 경로
            backup_path (Path): 백업 파일 경로

        Returns:
            bool: 롤백 성공 여부
        """
        try:
            if not backup_path.exists():
                logger.error(f"백업 파일 없음: {backup_path}")
                return False

            import shutil

            shutil.move(str(backup_path), str(csv_path))

            logger.info(f"롤백 완료: {backup_path} → {csv_path}")
            return True

        except Exception as e:
            logger.error(f"롤백 오류: {e}")
            return False

    def validate_merged_data(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        merged_df: pd.DataFrame,
        date_column: str = "trade_date",
    ) -> Dict[str, Any]:
        """
        합쳐진 데이터의 유효성 검증

        Args:
            old_df (pd.DataFrame): 기존 데이터
            new_df (pd.DataFrame): 새 데이터
            merged_df (pd.DataFrame): 합쳐진 데이터
            date_column (str): 날짜 컬럼명

        Returns:
            Dict[str, Any]: 검증 결과
        """
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": [],
            "stats": {
                "old_records": len(old_df),
                "new_records": len(new_df),
                "merged_records": len(merged_df),
                "duplicates_removed": len(old_df) + len(new_df) - len(merged_df),
            },
        }

        try:
            # 1. 레코드 수 검증 (수정된 로직)
            # 기존 데이터가 없는 경우 새 데이터만 있어야 함
            if len(old_df) == 0:
                expected_min = len(new_df)
            else:
                # 기존 데이터가 있는 경우, 최소한 기존 데이터 개수는 유지되어야 함
                expected_min = len(old_df)

            expected_max = len(old_df) + len(new_df)

            if len(merged_df) < expected_min:
                validation_result["warnings"].append(
                    f"합쳐진 데이터가 예상보다 적음: {len(merged_df)} < {expected_min}"
                )

            if len(merged_df) > expected_max:
                validation_result["warnings"].append(
                    f"합쳐진 데이터가 예상보다 많음: {len(merged_df)} > {expected_max}"
                )

            # 2. 날짜 연속성 검증
            if date_column in merged_df.columns and not merged_df.empty:
                dates = pd.to_datetime(
                    merged_df[date_column], format="%Y%m%d", errors="coerce"
                )
                dates = dates.dropna().sort_values()

                if len(dates) > 1:
                    # 큰 날짜 간격 확인 (7일 이상)
                    date_gaps = dates.diff().dt.days
                    large_gaps = date_gaps[date_gaps > 7]

                    if not large_gaps.empty:
                        validation_result["warnings"].append(
                            f"큰 날짜 간격 발견: 최대 {large_gaps.max()}일"
                        )

            # 3. 중복 데이터 재검증
            if date_column in merged_df.columns:
                duplicates = merged_df.duplicated(subset=[date_column], keep=False)
                if duplicates.any():
                    dup_count = duplicates.sum()
                    validation_result["warnings"].append(
                        f"여전히 중복 데이터 존재: {dup_count}건"
                    )

            # 4. 필수 컬럼 존재 검증
            if merged_df.empty:
                validation_result["warnings"].append("합쳐진 데이터가 비어있음")
            elif date_column not in merged_df.columns:
                validation_result["errors"].append(
                    f"필수 날짜 컬럼 '{date_column}' 없음"
                )
                validation_result["is_valid"] = False

        except Exception as e:
            validation_result["errors"].append(f"검증 중 오류: {e}")
            validation_result["is_valid"] = False

        return validation_result

    def update_metadata_incremental(
        self,
        feature_path: str,
        code: str,
        csv_path: Path,
        new_records: int,
        date_range: Tuple[str, str],
        date_column: str = "trade_date",
    ) -> bool:
        """
        증분 업데이트 후 메타데이터 갱신

        Args:
            feature_path (str): 피처 데이터가 저장된 상대 경로
            code (str): 종목/데이터 코드
            csv_path (Path): CSV 파일 경로
            new_records (int): 새로 추가된 레코드 수
            date_range (Tuple[str, str]): 업데이트 날짜 범위
            date_column (str): 날짜 컬럼명

        Returns:
            bool: 업데이트 성공 여부
        """
        try:
            # 기존 메타데이터 로드
            last_info = self.load_last_update_info(feature_path, code)

            if not last_info:
                # 메타데이터가 없으면 새로 생성
                feature_name = Path(feature_path).name
                last_info = self.create_update_info(
                    feature_name, code, csv_path, date_column
                )

            # 현재 시간
            now = datetime.now()

            # CSV 파일에서 최신 정보 추출
            current_start, current_end = self.get_csv_date_range(csv_path, date_column)
            total_records = 0

            if csv_path.exists():
                try:
                    df = pd.read_csv(csv_path)
                    total_records = len(df)
                except Exception as e:
                    logger.error(f"CSV 레코드 수 계산 오류: {e}")

            # 메타데이터 업데이트
            last_info.update(
                {
                    "last_update_date": now.strftime("%Y%m%d"),
                    "last_update_time": now.strftime("%H%M%S"),
                    "last_update_timestamp": now.isoformat(),
                    "total_records": total_records,
                    "date_range": {"start": current_start, "end": current_end},
                    "data_hash": self.calculate_file_hash(csv_path),
                    "collection_mode": "incremental",
                    "last_error": None,
                    "retry_count": 0,
                    "incremental_stats": {
                        "update_range": {"start": date_range[0], "end": date_range[1]},
                        "new_records_added": new_records,
                        "update_timestamp": now.isoformat(),
                    },
                }
            )

            # 메타데이터 저장
            if self.save_last_update_info(feature_path, code, last_info):
                # 히스토리에 추가
                self.add_to_history(feature_path, code, last_info)
                logger.info(
                    f"증분 업데이트 메타데이터 갱신 완료: {feature_path}/{code}"
                )
                return True
            else:
                logger.error(
                    f"증분 업데이트 메타데이터 저장 실패: {feature_path}/{code}"
                )
                return False

        except Exception as e:
            logger.error(f"증분 업데이트 메타데이터 갱신 오류: {e}")
            return False
