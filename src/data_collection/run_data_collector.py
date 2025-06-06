#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
피처 데이터 수집 및 CSV 저장 실행 스크립트

특정 피처 클래스를 사용하여 데이터를 수집하고 CSV 파일로 저장합니다.
"""

import os
import sys
import yaml
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
import traceback

# 로깅 설정 - WARNING 레벨로 변경하여 중요한 정보만 출력
current_date = datetime.now().strftime("%Y%m%d")
log_file = f"logs/data_collector_{current_date}.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.WARNING,  # INFO에서 WARNING으로 변경
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# 경로 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))  # 프로젝트 루트 디렉토리
if project_root not in sys.path:
    sys.path.append(project_root)

# 필요한 모듈 임포트 (DB 관련 제거)
from src.feature_engineering.feature_manager import FeatureManager
from src.utils.trading_calendar import (
    get_current_trading_date,
    get_trading_session_info,
)


def parse_args():
    """명령행 인수 파싱"""
    parser = argparse.ArgumentParser(
        description="피처 데이터 수집 및 CSV 저장 스크립트"
    )

    parser.add_argument(
        "--features",
        "-f",
        type=str,
        help="수집할 피처 이름 (쉼표로 구분, 기본값: 모든 피처)",
        default=None,
    )

    parser.add_argument(
        "--time",
        "-t",
        type=str,
        help="수집 시간 (HHMMSS 형식, 기본값: 현재 시간)",
        default=datetime.now().strftime("%H%M%S"),
    )

    parser.add_argument(
        "--config",
        "-c",
        type=str,
        help="feature.yaml 설정 파일 경로",
        default="config/features.yaml",
    )

    parser.add_argument(
        "--params",
        "-p",
        type=str,
        help="params.yaml 설정 파일 경로",
        default="config/params.yaml",
    )

    parser.add_argument(
        "--api-config",
        "-a",
        type=str,
        help="api_config.yaml 설정 파일 경로",
        default="config/api_config.yaml",
    )

    parser.add_argument(
        "--scheduled", "-s", action="store_true", help="스케줄된 피처만 실행"
    )

    parser.add_argument(
        "--test", action="store_true", help="테스트 모드 (CSV 저장 없음)"
    )

    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        help="CSV 파일 저장 디렉토리",
        default="data",
    )

    return parser.parse_args()


def get_schema_name(feature_name: str) -> str:
    """피처 이름에서 스키마 이름 결정

    Args:
        feature_name: 피처 이름

    Returns:
        스키마 이름
    """
    # 투자자 매매동향 데이터는 우선순위 높게 처리
    if "investor" in feature_name:
        return "market_data"
    elif "options" in feature_name:
        return "domestic_options"
    elif "futures" in feature_name and "overseas" in feature_name:
        return "overseas_futures"
    elif "futures" in feature_name:
        return "domestic_futures"
    else:
        return "sushi"


def combine_codes_data(data: Dict[str, Any]) -> pd.DataFrame:
    """여러 코드의 데이터를 하나의 DataFrame으로 합치기

    Args:
        data: 코드별 데이터 딕셔너리

    Returns:
        통합된 DataFrame
    """
    combined_data = []

    for code, code_data in data.items():
        if code_data is None:
            continue

        # API 응답에서 DataFrame 추출
        if isinstance(code_data, dict) and "output2" in code_data:
            df = pd.DataFrame(code_data["output2"])
        elif isinstance(code_data, pd.DataFrame):
            df = code_data
        else:
            continue

        if df.empty:
            continue

        # 코드 컬럼 추가
        df["code"] = code
        combined_data.append(df)

    if combined_data:
        return pd.concat(combined_data, ignore_index=True)
    else:
        return pd.DataFrame()


def filter_investor_data(df: pd.DataFrame) -> pd.DataFrame:
    """투자자 매매동향 데이터에서 외국인, 기관 데이터만 필터링

    Args:
        df: 원본 DataFrame

    Returns:
        필터링된 DataFrame
    """
    # 필요한 컬럼 정의 (외국인, 기관 데이터만)
    essential_columns = [
        # 외국인 데이터
        "frgn_seln_vol",
        "frgn_shnu_vol",
        "frgn_ntby_qty",
        "frgn_seln_tr_pbmn",
        "frgn_shnu_tr_pbmn",
        "frgn_ntby_tr_pbmn",
        # 기관 데이터
        "orgn_seln_vol",
        "orgn_shnu_vol",
        "orgn_ntby_qty",
        "orgn_seln_tr_pbmn",
        "orgn_shnu_tr_pbmn",
        "orgn_ntby_tr_pbmn",
    ]

    # 메타데이터 컬럼들
    meta_columns = ["code", "trade_date", "collection_time"]

    # 존재하는 컬럼만 선택
    available_columns = []
    for col in essential_columns:
        if col in df.columns:
            available_columns.append(col)

    for col in meta_columns:
        if col in df.columns:
            available_columns.append(col)

    return df[available_columns]


def get_csv_filename(feature_name: str, code: str) -> str:
    """피처명과 코드에 따른 적절한 CSV 파일명 생성

    Args:
        feature_name: 피처 이름
        code: 코드명

    Returns:
        CSV 파일명
    """
    # 콜옵션 특별 처리
    if "call_investor" in feature_name and code == "options":
        return "calloptions.csv"
    # 풋옵션 특별 처리
    elif "put_investor" in feature_name and code == "putoptions":
        return "putoptions.csv"
    else:
        return f"{code}.csv"


def save_feature_to_csv(
    feature_name: str,
    data: Any,
    start_date: str,
    end_date: str,
    output_dir: str = "data",
) -> bool:
    """피처 데이터를 CSV로 저장 (코드별로 분리 저장)

    Args:
        feature_name: 피처 이름
        data: 피처 데이터
        start_date: 시작 날짜 (YYYYMMDD)
        end_date: 종료 날짜 (YYYYMMDD)
        output_dir: 출력 디렉토리

    Returns:
        저장 성공 여부
    """
    try:
        # 스키마와 피처별 폴더 생성
        schema_name = get_schema_name(feature_name)
        feature_dir = os.path.join(output_dir, schema_name, feature_name)
        os.makedirs(feature_dir, exist_ok=True)

        saved_files = []

        if isinstance(data, dict):
            # 코드별로 개별 CSV 파일 저장
            for code, code_data in data.items():
                if code_data is None:
                    continue

                # API 응답에서 DataFrame 추출
                if isinstance(code_data, dict) and "output2" in code_data:
                    df = pd.DataFrame(code_data["output2"])
                elif isinstance(code_data, pd.DataFrame):
                    df = code_data
                else:
                    continue

                if df.empty:
                    continue

                # 코드 컬럼 추가
                df["code"] = code

                # 거래일자 및 수집 시간 정보 추가
                current_time = datetime.now()
                df["trade_date"] = get_current_trading_date()
                df["collection_time"] = current_time.strftime("%H:%M:%S")

                # 투자자 매매동향 데이터인 경우 필터링 적용
                if "investor" in feature_name:
                    df = filter_investor_data(df)

                # CSV 파일명 생성 (콜옵션 특별 처리)
                csv_filename = get_csv_filename(feature_name, code)
                csv_path = os.path.join(feature_dir, csv_filename)

                # CSV 저장
                df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                saved_files.append(csv_filename)

        elif isinstance(data, pd.DataFrame):
            # 단일 DataFrame인 경우
            if not data.empty:
                # 거래일자 및 수집 시간 정보 추가
                current_time = datetime.now()
                data["trade_date"] = get_current_trading_date()
                data["collection_time"] = current_time.strftime("%H:%M:%S")

                # 투자자 매매동향 데이터인 경우 필터링 적용
                if "investor" in feature_name:
                    data = filter_investor_data(data)

                csv_filename = f"{feature_name}.csv"
                csv_path = os.path.join(feature_dir, csv_filename)
                data.to_csv(csv_path, index=False, encoding="utf-8-sig")
                saved_files.append(csv_filename)

        if saved_files:
            logger.warning(
                f"✅ {feature_name}: {len(saved_files)}개 파일 저장 완료 ({', '.join(saved_files[:3])}{'...' if len(saved_files) > 3 else ''})"
            )
            return True
        else:
            logger.warning(f"⚠️ {feature_name}: 저장할 데이터가 없습니다")
            return False

    except Exception as e:
        logger.error(f"❌ {feature_name} CSV 저장 중 오류: {str(e)}")
        return False


def collect_and_save_data(
    features: Optional[List[str]] = None,
    time_str: Optional[str] = None,
    features_yaml_path: str = "config/features.yaml",
    params_yaml_path: str = "config/params.yaml",
    api_config_yaml_path: str = "config/api_config.yaml",
    scheduled_only: bool = False,
    test_mode: bool = False,
    output_dir: str = "data",
) -> None:
    """피처 데이터 수집 및 CSV 저장

    Args:
        features: 수집할 피처 이름 목록 (None이면 모든 피처)
        time_str: 수집 시간 (HHMMSS 형식, None이면 현재 시간)
        features_yaml_path: features.yaml 설정 파일 경로
        params_yaml_path: params.yaml 설정 파일 경로
        api_config_yaml_path: api_config.yaml 설정 파일 경로
        scheduled_only: True이면 스케줄된 피처만 실행
        test_mode: True이면 테스트 모드 (CSV 저장 없음)
        output_dir: CSV 파일 저장 디렉토리
    """
    try:
        os.makedirs("logs", exist_ok=True)
        if time_str is None:
            time_str = datetime.now().strftime("%H%M%S")

        # 핵심 정보만 INFO 레벨로 출력
        logger.warning(
            f"🚀 데이터 수집 프로세스 시작: scheduled_only={scheduled_only}, test_mode={test_mode}"
        )

        feature_manager = FeatureManager(
            features_yaml_path=features_yaml_path,
            params_yaml_path=params_yaml_path,
            api_config_yaml_path=api_config_yaml_path,
        )

        # params.yaml에서 날짜 범위 읽기
        with open(params_yaml_path, "r", encoding="utf-8") as f:
            params_config = yaml.safe_load(f)

        features_to_get_data_from: Dict[str, Any] = {}

        if scheduled_only:
            logger.info(f"스케줄된 피처 처리 시작: 시간 {time_str}")
            all_managed_features = feature_manager.get_all_features()
            triggered_feature_names = []
            for name, feature_obj in all_managed_features.items():
                # Feature 클래스의 inquiry 및 inquiry_time_list 속성 직접 사용
                if feature_obj.inquiry and time_str in feature_obj.inquiry_time_list:
                    try:
                        logger.info(f"'{name}' 피처에 대해 스케줄된 inquiry 실행 중...")
                        feature_obj.run(
                            clock=time_str
                        )  # .run()이 _perform_inquiry 호출
                        features_to_get_data_from[name] = feature_obj
                        triggered_feature_names.append(name)
                    except Exception as e:
                        logger.error(
                            f"'{name}' 피처의 스케줄된 inquiry 실행 중 오류: {e}",
                            exc_info=True,
                        )
            if triggered_feature_names:
                logger.info(
                    f"스케줄된 inquiry 실행 완료 피처: {', '.join(triggered_feature_names)}"
                )
            else:
                logger.info(f"{time_str}에 스케줄된 inquiry를 실행할 피처가 없습니다.")
        else:
            # Not scheduled_only: Run specified or all features on-demand
            candidate_features_for_on_demand: Dict[str, Any] = {}
            if features:
                logger.info(
                    f"지정된 피처들에 대해 on-demand inquiry 처리 시작: {features}"
                )
                for feature_name_req in features:
                    feature_obj = feature_manager.get_feature(feature_name_req)
                    if feature_obj:
                        candidate_features_for_on_demand[feature_name_req] = feature_obj
                    else:
                        logger.warning(
                            f"FeatureManager에서 '{feature_name_req}' 피처를 찾을 수 없습니다."
                        )
            else:
                logger.info("모든 피처에 대해 on-demand inquiry 처리 시작")
                candidate_features_for_on_demand = feature_manager.get_all_features()

            if not candidate_features_for_on_demand:
                logger.info("On-demand inquiry를 실행할 피처가 없습니다.")
            else:
                for name, feature_obj in candidate_features_for_on_demand.items():
                    try:
                        # On-demand 실행 시, 피처의 inquiry 플래그가 True인 경우 _perform_inquiry 직접 호출
                        if feature_obj.inquiry:
                            if hasattr(feature_obj, "_perform_inquiry") and callable(
                                feature_obj._perform_inquiry
                            ):
                                logger.info(
                                    f"'{name}' 피처에 대해 on-demand _perform_inquiry 실행 중 (시간: {time_str})..."
                                )
                                feature_obj._perform_inquiry(clock=time_str)
                                features_to_get_data_from[name] = (
                                    feature_obj  # 데이터 가져올 피처 목록에 추가
                                )
                            else:
                                logger.warning(
                                    f"'{name}' 피처는 inquiry가 활성화되어 있지만 _perform_inquiry 메서드가 없습니다."
                                )
                        else:
                            logger.info(
                                f"'{name}' 피처는 inquiry가 비활성화되어 있어 on-demand inquiry를 건너뜁니다."
                            )
                    except Exception as e:
                        logger.error(
                            f"'{name}' 피처의 on-demand _perform_inquiry 실행 중 오류: {e}",
                            exc_info=True,
                        )

        logger.warning(
            f"📊 총 {len(features_to_get_data_from)}개의 피처에 대해 데이터 수집 및 저장을 시도합니다."
        )

        success_count = 0
        failed_count = 0

        if test_mode:
            logger.warning("🧪 테스트 모드: CSV 파일 저장 없이 데이터만 확인합니다.")

        for feature_name, feature in features_to_get_data_from.items():
            try:
                # 피처별 날짜 범위 가져오기
                feature_params = params_config.get(feature_name, {})
                start_date = feature_params.get("start_date", "20250101")
                end_date = feature_params.get("end_date", "20250531")

                # 데이터 수집
                data = feature.call_feature()

                if (
                    data is None
                    or (isinstance(data, pd.DataFrame) and data.empty)
                    or (isinstance(data, dict) and not data)
                ):
                    logger.warning(f"⚠️ {feature_name}: 데이터가 없습니다")
                    failed_count += 1
                    continue

                if test_mode:
                    # 테스트 모드: 데이터 요약만 출력
                    if isinstance(data, dict):
                        logger.warning(
                            f"🔍 {feature_name}: {len(data)}개 코드 데이터 확인됨"
                        )
                    elif isinstance(data, pd.DataFrame):
                        logger.warning(
                            f"🔍 {feature_name}: {len(data)}행 데이터 확인됨"
                        )
                else:
                    # CSV 저장
                    if save_feature_to_csv(
                        feature_name, data, start_date, end_date, output_dir
                    ):
                        success_count += 1
                    else:
                        failed_count += 1

            except Exception as e:
                logger.error(f"❌ {feature_name} 처리 중 오류: {str(e)}")
                failed_count += 1
        # 완료 메시지
        if test_mode:
            logger.warning(
                f"🧪 테스트 모드 완료: {len(features_to_get_data_from)}개 피처 확인됨"
            )
        else:
            logger.warning(
                f"📁 CSV 저장 완료: 성공 {success_count}개, 실패 {failed_count}개"
            )
            if success_count > 0:
                logger.warning(f"💾 저장 위치: {output_dir}/ 디렉토리")

    except Exception as e:
        logger.error(f"❌ 데이터 수집 중 최상위 오류 발생: {str(e)}", exc_info=True)


# DB 관련 함수들이 제거되었습니다. 이제 CSV 저장만 사용합니다.


def main():
    """메인 함수"""
    args = parse_args()

    # 쉼표로 구분된 피처 이름을 리스트로 변환
    features = args.features.split(",") if args.features else None

    collect_and_save_data(
        features=features,
        time_str=args.time,
        features_yaml_path=args.config,
        params_yaml_path=args.params,
        api_config_yaml_path=args.api_config,
        scheduled_only=args.scheduled,
        test_mode=args.test,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
