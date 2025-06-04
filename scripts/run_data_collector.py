#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
피처 데이터 수집 및 저장 실행 스크립트

특정 피처 클래스를 사용하여 데이터를 수집하고 PostgreSQL 데이터베이스에 저장합니다.
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
from psycopg2.extras import execute_values

# 로깅 설정
current_date = datetime.now().strftime("%Y%m%d")
log_file = f"logs/data_collector_{current_date}.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,  # DEBUG에서 INFO로 변경 - 필요한 로그만 표시
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# 경로 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.append(script_dir)

# 필요한 모듈 임포트
from sushi.feature.feature_manager import FeatureManager
from sushi.database.db_manager import DBManager


def parse_args():
    """명령행 인수 파싱"""
    parser = argparse.ArgumentParser(description="피처 데이터 수집 및 저장 스크립트")

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
        "--db-config",
        "-d",
        type=str,
        help="db_config.yaml 설정 파일 경로",
        default="config/db_config.yaml",
    )

    parser.add_argument(
        "--scheduled", "-s", action="store_true", help="스케줄된 피처만 실행"
    )

    parser.add_argument(
        "--test", action="store_true", help="테스트 모드 (DB 저장 없음)"
    )

    return parser.parse_args()


def load_db_config(db_config_path: str) -> Dict[str, Any]:
    """데이터베이스 설정 로드

    Args:
        db_config_path: 데이터베이스 설정 파일 경로

    Returns:
        데이터베이스 설정
    """
    try:
        with open(db_config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"데이터베이스 설정 로드 실패: {str(e)}")
        return {}


def collect_and_save_data(
    features: Optional[List[str]] = None,
    time_str: Optional[str] = None,
    features_yaml_path: str = "config/features.yaml",
    params_yaml_path: str = "config/params.yaml",
    api_config_yaml_path: str = "config/api_config.yaml",
    db_config_yaml_path: str = "config/db_config.yaml",
    scheduled_only: bool = False,
    test_mode: bool = False,
) -> None:
    """피처 데이터 수집 및 저장

    Args:
        features: 수집할 피처 이름 목록 (None이면 모든 피처)
        time_str: 수집 시간 (HHMMSS 형식, None이면 현재 시간)
        features_yaml_path: features.yaml 설정 파일 경로
        params_yaml_path: params.yaml 설정 파일 경로
        api_config_yaml_path: api_config.yaml 설정 파일 경로
        db_config_yaml_path: db_config.yaml 설정 파일 경로
        scheduled_only: True이면 스케줄된 피처만 실행
        test_mode: True이면 테스트 모드 (DB 저장 없음)
    """
    try:
        os.makedirs("logs", exist_ok=True)
        if time_str is None:
            time_str = datetime.now().strftime("%H%M%S")
        logger.info(
            f"데이터 수집 프로세스 시작: 시간 {time_str}, scheduled_only: {scheduled_only}, test_mode: {test_mode}"
        )

        feature_manager = FeatureManager(
            features_yaml_path=features_yaml_path,
            params_yaml_path=params_yaml_path,
            api_config_yaml_path=api_config_yaml_path,
        )

        db_config = load_db_config(db_config_yaml_path)
        table_names = db_config.get("table_names", {})
        schema_names = db_config.get("schema_names", {})

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

        logger.info(
            f"총 {len(features_to_get_data_from)}개의 피처에 대해 데이터 가져오기 및 저장을 시도합니다."
        )

        if test_mode:
            logger.info("테스트 모드: 데이터베이스에 저장하지 않습니다.")
            for feature_name, feature in features_to_get_data_from.items():
                try:
                    logger.info(
                        f"피처 '{feature_name}'에서 데이터 가져오는 중 (테스트 모드)..."
                    )
                    data = feature.call_feature()
                    if data is not None:
                        if isinstance(data, pd.DataFrame):
                            # 데이터프레임이 비어있는지 명시적으로 확인
                            if data.empty:
                                logger.warning(
                                    f"코드 '{feature_name}'에 대한 데이터가 없습니다."
                                )
                                continue
                        elif isinstance(data, dict) and all(
                            isinstance(v, pd.DataFrame)
                            for v in data.values()
                            if v is not None
                        ):
                            # 모든 데이터프레임이 비어있는지 확인
                            if all(df.empty for df in data.values() if df is not None):
                                logger.warning(
                                    f"코드 '{feature_name}'에 대한 데이터가 없습니다."
                                )
                                continue
                        elif not data:  # 빈 컨테이너 체크
                            logger.warning(
                                f"코드 '{feature_name}'에 대한 데이터가 없습니다."
                            )
                            continue

                        # 테스트 모드에서는 데이터를 출력만 하고 저장하지 않음
                        logger.info(
                            f"피처 '{feature_name}'에서 가져온 데이터 (테스트 모드):"
                        )
                        # 데이터프레임인 경우
                        if isinstance(data, pd.DataFrame):
                            logger.info(f"데이터프레임 모양: {data.shape}")
                            logger.info(f"데이터프레임 예시:\n{data.head(2)}")
                        # 데이터프레임 딕셔너리인 경우
                        elif isinstance(data, dict):
                            for k, v in data.items():
                                if isinstance(v, pd.DataFrame) and not v.empty:
                                    logger.info(
                                        f"키 '{k}' 데이터프레임 모양: {v.shape}"
                                    )
                                    logger.info(f"데이터프레임 예시:\n{v.head(2)}")
                        continue
                except Exception as e:
                    logger.error(
                        f"피처 '{feature_name}' 데이터 처리 중 오류 발생 (테스트 모드): {str(e)}",
                        exc_info=True,
                    )
        else:
            # 실제 모드에서는 DB에 저장
            # DBManager가 컨텍스트 매니저를 지원하는지 확인 (예시 코드는 with 사용)
            # with DBManager(db_config_yaml_path) as db: # 원본 코드 방식
            db_manager = DBManager(
                db_config_yaml_path
            )  # 컨텍스트 매니저 아닐 경우 대비
            try:
                db_manager.connect()  # 명시적 연결 (DBManager 구현에 따라 다를 수 있음)
                for feature_name, feature in features_to_get_data_from.items():
                    try:
                        logger.info(f"피처 '{feature_name}'에서 데이터 가져오는 중...")
                        data = feature.call_feature()

                        if data is None:
                            logger.warning(
                                f"피처 '{feature_name}'에서 데이터를 가져오지 못했거나 데이터가 비어있습니다. DB 저장을 건너뜁니다."
                            )
                            continue

                        logger.info(
                            f"피처 '{feature_name}' 데이터 가져오기 성공. 데이터 타입: {type(data)}. DB 저장 시도..."
                        )

                        table_name = table_names.get(feature_name, feature_name)
                        # 스키마 결정 로직 (원본 유지)
                        if "options" in feature_name:
                            schema_name = schema_names.get(
                                "domestic_options", "domestic_options"
                            )
                        elif "futures" in feature_name and "overseas" in feature_name:
                            schema_name = schema_names.get(
                                "overseas_futures", "overseas_futures"
                            )
                        elif "futures" in feature_name:
                            schema_name = schema_names.get(
                                "domestic_futures", "domestic_futures"
                            )
                        elif "investor" in feature_name:
                            schema_name = schema_names.get("market_data", "market_data")
                        else:
                            schema_name = "ignacio"  # 기본 스키마

                        # investor_trends 피처는 모듈에서 이미 저장하므로 건너뜁니다
                        if "investor_trends" in feature_name:
                            logger.info(
                                f"피처 '{feature_name}'의 데이터는 모듈 내에서 이미 저장되었습니다."
                            )
                            continue

                        # 데이터 유형에 따라 저장 메서드 선택 (원본 유지)
                        target_data_to_save = data
                        if isinstance(data, dict):
                            pass  # DBManager가 dict를 처리한다고 가정

                        if "minute" in feature_name:
                            # 분봉 데이터에 대해 일반 save_minute_price_data 대신
                            # save_futures_minute_data 또는 save_overseas_futures_minute_data 함수를 사용
                            if "futures" in feature_name:
                                if "overseas" in feature_name:
                                    # 해외 선물 분봉 데이터 처리
                                    save_overseas_futures_minute_data(
                                        db_manager,
                                        schema_name,
                                        table_name,
                                        target_data_to_save,
                                    )
                                else:
                                    # 국내 선물 분봉 데이터 처리
                                    save_futures_minute_data(
                                        db_manager,
                                        schema_name,
                                        table_name,
                                        target_data_to_save,
                                    )
                            else:
                                db_manager.save_minute_price_data(
                                    table_name, target_data_to_save, schema_name
                                )
                        elif (
                            "daily" in feature_name
                        ):  # domestic_daily_futures_price 등이 해당
                            db_manager.save_daily_price_data(
                                table_name, target_data_to_save, schema_name
                            )
                        elif (
                            "options" in feature_name
                        ):  # domestic_options_open_interest 등이 해당
                            db_manager.save_options_data(
                                table_name, target_data_to_save, schema_name
                            )
                        elif "investor" in feature_name:
                            db_manager.save_investor_trends_data(
                                table_name, target_data_to_save, schema_name
                            )

                        logger.info(
                            f"피처 '{feature_name}' 데이터 저장 완료 (테이블: {schema_name}.{table_name})"
                        )

                    except Exception as e:
                        logger.error(
                            f"피처 '{feature_name}' DB 저장 중 오류 발생: {str(e)}",
                            exc_info=True,
                        )
            finally:
                if "db_manager" in locals() and db_manager.conn:  # 연결 객체가 있다면
                    db_manager.disconnect()  # 명시적 연결 해제

        logger.info("데이터 수집 및 저장 프로세스 완료")

    except Exception as e:
        logger.error(
            f"데이터 수집 및 저장 중 최상위 오류 발생: {str(e)}", exc_info=True
        )


def save_futures_minute_data(db_manager, schema_name, table_name, data):
    """
    선물 분봉 데이터를 DB에 저장합니다. 이 함수는 pd.DataFrame을 직접 처리합니다.

    Args:
        db_manager (DBManager): 데이터베이스 관리자 객체
        schema_name (str): 데이터를 저장할 스키마 이름
        table_name (str): 데이터를 저장할 테이블 이름
        data (dict): 코드별 분봉 데이터 (dict of pd.DataFrame)
    """
    try:
        db_manager.connect()

        # 스키마 생성
        db_manager.create_schema_if_not_exists(schema_name)

        # 테이블이 이미 존재하면 삭제 (컬럼 불일치 문제 해결)
        db_manager.cursor.execute(
            f"""
            DROP TABLE IF EXISTS {schema_name}.{table_name}
            """
        )
        db_manager.conn.commit()
        logger.info(f"테이블 '{schema_name}.{table_name}'가 존재하면 삭제했습니다.")

        # 테이블 생성 (분봉 데이터 전용 스키마)
        columns = {
            "id": "SERIAL PRIMARY KEY",
            "code": "VARCHAR(20) NOT NULL",
            "date": "DATE NOT NULL",
            "time": "TIME NOT NULL",
            "futs_prpr": "NUMERIC",  # 현재가
            "futs_oprc": "NUMERIC",  # 시가
            "futs_hgpr": "NUMERIC",  # 고가
            "futs_lwpr": "NUMERIC",  # 저가
            "cntg_vol": "NUMERIC",  # 체결 거래량
            "acml_tr_pbmn": "NUMERIC",  # 누적 거래 대금
            "hts_otst_stpl_qty": "NUMERIC",  # 미결제약정 수량
            "otst_stpl_qty_icdc": "NUMERIC",  # 미결제약정 변동
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        db_manager.create_table_if_not_exists(table_name, schema_name, columns)

        # 유니크 제약 조건 추가
        db_manager.cursor.execute(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = '{table_name}_code_date_time_uniq'
                ) THEN
                    ALTER TABLE {schema_name}.{table_name} 
                    ADD CONSTRAINT {table_name}_code_date_time_uniq 
                    UNIQUE (code, date, time);
                END IF;
            END
            $$;
        """
        )

        total_rows = 0

        # 각 코드별 데이터 처리
        for code, code_data in data.items():
            if code_data is None or (hasattr(code_data, "empty") and code_data.empty):
                logger.warning(f"코드 '{code}'에 대한 데이터가 없습니다.")
                continue

            # API 응답에서 DataFrame 추출
            if isinstance(code_data, dict) and "output2" in code_data:
                df = pd.DataFrame(code_data["output2"])
            elif hasattr(code_data, "head"):  # DataFrame인 경우
                df = code_data
            else:
                logger.warning(
                    f"코드 '{code}'에 대한 데이터 형식이 지원되지 않습니다: {type(code_data)}"
                )
                continue

            if df.empty:
                logger.warning(f"코드 '{code}'에 대한 DataFrame이 비어 있습니다.")
                continue

            # 필수 컬럼 확인
            required_columns = ["stck_bsop_date", "stck_cntg_hour", "futs_prpr"]
            if not all(col in df.columns for col in required_columns):
                logger.warning(
                    f"코드 '{code}'에 대한 데이터에 필수 컬럼이 누락되었습니다."
                )
                continue

            # 날짜 변환
            if "stck_bsop_date" in df.columns:
                # 문자열 날짜를 datetime으로 변환
                try:
                    df["date"] = pd.to_datetime(df["stck_bsop_date"], format="%Y%m%d")
                except Exception as e:
                    logger.warning(f"날짜 변환 오류: {e}")
                    continue

            # 시간 변환
            if "stck_cntg_hour" in df.columns:
                try:
                    # 시간 문자열에서 time 객체 생성
                    df["time"] = df["stck_cntg_hour"].apply(
                        lambda x: (
                            datetime.strptime(str(x), "%H%M%S").time()
                            if pd.notna(x) and len(str(x)) == 6
                            else None
                        )
                    )
                except Exception as e:
                    logger.warning(f"시간 변환 오류: {e}")
                    continue

            # 필수 값이 없는 행 필터링
            df = df.dropna(subset=["date", "time"])

            if df.empty:
                logger.warning(f"코드 '{code}'에 대한 유효한 데이터가 없습니다.")
                continue

            # 데이터 변환
            records = []
            for _, row in df.iterrows():
                try:
                    record = (
                        code,
                        row["date"].date(),  # date 객체로 변환
                        row["time"],
                        (
                            float(row.get("futs_prpr", 0))
                            if pd.notna(row.get("futs_prpr"))
                            else None
                        ),  # 현재가
                        (
                            float(row.get("futs_oprc", 0))
                            if pd.notna(row.get("futs_oprc"))
                            else None
                        ),  # 시가
                        (
                            float(row.get("futs_hgpr", 0))
                            if pd.notna(row.get("futs_hgpr"))
                            else None
                        ),  # 고가
                        (
                            float(row.get("futs_lwpr", 0))
                            if pd.notna(row.get("futs_lwpr"))
                            else None
                        ),  # 저가
                        (
                            int(row.get("cntg_vol", 0))
                            if pd.notna(row.get("cntg_vol"))
                            else None
                        ),  # 거래량
                        (
                            float(row.get("acml_tr_pbmn", 0))
                            if pd.notna(row.get("acml_tr_pbmn"))
                            else None
                        ),  # 거래대금
                        (
                            int(row.get("hts_otst_stpl_qty", 0))
                            if pd.notna(row.get("hts_otst_stpl_qty"))
                            else None
                        ),  # 미결제약정
                        (
                            int(row.get("otst_stpl_qty_icdc", 0))
                            if pd.notna(row.get("otst_stpl_qty_icdc"))
                            else None
                        ),  # 미결제약정 변동
                    )
                    records.append(record)
                except Exception as e:
                    logger.warning(f"행 변환 오류: {e}")
                    continue

            if not records:
                logger.warning(f"코드 '{code}'에 대한 유효한 레코드가 없습니다.")
                continue

            # 데이터 삽입 쿼리
            insert_sql = f"""
                INSERT INTO {schema_name}.{table_name} 
                (code, date, time, futs_prpr, futs_oprc, futs_hgpr, futs_lwpr, cntg_vol, acml_tr_pbmn, hts_otst_stpl_qty, otst_stpl_qty_icdc)
                VALUES %s
                ON CONFLICT (code, date, time) 
                DO UPDATE SET 
                    futs_prpr = EXCLUDED.futs_prpr,
                    futs_oprc = EXCLUDED.futs_oprc,
                    futs_hgpr = EXCLUDED.futs_hgpr,
                    futs_lwpr = EXCLUDED.futs_lwpr,
                    cntg_vol = EXCLUDED.cntg_vol,
                    acml_tr_pbmn = EXCLUDED.acml_tr_pbmn,
                    hts_otst_stpl_qty = EXCLUDED.hts_otst_stpl_qty,
                    otst_stpl_qty_icdc = EXCLUDED.otst_stpl_qty_icdc,
                    created_at = CURRENT_TIMESTAMP
            """

            # 데이터 삽입
            execute_values(db_manager.cursor, insert_sql, records)
            db_manager.conn.commit()

            total_rows += len(records)
            logger.info(
                f"코드 '{code}'에 대해 {len(records)}개의 레코드가 저장되었습니다."
            )

        logger.info(
            f"총 {total_rows}개의 분봉 데이터 레코드가 '{schema_name}.{table_name}'에 저장되었습니다."
        )

    except Exception as e:
        logger.error(f"분봉 데이터 저장 중 오류 발생: {str(e)}")
        if db_manager.conn:
            db_manager.conn.rollback()
        raise
    finally:
        # 연결은 외부에서 관리하므로 여기서는 커밋만 수행
        if db_manager.conn:
            db_manager.conn.commit()


def save_overseas_futures_minute_data(db_manager, schema_name, table_name, data):
    """
    해외 선물 분봉 데이터를 DB에 저장합니다. 이 함수는 pd.DataFrame을 직접 처리합니다.

    Args:
        db_manager (DBManager): 데이터베이스 관리자 객체
        schema_name (str): 데이터를 저장할 스키마 이름
        table_name (str): 데이터를 저장할 테이블 이름
        data (dict): 코드별 분봉 데이터 (dict of pd.DataFrame)
    """
    try:
        db_manager.connect()

        # 스키마 생성
        db_manager.create_schema_if_not_exists(schema_name)

        # 테이블이 이미 존재하면 삭제 (컬럼 불일치 문제 해결)
        db_manager.cursor.execute(
            f"""
            DROP TABLE IF EXISTS {schema_name}.{table_name}
            """
        )
        db_manager.conn.commit()
        logger.info(f"테이블 '{schema_name}.{table_name}'가 존재하면 삭제했습니다.")

        # 테이블 생성 (해외 선물 분봉 데이터 전용 스키마)
        columns = {
            "id": "SERIAL PRIMARY KEY",
            "code": "VARCHAR(20)",  # 종목코드 (예: ESM25)
            "datetime": "TIMESTAMP",  # 일시
            "date": "VARCHAR(8)",  # 일자 (YYYYMMDD)
            "time": "VARCHAR(6)",  # 시각 (HHMMSS)
            "open": "NUMERIC",  # 시가
            "high": "NUMERIC",  # 고가
            "low": "NUMERIC",  # 저가
            "close": "NUMERIC",  # 종가(체결가)
            "volume": "NUMERIC",  # 거래량
            "volume_tick": "NUMERIC",  # 체결수량
            "change": "NUMERIC",  # 전일대비
            "change_rate": "NUMERIC",  # 전일대비율
            "change_sign": "VARCHAR(2)",  # 전일대비부호
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        columns_str = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        db_manager.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                {columns_str}
            )
            """
        )
        db_manager.conn.commit()
        logger.info(f"테이블 '{schema_name}.{table_name}'를 생성했습니다.")

        # 각 종목 데이터를 테이블에 저장
        for code, df in data.items():
            # code 열 추가
            df["code"] = code

            # DataFrame을 딕셔너리 리스트로 변환
            records = df.to_dict("records")

            # 레코드가 비어있지 않은 경우에만 저장
            if records:
                # 저장할 컬럼 목록 생성 (테이블에 있는 컬럼 중 DataFrame에 있는 컬럼만)
                cols = [
                    col
                    for col in columns.keys()
                    if col in df.columns or col in ["id", "created_at"]
                ]
                cols_without_id = [
                    col for col in cols if col not in ["id", "created_at"]
                ]

                # SQL 쿼리 생성
                placeholders = ", ".join(["%s"] * len(cols_without_id))
                insert_query = f"""
                    INSERT INTO {schema_name}.{table_name} ({", ".join(cols_without_id)})
                    VALUES ({placeholders})
                """

                # 데이터 삽입
                values = []
                for record in records:
                    row = [record.get(col) for col in cols_without_id]
                    values.append(row)

                db_manager.cursor.executemany(insert_query, values)
                db_manager.conn.commit()
                logger.info(
                    f"종목 {code}의 {len(records)}개 분봉 데이터를 '{schema_name}.{table_name}' 테이블에 저장했습니다."
                )
            else:
                logger.warning(f"종목 {code}의 데이터가 비어있어 저장하지 않았습니다.")

    except Exception as e:
        logger.error(f"해외 선물 분봉 데이터 저장 중 오류 발생: {e}")
        db_manager.conn.rollback()
    finally:
        db_manager.disconnect()


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
        db_config_yaml_path=args.db_config,
        scheduled_only=args.scheduled,
        test_mode=args.test,
    )


if __name__ == "__main__":
    main()
