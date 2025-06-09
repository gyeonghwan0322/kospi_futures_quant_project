# -*- coding: utf-8 -*-
"""
국내 선물/옵션의 분 단위 시세 데이터를 조회하는 피처 모듈.
'선물옵션 분봉조회 [v1_국내선물-012]' API를 사용합니다.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime, timedelta
import time
import traceback

# abstract_feature 모듈에서 Feature 클래스를 임포트합니다.
from src.data_collection.abstract_feature import Feature
from src.data_collection.api_client import APIClient

logger = logging.getLogger(__name__)


class DomesticFuturesMinute(Feature):
    """
    국내 선물/옵션의 분 단위 시세 데이터를 조회하고 관리하는 피처.

    - `features.yaml` 설정을 통해 조회할 종목 코드(`code_list`),
      API 파라미터(`params`) 등을 설정합니다.
    - `call_feature` 메서드를 통해 저장된 분봉 데이터를 반환합니다.
    """

    def __init__(
        self,
        _feature_name: str,
        _code_list: List[str],
        _feature_query: APIClient,
        _quote_connect: bool,
        _params: Dict,
    ):
        """
        DomesticFuturesMinute 생성자.

        Args:
            _feature_name (str): 피처 이름.
            _code_list (list[str]): 조회 대상 종목 코드 리스트 (FID_INPUT_ISCD).
            _feature_query (APIClient): API 호출에 사용할 APIClient 객체.
            _quote_connect (bool): 사용되지 않음.
            _params (dict): 피처 설정 파라미터.
        """
        super().__init__(
            _feature_name,
            _code_list,
            _feature_query,
            False,
            _params,
        )
        # 기본 스키마 이름 (선물용)
        self.schema_name = "domestic_futures_minute"
        # 분봉 데이터 저장소 (종목 코드별 DataFrame 저장)
        self.minute_prices: Dict[str, pd.DataFrame] = {}

        # 코드별 스키마 매핑 설정
        self._setup_code_schema_mapping()

        # API 설정에서 API 정보 가져오기
        self.api_name = "선물옵션 분봉조회"
        api_endpoints = self.params.get("api_config", {}).get("api_endpoints", {})
        api_info = api_endpoints.get(self.api_name, {})
        self.tr_id = api_info.get("tr_id", "FHKIF03020200")

        self._initialize_params()

    def _setup_code_schema_mapping(self):
        """각 코드별로 적절한 스키마를 매핑"""
        self.code_schema_map = {}
        option_codes = []
        futures_codes = []

        for code in self.code_list:
            if code.startswith(("2", "3")):  # 옵션 코드 (콜옵션: 2xx, 풋옵션: 3xx)
                self.code_schema_map[code] = "domestic_options_minute"
                option_codes.append(code)
            else:  # 선물 코드
                self.code_schema_map[code] = "domestic_futures_minute"
                futures_codes.append(code)

        logger.info(
            f"📋 스키마 매핑 설정 완료 - 옵션: {len(option_codes)}개 → domestic_options_minute, "
            f"선물: {len(futures_codes)}개 → domestic_futures_minute"
        )

    def _initialize_params(self):
        """피처 파라미터 초기화 및 기본값 설정"""
        # 파라미터 설정
        self.market_code = self.params.get("market_code", "F")
        self.hour_cls_code = self.params.get("hour_cls_code", "60")
        self.include_past_data = self.params.get("pw_data_incu_yn", "Y")
        self.include_fake_tick = self.params.get("fake_tick_incu_yn", "N")

        # 날짜 범위 설정
        self.start_date = self.params.get("start_date", "20240101")
        self.end_date = self.params.get("end_date", "20251231")
        self.start_time = self.params.get("start_time", "090000")
        self.end_time = self.params.get("end_time", "153000")

        # 조회 제한 설정
        self.max_records_per_request = self.params.get("max_records_per_request", 102)
        self.pagination_delay_sec = self.params.get("pagination_delay_sec", 0.5)
        self.max_days_per_batch = self.params.get("max_days_per_batch", 1)

        # 파라미터 유효성 검증
        valid_hour_cls_codes = ["30", "60", "3600"]  # 30초, 1분, 1시간
        if self.hour_cls_code not in valid_hour_cls_codes:
            logger.warning(
                f"Invalid hour_cls_code '{self.hour_cls_code}'. Defaulting to '60'. Valid options: {valid_hour_cls_codes}"
            )
            self.hour_cls_code = "60"

        logger.info(f"DomesticFuturesMinute 파라미터 초기화 완료:")
        logger.info(f"  - 조회 기간: {self.start_date} ~ {self.end_date}")
        logger.info(f"  - 조회 시간: {self.start_time} ~ {self.end_time}")
        logger.info(f"  - 봉 간격: {self.hour_cls_code}")

    def _generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """조회 기간의 날짜 리스트 생성 (주말 제외)"""
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")

        dates = []
        current_dt = start_dt

        while current_dt <= end_dt:
            # 주말(토요일=5, 일요일=6) 제외
            if current_dt.weekday() < 5:
                dates.append(current_dt.strftime("%Y%m%d"))
            current_dt += timedelta(days=1)

        return dates

    def _call_minute_api(
        self, code: str, target_date: str, target_time: str
    ) -> Optional[Dict]:
        """선물옵션 분봉조회 API 호출"""
        try:
            # API 파라미터 구성
            params = {
                "FID_COND_MRKT_DIV_CODE": self.market_code,  # 시장 구분 코드
                "FID_INPUT_ISCD": code,  # 종목코드
                "FID_HOUR_CLS_CODE": self.hour_cls_code,  # 시간 구분 코드
                "FID_PW_DATA_INCU_YN": self.include_past_data,  # 과거 데이터 포함 여부
                "FID_FAKE_TICK_INCU_YN": self.include_fake_tick,  # 허봉 포함 여부
                "FID_INPUT_DATE_1": target_date,  # 조회 시작일
                "FID_INPUT_HOUR_1": target_time,  # 조회 시작시간
            }

            logger.debug(f"분봉 API 호출: {code}, {target_date} {target_time}")
            logger.debug(f"파라미터: {params}")

            response = self._feature_query.call_api(
                path="/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice",
                method="GET",
                tr_id=self.tr_id,
                params=params,
            )

            if response and response.get("rt_cd") == "0":
                return response
            else:
                error_msg = (
                    response.get("msg1", "Unknown error") if response else "No response"
                )
                logger.error(
                    f"API 호출 실패: {code}, {target_date} {target_time} - {error_msg}"
                )
                return None

        except Exception as e:
            logger.error(f"분봉 API 호출 중 오류: {e}")
            logger.error(traceback.format_exc())
            return None

    def _process_minute_data(self, raw_data: Dict, code: str) -> pd.DataFrame:
        """API 응답 데이터를 DataFrame으로 변환"""
        if not raw_data or "output2" not in raw_data:
            return pd.DataFrame()

        try:
            # DataFrame 생성
            df = pd.DataFrame(raw_data["output2"])

            if df.empty:
                return df

            # 날짜와 시간 컬럼을 결합하여 DatetimeIndex 생성
            if "stck_bsop_date" in df.columns and "stck_cntg_hour" in df.columns:

                def adjust_time(row):
                    date_str = row["stck_bsop_date"]
                    time_str = row["stck_cntg_hour"]
                    hour = int(time_str[:2])

                    # API 문서에 따르면 자정 이후 시간은 +24시간으로 표시됨
                    if hour >= 24:
                        # 다음날로 날짜 조정하고 시간은 24 빼기
                        dt = datetime.strptime(date_str, "%Y%m%d") + timedelta(days=1)
                        time_adjusted = f"{hour-24:02d}{time_str[2:]}"
                        dt = datetime.strptime(
                            dt.strftime("%Y%m%d") + time_adjusted, "%Y%m%d%H%M%S"
                        )
                    else:
                        dt = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
                    return dt

                df["datetime"] = df.apply(adjust_time, axis=1)
                df["datetime"] = pd.to_datetime(df["datetime"])
                df = df.set_index("datetime")
                df = df.sort_index()  # 시간 순 정렬

            # 종목 코드 추가
            df["code"] = code

            # 수치형 변환이 필요한 컬럼들
            numeric_columns = [
                "futs_prpr",  # 현재가
                "futs_oprc",  # 시가
                "futs_hgpr",  # 고가
                "futs_lwpr",  # 저가
                "cntg_vol",  # 체결 거래량
                "acml_tr_pbmn",  # 누적 거래 대금
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # 컬럼 순서 정리
            base_columns = ["code"]
            other_columns = [
                col
                for col in df.columns
                if col not in base_columns + ["stck_bsop_date", "stck_cntg_hour"]
            ]
            df = df[base_columns + other_columns]

            logger.debug(f"분봉 데이터 처리 완료: {code}, {len(df)}건")
            return df

        except Exception as e:
            logger.error(f"분봉 데이터 처리 중 오류: {e}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def _collect_code_data(self, code: str) -> pd.DataFrame:
        """단일 종목의 전체 기간 분봉 데이터 수집"""
        logger.info(f"종목 {code} 분봉 데이터 수집 시작")

        all_data_list = []
        date_list = self._generate_date_range(self.start_date, self.end_date)

        for target_date in date_list:
            current_time = self.end_time  # 마지막 시간부터 역순으로 조회

            while current_time >= self.start_time:
                # API 호출
                response = self._call_minute_api(code, target_date, current_time)

                if response:
                    # 데이터 처리
                    processed_data = self._process_minute_data(response, code)

                    if not processed_data.empty:
                        all_data_list.append(processed_data)

                        # 마지막 시간 확인 (다음 조회를 위해)
                        if len(processed_data) >= self.max_records_per_request:
                            # 더 이전 데이터가 있을 수 있음
                            last_datetime = processed_data.index.min()
                            if isinstance(last_datetime, pd.Timestamp):
                                current_time = (
                                    last_datetime - timedelta(minutes=1)
                                ).strftime("%H%M%S")
                            else:
                                break
                        else:
                            # 해당 날짜의 모든 데이터 수집 완료
                            break
                    else:
                        # 더 이상 데이터가 없음
                        break
                else:
                    # API 호출 실패
                    break

                # API 호출 간격 조절
                time.sleep(self.pagination_delay_sec)

            logger.debug(f"날짜 {target_date} 처리 완료")

        # 모든 데이터 통합
        if all_data_list:
            combined_df = pd.concat(all_data_list, ignore_index=False)
            combined_df = combined_df.sort_index().drop_duplicates()  # 중복 제거
            logger.info(f"종목 {code} 데이터 수집 완료: {len(combined_df)}건")
            return combined_df
        else:
            logger.warning(f"종목 {code}에 대한 데이터가 없습니다.")
            return pd.DataFrame()

    def collect_data(self):
        """
        모든 대상 종목의 분봉 데이터를 조회하고 업데이트합니다.
        """
        time_display = {"30": "30초", "60": "1분", "3600": "1시간"}.get(
            self.hour_cls_code, f"{self.hour_cls_code}초"
        )

        logger.info(
            f"📊 분봉 데이터 수집 시작 - 코드: {self.code_list} "
            f"(간격: {time_display}, 기간: {self.start_date}~{self.end_date})"
        )

        if not self.code_list:
            logger.warning("조회할 종목 코드가 없습니다.")
            return

        for code in self.code_list:
            try:
                # 종목별 데이터 수집
                data = self._collect_code_data(code)

                if not data.empty:
                    # 메모리에 저장
                    self.minute_prices[code] = data

                    # 파일로 저장 (스키마별)
                    schema_name = self.code_schema_map.get(code, self.schema_name)
                    self.save_data_to_file_with_schema(data, code.lower(), schema_name)

                    logger.info(
                        f"✅ {code}: {time_display} 분봉 데이터 저장 완료 - 총 {len(data)}건"
                    )

            except Exception as e:
                logger.error(f"종목 {code} 데이터 수집 중 오류: {e}")
                logger.error(traceback.format_exc())

        logger.info("📊 분봉 데이터 수집 완료")

    def call_feature(
        self, code: Optional[str] = None, interval: Optional[str] = None, **kwargs
    ) -> Optional[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:
        """
        저장된 분봉 데이터를 반환합니다.

        Args:
            code (Optional[str]): 조회할 특정 종목 코드. None이면 모든 종목의 데이터 반환.
            interval (Optional[str]): 시간 간격 (현재는 사용하지 않음, 호환성을 위해 유지).
            **kwargs: 추가 파라미터 (현재 사용 안 함).

        Returns:
            pd.DataFrame or Dict[str, pd.DataFrame] or None:
            - code가 지정된 경우 해당 코드의 데이터프레임 반환.
            - code가 None인 경우 {코드: 데이터프레임} 형태의 딕셔너리 반환.
            - 데이터가 없는 경우 None 반환.
        """
        if code:
            if code in self.minute_prices:
                return self.minute_prices[code].copy()
            else:
                # 코드별 적절한 스키마 사용
                schema_name = self.code_schema_map.get(code, self.schema_name)
                data = self.get_data_with_schema(schema_name, code.lower())
                if data is not None:
                    self.minute_prices[code] = data
                    return data
                logger.warning(f"No data available for code {code}.")
                return None
        else:
            # 모든 코드의 데이터 반환
            if not self.minute_prices:
                # 저장소에서 모든 코드의 데이터 로드 시도 (코드별 스키마 사용)
                for c in self.code_list:
                    schema_name = self.code_schema_map.get(c, self.schema_name)
                    data = self.get_data_with_schema(schema_name, c.lower())
                    if data is not None:
                        self.minute_prices[c] = data

            return (
                {k: v.copy() for k, v in self.minute_prices.items()}
                if self.minute_prices
                else None
            )
