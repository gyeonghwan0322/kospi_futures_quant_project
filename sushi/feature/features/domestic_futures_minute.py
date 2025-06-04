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

# abstract_feature 모듈에서 Feature 클래스를 임포트합니다.
from sushi.feature.abstract_feature import Feature
from sushi.feature.api_client import APIClient

logger = logging.getLogger(__name__)


class DomesticFuturesMinute(Feature):
    """
    국내 선물/옵션의 분 단위 시세 데이터를 조회하고 관리하는 피처.

    - `features.yaml` 설정을 통해 조회할 종목 코드(`code_list`),
      조회 주기(`inquiry_time_list`), API 파라미터(`params`) 등을 설정합니다.
    - `_perform_inquiry` 메서드를 통해 주기적으로 API를 호출하여 데이터를 업데이트합니다.
    - `call_feature` 메서드를 통해 저장된 분봉 데이터를 반환합니다.
    """

    API_NAME = "선물옵션 분봉조회 [v1_국내선물-012]"

    def __init__(
        self,
        _feature_name: str,
        _code_list: List[str],
        _feature_query: APIClient,
        _quote_connect: bool,
        _inquiry: bool,
        _inquiry_time_list: List[str],
        _inquiry_name_list: List[str],
        _params: Dict,
    ):
        """
        DomesticFuturesMinute 생성자.

        Args:
            _feature_name (str): 피처 이름.
            _code_list (list[str]): 조회 대상 종목 코드 리스트 (FID_INPUT_ISCD).
            _feature_query (APIClient): API 호출에 사용할 APIClient 객체.
            _quote_connect (bool): 사용되지 않음.
            _inquiry (bool): 시간 기반 조회 사용 여부.
            _inquiry_time_list (list[str]): 조회 수행 시각 리스트 (HHMMSS).
            _inquiry_name_list (list[str]): 사용되지 않음.
            _params (dict): 피처 설정 파라미터. 다음 키들을 포함할 수 있음:
                - api_config (dict): api_config.yaml 내용.
                - market_code (str): 시장 구분 코드 (F, O, JF, JO, CF, CM, EU). 기본값 'F'.
                - start_date (str): 조회 시작일 (YYYYMMDD). 기본값: 당일.
                - end_date (str): 조회 종료일 (YYYYMMDD). 기본값: 당일.
                - hour_cls_code (str): 시간 구분 코드 ('0': 장중, '1': 시간외). 기본값 '0'.
                - interval_code (str): 조회 간격 구분 ('01', '03', '05', '10', '15', '30', '60'). 기본값 '01'.
        """
        super().__init__(
            _feature_name,
            _code_list,
            _feature_query,
            False,
            _inquiry,
            _inquiry_time_list,
            _inquiry_name_list,
            _params,
        )
        self.schema_name = "domestic_futures_minute"  # 스키마 이름 설정
        # 분봉 데이터 저장소 (종목 코드별 DataFrame 저장)
        self.minute_prices: Dict[str, pd.DataFrame] = {}
        self._initialize_params()

    def _initialize_params(self):
        """피처 파라미터 초기화 및 기본값 설정"""
        # 모든 파라미터를 params.yaml에서 가져옴
        self.market_code = self.params.get("market_code")
        self.hour_cls_code = self.params.get("hour_cls_code")
        self.interval_code = self.params.get("interval_code")
        self.include_past_data = self.params.get("pw_data_incu_yn")
        self.include_fake_tick = self.params.get("fake_tick_incu_yn")
        self.start_time = self.params.get("start_time")
        self.pagination_delay_sec = self.params.get("pagination_delay_sec")

        # 파라미터 유효성 검증
        valid_intervals = ["01", "03", "05", "10", "15", "30", "60"]
        if self.interval_code not in valid_intervals:
            self.log_warning(
                f"Invalid interval_code '{self.interval_code}'. Defaulting to '01'. Valid options: {valid_intervals}"
            )
            self.interval_code = "01"

    def _perform_inquiry(self, clock: str):
        """
        설정된 시간에 맞추어 모든 대상 종목의 분봉 데이터를 조회하고 업데이트합니다.

        Args:
            clock (str): 현재 시각 (HHMMSS).
        """
        self.log_info(
            f"Performing minute price inquiry for codes {self.code_list} at {clock} "
            f"(interval: {self.interval_code} min, date: {self.start_date}, time: {self.start_time})."
        )

        if not self.code_list:
            self.log_warning("No codes specified for inquiry.")
            return

        for code in self.code_list:
            try:
                params = {
                    "FID_COND_MRKT_DIV_CODE": self.market_code,  # 시장 구분 코드
                    "FID_INPUT_ISCD": code,  # 종목코드
                    "FID_HOUR_CLS_CODE": self.hour_cls_code,  # 시간 구분 코드(30,60,3600 등)
                    "FID_PW_DATA_INCU_YN": self.include_past_data,  # 과거 데이터 포함 여부
                    "FID_FAKE_TICK_INCU_YN": self.include_fake_tick,  # 허봉 포함 여부
                    "FID_INPUT_DATE_1": self.start_date,  # 조회 시작일(YYYYMMDD)
                    "FID_INPUT_HOUR_1": self.start_time,  # 조회 시작시간(HHMMSS)
                }

                self.log_debug(
                    f"Requesting minute price for {code} with params: {params}"
                )

                # 개선된 API 호출 메서드 사용
                response = self.get_api(self.API_NAME, params)

                parsed_data = self.parse_api_response(self.API_NAME, response)

                if parsed_data is not None:
                    # 내부 상태 저장
                    self.minute_prices[code] = parsed_data
                    # 스키마/테이블 기반 저장
                    self.save_data_with_schema(
                        self.schema_name, code.lower(), parsed_data
                    )
                    self.log_info(
                        f"Successfully updated {self.interval_code}-min price data for {code}. Total {len(parsed_data)} records."
                    )
                else:
                    self.log_warning(f"Parsed data is None for code {code}.")

            except Exception as e:
                import traceback

                self.log_error(
                    f"Error during inquiry for code {code}: {e}\n{traceback.format_exc()}"
                )
                self.health_check_value = f"Inquiry failed for {code} at {clock}"

        self.health_check_value = f"Inquiry completed at {clock}"

    def parse_api_response(
        self, api_name: str, response_data: Dict
    ) -> Optional[pd.DataFrame]:
        """
        '선물옵션 분봉조회' API 응답 데이터를 파싱하여 DataFrame으로 변환합니다.

        Args:
            api_name (str): API 이름.
            response_data (Dict): API 응답 원본 딕셔너리.

        Returns:
            Optional[pd.DataFrame]: 파싱된 분봉 데이터프레임. 'output2'가 없거나 오류 시 None 반환.
                                     인덱스는 날짜와 시간을 결합한 DatetimeIndex 로 설정됩니다.
                                     컬럼명은 response_api.json 의 필드명을 따릅니다.
        """
        if api_name != self.API_NAME:
            self.log_error(
                f"parse_api_response called with incorrect API name: {api_name}"
            )
            return None

        # 개선된 공통 파싱 메서드 사용
        df = self.parse_api_basic(
            api_name=api_name,
            response_data=response_data,
            output_key="output2",
            date_column=None,  # 날짜와 시간은 아래에서 별도 처리
            numeric_columns=[
                "futs_prpr",  # 현재가
                "futs_oprc",  # 시가
                "futs_hgpr",  # 고가
                "futs_lwpr",  # 저가
                "cntg_vol",  # 체결 거래량
                "acml_tr_pbmn",  # 누적 거래 대금
            ],
        )

        if df is None:
            return None

        try:
            # 날짜와 시간 컬럼을 결합하여 DatetimeIndex 생성
            if "stck_bsop_date" in df.columns and "stck_cntg_hour" in df.columns:
                # 야간 시간 처리 ('260000' -> 다음날 04:00)
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

                try:
                    df["datetime"] = df.apply(adjust_time, axis=1)
                    df["datetime"] = pd.to_datetime(df["datetime"])
                    df = df.set_index("datetime")
                    df = df.sort_index()  # 시간 순 정렬
                except Exception as time_e:
                    self.log_error(f"Error creating datetime index: {time_e}")
                    # 인덱스 생성 실패 시 원본 컬럼 유지
                    if "stck_bsop_date" in df.columns:  # 날짜만이라도 변환 시도
                        df["stck_bsop_date"] = pd.to_datetime(
                            df["stck_bsop_date"], format="%Y%m%d", errors="coerce"
                        )

            else:
                self.log_warning(
                    "Columns 'stck_bsop_date' or 'stck_cntg_hour' not found for setting index."
                )

            return df

        except Exception as e:
            self.log_error(f"Error parsing minute API response into DataFrame: {e}")
            import traceback

            self.log_error(traceback.format_exc())
            return None

    def call_feature(
        self, code: Optional[str] = None, interval: Optional[str] = None, **kwargs
    ) -> Optional[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:
        """
        저장된 분봉 데이터를 반환합니다.

        Args:
            code (Optional[str]): 조회할 특정 종목 코드. None이면 모든 종목의 데이터 반환.
            interval (Optional[str]): 분봉 간격 (현재는 사용하지 않음, 호환성 위해 유지).
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
                # 스키마/테이블 방식으로 데이터 로드 시도
                data = self.get_data_with_schema(self.schema_name, code.lower())
                if data is not None:
                    self.minute_prices[code] = data
                    return data
                self.log_warning(f"No data available for code {code}.")
                return None
        else:
            # 모든 코드의 데이터 반환
            if not self.minute_prices:
                # 저장소에서 모든 코드의 데이터 로드 시도
                for c in self.code_list:
                    data = self.get_data_with_schema(self.schema_name, c.lower())
                    if data is not None:
                        self.minute_prices[c] = data

            return (
                {k: v.copy() for k, v in self.minute_prices.items()}
                if self.minute_prices
                else None
            )

    # @staticmethod
    # def resample_ohlc(df: pd.DataFrame, rule: str) -> Optional[pd.DataFrame]:
    #     """
    #     분봉 데이터를 더 긴 주기의 OHLC 데이터로 리샘플링하는 정적 메서드 예시.

    #     Args:
    #         df (pd.DataFrame): 분봉 데이터프레임 (DatetimeIndex 포함).
    #         rule (str): 리샘플링 주기 (예: '15T', '1H', 'D'). Pandas resample rule string.

    #     Returns:
    #         Optional[pd.DataFrame]: 리샘플링된 OHLC 데이터프레임. 오류 시 None.
    #     """
    #     if df is None or df.empty or not isinstance(df.index, pd.DatetimeIndex):
    #         logger.warning("Invalid DataFrame for resampling.")
    #         return None
    #     try:
    #         # 리샘플링할 컬럼 확인 (API 응답 필드명 기준)
    #         ohlc_dict = {
    #             "futs_oprc": "first",  # 시가
    #             "futs_hgpr": "max",  # 고가
    #             "futs_lwpr": "min",  # 저가
    #             "futs_prpr": "last",  # 종가
    #             "cntg_vol": "sum",  # 거래량 합계
    #         }
    #         # 실제 DataFrame에 있는 컬럼만 사용
    #         valid_cols = {k: v for k, v in ohlc_dict.items() if k in df.columns}
    #         if not valid_cols:
    #             logger.warning("No valid OHLC columns found for resampling.")
    #             return None

    #         resampled_df = df.resample(rule).agg(valid_cols)
    #         # 컬럼 이름 변경 (선택 사항)
    #         resampled_df = resampled_df.rename(
    #             columns={
    #                 "futs_oprc": "open",
    #                 "futs_hgpr": "high",
    #                 "futs_lwpr": "low",
    #                 "futs_prpr": "close",
    #                 "cntg_vol": "volume",
    #             }
    #         )
    #         return resampled_df.dropna(subset=["open"])  # 시가 없는 행 제거

    #     except Exception as e:
    #         logger.error(f"Error resampling minute data (rule={rule}): {e}")
    #         return None
