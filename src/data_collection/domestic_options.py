# -*- coding: utf-8 -*-
"""
국내 옵션 데이터를 조회하고 내재변동성(IV)을 추정하는 피처 모듈.
'국내옵션전광판_콜풋[국내선물-022]' API를 사용합니다.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import time
import traceback

# abstract_feature 모듈에서 Feature 클래스를 임포트합니다.
from src.data_collection.abstract_feature import Feature
from src.data_collection.api_client import APIClient

logger = logging.getLogger(__name__)


class DomesticOptionsIV(Feature):
    """
    국내 옵션 데이터(KOSPI200, KOSDAQ150 등)를 조회하고 내재변동성(IV)을 추정하는 피처.

    - `features.yaml` 설정을 통해 시장 유형(`market_type`),
      델타 범위(`delta_range`), 만기 유형(`maturity_types`) 등을 설정합니다.
    - `_perform_inquiry` 메서드를 통해 주기적으로, 또는 특정 시점에 API를 호출하여 데이터를 업데이트합니다.
    - `call_feature` 메서드를 통해 저장된 옵션 데이터와 IV 값을 반환합니다.
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
        DomesticOptionsIV 생성자.

        Args:
            _feature_name (str): 피처 이름.
            _code_list (list[str]): 조회 대상 종목 코드 리스트 (미사용, 시장 유형으로 대체).
            _feature_query (APIClient): API 호출에 사용할 APIClient 객체.
            _quote_connect (bool): 사용되지 않음.
            _params (dict): 피처 설정 파라미터. 다음 키들을 포함할 수 있음:
                - api_config (dict): api_config.yaml 내용.
                - market_type (str): 시장 유형 ("KOSPI200", "KOSDAQ150", "MINI_KOSPI").
                - delta_range (list): 조회할 옵션의 델타 범위 (e.g., [0.25, 0.75]).
                - maturity_types (list): 조회할 만기 유형 리스트 (e.g., ["MONTHLY", "QUARTERLY"]).
        """
        super().__init__(
            _feature_name,
            _code_list,
            _feature_query,
            False,
            _params,
        )
        self.schema_name = "domestic_options_iv"  # 스키마 이름 설정
        # 옵션 데이터 저장소
        self.option_data = {}  # 옵션 데이터 (call, put 별로 저장)
        self.iv_data = {}  # 추정된 IV 데이터
        self.maturity_info = {}  # 만기 정보

        # API 설정에서 API 정보 가져오기
        self.api_name_board = "국내옵션전광판_콜풋"
        api_endpoints = self.params.get("api_config", {}).get("api_endpoints", {})
        board_api_info = api_endpoints.get(self.api_name_board, {})
        self.board_tr_id = board_api_info.get("tr_id", "FHPIF05030100")

        self._initialize_params()

        # 데이터 저장 관련 설정
        self.options_data: Dict[str, pd.DataFrame] = {}

    def _initialize_params(self):
        """피처 파라미터 초기화 및 기본값 설정"""
        # 모든 파라미터를 params.yaml에서 가져옴
        self.market_type = self.params.get("market_type", "KOSPI200")
        self.delta_range = self.params.get("delta_range", [0.25, 0.75])
        self.maturity_types = self.params.get("maturity_types", ["MONTHLY"])
        self.pagination_delay_sec = self.params.get("pagination_delay_sec", 1.0)

        # 새로 추가된 파라미터들
        self.market_div_code = self.params.get("market_div_code", "O")
        self.screen_div_code = self.params.get("screen_div_code", "20503")
        self.call_market_code = self.params.get("call_market_code", "CO")
        self.put_market_code = self.params.get("put_market_code", "PO")
        self.market_cls_code = self.params.get("market_cls_code", "")
        self.maturity_months = self.params.get("maturity_months", [])

        # 시장 유형에 따른 설정
        self.market_config = self._get_market_config()

        # 오늘 날짜
        self.today = datetime.now().strftime("%Y%m%d")

        # 로깅
        self.log_info(
            f"Initialized with market_type={self.market_type}, "
            f"delta_range={self.delta_range}, maturity_types={self.maturity_types}, "
            f"maturity_months={self.maturity_months}"
        )

    def _get_market_config(self) -> Dict:
        """시장 유형에 따른 설정 반환 - params에서 market_mappings 사용"""
        market_mappings = self.params.get("market_mappings", {})

        if self.market_type in market_mappings:
            return market_mappings[self.market_type]

        # 기본값 (KOSPI200)
        default_config = {
            "cond_mrkt_div_code": "O",  # 옵션 시장
            "cond_scr_div_code": "20503",  # 화면 번호
            "cond_mrkt_cls_code": "",  # KOSPI200 옵션
            "mrkt_cls_code": "CO",  # 콜옵션
            "mrkt_cls_code1": "PO",  # 풋옵션
        }

        self.log_warning(
            f"시장 유형 '{self.market_type}'의 매핑 정보가 없습니다. 기본값 사용."
        )
        return default_config

    # 월물 리스트 조회는 더 이상 사용하지 않음 - params에서 maturity_months 직접 설정

    def _get_maturity_info(self) -> Dict[str, str]:
        """
        월물 정보(코드, 만기일) 가져오기 - params.yaml에서 설정된 월물 리스트 사용

        Returns:
            Dict[str, str]: 월물 코드를 키로 하고 만기일을 값으로 하는 딕셔너리
        """
        maturity_info = {}

        if not self.maturity_months:
            self.log_warning("maturity_months가 설정되지 않았습니다")
            return {}

        for month_str in self.maturity_months:
            # YYYYMM 형식을 월 말일로 변환
            try:
                if len(month_str) == 6 and month_str.isdigit():  # YYYYMM 형식
                    year = int(month_str[:4])
                    month = int(month_str[4:6])

                    # 월의 마지막 날짜 계산
                    if month == 12:
                        next_month = datetime(year + 1, 1, 1)
                    else:
                        next_month = datetime(year, month + 1, 1)
                    last_day = next_month - timedelta(days=1)
                    exp_date = last_day.strftime("%Y%m%d")

                    maturity_info[month_str] = exp_date

            except Exception as e:
                self.log_warning(f"월물 {month_str} 파싱 실패: {e}")
                continue

        self.log_info(f"설정된 월물 정보: {list(maturity_info.keys())}")
        return maturity_info

    def _classify_maturity(self, code: str) -> str:
        """만기 코드 분류 (월물, 분기물)"""
        # 월물/분기물 (YYYYMM)
        if len(code) == 6 and code.isdigit():  # 월물 코드가 YYYYMM 형식인 경우
            year = int(code[:4])
            month = int(code[4:6])

            # 분기물 (3, 6, 9, 12월)
            if month in [3, 6, 9, 12]:
                return "QUARTERLY"
            # 일반 월물
            return "MONTHLY"

        return "UNKNOWN"

    def _convert_to_api_format(self, code: str) -> str:
        """만기 코드를 API 요청에 맞는 형식으로 변환"""
        # 일반/분기 옵션 (YYYYMM)
        if len(code) == 6 and code.isdigit():
            return code

        return ""

    # 스케줄 관련 메서드 제거됨 (사용 안 함)

    def collect_data(self):
        """
        옵션 데이터를 조회하고 델타 0.25~0.75 범위의 데이터만 저장합니다.
        """
        self.log_warning("📊 옵션 데이터 수집 시작")

        # 월물 정보 조회
        self.maturity_info = self._get_maturity_info()
        if not self.maturity_info:
            self.log_warning("옵션 월물 정보가 없습니다")
            return

        # 모든 월물에 대해 옵션 데이터 조회
        for maturity_code, exp_date in self.maturity_info.items():
            self.log_warning(f"📈 {maturity_code} 옵션 처리 중 (만기: {exp_date})...")

            # 콜옵션 데이터 조회
            call_data = self._fetch_option_data("call", maturity_code)

            # 풋옵션 데이터 조회
            put_data = self._fetch_option_data("put", maturity_code)

            # 데이터가 없으면 건너뛰기
            if (
                call_data is None
                or put_data is None
                or call_data.empty
                or put_data.empty
            ):
                self.log_warning(f"{maturity_code} 옵션 데이터가 없습니다")
                continue

            # 델타 범위 필터링 적용
            filtered_call = self._filter_by_delta(call_data)
            filtered_put = self._filter_by_delta(put_data)

            if filtered_call is not None and filtered_put is not None:
                # 콜옵션과 풋옵션 데이터를 만기월별로 저장
                self.option_data[maturity_code] = {
                    "call_options": filtered_call,  # 콜옵션으로 명확히 표기
                    "put_options": filtered_put,  # 풋옵션으로 명확히 표기
                }

                self.log_warning(f"✅ {maturity_code} 콜/풋 옵션 데이터 업데이트 완료")
            else:
                self.log_warning(
                    f"{maturity_code} 옵션의 델타 범위 필터링 후 데이터 없음"
                )

        self.log_warning("📊 옵션 데이터 수집 완료")
        self.health_check_value = "옵션 데이터 수집 완료"

    def _filter_by_delta(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        델타 범위(0.25~0.75)로 옵션 데이터 필터링

        Args:
            df (pd.DataFrame): 원본 옵션 데이터

        Returns:
            Optional[pd.DataFrame]: 필터링된 데이터 또는 None
        """
        if df is None or df.empty or "delta_val" not in df.columns:
            return None

        try:
            min_delta, max_delta = self.delta_range
            delta_filter = (df["delta_val"].abs() >= min_delta) & (
                df["delta_val"].abs() <= max_delta
            )
            filtered_df = df[delta_filter].copy()

            if filtered_df.empty:
                self.log_warning(
                    f"델타 범위 {self.delta_range}에 해당하는 옵션이 없습니다"
                )
                return None

            # 현재 시간 추가
            filtered_df["timestamp"] = datetime.now().strftime("%Y%m%d%H%M%S")

            # 행사가 기준 정렬
            filtered_df = filtered_df.sort_values("acpr")

            return filtered_df

        except Exception as e:
            self.log_error(f"델타 필터링 중 오류: {e}")
            return None

    def _fetch_option_data(
        self, option_type: str, mtrt_cnt: str
    ) -> Optional[pd.DataFrame]:
        """
        특정 옵션 타입(콜/풋)과 월물에 대한 옵션 데이터 조회

        Args:
            option_type (str): 옵션 타입 ('call' 또는 'put')
            mtrt_cnt (str): 월물 번호

        Returns:
            Optional[pd.DataFrame]: 조회된 옵션 데이터 또는 None (오류 발생 시)
        """
        self.log_info(f"Fetching options data for maturity: {mtrt_cnt}")

        # API 요청 파라미터 준비 (작동하는 코드와 동일한 구조)
        params = {
            "FID_COND_MRKT_DIV_CODE": self.market_config[
                "cond_mrkt_div_code"
            ],  # 조건 시장 분류 코드
            "FID_COND_SCR_DIV_CODE": self.market_config[
                "cond_scr_div_code"
            ],  # 조건 화면 분류 코드
            "FID_MRKT_CLS_CODE": self.market_config[
                "mrkt_cls_code"
            ],  # 시장 구분 코드 (콜옵션)
            "FID_MTRT_CNT": mtrt_cnt,  # 만기 수
            "FID_COND_MRKT_CLS_CODE": self.market_config[
                "cond_mrkt_cls_code"
            ],  # 조건 시장 구분 코드
            "FID_MRKT_CLS_CODE1": self.market_config[
                "mrkt_cls_code1"
            ],  # 시장 구분 코드 (풋옵션)
        }

        # 개선된 API 호출 메서드 사용 (올바른 TR_ID 사용)
        response = self.get_api(self.api_name_board, params, tr_id=self.board_tr_id)

        # API 응답 확인
        if not self.handle_api_error(response, self.api_name_board):
            return None

        # 콜/풋 데이터 모두 파싱 (작동하는 코드와 동일한 방식)
        parsed_data = self._parse_option_response(response)

        if parsed_data and option_type in parsed_data:
            return parsed_data[option_type]
        else:
            self.log_warning(
                f"No {option_type} option data found for maturity {mtrt_cnt}"
            )
            return None

    def _parse_option_response(
        self, response_data: Dict
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        옵션 전광판 API 응답을 콜/풋 DataFrame으로 분리 파싱

        Args:
            response_data (Dict): API 응답 원본 딕셔너리

        Returns:
            Optional[Dict[str, pd.DataFrame]]: {"call": 콜옵션_DF, "put": 풋옵션_DF} 형태
        """
        try:
            result = {}

            # 콜옵션 데이터 파싱 (output1)
            call_df = self.parse_api_basic(
                api_name=self.api_name_board,
                response_data=response_data,
                output_key="output1",
                date_column=None,
                numeric_columns=[
                    "acpr",  # 행사가
                    "optn_prpr",  # 옵션 현재가
                    "optn_prdy_vrss",  # 옵션 전일 대비
                    "optn_prdy_ctrt",  # 옵션 전일 대비율
                    "delta_val",  # 델타 값
                    "gama",  # 감마
                    "vega",  # 베가
                    "theta",  # 세타
                    "hts_ints_vltl",  # HTS 내재 변동성
                    "acml_vol",  # 누적 거래량
                    "hts_otst_stpl_qty",  # HTS 미결제 약정 수량
                ],
            )

            if call_df is not None and not call_df.empty:
                result["call"] = call_df

            # 풋옵션 데이터 파싱 (output2)
            put_df = self.parse_api_basic(
                api_name=self.api_name_board,
                response_data=response_data,
                output_key="output2",
                date_column=None,
                numeric_columns=[
                    "acpr",  # 행사가
                    "optn_prpr",  # 옵션 현재가
                    "optn_prdy_vrss",  # 옵션 전일 대비
                    "optn_prdy_ctrt",  # 옵션 전일 대비율
                    "delta_val",  # 델타 값
                    "gama",  # 감마
                    "vega",  # 베가
                    "theta",  # 세타
                    "hts_ints_vltl",  # HTS 내재 변동성
                    "acml_vol",  # 누적 거래량
                    "hts_otst_stpl_qty",  # HTS 미결제 약정 수량
                ],
            )

            if put_df is not None and not put_df.empty:
                result["put"] = put_df

            return result if result else None

        except Exception as e:
            self.log_error(f"옵션 전광판 API 응답 파싱 중 오류: {str(e)}")
            self.log_error(traceback.format_exc())
            return None

    def call_feature(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        저장된 옵션 데이터를 단일 DataFrame으로 반환합니다.

        Returns:
            Optional[pd.DataFrame]: 모든 옵션 데이터를 하나의 DataFrame으로 반환 또는 None.
        """
        if not self.option_data:
            self.log_warning(
                "옵션 데이터가 없습니다. collect_data()를 먼저 실행하세요."
            )
            return None

        # 모든 만기의 콜/풋 옵션 데이터를 하나의 DataFrame으로 합치기
        all_dataframes = []
        for maturity_code, data in self.option_data.items():
            combined_df = self._combine_call_put_data(data)
            if combined_df is not None:
                combined_df["maturity"] = maturity_code
                all_dataframes.append(combined_df)

        if all_dataframes:
            return pd.concat(all_dataframes, ignore_index=True)

        return None

    def _combine_call_put_data(
        self, option_data: Dict[str, pd.DataFrame]
    ) -> Optional[pd.DataFrame]:
        """콜/풋 옵션 데이터를 하나의 DataFrame으로 합치기"""
        if not option_data or not isinstance(option_data, dict):
            return None

        dataframes = []

        if "call_options" in option_data and option_data["call_options"] is not None:
            call_df = option_data["call_options"].copy()
            call_df["option_type"] = "CALL"
            dataframes.append(call_df)

        if "put_options" in option_data and option_data["put_options"] is not None:
            put_df = option_data["put_options"].copy()
            put_df["option_type"] = "PUT"
            dataframes.append(put_df)

        if dataframes:
            return pd.concat(dataframes, ignore_index=True)

        return None

    def get_options_chain(
        self, market_type: Optional[str] = None, maturity_type: Optional[str] = None
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        특정 시장 및 만기 유형의 옵션 체인(콜/풋 옵션 데이터) 반환

        Args:
            market_type (Optional[str]): 시장 유형 (기본값: self.market_type)
            maturity_type (Optional[str]): 만기 유형 (기본값: 첫 번째 설정된 만기 월물)

        Returns:
            Optional[Dict[str, pd.DataFrame]]:
                'call' 및 'put' 키를 가진 옵션 데이터 딕셔너리.
                데이터가 없는 경우 None 반환.
        """
        target_market = market_type if market_type else self.market_type
        target_maturity = (
            maturity_type
            if maturity_type
            else (self.maturity_months[0] if self.maturity_months else None)
        )

        if not target_maturity:
            self.log_warning("만기 유형이 지정되지 않았거나 설정되지 않았습니다.")
            return None

        if target_maturity in self.option_data:
            return {
                "call": self.option_data[target_maturity]["call_options"].copy(),
                "put": self.option_data[target_maturity]["put_options"].copy(),
            }
        else:
            self.log_warning(f"{target_maturity}에 대한 옵션 체인이 없습니다.")
            return None
