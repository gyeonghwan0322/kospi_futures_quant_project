# -*- coding: utf-8 -*-
"""
국내 옵션 전광판 데이터를 조회하는 피처 모듈.
'국내옵션전광판_콜풋[국내선물-022]' API를 사용합니다.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime

# abstract_feature 모듈에서 Feature 클래스를 임포트합니다.
from src.feature_engineering.abstract_feature import Feature
from src.data_collection.api_client import APIClient

logger = logging.getLogger(__name__)


class DomesticOptionsDisplayBoard(Feature):
    """
    국내 옵션 전광판 데이터를 조회하고 관리하는 피처.

    콜옵션과 풋옵션의 그릭스, 미결제약정, 현재가, 매수/매도호가 등을 수집합니다.
    - output1: 콜옵션 데이터
    - output2: 풋옵션 데이터
    """

    API_NAME = "국내옵션전광판_콜풋[국내선물-022]"

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
        DomesticOptionsDisplayBoard 생성자.

        Args:
            _feature_name (str): 피처 이름.
            _code_list (list[str]): 사용되지 않음 (만기월별 조회).
            _feature_query (APIClient): API 호출에 사용할 APIClient 객체.
            _quote_connect (bool): 사용되지 않음.
            _inquiry (bool): 시간 기반 조회 사용 여부.
            _inquiry_time_list (list[str]): 조회 수행 시각 리스트 (HHMMSS).
            _inquiry_name_list (list[str]): 사용되지 않음.
            _params (dict): 피처 설정 파라미터. 다음 키들을 포함할 수 있음:
                - maturity_months (list): 조회할 만기월 리스트 (YYYYMM 형식)
                - market_div_code (str): 조건 시장 분류 코드 (기본값: 'O')
                - screen_div_code (str): 조건 화면 분류 코드 (기본값: '20503')
                - call_market_code (str): 콜옵션 시장 구분 코드 (기본값: 'CO')
                - put_market_code (str): 풋옵션 시장 구분 코드 (기본값: 'PO')
                - market_cls_code (str): 조건 시장 구분 코드 (기본값: '')
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
        self.schema_name = "domestic_options_display"  # 스키마 이름 설정
        # 옵션 데이터 저장소 (만기월별로 콜/풋 데이터 분리 저장)
        self.options_data: Dict[str, Dict[str, pd.DataFrame]] = {}
        self._initialize_params()

    def _initialize_params(self):
        """피처 파라미터 초기화 및 기본값 설정"""
        # 전역 상수 가져오기
        api_constants = self.params.get("api_constants", {})
        market_codes = api_constants.get("market_codes", {})
        option_market_codes = api_constants.get("option_market_codes", {})
        screen_codes = api_constants.get("screen_codes", {})

        self.maturity_months = self.params.get("maturity_months", [])
        self.market_div_code = self.params.get(
            "market_div_code", market_codes.get("option", "O")
        )
        self.screen_div_code = self.params.get(
            "screen_div_code", screen_codes.get("options_display_board", "20503")
        )
        self.call_market_code = self.params.get(
            "call_market_code", option_market_codes.get("call", "CO")
        )
        self.put_market_code = self.params.get(
            "put_market_code", option_market_codes.get("put", "PO")
        )
        self.market_cls_code = self.params.get(
            "market_cls_code", ""
        )  # KOSPI200 (빈 문자열)
        self.pagination_delay_sec = self.params.get(
            "pagination_delay_sec",
            api_constants.get("default_pagination_delay_sec", 1.0),
        )

        if not self.maturity_months:
            # 기본값: 다음월부터 3개월 (옵션 만기월은 보통 다음달부터 시작)
            current_date = datetime.now()
            for i in range(3):
                # 다음 달부터 시작
                month_date = current_date.replace(day=1)
                months_to_add = i + 1

                new_month = month_date.month + months_to_add
                new_year = month_date.year

                # 연도 넘김 처리
                while new_month > 12:
                    new_month -= 12
                    new_year += 1

                month_date = month_date.replace(year=new_year, month=new_month)
                self.maturity_months.append(month_date.strftime("%Y%m"))

    def _get_additional_api_params(self) -> Dict[str, str]:
        """옵션 전광판 조회를 위한 추가 API 파라미터 반환"""
        return {
            "FID_COND_MRKT_DIV_CODE": self.market_div_code,  # 조건 시장 분류 코드
            "FID_COND_SCR_DIV_CODE": self.screen_div_code,  # 조건 화면 분류 코드
            "FID_MRKT_CLS_CODE": self.call_market_code,  # 시장 구분 코드 (콜옵션)
            "FID_COND_MRKT_CLS_CODE": self.market_cls_code,  # 조건 시장 구분 코드
            "FID_MRKT_CLS_CODE1": self.put_market_code,  # 시장 구분 코드 (풋옵션)
        }

    def _perform_inquiry(self, clock: str):
        """
        설정된 시간에 맞추어 모든 만기월의 옵션 전광판 데이터를 조회하고 업데이트합니다.

        Args:
            clock (str): 현재 시각 (HHMMSS).
        """
        self.log_warning(
            f"🎯 국내 옵션 전광판 데이터 조회 시작 - 만기월: {self.maturity_months}, 시간: {clock}"
        )

        collected_data = {}

        for maturity in self.maturity_months:
            try:
                # API 파라미터 구성
                additional_params = self._get_additional_api_params()
                additional_params["FID_MTRT_CNT"] = maturity  # 만기 수

                params = {**additional_params}

                self.log_debug(
                    f"옵션 전광판 조회 - 만기월: {maturity}, 파라미터: {params}"
                )

                # API 호출
                response = self.get_api(
                    self.API_NAME, params, tr_id=self.get_tr_id(self.API_NAME)
                )

                # 응답 파싱
                parsed_data = self.parse_api_response(self.API_NAME, response)

                if parsed_data:
                    # 콜/풋 데이터 분리 저장
                    collected_data[maturity] = parsed_data
                    self.options_data[maturity] = parsed_data

                    call_count = len(parsed_data.get("call", pd.DataFrame()))
                    put_count = len(parsed_data.get("put", pd.DataFrame()))

                    self.log_info(
                        f"✅ 만기월 {maturity} 옵션 데이터 수집 완료 - 콜: {call_count}건, 풋: {put_count}건"
                    )
                else:
                    self.log_warning(f"⚠️ 만기월 {maturity} 옵션 데이터가 없습니다")

                # API 호출 간 지연
                if self.pagination_delay_sec > 0:
                    import time

                    time.sleep(self.pagination_delay_sec)

            except Exception as e:
                self.log_error(
                    f"❌ 만기월 {maturity} 옵션 데이터 조회 중 오류: {str(e)}"
                )
                import traceback

                self.log_error(traceback.format_exc())

        # 수집된 데이터 저장
        if collected_data:
            # 인스턴스 변수에 저장 (call_feature에서 사용)
            self.options_data = collected_data
            # 파일로도 저장
            self._save_options_data(collected_data)

        self.log_warning(
            f"🎯 국내 옵션 전광판 데이터 조회 완료 (총 {len(self.maturity_months)}개 만기월)"
        )
        self.health_check_value = f"옵션 전광판 데이터 조회 완료 (시간: {clock})"

    def _save_options_data(self, data_dict: Dict[str, Dict[str, pd.DataFrame]]):
        """수집된 옵션 데이터를 파일로 저장"""
        try:
            for maturity, option_data in data_dict.items():
                # 콜옵션 데이터 저장
                if "call" in option_data and not option_data["call"].empty:
                    call_df = option_data["call"].copy()
                    call_df["maturity"] = maturity
                    call_df["option_type"] = "call"
                    call_df["collection_time"] = datetime.now().strftime("%H:%M:%S")

                    self.save_data_with_schema(
                        self.schema_name, f"{maturity}_call", call_df
                    )

                # 풋옵션 데이터 저장
                if "put" in option_data and not option_data["put"].empty:
                    put_df = option_data["put"].copy()
                    put_df["maturity"] = maturity
                    put_df["option_type"] = "put"
                    put_df["collection_time"] = datetime.now().strftime("%H:%M:%S")

                    self.save_data_with_schema(
                        self.schema_name, f"{maturity}_put", put_df
                    )

        except Exception as e:
            self.log_error(f"옵션 데이터 저장 중 오류: {str(e)}")

    def parse_api_response(
        self, api_name: str, response_data: Dict
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        '국내옵션전광판_콜풋' API 응답 데이터를 파싱하여 콜/풋 DataFrame으로 분리 변환합니다.

        Args:
            api_name (str): API 이름.
            response_data (Dict): API 응답 원본 딕셔너리.

        Returns:
            Optional[Dict[str, pd.DataFrame]]: {"call": 콜옵션_DF, "put": 풋옵션_DF} 형태.
                                               오류 시 None 반환.
        """
        if api_name != self.API_NAME:
            self.log_error(
                f"parse_api_response called with incorrect API name: {api_name}"
            )
            return None

        try:
            result = {}

            # 콜옵션 데이터 파싱 (output1)
            call_df = self.parse_api_basic(
                api_name=api_name,
                response_data=response_data,
                output_key="output1",
                date_column=None,
                numeric_columns=[
                    "acpr",  # 행사가
                    "unch_prpr",  # 환산 현재가
                    "optn_prpr",  # 옵션 현재가
                    "optn_prdy_vrss",  # 옵션 전일 대비
                    "optn_prdy_ctrt",  # 옵션 전일 대비율
                    "optn_bidp",  # 옵션 매수호가
                    "optn_askp",  # 옵션 매도호가
                    "tmvl_val",  # 시간가치 값
                    "nmix_sdpr",  # 지수 기준가
                    "acml_vol",  # 누적 거래량
                    "seln_rsqn",  # 매도 잔량
                    "shnu_rsqn",  # 매수 잔량
                    "acml_tr_pbmn",  # 누적 거래 대금
                    "hts_otst_stpl_qty",  # HTS 미결제 약정 수량
                    "otst_stpl_qty_icdc",  # 미결제 약정 수량 증감
                    "delta_val",  # 델타 값
                    "gama",  # 감마
                    "vega",  # 베가
                    "theta",  # 세타
                    "rho",  # 로우
                    "hts_ints_vltl",  # HTS 내재 변동성
                    "invl_val",  # 내재가치 값
                    "esdg",  # 괴리도
                    "dprt",  # 괴리율
                    "hist_vltl",  # 역사적 변동성
                    "hts_thpr",  # HTS 이론가
                    "optn_oprc",  # 옵션 시가
                    "optn_hgpr",  # 옵션 최고가
                    "optn_lwpr",  # 옵션 최저가
                    "optn_mxpr",  # 옵션 상한가
                    "optn_llam",  # 옵션 하한가
                    "total_askp_rsqn",  # 총 매도호가 잔량
                    "total_bidp_rsqn",  # 총 매수호가 잔량
                    "futs_antc_cnpr",  # 선물예상체결가
                    "futs_antc_cntg_vrss",  # 선물예상체결대비
                    "antc_cntg_prdy_ctrt",  # 예상 체결 전일 대비율
                ],
            )

            if call_df is not None:
                result["call"] = call_df

            # 풋옵션 데이터 파싱 (output2)
            put_df = self.parse_api_basic(
                api_name=api_name,
                response_data=response_data,
                output_key="output2",
                date_column=None,
                numeric_columns=[
                    "acpr",  # 행사가
                    "unch_prpr",  # 환산 현재가
                    "optn_prpr",  # 옵션 현재가
                    "optn_prdy_vrss",  # 옵션 전일 대비
                    "optn_prdy_ctrt",  # 옵션 전일 대비율
                    "optn_bidp",  # 옵션 매수호가
                    "optn_askp",  # 옵션 매도호가
                    "tmvl_val",  # 시간가치 값
                    "nmix_sdpr",  # 지수 기준가
                    "acml_vol",  # 누적 거래량
                    "seln_rsqn",  # 매도 잔량
                    "shnu_rsqn",  # 매수 잔량
                    "acml_tr_pbmn",  # 누적 거래 대금
                    "hts_otst_stpl_qty",  # HTS 미결제 약정 수량
                    "otst_stpl_qty_icdc",  # 미결제 약정 수량 증감
                    "delta_val",  # 델타 값
                    "gama",  # 감마
                    "vega",  # 베가
                    "theta",  # 세타
                    "rho",  # 로우
                    "hts_ints_vltl",  # HTS 내재 변동성
                    "invl_val",  # 내재가치 값
                    "esdg",  # 괴리도
                    "dprt",  # 괴리율
                    "hist_vltl",  # 역사적 변동성
                    "hts_thpr",  # HTS 이론가
                    "optn_oprc",  # 옵션 시가
                    "optn_hgpr",  # 옵션 최고가
                    "optn_lwpr",  # 옵션 최저가
                    "optn_mxpr",  # 옵션 상한가
                    "optn_llam",  # 옵션 하한가
                    "total_askp_rsqn",  # 총 매도호가 잔량
                    "total_bidp_rsqn",  # 총 매수호가 잔량
                    "futs_antc_cnpr",  # 선물예상체결가
                    "futs_antc_cntg_vrss",  # 선물예상체결대비
                    "antc_cntg_prdy_ctrt",  # 예상 체결 전일 대비율
                ],
            )

            if put_df is not None:
                result["put"] = put_df

            return result if result else None

        except Exception as e:
            self.log_error(f"옵션 전광판 API 응답 파싱 중 오류: {str(e)}")
            import traceback

            self.log_error(traceback.format_exc())
            return None

    def call_feature(
        self,
        maturity: Optional[str] = None,
        option_type: Optional[str] = None,
        **kwargs,
    ) -> Optional[
        Union[pd.DataFrame, Dict[str, pd.DataFrame], Dict[str, Dict[str, pd.DataFrame]]]
    ]:
        """
        저장된 옵션 전광판 데이터를 반환합니다.

        Args:
            maturity (Optional[str]): 조회할 특정 만기월 (YYYYMM). None이면 모든 만기월 반환.
            option_type (Optional[str]): 옵션 타입 ('call' 또는 'put'). None이면 콜/풋 모두 반환.
            **kwargs: 추가 파라미터 (현재 사용 안 함).

        Returns:
            pd.DataFrame or Dict: 요청된 옵션 데이터.
            - maturity와 option_type 모두 지정: 해당 DataFrame 반환
            - maturity만 지정: {"call": DF, "put": DF} 반환
            - 모두 None: {maturity: {"call": DF, "put": DF}} 반환
        """
        if maturity and option_type:
            # 특정 만기월의 특정 옵션 타입 데이터 반환
            return self.options_data.get(maturity, {}).get(option_type)
        elif maturity:
            # 특정 만기월의 콜/풋 데이터 반환
            return self.options_data.get(maturity)
        else:
            # CSV 저장을 위해 평탄화된 구조로 모든 데이터 반환
            # {만기월_옵션타입: DataFrame} 형태로 변환
            flattened_data = {}
            if self.options_data:
                for maturity_key, maturity_data in self.options_data.items():
                    if isinstance(maturity_data, dict):
                        for option_type_key, df in maturity_data.items():
                            # 코드 형태: "202507_call", "202507_put" 등
                            code = f"{maturity_key}_{option_type_key}"
                            flattened_data[code] = df

            return flattened_data if flattened_data else None


class DomesticWeeklyOptionsDisplayBoard(DomesticOptionsDisplayBoard):
    """
    국내 위클리 옵션 전광판 데이터를 조회하고 관리하는 피처.

    API 문서 기반으로 위클리 옵션의 그릭스, 미결제약정, 내재변동성 등을 수집합니다.
    - WKM: KOSPI200위클리(월) - 월요일 만료
    - WKI: KOSPI200위클리(목) - 목요일 만료
    - 만기 형식: YYMMWW (예: 250601 = 2025년 6월 1주차)
    """

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
        super().__init__(
            _feature_name,
            _code_list,
            _feature_query,
            _quote_connect,
            _inquiry,
            _inquiry_time_list,
            _inquiry_name_list,
            _params,
        )
        self.schema_name = "domestic_weekly_options_display"  # 위클리 전용 스키마
        # 위클리 옵션 패턴 초기화
        self._initialize_weekly_patterns()

    def _initialize_weekly_patterns(self):
        """위클리 옵션 패턴 초기화 (API 문서 기반)"""
        # 위클리 옵션 파라미터 가져오기
        weekly_params = self.params.get("weekly_patterns", [])

        # 기본 패턴 설정 (2025년 6월)
        if not weekly_params:
            weekly_params = [
                {
                    "maturity_code": "250601",  # 2025년 6월 1주차
                    "market_cls_code": "WKM",  # KOSPI200위클리(월)
                    "week_type": "monday",
                    "description": "2025년 6월 1주차 (월요일 만료)",
                },
                {
                    "maturity_code": "250602",  # 2025년 6월 2주차
                    "market_cls_code": "WKI",  # KOSPI200위클리(목)
                    "week_type": "thursday",
                    "description": "2025년 6월 2주차 (목요일 만료)",
                },
            ]

        self.weekly_patterns = weekly_params
        self.log_warning(f"🔧 위클리 옵션 전광판 패턴: {len(self.weekly_patterns)}개")
        for pattern in self.weekly_patterns:
            self.log_warning(
                f"🔧   {pattern['maturity_code']} ({pattern['market_cls_code']}) - {pattern['description']}"
            )

    def _perform_inquiry(self, clock: str):
        """
        설정된 시간에 맞추어 모든 위클리 만기의 옵션 전광판 데이터를 조회합니다.

        Args:
            clock (str): 현재 시각 (HHMMSS).
        """
        self.log_warning(
            f"🎯 국내 위클리 옵션 전광판 데이터 조회 시작 - {len(self.weekly_patterns)}개 패턴, 시간: {clock}"
        )

        collected_data = {}

        for pattern in self.weekly_patterns:
            weekly_code = pattern["maturity_code"]
            market_cls_code = pattern["market_cls_code"]
            description = pattern["description"]

            try:
                # API 파라미터 구성 (위클리 옵션 전용)
                params = {
                    "FID_COND_MRKT_DIV_CODE": self.market_div_code,  # O (옵션)
                    "FID_COND_SCR_DIV_CODE": self.screen_div_code,  # 20503
                    "FID_MRKT_CLS_CODE": self.call_market_code,  # CO (콜옵션)
                    "FID_MTRT_CNT": weekly_code,  # 250601, 250602 등
                    "FID_COND_MRKT_CLS_CODE": market_cls_code,  # WKM(월) 또는 WKI(목)
                    "FID_MRKT_CLS_CODE1": self.put_market_code,  # PO (풋옵션)
                }

                self.log_warning(f"🔍 위클리 옵션 전광판 조회 - {description}")
                self.log_debug(f"🔍 API 파라미터: {params}")

                # API 호출
                response = self.get_api(
                    self.API_NAME, params, tr_id=self.get_tr_id(self.API_NAME)
                )

                # 응답 파싱
                parsed_data = self.parse_api_response(self.API_NAME, response)

                if parsed_data:
                    # 콜/풋 데이터 분리 저장
                    collected_data[weekly_code] = parsed_data
                    self.options_data[weekly_code] = parsed_data

                    call_count = len(parsed_data.get("call", pd.DataFrame()))
                    put_count = len(parsed_data.get("put", pd.DataFrame()))

                    self.log_info(
                        f"✅ {description} 옵션 데이터 수집 완료 - 콜: {call_count}건, 풋: {put_count}건"
                    )
                else:
                    self.log_warning(f"⚠️ {description} 옵션 데이터가 없습니다")

                # API 호출 간 지연
                if self.pagination_delay_sec > 0:
                    import time

                    time.sleep(self.pagination_delay_sec)

            except Exception as e:
                self.log_error(f"❌ {description} 옵션 데이터 조회 중 오류: {str(e)}")
                import traceback

                self.log_error(traceback.format_exc())

        # 수집된 데이터 저장
        if collected_data:
            # 인스턴스 변수에 저장 (call_feature에서 사용)
            self.options_data = collected_data
            # 파일로도 저장
            self._save_weekly_options_data(collected_data)

        self.log_warning(
            f"🎯 국내 위클리 옵션 전광판 데이터 조회 완료 (총 {len(self.weekly_patterns)}개 위클리 패턴)"
        )
        self.health_check_value = f"위클리 옵션 전광판 데이터 조회 완료 (시간: {clock})"

    def _save_weekly_options_data(self, data_dict: Dict[str, Dict[str, pd.DataFrame]]):
        """수집된 위클리 옵션 데이터를 파일로 저장"""
        try:
            for weekly_code, option_data in data_dict.items():
                # 콜옵션 데이터 저장
                if "call" in option_data and not option_data["call"].empty:
                    call_df = option_data["call"].copy()
                    call_df["weekly_maturity"] = weekly_code
                    call_df["option_type"] = "call"
                    call_df["collection_time"] = datetime.now().strftime("%H:%M:%S")

                    self.save_data_with_schema(
                        self.schema_name, f"{weekly_code}_call", call_df
                    )

                # 풋옵션 데이터 저장
                if "put" in option_data and not option_data["put"].empty:
                    put_df = option_data["put"].copy()
                    put_df["weekly_maturity"] = weekly_code
                    put_df["option_type"] = "put"
                    put_df["collection_time"] = datetime.now().strftime("%H:%M:%S")

                    self.save_data_with_schema(
                        self.schema_name, f"{weekly_code}_put", put_df
                    )

        except Exception as e:
            self.log_error(f"위클리 옵션 데이터 저장 중 오류: {str(e)}")

    def call_feature(
        self,
        weekly_code: Optional[str] = None,
        option_type: Optional[str] = None,
        **kwargs,
    ) -> Optional[
        Union[pd.DataFrame, Dict[str, pd.DataFrame], Dict[str, Dict[str, pd.DataFrame]]]
    ]:
        """
        저장된 위클리 옵션 전광판 데이터를 반환합니다.

        Args:
            weekly_code (Optional[str]): 조회할 특정 위클리 만기 (예: "250601"). None이면 모든 만기 반환.
            option_type (Optional[str]): 옵션 타입 ('call' 또는 'put'). None이면 콜/풋 모두 반환.
            **kwargs: 추가 파라미터 (현재 사용 안 함).

        Returns:
            pd.DataFrame or Dict: 요청된 위클리 옵션 데이터.
        """
        if weekly_code and option_type:
            # 특정 위클리 만기의 특정 옵션 타입 데이터 반환
            return self.options_data.get(weekly_code, {}).get(option_type)
        elif weekly_code:
            # 특정 위클리 만기의 콜/풋 데이터 반환
            return self.options_data.get(weekly_code)
        else:
            # CSV 저장을 위해 평탄화된 구조로 모든 데이터 반환
            flattened_data = {}
            if self.options_data:
                for weekly_key, weekly_data in self.options_data.items():
                    if isinstance(weekly_data, dict):
                        for option_type_key, df in weekly_data.items():
                            # 코드 형태: "250601_call", "250601_put" 등
                            code = f"{weekly_key}_{option_type_key}"
                            flattened_data[code] = df

            return flattened_data if flattened_data else None
