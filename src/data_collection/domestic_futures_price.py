# -*- coding: utf-8 -*-
"""
국내 선물/옵션 시세 및 미결제약정 데이터를 조회하는 피처 모듈.
'선물옵션기간별시세(일/주/월/년) [v1_국내선물-008]' API를 사용합니다.

지원 시장:
- 지수선물(F)
- 상품선물(금, CF)
- 금리선물(국채, CF)
- 통화선물(달러, CF)
- 야간선물(CM)
- 야간옵션(EU)
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime, timedelta
import time

from src.feature_engineering.abstract_feature import Feature
from src.data_collection.api_client import APIClient
from src.utils.api_config_manager import get_api_config

logger = logging.getLogger(__name__)


class DomesticFuturesPrice(Feature):
    """
    국내 선물/옵션 시세 및 미결제약정 데이터를 조회하고 관리하는 피처.

    - `features.yaml` 설정을 통해 조회할 종목 코드(`code_list`), 시장 구분(`market_code`),
      조회 주기(`inquiry_time_list`), 조회 기간 타입(`period_code`), API 파라미터(`params`) 등을 설정합니다.
    - `_perform_inquiry` 메서드를 통해 주기적으로 API를 호출하여 데이터를 업데이트합니다.
    - `call_feature` 메서드를 통해 저장된 데이터를 반환합니다.
    """

    API_NAME = "선물옵션기간별시세(일/주/월/년) [v1_국내선물-008]"

    def __init__(
        self,
        _feature_name: str,
        _code_list: List[str],  # 선물/옵션 코드 리스트
        _feature_query: APIClient,
        _quote_connect: bool,
        _inquiry: bool,
        _inquiry_time_list: List[str],
        _inquiry_name_list: List[str],
        _params: Dict,
    ):
        """
        DomesticFuturesPrice 생성자.

        Args:
            _feature_name (str): 피처 이름
            _code_list (List[str]): 조회할 선물/옵션 종목 코드 리스트
            _feature_query (APIClient): API 클라이언트
            _quote_connect (bool): 시세 연결 여부 (현재 미사용)
            _inquiry (bool): 조회 수행 여부
            _inquiry_time_list (List[str]): 조회 시간 리스트
            _inquiry_name_list (List[str]): 조회 이름 리스트
            _params (Dict): 파라미터
        """
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
        self.schema_name = "domestic_futures_price"  # 스키마 이름 설정
        self.futures_data = {}  # 수집된 데이터 저장
        self._initialize_params()

    def _initialize_params(self):
        """피처 파라미터 초기화 및 기본값 설정"""
        if not self.code_list:
            raise ValueError(
                f"Missing required 'code_list' for feature {self.feature_name}"
            )

        # 모든 파라미터를 params.yaml에서 가져옴
        self.market_code = self.params.get("market_code")
        self.period_code = self.params.get("period_code")
        self.pagination_delay_sec = self.params.get("pagination_delay_sec", 1.0)
        self.max_days_per_request = self.params.get(
            "max_days_per_request", 90
        )  # 한 번에 조회할 최대 일수

    def _get_additional_api_params(self) -> Dict[str, str]:
        """
        선물옵션 API에 필요한 추가 파라미터를 반환합니다

        Returns:
            Dict[str, str]: 추가 파라미터
        """
        return {
            "FID_COND_MRKT_DIV_CODE": self.market_code,  # 시장 구분 (필수)
            "FID_PERIOD_DIV_CODE": self.period_code,  # 기간 구분 (필수)
        }

    def _perform_inquiry(self, clock: str):
        """
        설정된 시간에 맞추어 지정된 종목 코드들의 시세 및 미결제약정 데이터를 조회합니다.

        Args:
            clock (str): 현재 시각 (HHMMSS).
        """
        api_config = get_api_config()

        self.log_warning(
            f"📈 국내 선물옵션 일별 가격 조회 시작 - 코드: {self.code_list}, 기간: {self.start_date}~{self.end_date}"
        )

        # 일반 데이터 조회
        for code in self.code_list:
            try:
                # API 설정에서 파라미터 자동 구성
                params = api_config.build_api_params(
                    api_name="선물옵션기간별시세",
                    symbol_code=code,
                    start_date=self.start_date,
                    end_date=self.end_date,
                )

                # 종목 유형 확인
                symbol_type = api_config.get_symbol_type(code)
                self.log_info(f"📊 {code} 데이터 조회 시작 (유형: {symbol_type})")

                # API 호출
                response = self.get_api(
                    self.API_NAME,
                    params,
                    tr_id=api_config.get_tr_id("선물옵션기간별시세"),
                )

                # 응답 파싱
                parsed_df = self.parse_api_response(self.API_NAME, response)

                if parsed_df is not None and not parsed_df.empty:
                    # 메모리에 저장
                    self.futures_data[code] = parsed_df

                    # CSV 파일로 저장
                    self.save_data_with_schema(
                        schema_name=getattr(self, "schema_name", "domestic_futures"),
                        table_name=f"{self.feature_name}/{code}",
                        data=parsed_df,
                    )

                    self.log_warning(
                        f"✅ {code}: 데이터 수집 완료 - {len(parsed_df)}건 (유형: {symbol_type})"
                    )
                else:
                    self.log_warning(f"⚠️ {code}: 수집된 데이터가 없습니다")

            except Exception as e:
                self.log_error(f"❌ {code} 조회 중 오류: {str(e)}")
                continue

        self.log_warning(
            f"📈 국내 선물옵션 일별 가격 조회 완료 (총 {len(self.code_list)}개 종목)"
        )
        self.health_check_value = f"국내 선물옵션 일별 가격 조회 완료 (시간: {clock})"

    def parse_api_response(
        self, api_name: str, response_data: Dict
    ) -> Optional[pd.DataFrame]:
        """
        선물옵션기간별시세 API 응답에서 필요한 데이터만 파싱하여 DataFrame으로 변환합니다.
        OHLC, 거래량, 거래대금, 미결제약정수량, 미결제약정수량 증감만 추출합니다.

        Args:
            api_name (str): API 이름 (반드시 self.API_NAME과 동일해야 함).
            response_data (Dict): API 응답 원본 딕셔너리.

        Returns:
            Optional[pd.DataFrame]: 파싱된 데이터프레임 또는 None.
        """
        # 개선된 오류 처리 사용
        if not self.handle_api_error(response_data, api_name):
            return None

        # 개선된 공통 파싱 메서드 사용
        df = self.parse_api_basic(
            api_name=api_name,
            response_data=response_data,
            output_key="output2",
            date_column="stck_bsop_date",
            date_format="%Y%m%d",
            numeric_columns=[
                "futs_oprc",  # 시가
                "futs_hgpr",  # 고가
                "futs_lwpr",  # 저가
                "futs_prpr",  # 현재가/종가
                "acml_vol",  # 거래량
                "acml_tr_pbmn",  # 거래대금
                "hts_otst_stpl_qty",  # 미결제약정 수량
                "otst_stpl_qty_icdc",  # 미결제약정 수량 증감
            ],
        )

        if df is None:
            return None

        try:
            # output1에서 미결제약정 데이터 추출 (단일 값으로 모든 행에 동일하게 적용)
            if "output1" in response_data and isinstance(
                response_data["output1"], dict
            ):
                output1 = response_data["output1"]
                hts_otst_stpl_qty = output1.get("hts_otst_stpl_qty")
                otst_stpl_qty_icdc = output1.get("otst_stpl_qty_icdc")

                if (
                    hts_otst_stpl_qty is not None
                    and "hts_otst_stpl_qty" not in df.columns
                ):
                    df["hts_otst_stpl_qty"] = hts_otst_stpl_qty
                if (
                    otst_stpl_qty_icdc is not None
                    and "otst_stpl_qty_icdc" not in df.columns
                ):
                    df["otst_stpl_qty_icdc"] = otst_stpl_qty_icdc

            # 데이터 정리: 표준 컬럼만 유지
            required_columns = [
                "stck_bsop_date",  # 기준일자
                "futs_prpr",  # 현재가/종가
                "futs_oprc",  # 시가
                "futs_hgpr",  # 고가
                "futs_lwpr",  # 저가
                "acml_vol",  # 거래량
                "acml_tr_pbmn",  # 거래대금
                "mod_yn",  # 수정여부
            ]

            # 선택적 컬럼들 (있으면 포함)
            optional_columns = [
                "hts_otst_stpl_qty",  # 미결제약정 수량
                "otst_stpl_qty_icdc",  # 미결제약정 수량 증감
            ]

            # 존재하는 컬럼만 선택
            final_columns = []
            for col in required_columns:
                if col in df.columns:
                    final_columns.append(col)

            for col in optional_columns:
                if col in df.columns:
                    final_columns.append(col)

            # 날짜 컬럼명 확인 및 표준화
            date_column = None
            possible_date_columns = [
                "stck_bsop_date",
                "date",
                "bsop_date",
                "trad_date",
                "bas_date",
                "std_date",
                "prdy_date",
                "curr_date",
                "today_date",
            ]
            for possible_date_col in possible_date_columns:
                if possible_date_col in df.columns:
                    date_column = possible_date_col
                    break

            # 날짜 컬럼이 없는 경우, 인덱스 기반으로 날짜 생성 시도
            if date_column is None and not df.empty:
                # 날짜 컬럼이 없는 것은 정상적인 상황일 수 있으므로 debug 레벨로 변경
                self.log_debug(
                    f"날짜 컬럼이 없어서 데이터를 건너뜁니다. 컬럼: {list(df.columns)}"
                )
                return None

            if date_column is None:
                self.log_error(f"날짜 컬럼을 찾을 수 없음: {df.columns.tolist()}")
                return None

            # 날짜 컬럼명을 표준화
            if date_column != "stck_bsop_date":
                df = df.rename(columns={date_column: "stck_bsop_date"})

            # 필수 컬럼들이 모두 있는지 확인
            if "stck_bsop_date" not in df.columns or "futs_prpr" not in df.columns:
                self.log_error(f"필수 컬럼이 누락됨: {df.columns.tolist()}")
                return None

            # 컬럼 정리된 DataFrame 생성
            df = df[final_columns].copy()

            return df

        except Exception as e:
            import traceback

            self.log_error(f"Error parsing API response: {e}\n{traceback.format_exc()}")
            return None

    def call_feature(
        self,
        code: Optional[str] = None,
        **kwargs,
    ) -> Any:
        """
        저장된 선물/옵션 데이터를 반환합니다.

        Args:
            code (Optional[str]): 조회할 특정 종목 코드. None이면 모든 종목의 데이터 반환.
            **kwargs: 추가 파라미터 (현재 사용 안 함).

        Returns:
            pd.DataFrame or Dict[str, pd.DataFrame] or None:
            - code가 지정된 경우 해당 코드의 데이터프레임 반환.
            - code가 None인 경우 {코드: 데이터프레임} 형태의 딕셔너리 반환.
            - 데이터가 없는 경우 None 반환.
        """
        if code:
            if code in self.futures_data:
                return self.futures_data[code].copy()
            else:
                # 스키마/테이블 방식으로 데이터 로드 시도
                data = self.get_data_with_schema(self.schema_name, code.lower())
                if data is not None:
                    self.futures_data[code] = data
                    return data
                self.log_warning(f"No data available for code {code}.")
                return None
        else:
            # 모든 코드의 데이터 반환
            if not self.futures_data:
                # 저장소에서 모든 코드의 데이터 로드 시도
                for c in self.code_list:
                    data = self.get_data_with_schema(self.schema_name, c.lower())
                    if data is not None:
                        self.futures_data[c] = data

            return (
                {k: v.copy() for k, v in self.futures_data.items()}
                if self.futures_data
                else None
            )

    # @staticmethod
    # def calculate_moving_average(
    #     df: pd.DataFrame, window: int, price_col: str = "futs_prpr"
    # ) -> Optional[pd.Series]:
    #     """
    #     가격 데이터에 대한 이동평균을 계산합니다.

    #     Args:
    #         df (pd.DataFrame): 시세 데이터프레임.
    #         window (int): 이동평균 윈도우 크기.
    #         price_col (str): 사용할 가격 컬럼명.

    #     Returns:
    #         pd.Series or None: 계산된 이동평균 시리즈 또는 계산 실패 시 None.
    #     """
    #     try:
    #         if price_col not in df.columns:
    #             logger.warning(f"Column '{price_col}' not found in DataFrame.")
    #             return None

    #         return df[price_col].rolling(window=window).mean()
    #     except Exception as e:
    #         logger.error(f"Error calculating moving average: {e}")
    #         return None

    # @staticmethod
    # def calculate_open_interest_momentum(
    #     df: pd.DataFrame, window: int = 5
    # ) -> pd.Series:
    #     """
    #     미결제약정량의 모멘텀(변화율)을 계산합니다.

    #     Args:
    #         df (pd.DataFrame): 미결제약정 데이터프레임.
    #         window (int): 모멘텀 계산을 위한 윈도우 크기 (기본값: 5).

    #     Returns:
    #         pd.Series: 미결제약정량 모멘텀(N일간 변화율, %).
    #     """
    #     try:
    #         if "optr_opnt_qty" not in df.columns:
    #             logger.warning("Column 'optr_opnt_qty' not found in DataFrame.")
    #             return pd.Series(index=df.index)

    #         # 미결제약정량 변화율 (%) 계산
    #         pct_change = df["optr_opnt_qty"].pct_change(periods=window) * 100
    #         return pct_change

    #     except Exception as e:
    #         logger.error(f"Error calculating open interest momentum: {e}")
    #         return pd.Series(index=df.index)
