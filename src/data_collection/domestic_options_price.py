# -*- coding: utf-8 -*-
"""
국내 옵션 개별 시세 데이터를 조회하는 피처 모듈.
'국내선물옵션 일별시세[v1_국내선물-011]' API를 사용합니다.

지원 옵션:
- 일반 월물 옵션 (201W06, 301W06 등)
- 위클리 옵션 L타입 (월요일 만료)
- 위클리 옵션 N타입 (목요일 만료)
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime
import traceback

from src.data_collection.abstract_feature import Feature
from src.data_collection.api_client import APIClient

logger = logging.getLogger(__name__)


class DomesticOptionsPrice(Feature):
    """
    국내 옵션 개별 종목 시세 데이터를 조회하고 관리하는 피처.

    일반 월물 옵션과 위클리 옵션 모두 지원합니다:
    - 일반 옵션: KOSPI200 콜/풋 옵션 (201W06, 301W06 등)
    - 위클리 옵션 L타입: 월요일 만료 (209DXW... 형태)
    - 위클리 옵션 N타입: 목요일 만료 (2AF97W... 형태)
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
        DomesticOptionsPrice 생성자.

        Args:
            _feature_name (str): 피처 이름.
            _code_list (list[str]): 옵션 종목코드 리스트 (일반 옵션 + 위클리 옵션).
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
        self.schema_name = "domestic_options"  # data\domestic_options에 저장
        # 옵션 시세 데이터 저장소 (종목코드별)
        self.options_data: Dict[str, pd.DataFrame] = {}

        # API 설정에서 API 정보 가져오기
        self.api_name = "국내선물옵션 일별시세"
        api_endpoints = self.params.get("api_config", {}).get("api_endpoints", {})
        api_info = api_endpoints.get(self.api_name, {})
        self.tr_id = api_info.get("tr_id", "FHKIF03020100")

        self._initialize_params()

    def _initialize_params(self):
        """피처 파라미터 초기화 및 기본값 설정"""
        # 전역 상수 가져오기
        api_constants = self.params.get("api_constants", {})

        self.market_code = self.params.get("market_code", "O")  # 옵션
        self.period_code = self.params.get("period_code", "D")  # 일봉
        self.pagination_delay_sec = self.params.get(
            "pagination_delay_sec",
            api_constants.get("default_pagination_delay_sec", 1.0),
        )
        self.max_days_per_request = self.params.get(
            "max_days_per_request", 90
        )  # 한 번에 조회할 최대 일수

        # 옵션 종목코드는 features.yaml의 code_list에서 관리
        if not self.code_list:
            self.log_warning(
                "옵션 코드가 설정되지 않음 - features.yaml에서 code_list 설정 필요"
            )
            self.code_list = []

        self.log_warning(f"🔧 옵션 시세 조회 대상: {len(self.code_list)}개 종목")

        # 옵션 타입별 분류
        self._classify_option_codes()

    def _classify_option_codes(self):
        """옵션 코드를 타입별로 분류"""
        self.regular_options = []  # 일반 월물 옵션
        self.weekly_l_options = []  # 위클리 L타입
        self.weekly_n_options = []  # 위클리 N타입

        for code in self.code_list:
            if code.startswith(("201", "301")):  # 일반 KOSPI200 옵션
                self.regular_options.append(code)
            elif code.startswith("209DXW") or code.startswith("309DXW"):  # L타입 위클리
                self.weekly_l_options.append(code)
            elif code.startswith("2AF97W") or code.startswith("3AF97W"):  # N타입 위클리
                self.weekly_n_options.append(code)
            else:
                self.regular_options.append(code)  # 기본적으로 일반 옵션으로 분류

        self.log_warning(
            f"📊 옵션 분류 - 일반: {len(self.regular_options)}개, "
            f"위클리L: {len(self.weekly_l_options)}개, "
            f"위클리N: {len(self.weekly_n_options)}개"
        )

    def update_weekly_codes_from_csv(self, csv_file_path: str) -> List[str]:
        """
        CSV 파일에서 최신 위클리 옵션 종목코드를 읽어와 업데이트합니다.

        Args:
            csv_file_path (str): fo_idx_code_mts.csv 파일 경로

        Returns:
            List[str]: 업데이트된 위클리 옵션 종목코드 리스트
        """
        try:
            import pandas as pd

            # CSV 파일 읽기
            df = pd.read_csv(csv_file_path, encoding="utf-8")

            # 위클리 옵션 필터링 (L, N 타입)
            weekly_mask = df["SYMB_TP_CODE"].isin(["L", "N"])
            weekly_df = df[weekly_mask]

            # 종목코드 추출
            weekly_codes = weekly_df["SHTN_CODE"].tolist()

            self.log_warning(f"📋 CSV에서 {len(weekly_codes)}개 위클리 옵션 코드 발견")

            # 타입별 분석
            l_codes = weekly_df[weekly_df["SYMB_TP_CODE"] == "L"]["SHTN_CODE"].tolist()
            n_codes = weekly_df[weekly_df["SYMB_TP_CODE"] == "N"]["SHTN_CODE"].tolist()

            self.log_info(
                f"L 타입 (월요일): {len(l_codes)}개, N 타입 (목요일): {n_codes}개"
            )

            # 코드 리스트 업데이트
            self.code_list = weekly_codes

            return weekly_codes

        except Exception as e:
            self.log_error(f"CSV에서 위클리 옵션 코드 업데이트 실패: {str(e)}")
            return self.code_list

    def _get_additional_api_params(self) -> Dict[str, str]:
        """위클리 옵션 시세 조회를 위한 추가 API 파라미터 반환"""
        return {
            "FID_COND_MRKT_DIV_CODE": self.market_code,  # 옵션 (O)
            "FID_PERIOD_DIV_CODE": self.period_code,  # 일봉 (D)
        }

    # 스케줄 관련 메서드 제거됨 (사용 안 함)

    def collect_data(self):
        """
        모든 옵션 종목의 시세 데이터를 조회합니다.
        """
        self.log_warning(
            f"🎯 국내 옵션 시세 데이터 조회 시작 - {len(self.code_list)}개 종목"
        )

        for index, code in enumerate(self.code_list):
            try:
                # 파라미터에서 날짜 범위 가져오기
                start_date = self.start_date
                end_date = self.end_date

                self.log_warning(f"📊 {code} 조회 시작: {start_date} ~ {end_date}")

                # API 설정에서 TR ID 사용

                # API 파라미터 준비
                params = {
                    "FID_COND_MRKT_DIV_CODE": self.market_code,  # 옵션 (O)
                    "FID_PERIOD_DIV_CODE": self.period_code,  # 일봉 (D)
                    "FID_INPUT_ISCD": code,  # 종목코드
                    "FID_INPUT_DATE_1": start_date,  # 시작일
                    "FID_INPUT_DATE_2": end_date,  # 종료일
                }

                # API 호출 (domestic_futures_price.py와 동일한 방식)
                response_data = self.get_api(self.api_name, params, tr_id=self.tr_id)

                # 응답 파싱
                parsed_data = self.parse_api_response(self.api_name, response_data)

                if parsed_data is not None and not parsed_data.empty:
                    # 메모리에 저장
                    self.options_data[code] = parsed_data

                    # 데이터 범위 확인 (안전하게 처리)
                    total_records = len(parsed_data)

                    # 날짜 컬럼이 있으면 범위 표시, 없으면 기본 정보만 표시
                    if "stck_bsop_date" in parsed_data.columns:
                        start_date_str = parsed_data["stck_bsop_date"].min()
                        end_date_str = parsed_data["stck_bsop_date"].max()
                        date_info = f"({start_date_str} ~ {end_date_str})"
                    else:
                        date_info = f"(컬럼: {list(parsed_data.columns)})"

                    self.log_warning(
                        f"✅ {code}: 옵션 시세 조회 완료 - {total_records}건 수집 {date_info}"
                    )
                else:
                    self.log_warning(f"{code}: 수집된 데이터가 없습니다")

                # 다음 종목 처리 전 지연
                if index < len(self.code_list) - 1 and self.pagination_delay_sec:
                    import time

                    time.sleep(self.pagination_delay_sec)

            except Exception as e:
                self.log_error(f"{code} 조회 중 오류: {str(e)}")
                self.log_error(traceback.format_exc())
                continue

        self.log_warning(
            f"🎯 국내 옵션 시세 데이터 조회 완료 (총 {len(self.code_list)}개 종목)"
        )
        self.health_check_value = "옵션 시세 데이터 조회 완료"

    def parse_api_response(
        self, api_name: str, response_data: Dict
    ) -> Optional[pd.DataFrame]:
        """
        국내선물옵션 일별시세 API 응답에서 필요한 데이터만 파싱하여 DataFrame으로 변환합니다.
        domestic_futures_price.py와 동일한 방식으로 파싱합니다.

        Args:
            api_name (str): API 이름 (반드시 self.API_NAME과 동일해야 함).
            response_data (Dict): API 응답 원본 딕셔너리.

        Returns:
            Optional[pd.DataFrame]: 파싱된 데이터프레임 또는 None.
        """
        # 개선된 오류 처리 사용
        if not self.handle_api_error(response_data, api_name):
            return None

        # 직접 파싱 (domestic_futures_price.py와 동일한 방식)
        df_data = None
        selected_key = None

        # output2 (기간별 조회데이터 배열)를 우선적으로 확인
        if "output2" in response_data and response_data["output2"]:
            data = response_data["output2"]
            if isinstance(data, list) and data:
                df_data = data
                selected_key = "output2"

        # output2가 없거나 비어있으면 다른 키 확인
        if not df_data:
            other_keys = ["output", "output1"]
            for key in other_keys:
                if key in response_data and response_data[key]:
                    data = response_data[key]
                    if isinstance(data, list) and data:
                        df_data = data
                        selected_key = key
                        self.log_warning(f"대체 데이터 사용: {key}, 개수: {len(data)}")
                        break

        if not df_data:
            return None

        # DataFrame 생성
        df = pd.DataFrame(df_data)

        if df.empty:
            return None

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

            # 날짜 컬럼명 확인 및 표준화 (domestic_futures_price.py와 동일)
            date_column = None
            possible_date_columns = [
                "stck_bsop_date",  # 주식 영업일자
                "bsop_date",  # 영업일자
                "date",  # 일자
                "trd_date",  # 거래일자
                "bas_date",  # 기준일자
                "std_date",  # 표준일자
                "data_date",  # 데이터 일자
                "business_date",  # 영업일
                "trading_date",  # 거래일
                "curr_date",  # 현재일자
                "today_date",  # 당일 일자
            ]

            for possible_date_col in possible_date_columns:
                if possible_date_col in df.columns:
                    date_column = possible_date_col
                    break

            # 날짜 컬럼이 없는 경우 - 현재가 데이터일 가능성 (위클리 옵션은 현재가 위주)
            if date_column is None:
                # 위클리 옵션은 현재가 데이터이므로 현재 날짜를 추가
                from datetime import datetime

                current_date = datetime.now().strftime("%Y%m%d")
                df["stck_bsop_date"] = current_date
                date_column = "stck_bsop_date"
                self.log_warning(
                    f"날짜 컬럼이 없어서 현재 날짜({current_date})로 설정합니다."
                )

            # 날짜 컬럼명을 표준화
            if date_column != "stck_bsop_date":
                df = df.rename(columns={date_column: "stck_bsop_date"})

            # 숫자형 컬럼 변환 (domestic_futures_price.py와 동일)
            numeric_columns = [
                "futs_oprc",  # 시가
                "futs_hgpr",  # 고가
                "futs_lwpr",  # 저가
                "futs_prpr",  # 현재가/종가
                "acml_vol",  # 거래량
                "acml_tr_pbmn",  # 거래대금
                "hts_otst_stpl_qty",  # 미결제약정 수량
                "otst_stpl_qty_icdc",  # 미결제약정 수량 증감
            ]

            for col in numeric_columns:
                if col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    except Exception as e:
                        self.log_warning(f"컬럼 {col} 숫자 변환 실패: {e}")

            # 필수 컬럼들이 모두 있는지 확인
            if "stck_bsop_date" not in df.columns:
                self.log_error(f"날짜 컬럼이 누락됨: {df.columns.tolist()}")
                return None

            # 컬럼 정리된 DataFrame 생성
            available_columns = [col for col in final_columns if col in df.columns]
            if available_columns:
                df = df[available_columns].copy()

            return df

        except Exception as e:
            self.log_error(f"Error parsing API response: {e}\n{traceback.format_exc()}")
            return None

    def call_feature(
        self,
        code: Optional[str] = None,
        option_type: Optional[str] = None,
        weekly_type: Optional[str] = None,
        **kwargs,
    ) -> Optional[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:
        """
        저장된 옵션 시세 데이터를 반환합니다.

        Args:
            code (Optional[str]): 조회할 특정 종목코드. None이면 모든 종목 반환.
            option_type (Optional[str]): 옵션 타입 ('regular', 'weekly'). None이면 모든 타입 반환.
            weekly_type (Optional[str]): 위클리 타입 ('L' 또는 'N'). option_type='weekly'일 때만 사용.
            **kwargs: 추가 파라미터.

        Returns:
            pd.DataFrame or Dict: 요청된 옵션 시세 데이터.
        """
        if code:
            # 특정 종목코드 데이터 반환
            data = self.options_data.get(code)
            if data is not None:
                return data.copy()
            else:
                # 스키마/테이블 방식으로 데이터 로드 시도
                data = self.get_data_with_schema(self.schema_name, code.lower())
                if data is not None:
                    self.options_data[code] = data
                    return data
                self.log_warning(f"No data available for code {code}.")
                return None
        elif option_type == "regular":
            # 일반 옵션 데이터만 반환
            filtered_data = {}
            for option_code in self.regular_options:
                if option_code in self.options_data:
                    filtered_data[option_code] = self.options_data[option_code].copy()
            return filtered_data if filtered_data else None
        elif option_type == "weekly":
            # 위클리 옵션 데이터 반환
            if weekly_type:
                # 특정 위클리 타입
                filtered_data = {}
                target_codes = (
                    self.weekly_l_options
                    if weekly_type == "L"
                    else self.weekly_n_options
                )
                for option_code in target_codes:
                    if option_code in self.options_data:
                        filtered_data[option_code] = self.options_data[
                            option_code
                        ].copy()
                return filtered_data if filtered_data else None
            else:
                # 모든 위클리 옵션
                filtered_data = {}
                for option_code in self.weekly_l_options + self.weekly_n_options:
                    if option_code in self.options_data:
                        filtered_data[option_code] = self.options_data[
                            option_code
                        ].copy()
                return filtered_data if filtered_data else None
        else:
            # 모든 옵션 데이터 반환
            if not self.options_data:
                # 저장소에서 모든 코드의 데이터 로드 시도
                for c in self.code_list:
                    data = self.get_data_with_schema(self.schema_name, c.lower())
                    if data is not None:
                        self.options_data[c] = data

            return (
                {k: v.copy() for k, v in self.options_data.items()}
                if self.options_data
                else None
            )
