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
import traceback

from src.data_collection.abstract_feature import Feature
from src.data_collection.api_client import APIClient

logger = logging.getLogger(__name__)


class DomesticFuturesPrice(Feature):
    """
    국내 선물/옵션 시세 및 미결제약정 데이터를 조회하고 관리하는 피처.

    - `features.yaml` 설정을 통해 조회할 종목 코드(`code_list`), 시장 구분(`market_code`),
      조회 기간 타입(`period_code`), API 파라미터(`params`) 등을 설정합니다.
    - `call_feature` 메서드를 통해 저장된 데이터를 반환합니다.
    """

    def __init__(
        self,
        _feature_name: str,
        _code_list: List[str],  # 선물/옵션 코드 리스트
        _feature_query: APIClient,
        _quote_connect: bool,
        _params: Dict,
    ):
        """
        DomesticFuturesPrice 생성자.

        Args:
            _feature_name (str): 피처 이름
            _code_list (List[str]): 조회할 선물/옵션 종목 코드 리스트
            _feature_query (APIClient): API 클라이언트
            _quote_connect (bool): 시세 연결 여부 (현재 미사용)
            _params (Dict): 파라미터
        """
        super().__init__(
            _feature_name,
            _code_list,
            _feature_query,
            _quote_connect,
            _params,
        )
        self.schema_name = "domestic_futures_price"  # 스키마 이름 설정
        self.futures_data = {}  # 수집된 데이터 저장

        # API 설정에서 API 정보 가져오기
        self.api_name = "선물옵션기간별시세"
        api_endpoints = self.params.get("api_config", {}).get("api_endpoints", {})
        api_info = api_endpoints.get(self.api_name, {})
        self.tr_id = api_info.get("tr_id", "FHKIF03020100")

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

        # 날짜 범위 정보
        self.log_info(
            f"{self.feature_name}: 날짜 범위 설정 - {self.start_date} ~ {self.end_date}"
        )
        self.log_info(
            f"{self.feature_name}: 파라미터 - market_code:{self.market_code}, period_code:{self.period_code}"
        )

    def _split_date_range(
        self, start_date: str, end_date: str, max_days: int = 90
    ) -> List[tuple]:
        """
        날짜 범위를 API 제한(100건)에 맞게 분할합니다.

        Args:
            start_date (str): 시작 날짜 (YYYYMMDD)
            end_date (str): 종료 날짜 (YYYYMMDD)
            max_days (int): 한 번에 조회할 최대 일수

        Returns:
            List[tuple]: (시작날짜, 종료날짜) 튜플 리스트
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")

            date_ranges = []
            current_start = start_dt

            while current_start <= end_dt:
                current_end = min(current_start + timedelta(days=max_days - 1), end_dt)
                date_ranges.append(
                    (current_start.strftime("%Y%m%d"), current_end.strftime("%Y%m%d"))
                )
                current_start = current_end + timedelta(days=1)

            return date_ranges
        except Exception as e:
            self.log_error(f"날짜 범위 분할 중 오류: {e}")
            return [(start_date, end_date)]

    def collect_data(self):
        """
        지정된 종목 코드들의 시세 및 미결제약정 데이터를 조회하고 업데이트합니다.
        증분 업데이트를 지원하여 마지막 업데이트 이후의 데이터만 조회합니다.
        """
        self.log_warning(
            f"📈 국내 선물옵션 일별 가격 조회 시작 - 코드: {self.code_list}"
        )

        for index, code in enumerate(self.code_list):
            try:
                # 파라미터에서 날짜 범위 가져오기
                start_date = self.start_date
                end_date = self.end_date

                self.log_warning(f"📊 {code}: 데이터 조회 ({start_date}~{end_date})")

                # 날짜 범위를 분할하여 연속조회 준비
                date_ranges = self._split_date_range(
                    start_date, end_date, self.max_days_per_request
                )

                # 종목별 전체 데이터를 수집할 리스트
                all_data_frames = []
                total_records = 0

                self.log_warning(
                    f"📊 {code}: {len(date_ranges)}개 구간으로 분할하여 조회 시작"
                )

                for range_idx, (range_start, range_end) in enumerate(date_ranges):
                    try:
                        # API 파라미터 설정
                        params = {
                            "FID_COND_MRKT_DIV_CODE": self.market_code,  # 시장 구분 (필수)
                            "FID_INPUT_ISCD": code,  # 종목코드 (필수)
                            "FID_PERIOD_DIV_CODE": self.period_code,  # 기간 구분 (필수)
                            "FID_INPUT_DATE_1": range_start,  # 조회 시작일 (필수)
                            "FID_INPUT_DATE_2": range_end,  # 조회 종료일 (필수)
                        }

                        # 개선된 API 호출 메서드 사용
                        response = self.perform_api_request(
                            method="GET",
                            api_name=self.api_name,
                            tr_id=self.tr_id,
                            params=params,
                        )

                        # 응답 파싱
                        parsed_df = self.parse_api_response(self.api_name, response)

                        if parsed_df is not None and not parsed_df.empty:
                            all_data_frames.append(parsed_df)
                            total_records += len(parsed_df)

                        # 구간 간 지연
                        if (
                            range_idx < len(date_ranges) - 1
                            and self.pagination_delay_sec
                        ):
                            time.sleep(self.pagination_delay_sec)

                    except Exception as range_e:
                        self.log_error(
                            f"{code} {range_idx+1}구간 ({range_start}~{range_end}) 조회 중 오류: {range_e}"
                        )
                        continue

                # 모든 구간 데이터를 하나로 합치기
                if all_data_frames:
                    combined_data = pd.concat(all_data_frames, ignore_index=True)

                    # 중복 제거 및 날짜순 정렬
                    if "stck_bsop_date" in combined_data.columns:
                        combined_data = combined_data.drop_duplicates(
                            subset=["stck_bsop_date"]
                        )
                        combined_data = combined_data.sort_values("stck_bsop_date")

                    # 메모리에 저장
                    self.futures_data[code] = combined_data

                    # 데이터 범위 확인
                    start_date_str = combined_data["stck_bsop_date"].min()
                    end_date_str = combined_data["stck_bsop_date"].max()

                    self.log_warning(
                        f"✅ {code}: 일별 가격 조회 완료 - {total_records}건 수집 "
                        f"({start_date_str} ~ {end_date_str})"
                    )
                else:
                    self.log_warning(f"{code}: 수집된 데이터가 없습니다")

                # 다음 종목 처리 전 지연
                if index < len(self.code_list) - 1 and self.pagination_delay_sec:
                    time.sleep(self.pagination_delay_sec)

            except Exception as e:
                self.log_error(f"{code} 연속조회 중 오류: {str(e)}")
                self.log_error(traceback.format_exc())
                continue

        self.log_warning(
            f"📈 국내 선물옵션 일별 가격 조회 완료 (총 {len(self.code_list)}개 종목)"
        )
        self.health_check_value = "국내 선물옵션 일별 가격 조회 완료"

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

        # output2 (기간별 조회데이터 배열)를 우선적으로 확인
        df_data = None
        selected_key = None

        # API 문서에 따르면 output2가 기간별 조회데이터 (배열)
        if "output2" in response_data and response_data["output2"]:
            data = response_data["output2"]
            if isinstance(data, list) and data:
                df_data = data
                selected_key = "output2"
            else:
                self.log_warning(f"output2가 배열이 아니거나 비어있음: {type(data)}")

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

        try:
            # DataFrame 생성
            df = pd.DataFrame(df_data)

            if df.empty:
                self.log_warning("DataFrame이 비어있습니다.")
                return None

            # 날짜 컬럼 찾기
            date_column = None
            possible_date_columns = [
                "stck_bsop_date",  # 주식 영업일자
                "bsop_date",  # 영업일자
                "date",  # 일자
                "trad_date",  # 거래일자
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

            # 날짜 컬럼이 없는 경우 - 현재가 데이터일 가능성
            if date_column is None:
                return None

            # 날짜 컬럼명을 표준화
            if date_column != "stck_bsop_date":
                df = df.rename(columns={date_column: "stck_bsop_date"})

            # 필수 가격 컬럼 확인
            required_price_columns = ["futs_prpr"]  # 현재가/종가는 필수
            if not all(col in df.columns for col in required_price_columns):
                self.log_error(f"필수 가격 컬럼이 누락됨: {list(df.columns)}")
                return None

            # 숫자형 컬럼 변환
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

            # 최종 DataFrame 생성
            if final_columns:
                df = df[final_columns].copy()
                return df
            else:
                return None

        except Exception as e:
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
