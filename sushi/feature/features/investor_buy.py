# -*- coding: utf-8 -*-
"""
투자자 매매동향 데이터를 조회하는 모듈.
"""

import logging
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union

from sushi.feature.api_client import APIClient
from sushi.feature.abstract_feature import Feature

logger = logging.getLogger(__name__)


class InvestorTrends(Feature):
    """투자자 매매동향 데이터 수집 클래스"""

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
        InvestorTrends 생성자

        Args:
            _feature_name (str): 피처 이름
            _code_list (List[str]): 시장 코드 리스트 (kospi, kosdaq 등)
            _feature_query (APIClient): API 클라이언트
            _quote_connect (bool): 시세 연결 여부
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
        self.schema_name = "investor_trends"  # 스키마 이름 설정
        self.trends_data = {}  # 수집된 데이터 저장
        self._initialize_params()

    def _initialize_params(self):
        """피처 파라미터 초기화 및 기본값 설정"""
        # 모든 파라미터를 params.yaml에서 가져옴
        self.market = self.params.get("market")
        self.sector = self.params.get("sector")
        self.mode = self.params.get("mode")
        self.date_range = self.params.get("date_range")

        # 시간대별 투자자매매동향만 지원 (일별 제거)
        self.is_daily = False

        # 날짜 관련 설정은 상위 클래스(Feature)에서 처리됨
        # self.start_date와 self.end_date는 이미 _initialize()에서 설정됨

    def _perform_inquiry(self, clock: str):
        """
        정해진 시간에 투자자 매매 동향 데이터를 조회합니다.
        시간대별 투자자매매동향만 지원합니다.

        Args:
            clock (str): 현재 시각 (HHMMSS)
        """
        self.log_info(f"투자자 매매동향 조회 시작 (시간: {clock})")

        if not self.code_list:
            self.log_warning("조회할 시장 코드가 지정되지 않았습니다.")
            return

        # APIClient에서 설정된 기본 URL 가져오기
        base_url = (
            self.feature_query.base_url
            if hasattr(self.feature_query, "base_url")
            else "https://openapi.koreainvestment.com:9443"
        )

        for market_code in self.code_list:
            try:
                market = market_code.lower()
                if market not in [
                    "kospi",
                    "kosdaq",
                    "kospi200",
                    "options",
                    "putoptions",
                ]:
                    self.log_warning(f"지원되지 않는 시장 코드: {market}")
                    continue

                self.log_info(f"{market.upper()} 시간대별 투자자 매매동향 데이터 수집")

                # 시간대별 투자매매동향 API 사용
                api_name = "투자자시간대별매매동향"
                tr_id = "FHPTJ04030000"
                # API 엔드포인트 직접 지정
                url_path = (
                    "/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market"
                )

                # 시장별 파라미터 설정
                if market == "kospi":
                    market_div_code = "KSP"  # 코스피
                    input_code_2 = "0001"  # 코스피 종합지수
                elif market == "kosdaq":
                    market_div_code = "KSQ"  # 코스닥
                    input_code_2 = "1001"  # 코스닥 종합지수
                elif market == "kospi200":
                    market_div_code = "K2I"  # 선물
                    input_code_2 = "F001"  # 선물
                elif market == "options":
                    market_div_code = "K2I"  # 선물
                    input_code_2 = "OC01"  # 콜옵션
                elif market == "putoptions":
                    market_div_code = "K2I"  # 선물
                    input_code_2 = "OP01"  # 풋옵션

                params = {
                    "fid_input_iscd_2": input_code_2,
                    "fid_input_iscd": market_div_code,
                }

                # API 요청
                self.log_debug(
                    f"Requesting {market} investor trends with params: {params}"
                )

                url = f"{base_url}{url_path}"
                self.log_info(f"API URL: {url}, TR_ID: {tr_id}")

                # 개선된 API 호출 메서드 사용
                response = self.perform_api_request(
                    method="GET", api_name=api_name, tr_id=tr_id, params=params
                )

                # 응답 데이터 처리
                if not self.handle_api_error(response, api_name):
                    # 모의투자에서는 지원하지 않을 수 있음
                    if "모의투자에서는 지원하지 않는 서비스" in str(
                        response.get("msg1", "")
                    ):
                        self.log_warning(
                            "모의투자 환경에서는 이 API가 지원되지 않습니다."
                        )
                    continue

                # API 응답 구조에 따라 데이터 위치 확인
                # 시간대별 데이터에서 output이 포함되어 있을 수 있음
                data_keys = ["output", "output1"]
                data_key = None
                items = None

                # 가능한 모든 데이터 키를 확인
                for key in data_keys:
                    if key in response and response[key]:
                        data_key = key
                        items = response[key]
                        self.log_debug(f"데이터를 찾았습니다: {key}")
                        break

                if not data_key or not items:
                    # 데이터가 없는 경우
                    self.log_warning(f"{market} 데이터 없음: {list(response.keys())}")
                    continue

                # 단일 레코드 또는 배열 처리
                if isinstance(items, dict):
                    items = [items]  # 단일 레코드를 리스트로 변환

                if not items:
                    self.log_info(f"{market} 해당 시간에 데이터가 없습니다.")
                    continue

                # 전체 응답 구조 로깅 (디버깅용)
                self.log_debug(f"응답 데이터 구조: {list(response.keys())}")
                self.log_debug(f"항목 개수: {len(items)}")
                self.log_debug(
                    f"첫 번째 항목 키: {list(items[0].keys())[:10] if items and isinstance(items[0], dict) else None}"
                )

                # DataFrame 변환
                df = pd.DataFrame(items)

                # 시간 컬럼 처리 (시간대별 데이터의 경우)
                time_column = None
                if "stck_bsop_time" in df.columns:
                    time_column = "stck_bsop_time"
                elif "time" in df.columns:
                    time_column = "time"

                if time_column:
                    df.rename(columns={time_column: "time"}, inplace=True)
                    # 시간 형식 처리 (HHMMSS)
                    df["time"] = pd.to_datetime(df["time"], format="%H%M%S").dt.time

                # 데이터 저장 - 스키마/테이블 방식 사용
                self.trends_data[market] = df
                # 스키마/테이블 기반 저장
                self.save_data_with_schema(self.schema_name, market.lower(), df)

                self.log_info(
                    f"{market} 시간대별 투자자 매매동향 데이터 {len(df)}개 수집 완료"
                )

            except Exception as e:
                self.log_error(
                    f"{market_code} 투자자 매매동향 데이터 수집 중 오류 발생: {str(e)}"
                )
                import traceback

                self.log_error(traceback.format_exc())
                continue

        self.health_check_value = f"투자자 매매동향 조회 완료 (시간: {clock})"

    def call_feature(
        self, market: Optional[str] = None, **kwargs
    ) -> Optional[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:
        """
        투자자 매매동향 데이터를 반환합니다.

        Args:
            market (Optional[str]): 특정 시장 코드. None이면 모든 시장 데이터 반환.
            **kwargs: 추가 인자 (현재 사용 안 함).

        Returns:
            Optional[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:
                - market이 주어지면 해당 시장의 DataFrame 반환.
                - market이 None이면 모든 시장 데이터를 딕셔너리로 반환.
                - 데이터가 없으면 None 반환.
        """
        if market:
            market = market.lower()
            if market in self.trends_data:
                return self.trends_data[market].copy()
            else:
                # 스키마/테이블 방식으로 데이터 로드 시도
                data = self.get_data_with_schema(self.schema_name, market)
                if data is not None:
                    self.trends_data[market] = data
                    return data
                self.log_warning(f"시장 '{market}'에 대한 데이터가 없습니다.")
                return None
        else:
            # 모든 시장 데이터 반환
            if not self.trends_data:
                # 저장소에서 모든 코드의 데이터 로드 시도
                for code in self.code_list:
                    code = code.lower()
                    data = self.get_data_with_schema(self.schema_name, code)
                    if data is not None:
                        self.trends_data[code] = data

            return self.trends_data if self.trends_data else None
