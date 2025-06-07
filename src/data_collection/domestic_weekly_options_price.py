# -*- coding: utf-8 -*-
"""
국내 위클리 옵션 개별 시세 데이터를 조회하는 피처 모듈.
'국내선물옵션 일별시세[v1_국내선물-011]' API를 사용합니다.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from datetime import datetime

from src.feature_engineering.abstract_feature import Feature
from src.data_collection.api_client import APIClient
from src.utils.api_config_manager import get_api_config

logger = logging.getLogger(__name__)


class DomesticWeeklyOptionsPrice(Feature):
    """
    국내 위클리 옵션 개별 종목 시세 데이터를 조회하고 관리하는 피처.

    실제 거래되는 위클리 옵션 종목코들을 이용하여 개별 시세 데이터를 수집합니다.
    - L 타입: 월요일 만료 (209DXW... 형태)
    - N 타입: 목요일 만료 (2AF97W... 형태)
    """

    API_NAME = "국내선물옵션 일별시세[v1_국내선물-011]"

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
        DomesticWeeklyOptionsPrice 생성자.

        Args:
            _feature_name (str): 피처 이름.
            _code_list (list[str]): 위클리 옵션 종목코드 리스트.
            _feature_query (APIClient): API 호출에 사용할 APIClient 객체.
            _quote_connect (bool): 사용되지 않음.
            _inquiry (bool): 시간 기반 조회 사용 여부.
            _inquiry_time_list (list[str]): 조회 수행 시각 리스트 (HHMMSS).
            _inquiry_name_list (list[str]): 사용되지 않음.
            _params (dict): 피처 설정 파라미터.
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
        self.schema_name = "domestic_weekly_options_price"  # 스키마 이름 설정
        # 위클리 옵션 시세 데이터 저장소 (종목코드별)
        self.weekly_options_data: Dict[str, pd.DataFrame] = {}
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

        # 위클리 옵션 종목코드는 features.yaml의 code_list에서 관리
        if not self.code_list:
            self.log_warning(
                "⚠️ 위클리 옵션 코드가 설정되지 않음 - features.yaml에서 code_list 설정 필요"
            )
            self.code_list = []

        self.log_warning(f"🔧 위클리 옵션 시세 조회 대상: {len(self.code_list)}개 종목")

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

            self.log_warning(f"📊 CSV에서 {len(weekly_codes)}개 위클리 옵션 코드 발견")

            # 타입별 분석
            l_codes = weekly_df[weekly_df["SYMB_TP_CODE"] == "L"]["SHTN_CODE"].tolist()
            n_codes = weekly_df[weekly_df["SYMB_TP_CODE"] == "N"]["SHTN_CODE"].tolist()

            self.log_warning(f"📊   L 타입 (월요일): {len(l_codes)}개")
            self.log_warning(f"📊   N 타입 (목요일): {len(n_codes)}개")

            # 코드 리스트 업데이트
            self.code_list = weekly_codes

            return weekly_codes

        except Exception as e:
            self.log_error(f"❌ CSV에서 위클리 옵션 코드 업데이트 실패: {str(e)}")
            return self.code_list

    def _get_additional_api_params(self) -> Dict[str, str]:
        """위클리 옵션 시세 조회를 위한 추가 API 파라미터 반환"""
        return {
            "FID_COND_MRKT_DIV_CODE": self.market_code,  # 옵션 (O)
            "FID_PERIOD_DIV_CODE": self.period_code,  # 일봉 (D)
        }

    def _perform_inquiry(self, clock: str):
        """
        설정된 시간에 맞추어 모든 위클리 옵션 종목의 시세 데이터를 조회합니다.

        Args:
            clock (str): 현재 시각 (HHMMSS).
        """
        api_config = get_api_config()
        self.log_warning(
            f"🎯 국내 위클리 옵션 시세 데이터 조회 시작 - {len(self.code_list)}개 종목, 시간: {clock}"
        )

        collected_data = {}

        # 일반 데이터 조회
        for code in self.code_list:
            try:
                # API 설정에서 파라미터 자동 구성 - 선물옵션기간별시세 API 사용
                params = api_config.build_api_params(
                    api_name="선물옵션기간별시세",
                    symbol_code=code,
                    start_date=self.start_date,
                    end_date=self.end_date,
                )

                # 종목 유형 확인
                symbol_type = api_config.get_symbol_type(code)
                is_put_option = api_config.is_put_option(code)

                self.log_info(
                    f"📊 {code} 위클리 옵션 시세 조회 시작 (유형: {symbol_type})"
                )

                # API 호출 - 선물옵션기간별시세 TR ID 사용
                response = self.get_api(
                    self.API_NAME,
                    params,
                    tr_id=api_config.get_tr_id("선물옵션기간별시세"),
                )

                # 응답 파싱
                parsed_df = self.parse_api_response(self.API_NAME, response)

                if parsed_df is not None and not parsed_df.empty:
                    # 풋옵션의 경우 날짜 컬럼이 없으므로 현재 날짜로 설정
                    if is_put_option and "stck_bsop_date" not in parsed_df.columns:
                        current_date = datetime.now().strftime("%Y-%m-%d")
                        parsed_df["stck_bsop_date"] = current_date
                        self.log_info(
                            f"📅 {code}: 풋옵션 날짜 컬럼 추가 - {current_date}"
                        )

                    # 메모리에 저장
                    self.weekly_options_data[code] = parsed_df
                    collected_data[code] = parsed_df

                    # CSV 파일로 저장
                    self.save_data_with_schema(
                        schema_name=getattr(
                            self, "schema_name", "domestic_weekly_options_price"
                        ),
                        table_name=f"{self.feature_name}/{code}",
                        data=parsed_df,
                    )

                    self.log_warning(
                        f"✅ {code}: 위클리 옵션 시세 수집 완료 - {len(parsed_df)}건 (유형: {symbol_type})"
                    )
                else:
                    self.log_warning(f"⚠️ {code}: 수집된 데이터가 없습니다")

            except Exception as e:
                self.log_error(f"❌ {code} 조회 중 오류: {str(e)}")
                continue

        # 성공 결과 로깅
        if collected_data:
            success_count = len(collected_data)
            self.log_warning(f"✅ 위클리 옵션 시세 수집 완료: {success_count}개 종목")
        else:
            self.log_warning("⚠️ 위클리 옵션 시세 데이터가 없습니다")

        self.log_warning(
            f"🎯 국내 위클리 옵션 시세 데이터 조회 완료 (총 {len(self.code_list)}개 종목)"
        )
        self.health_check_value = f"위클리 옵션 시세 데이터 조회 완료 (시간: {clock})"

    def parse_api_response(
        self, api_name: str, response_data: Dict
    ) -> Optional[pd.DataFrame]:
        """
        선물옵션기간별시세 API 응답에서 필요한 데이터만 파싱하여 DataFrame으로 변환합니다.

        Args:
            api_name (str): API 이름.
            response_data (Dict): API 응답 원본 딕셔너리.

        Returns:
            Optional[pd.DataFrame]: 파싱된 데이터프레임 또는 None.
        """
        if api_name != self.API_NAME:
            self.log_error(
                f"parse_api_response called with incorrect API name: {api_name}"
            )
            return None

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

        return df

    def call_feature(
        self,
        code: Optional[str] = None,
        weekly_type: Optional[str] = None,
        **kwargs,
    ) -> Optional[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:
        """
        저장된 위클리 옵션 시세 데이터를 반환합니다.

        Args:
            code (Optional[str]): 조회할 특정 종목코드. None이면 모든 종목 반환.
            weekly_type (Optional[str]): 위클리 타입 ('L' 또는 'N'). None이면 모든 타입 반환.
            **kwargs: 추가 파라미터.

        Returns:
            pd.DataFrame or Dict: 요청된 위클리 옵션 시세 데이터.
        """
        if code:
            # 특정 종목코드 데이터 반환
            return self.weekly_options_data.get(code)
        elif weekly_type:
            # 특정 위클리 타입 데이터 반환
            filtered_data = {}
            for option_code, data in self.weekly_options_data.items():
                if weekly_type == "L" and option_code.startswith("209DXW"):
                    filtered_data[option_code] = data
                elif weekly_type == "N" and option_code.startswith("2AF97W"):
                    filtered_data[option_code] = data
            return filtered_data if filtered_data else None
        else:
            # 모든 위클리 옵션 데이터 반환
            return self.weekly_options_data if self.weekly_options_data else None
