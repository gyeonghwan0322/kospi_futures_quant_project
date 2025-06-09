# -*- coding: utf-8 -*-
"""
시장별 투자자매매동향(일별) 데이터를 조회하는 피처 모듈.
'시장별 투자자매매동향(일별) [국내주식-075]' API를 사용합니다.

지원 시장:
- KOSPI (KSP)
- KOSDAQ (KSQ)
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime, timedelta
import time
import traceback

from src.data_collection.abstract_feature import Feature
from src.data_collection.api_client import APIClient

logger = logging.getLogger(__name__)


class InvestorDaily(Feature):
    """
    시장별 투자자매매동향(일별) 데이터를 조회하고 관리하는 피처.

    - `features.yaml` 설정을 통해 조회할 시장 구분(`code_list`),
      조회 기간(`start_date`, `end_date`) 등을 설정합니다.
    - `call_feature` 메서드를 통해 저장된 데이터를 반환합니다.
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
        InvestorDaily 생성자.

        Args:
            _feature_name (str): 피처 이름.
            _code_list (List[str]): 조회 대상 시장 코드 리스트 (kospi, kosdaq).
            _feature_query (APIClient): API 호출에 사용할 APIClient 객체.
            _quote_connect (bool): 사용되지 않음.
            _params (Dict): 피처 설정 파라미터.
        """
        super().__init__(
            _feature_name,
            _code_list,
            _feature_query,
            False,
            _params,
        )
        self.schema_name = "market_data"
        # 일별 투자자매매동향 데이터 저장소
        self.daily_investor_data: Dict[str, pd.DataFrame] = {}

        # API 설정에서 API 정보 가져오기
        self.api_name = "시장별 투자자매매동향(일별)"
        api_endpoints = self.params.get("api_config", {}).get("api_endpoints", {})
        api_info = api_endpoints.get(self.api_name, {})
        self.tr_id = api_info.get("tr_id", "FHPTJ04040000")

        self._initialize_params()

    def _initialize_params(self):
        """API 파라미터 초기화"""
        # 기본값 설정
        self.start_date = self.params.get("start_date", "20240101")
        self.end_date = self.params.get("end_date", "20251231")
        self.pagination_delay_sec = self.params.get("pagination_delay_sec", 1.0)

        # 시장별 매핑 정보 설정
        self.market_mappings = self.params.get(
            "market_mappings",
            {
                "kospi": {
                    "market_div_code": "U",  # 업종 구분
                    "input_iscd": "U",  # 업종분류코드
                    "input_iscd_1": "KSP",  # 코스피
                },
                "kosdaq": {
                    "market_div_code": "U",  # 업종 구분
                    "input_iscd": "U",  # 업종분류코드
                    "input_iscd_1": "KSQ",  # 코스닥
                },
            },
        )

        logger.info(f"InvestorDaily 파라미터 초기화 완료:")
        logger.info(f"  - 조회 기간: {self.start_date} ~ {self.end_date}")
        logger.info(f"  - 지원 시장: {list(self.market_mappings.keys())}")

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

    def _call_daily_investor_api(
        self, market_code: str, target_date: str
    ) -> Optional[Dict]:
        """시장별 투자자매매동향(일별) API 호출"""
        try:
            if market_code not in self.market_mappings:
                logger.error(f"지원하지 않는 시장 코드: {market_code}")
                return None

            mapping = self.market_mappings[market_code]

            # API 파라미터 구성
            params = {
                "FID_COND_MRKT_DIV_CODE": mapping[
                    "market_div_code"
                ],  # 조건 시장 분류 코드
                "FID_INPUT_ISCD": mapping["input_iscd"],  # 입력 종목코드 (업종분류코드)
                "FID_INPUT_DATE_1": target_date,  # 입력 날짜1
                "FID_INPUT_ISCD_1": mapping["input_iscd_1"],  # 입력 종목코드 (시장구분)
            }

            logger.debug(f"일별 투자자매매동향 API 호출: {market_code}, {target_date}")
            logger.debug(f"파라미터: {params}")

            response = self._feature_query.call_api(
                path="/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market",
                method="GET",
                tr_id=self.tr_id,
                params=params,
            )

            if response and response.get("rt_cd") == "0":
                return response.get("output1", [])
            else:
                error_msg = (
                    response.get("msg1", "Unknown error") if response else "No response"
                )
                logger.error(
                    f"API 호출 실패: {market_code}, {target_date} - {error_msg}"
                )
                return None

        except Exception as e:
            logger.error(f"일별 투자자매매동향 API 호출 중 오류: {e}")
            logger.error(traceback.format_exc())
            return None

    def _process_daily_investor_data(
        self, raw_data: List[Dict], market_code: str
    ) -> pd.DataFrame:
        """API 응답 데이터를 DataFrame으로 변환"""
        if not raw_data:
            return pd.DataFrame()

        try:
            # DataFrame 생성
            df = pd.DataFrame(raw_data)

            # 날짜 컬럼 처리
            if "stck_bsop_date" in df.columns:
                df["trade_date"] = pd.to_datetime(df["stck_bsop_date"], format="%Y%m%d")
                df = df.drop("stck_bsop_date", axis=1)

            # 시장 코드 추가
            df["market_code"] = market_code

            # 수치형 변환이 필요한 컬럼들
            numeric_columns = [
                "bstp_nmix_prpr",
                "bstp_nmix_prdy_vrss",
                "bstp_nmix_prdy_ctrt",
                "bstp_nmix_oprc",
                "bstp_nmix_hgpr",
                "bstp_nmix_lwpr",
                "stck_prdy_clpr",
                "frgn_ntby_qty",
                "frgn_reg_ntby_qty",
                "frgn_nreg_ntby_qty",
                "prsn_ntby_qty",
                "orgn_ntby_qty",
                "scrt_ntby_qty",
                "ivtr_ntby_qty",
                "pe_fund_ntby_vol",
                "bank_ntby_qty",
                "insu_ntby_qty",
                "mrbn_ntby_qty",
                "fund_ntby_qty",
                "etc_ntby_qty",
                "etc_orgt_ntby_vol",
                "etc_corp_ntby_vol",
                "frgn_ntby_tr_pbmn",
                "frgn_reg_ntby_pbmn",
                "frgn_nreg_ntby_pbmn",
                "prsn_ntby_tr_pbmn",
                "orgn_ntby_tr_pbmn",
                "scrt_ntby_tr_pbmn",
                "ivtr_ntby_tr_pbmn",
                "pe_fund_ntby_tr_pbmn",
                "bank_ntby_tr_pbmn",
                "insu_ntby_tr_pbmn",
                "mrbn_ntby_tr_pbmn",
                "fund_ntby_tr_pbmn",
                "etc_ntby_tr_pbmn",
                "etc_orgt_ntby_tr_pbmn",
                "etc_corp_ntby_tr_pbmn",
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # 컬럼 순서 정리
            base_columns = ["trade_date", "market_code"]
            other_columns = [col for col in df.columns if col not in base_columns]
            df = df[base_columns + other_columns]

            logger.info(
                f"일별 투자자매매동향 데이터 처리 완료: {market_code}, {len(df)}건"
            )
            return df

        except Exception as e:
            logger.error(f"일별 투자자매매동향 데이터 처리 중 오류: {e}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def collect_data(self):
        """시장별 일별 투자자매매동향 데이터 수집"""
        logger.info("일별 투자자매매동향 데이터 수집 시작")

        try:
            # 조회 날짜 범위 생성
            date_list = self._generate_date_range(self.start_date, self.end_date)
            logger.info(f"조회 대상 날짜: {len(date_list)}일")

            for market_code in self.code_list:
                logger.info(f"시장 {market_code} 데이터 수집 시작")

                market_data_list = []

                for target_date in date_list:
                    # API 호출
                    raw_data = self._call_daily_investor_api(market_code, target_date)

                    if raw_data:
                        # 데이터 처리
                        processed_data = self._process_daily_investor_data(
                            raw_data, market_code
                        )
                        if not processed_data.empty:
                            market_data_list.append(processed_data)

                    # API 호출 간격 조절
                    time.sleep(self.pagination_delay_sec)

                # 시장별 데이터 통합
                if market_data_list:
                    combined_df = pd.concat(market_data_list, ignore_index=True)
                    combined_df = combined_df.sort_values("trade_date").reset_index(
                        drop=True
                    )

                    # 데이터 저장
                    self.daily_investor_data[market_code] = combined_df

                    # 파일로 저장
                    self.save_data_to_file(combined_df, market_code)

                    logger.info(
                        f"시장 {market_code} 데이터 수집 완료: {len(combined_df)}건"
                    )
                else:
                    logger.warning(f"시장 {market_code}에 대한 데이터가 없습니다.")

        except Exception as e:
            logger.error(f"일별 투자자매매동향 데이터 수집 중 오류 발생: {e}")
            logger.error(traceback.format_exc())

    def call_feature(self, code: str) -> Optional[pd.DataFrame]:
        """지정된 시장의 일별 투자자매매동향 데이터 반환

        Args:
            code (str): 시장 코드 (kospi, kosdaq)

        Returns:
            Optional[pd.DataFrame]: 해당 시장의 일별 투자자매매동향 데이터
        """
        if code in self.daily_investor_data:
            return self.daily_investor_data[code].copy()

        logger.warning(f"시장 코드 '{code}'에 대한 데이터가 없습니다.")
        return None

    def get_all_data(self) -> Dict[str, pd.DataFrame]:
        """모든 시장의 일별 투자자매매동향 데이터 반환

        Returns:
            Dict[str, pd.DataFrame]: 시장별 일별 투자자매매동향 데이터
        """
        return {market: df.copy() for market, df in self.daily_investor_data.items()}
