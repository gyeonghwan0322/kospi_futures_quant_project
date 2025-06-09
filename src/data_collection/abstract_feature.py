# -*- coding: utf-8 -*-
"""
피처 추상 클래스 모듈

모든 피처 클래스가 상속해야 하는 기본 추상 클래스를 정의합니다.
"""

from abc import ABCMeta, abstractmethod
import logging
import copy
from typing import Dict, List, Any, Optional, Union, Callable, TypeVar
import os
from datetime import datetime, timedelta
import pandas as pd
import functools
import traceback

logger = logging.getLogger(__name__)

# 데코레이터의 반환 타입을 위한 제네릭 타입 변수
T = TypeVar("T")


def api_error_handler(func: Callable[..., T]) -> Callable[..., Optional[T]]:
    """
    API 호출 함수에 오류 처리를 추가하는 데코레이터

    Args:
        func: 데코레이팅할 함수

    Returns:
        오류 처리가 추가된 래퍼 함수
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            result = func(self, *args, **kwargs)

            # API 응답이 딕셔너리이고 오류 코드가 있는 경우
            if (
                isinstance(result, dict)
                and "rt_cd" in result
                and result.get("rt_cd") != "0"
            ):
                self.log_error(
                    f"API Error in {func.__name__}: {result.get('msg1')} (code: {result.get('rt_cd')})"
                )

            return result
        except Exception as e:
            self.log_error(f"Exception in {func.__name__}: {e}")
            self.log_error(traceback.format_exc())
            return None

    return wrapper


class Feature(metaclass=ABCMeta):
    """
    모든 피처 클래스의 기반이 되는 추상 기본 클래스 (Abstract Base Class).

    `FeatureManager`는 설정을 기반으로 이 클래스의 하위 클래스 인스턴스들을 생성하고 관리합니다.
    각 피처는 특정 금융 데이터(시세, 조회 결과 등)를 처리하거나 계산하는 로직을 캡슐화합니다.

    주요 역할 및 상호작용:
    - **속성 정의**: 피처 이름, 필요한 쿼리 객체, 조회할 코드 리스트(`code_list`),
      개별 파라미터(`params`) 등 피처의 동작 방식을 정의하는 속성들을 가집니다.
    - **값 제공**: 외부(주로 전략 모듈)에서 `call_feature` 메서드를 통해 피처가 계산한 최종 값이나
      내부 상태를 요청할 수 있습니다.
    - **API 활용**: API를 활용하여 표준화된 방식으로 API 요청 및 응답을 처리합니다.
    """

    def __init__(
        self,
        _feature_name,
        _code_list,
        _feature_query,
        _quote_connect,
        _params,
    ):
        """
        Feature 생성자.
        `FeatureManager`가 `config/features.yaml` 설정에 따라 호출합니다.

        Args:
            _feature_name (str): 피처의 고유 이름 (설정 파일의 키와 동일).
            _code_list (list[str]): 이 피처가 사용할 코드 리스트 (종목 코드, 지수 코드 등).
            _feature_query (APIQuery): 데이터 조회에 사용할 API 쿼리 객체.
            _quote_connect (bool): 이 매개변수는 이전 버전과의 호환성을 위해 유지되었지만 사용하지 않습니다.
            _params (dict): 이 피처의 동작을 상세하게 제어하는 파라미터 딕셔너리.
                            `FeatureManager`가 전체 파라미터 설정에서 `param_key`에 해당하는 부분을 추출하여 전달합니다.
        """
        self.feature_name = _feature_name
        self.code_list = _code_list
        self.feature_query = _feature_query

        self.quote_connect = False
        self.params = _params
        self.health_check_value = "Initialized"

        # 피처 데이터 저장소
        self.feature_data = {}

        # 초기화 이후 추가 설정
        self._initialize()

    def _initialize(self):
        """
        피처 초기화 메서드.
        하위 클래스가 추가 초기화를 수행할 경우 super()._initialize()를 호출한 후 처리.
        """
        # 날짜 범위 설정
        if hasattr(self, "params") and isinstance(self.params, dict):
            # 시스템 시간 대신 직접 start_date/end_date 사용 (명시적 지정 시)
            if "start_date" in self.params and "end_date" in self.params:
                self.start_date = self.params.get("start_date")
                self.end_date = self.params.get("end_date")
                self.log_info(
                    f"Set date range from params: {self.start_date} ~ {self.end_date}"
                )
            # fetch_days 기반 설정
            elif "fetch_days" in self.params:
                fetch_days = self.params.get("fetch_days", 1)
                today = datetime.now().strftime("%Y%m%d")
                # 오늘 - fetch_days ~ 오늘
                self.start_date = (
                    datetime.now() - timedelta(days=fetch_days)
                ).strftime("%Y%m%d")
                self.end_date = today
                self.log_info(
                    f"Set date range based on fetch_days={fetch_days}: {self.start_date} ~ {self.end_date}"
                )
            else:
                # 기본값: 오늘
                today = datetime.now().strftime("%Y%m%d")
                self.start_date = today
                self.end_date = today

        # 하위 클래스의 파라미터 초기화 메서드 호출
        self._initialize_params()

    @abstractmethod
    def _initialize_params(self):
        """
        (하위 클래스 구현 필수)
        피처 파라미터 초기화 메서드.

        모든 피처 클래스는 이 메서드를 구현하여 파라미터 초기화 로직을 정의해야 합니다.
        params.yaml에 정의된 설정값들을 로드하여 클래스 속성으로 설정합니다.
        하드코딩된 값은 최소화하고 가능한 모든 설정을 params.yaml에서 가져와 사용합니다.
        """
        pass

    @api_error_handler
    def parse_api_response(self, api_name: str, response_data: Dict) -> Dict:
        """
        API 응답 데이터를 파싱합니다.

        기본 구현은 오류 확인 후 원본 데이터를 반환합니다.
        하위 클래스는 필요에 따라 이 메서드를 오버라이드하여 특정 응답 형식에 맞게
        파싱 로직을 구현하거나, parse_api_basic 메서드를 호출하여 간단한 파싱을 수행할 수 있습니다.

        Args:
            api_name (str): API 이름
            response_data (Dict): API 응답 데이터

        Returns:
            Dict: 파싱된 응답 데이터
        """
        # 오류 처리 (기본 오류 검사)
        if not response_data:
            self.log_error(f"Empty response data for {api_name}")
            return {}

        rt_cd = response_data.get("rt_cd")
        if rt_cd and rt_cd != "0":
            self.log_error(
                f"API Error for {api_name}: {response_data.get('msg1')} (rt_cd: {rt_cd})"
            )

        # 기본 구현은 원본 데이터 반환
        return response_data

    @abstractmethod
    def call_feature(self, *args, **kwargs):
        """
        (하위 클래스 구현 필수)
        외부(주로 전략 모듈)에서 이 피처의 계산 결과나 상태를 요청할 때 호출되는 메서드.

        피처의 주요 기능/계산 결과에 접근하는 표준 인터페이스 역할을 합니다.
        하위 클래스는 이 메서드를 구현하여 적절한 데이터나 계산 결과를 반환해야 합니다.

        Args:
            *args: 가변 위치 인자.
            **kwargs: 가변 키워드 인자.

        Returns:
            Any: 피처의 계산 결과 또는 상태 정보. 반환 형식은 하위 클래스에 따라 다름.
        """
        pass

    def save_data(self, key: str, data: Any):
        """
        피처 데이터를 저장합니다.

        Args:
            key (str): 데이터 키
            data (Any): 저장할 데이터
        """
        self.feature_data[key] = data

    def get_data(self, key: str, default: Any = None) -> Any:
        """
        저장된 피처 데이터를 가져옵니다.

        Args:
            key (str): 데이터 키
            default (Any, optional): 키가 없을 경우 반환할 기본값

        Returns:
            Any: 저장된 데이터 또는 기본값
        """
        return self.feature_data.get(key, default)

    def save_data_with_schema(self, schema_name: str, table_name: str, data: Any):
        """
        스키마와 테이블 이름을 지정하여 피처 데이터를 저장합니다.

        Args:
            schema_name (str): 스키마 이름
            table_name (str): 테이블 이름
            data (Any): 저장할 데이터
        """
        key = f"{schema_name}.{table_name}"
        self.feature_data[key] = data
        self.log_debug(f"데이터 저장: {key}")

    def get_data_with_schema(
        self, schema_name: str, table_name: str, default: Any = None
    ) -> Any:
        """
        스키마와 테이블 이름으로 저장된 피처 데이터를 가져옵니다.

        Args:
            schema_name (str): 스키마 이름
            table_name (str): 테이블 이름
            default (Any, optional): 키가 없을 경우 반환할 기본값

        Returns:
            Any: 저장된 데이터 또는 기본값
        """
        key = f"{schema_name}.{table_name}"
        return self.feature_data.get(key, default)

    def clear_data(self, key: Optional[str] = None):
        """
        피처 데이터를 삭제합니다.

        Args:
            key (Optional[str], optional): 삭제할 데이터 키. None이면 모든 데이터 삭제.
        """
        if key is None:
            self.feature_data.clear()
        elif key in self.feature_data:
            del self.feature_data[key]

    def log_info(self, message: str):
        """
        정보 로그를 기록합니다.

        Args:
            message (str): 로그 메시지
        """
        logger.info(f"[{self.feature_name}] {message}")

    def log_error(self, message: str):
        """
        오류 로그를 기록합니다.

        Args:
            message (str): 로그 메시지
        """
        logger.error(f"[{self.feature_name}] {message}")

    def log_debug(self, message: str):
        """
        디버그 로그를 기록합니다.

        Args:
            message (str): 로그 메시지
        """
        logger.debug(f"[{self.feature_name}] {message}")

    def log_warning(self, message: str):
        """
        경고 로그를 기록합니다.

        Args:
            message (str): 로그 메시지
        """
        logger.warning(f"[{self.feature_name}] {message}")

    def __str__(self) -> str:
        """문자열 표현을 반환합니다."""
        return f"Feature({self.feature_name})"

    def __repr__(self) -> str:
        """공식 문자열 표현을 반환합니다."""
        return f"Feature(name='{self.feature_name}', codes={len(self.code_list)})"

    def handle_api_error(self, response_data: Dict, api_name: str) -> bool:
        """
        API 오류 처리 통합 메서드

        Args:
            response_data (Dict): API 응답 데이터
            api_name (str): API 이름

        Returns:
            bool: 성공(True) 또는 오류(False)
        """
        if not response_data:
            self.log_error(f"No response data received for {api_name}")
            return False

        rt_cd = response_data.get("rt_cd")
        if rt_cd != "0":
            self.log_error(
                f"API Error for {api_name}: {response_data.get('msg1')} (rt_cd: {rt_cd})"
            )

            # 모의투자 지원하지 않는 API 처리
            if "모의투자에서는 지원하지 않는 서비스" in str(
                response_data.get("msg1", "")
            ):
                self.log_warning(
                    f"API {api_name} is not supported in the sandbox environment"
                )
            return False

        return True

    def parse_api_basic(
        self,
        api_name: str,
        response_data: Dict,
        output_key: str = "output2",
        date_column: str = "stck_bsop_date",
        date_format: str = "%Y%m%d",
        numeric_columns: List[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        API 응답 데이터 기본 파싱 메서드

        일반적인 API 응답에 대한 기본 파싱 패턴을 구현한 메서드입니다.
        하위 클래스는 이 메서드를 호출하여 기본 파싱 후 추가 처리를 수행할 수 있습니다.

        Args:
            api_name (str): API 이름
            response_data (Dict): API 응답 데이터
            output_key (str): 데이터가 포함된 응답 키 (기본값: "output2")
            date_column (str): 날짜 컬럼 이름 (기본값: "stck_bsop_date")
            date_format (str): 날짜 형식 (기본값: "%Y%m%d")
            numeric_columns (List[str]): 숫자로 변환할 컬럼 리스트

        Returns:
            Optional[pd.DataFrame]: 파싱된 데이터프레임 또는 None
        """
        # 오류 처리
        if not self.handle_api_error(response_data, api_name):
            return None

        # 데이터 추출
        output = response_data.get(output_key)
        if not output:
            # output1도 확인 (일부 API는 output1에 데이터가 있음)
            output = response_data.get("output1")
            if not output:
                self.log_warning(
                    f"No valid '{output_key}' data found in response for {api_name}"
                )
                return None

        # 리스트가 아닌 경우 리스트로 변환 (단일 객체인 경우)
        if isinstance(output, dict):
            output = [output]

        if not isinstance(output, list) or not output:
            self.log_warning(f"Invalid or empty data format in response for {api_name}")
            return None

        try:
            # DataFrame 변환
            df = pd.DataFrame(output)

            # 날짜 컬럼 처리
            if date_column in df.columns:
                df[date_column] = pd.to_datetime(
                    df[date_column], format=date_format, errors="coerce"
                )

            # 숫자형 컬럼 변환
            if numeric_columns:
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(
                            df[col].replace(r"^\s*$", "0", regex=True), errors="coerce"
                        )

            return df

        except Exception as e:
            self.log_error(f"Error parsing API response for {api_name}: {e}")
            self.log_error(traceback.format_exc())
            return None

    def perform_api_request(
        self,
        method: str,
        api_name: str,
        tr_id: Optional[str] = None,
        params: Optional[Dict] = None,
        body: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        url_path: Optional[str] = None,
    ) -> Dict:
        """
        API 요청을 수행하는 통합 메서드

        Args:
            method (str): HTTP 메서드 ('GET', 'POST' 등)
            api_name (str): API 이름 (로깅용)
            tr_id (Optional[str]): 거래 ID (TR_ID)
            params (Optional[Dict]): URL 쿼리 파라미터
            body (Optional[Dict]): 요청 본문 데이터
            headers (Optional[Dict]): 요청 헤더
            url_path (Optional[str]): API 엔드포인트 경로 (직접 지정 시)

        Returns:
            Dict: API 응답 데이터
        """
        try:
            # APIClient의 request 메서드 직접 호출
            if hasattr(self.feature_query, "request") and callable(
                self.feature_query.request
            ):
                return self.feature_query.request(
                    method=method,
                    api_name=api_name,
                    tr_id=tr_id,
                    params=params,
                    body=body,
                    headers=headers,
                )
            else:
                self.log_error("Feature query object does not have a request method")
                return {"rt_cd": "-1", "msg1": "Feature query has no request method"}
        except Exception as e:
            self.log_error(f"API request failed for {api_name}: {str(e)}")
            return {"rt_cd": "-1", "msg1": f"API request exception: {str(e)}"}

    def get_api(
        self,
        api_name: str,
        params: Optional[Dict] = None,
        tr_id: Optional[str] = None,
        url_path: Optional[str] = None,
    ) -> Dict:
        """
        GET 방식 API 요청 유틸리티 메서드

        Args:
            api_name (str): API 이름 (로깅용)
            params (Optional[Dict]): URL 쿼리 파라미터
            tr_id (Optional[str]): 거래 ID (명시적 지정 시)
            url_path (Optional[str]): API 엔드포인트 경로 (직접 지정 시)

        Returns:
            Dict: API 응답 데이터
        """
        return self.perform_api_request(
            method="GET",
            api_name=api_name,
            tr_id=tr_id,
            params=params,
            url_path=url_path,
        )

    def post_api(
        self,
        api_name: str,
        body: Optional[Dict] = None,
        tr_id: Optional[str] = None,
        url_path: Optional[str] = None,
    ) -> Dict:
        """
        POST 방식 API 요청 유틸리티 메서드

        Args:
            api_name (str): API 이름 (로깅용)
            body (Optional[Dict]): 요청 본문 데이터
            tr_id (Optional[str]): 거래 ID (명시적 지정 시)
            url_path (Optional[str]): API 엔드포인트 경로 (직접 지정 시)

        Returns:
            Dict: API 응답 데이터
        """
        return self.perform_api_request(
            method="POST", api_name=api_name, tr_id=tr_id, body=body, url_path=url_path
        )
