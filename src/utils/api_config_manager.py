"""
API 설정 관리 유틸리티

YAML 파일에서 API 설정을 로드하고 관리하는 클래스
"""

import yaml
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class ApiConfigManager:
    """API 설정 관리 클래스"""

    def __init__(self, config_path: str = "config/api_config.yaml"):
        """
        API 설정 관리자 초기화

        Args:
            config_path (str): 설정 파일 경로
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """설정 파일 로드"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)
                logger.info(f"API 설정 로드 완료: {self.config_path}")
                return config
        except FileNotFoundError:
            logger.error(f"설정 파일을 찾을 수 없습니다: {self.config_path}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"YAML 파싱 오류: {e}")
            return {}

    def get_tr_id(self, api_name: str) -> str:
        """
        API 이름으로 TR ID 조회

        Args:
            api_name (str): API 이름

        Returns:
            str: TR ID
        """
        tr_ids = self.config.get("tr_ids", {})
        return tr_ids.get(api_name, "FHKIF03020100")  # 기본값

    def get_api_endpoint(self, api_name: str) -> str:
        """
        API 이름으로 엔드포인트 조회

        Args:
            api_name (str): API 이름

        Returns:
            str: API 엔드포인트
        """
        endpoints = self.config.get("api_endpoints", {})
        return endpoints.get(api_name, "")

    def get_data_schema(self, api_name: str, symbol_code: str = None) -> Dict[str, Any]:
        """
        API와 종목코드에 따른 데이터 스키마 조회

        Args:
            api_name (str): API 이름
            symbol_code (str): 종목코드 (옵션 구분용)

        Returns:
            Dict[str, Any]: 데이터 스키마 정보
        """
        schemas = self.config.get("data_schemas", {})

        # 위클리옵션의 경우 콜/풋 구분
        if api_name == "위클리옵션시세" and symbol_code:
            if self.is_call_option(symbol_code):
                return schemas.get("위클리옵션시세_콜옵션", {})
            elif self.is_put_option(symbol_code):
                return schemas.get("위클리옵션시세_풋옵션", {})

        return schemas.get(api_name, {})

    def get_api_parameters(self, api_name: str) -> Dict[str, List[str]]:
        """
        API 파라미터 템플릿 조회

        Args:
            api_name (str): API 이름

        Returns:
            Dict[str, List[str]]: 필수/선택 파라미터 목록
        """
        parameters = self.config.get("api_parameters", {})
        return parameters.get(api_name, {"required": [], "optional": []})

    def is_call_option(self, symbol_code: str) -> bool:
        """콜옵션 여부 확인"""
        patterns = self.config.get("symbol_patterns", {}).get("options", {})
        call_patterns = [
            patterns.get("call_weekly", ""),
            patterns.get("call_monthly", ""),
        ]

        for pattern in call_patterns:
            if pattern and re.match(pattern, symbol_code):
                return True
        return False

    def is_put_option(self, symbol_code: str) -> bool:
        """풋옵션 여부 확인"""
        patterns = self.config.get("symbol_patterns", {}).get("options", {})
        put_patterns = [patterns.get("put_weekly", ""), patterns.get("put_monthly", "")]

        for pattern in put_patterns:
            if pattern and re.match(pattern, symbol_code):
                return True
        return False

    def get_symbol_type(self, symbol_code: str) -> str:
        """
        종목코드로 상품 유형 판별

        Args:
            symbol_code (str): 종목코드

        Returns:
            str: 상품 유형 (futures, call_option, put_option, unknown)
        """
        patterns = self.config.get("symbol_patterns", {})

        # 선물 확인
        for futures_type, pattern in patterns.get("futures", {}).items():
            if re.match(pattern, symbol_code):
                return f"futures_{futures_type}"

        # 옵션 확인
        for option_type, pattern in patterns.get("options", {}).items():
            if re.match(pattern, symbol_code):
                return option_type

        return "unknown"

    def get_market_code(self, symbol_type: str) -> str:
        """상품 유형으로 시장코드 조회"""
        market_codes = self.config.get("market_codes", {})

        # 연속 선물의 경우 특별 처리 - F 코드 사용
        if symbol_type == "futures_continuous":
            return "F"  # 연속 선물도 일반 선물과 동일한 F 코드 사용
        elif "futures" in symbol_type:
            return market_codes.get("선물", "F")
        elif "option" in symbol_type or "weekly" in symbol_type:
            return market_codes.get(
                "옵션", "O"
            )  # 위클리 옵션도 일반 옵션과 동일한 O 코드 사용
        else:
            return market_codes.get("혼합", "Z")

    def get_period_code(self, period_type: str) -> str:
        """기간 유형으로 기간코드 조회"""
        period_codes = self.config.get("period_codes", {})
        return period_codes.get(period_type, "D")

    def get_error_config(self) -> Dict[str, Any]:
        """오류 처리 설정 조회"""
        return self.config.get(
            "error_handling", {"max_retries": 3, "retry_delay": 1.0, "timeout": 30}
        )

    def get_logging_config(self) -> Dict[str, Any]:
        """로깅 설정 조회"""
        return self.config.get(
            "logging",
            {
                "level": "WARNING",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file_prefix": "data_collector",
            },
        )

    def build_api_params(
        self,
        api_name: str,
        symbol_code: str,
        start_date: str = None,
        end_date: str = None,
        **additional_params,
    ) -> Dict[str, str]:
        """
        API 파라미터 자동 구성

        Args:
            api_name (str): API 이름
            symbol_code (str): 종목코드
            start_date (str): 시작일 (YYYYMMDD)
            end_date (str): 종료일 (YYYYMMDD)
            **additional_params: 추가 파라미터

        Returns:
            Dict[str, str]: 구성된 API 파라미터
        """
        param_template = self.get_api_parameters(api_name)
        symbol_type = self.get_symbol_type(symbol_code)

        # 기본 파라미터 설정
        params = {"FID_INPUT_ISCD": symbol_code}

        # 날짜 파라미터 추가
        if start_date and "FID_INPUT_DATE_1" in param_template.get("required", []):
            params["FID_INPUT_DATE_1"] = start_date

        if end_date and "FID_INPUT_DATE_2" in param_template.get("required", []):
            params["FID_INPUT_DATE_2"] = end_date
        elif start_date and "FID_INPUT_DATE_2" in param_template.get("optional", []):
            params["FID_INPUT_DATE_2"] = start_date  # 분봉의 경우 단일 날짜

        # 시장/기간 코드 자동 설정
        if "FID_COND_MRKT_DIV_CODE" in param_template.get("optional", []):
            params["FID_COND_MRKT_DIV_CODE"] = self.get_market_code(symbol_type)

        if "FID_PERIOD_DIV_CODE" in param_template.get("optional", []):
            params["FID_PERIOD_DIV_CODE"] = self.get_period_code("일별")

        # 추가 파라미터 병합
        params.update(additional_params)

        return params

    def validate_symbol_code(self, symbol_code: str) -> bool:
        """종목코드 유효성 검증"""
        symbol_type = self.get_symbol_type(symbol_code)
        return symbol_type != "unknown"

    def get_date_column_for_symbol(
        self, api_name: str, symbol_code: str
    ) -> Optional[str]:
        """
        종목코드에 따른 날짜 컬럼명 조회

        Args:
            api_name (str): API 이름
            symbol_code (str): 종목코드

        Returns:
            Optional[str]: 날짜 컬럼명 (없으면 None)
        """
        schema = self.get_data_schema(api_name, symbol_code)
        return schema.get("date_column")

    def reload_config(self):
        """설정 파일 다시 로드"""
        self.config = self._load_config()
        logger.info("API 설정 다시 로드 완료")


# 전역 설정 관리자 인스턴스
_config_manager = None


def get_api_config() -> ApiConfigManager:
    """전역 API 설정 관리자 반환"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ApiConfigManager()
    return _config_manager
