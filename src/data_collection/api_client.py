import json
import logging
import requests
import time
import os
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import yaml  # yaml 패키지 추가

logger = logging.getLogger(__name__)


@dataclass
class RequestHeader:
    """API 요청 헤더"""

    content_type: Optional[str] = "application/json; charset=utf-8"  # 컨텐츠타입
    authorization: Optional[str] = None  # 접근토큰
    appkey: str = ""  # 앱키
    appsecret: str = ""  # 앱시크릿키
    tr_id: str = ""  # 거래ID
    custtype: str = "P"  # 고객타입 (개인: P, 법인: B)


class APIClient:
    """통합된 API 클라이언트 - 스키마 관리와 요청 처리 모두 담당"""

    # 토큰 관련 클래스 변수
    _last_auth_time: datetime = datetime.min
    _access_token: Optional[str] = None
    _token_expired_at: datetime = datetime.min
    _auth_in_progress: bool = False
    # 한국투자증권 API 정책에 따라 6시간 이내 재호출 시 기존 토큰 리턴
    _min_auth_interval: timedelta = timedelta(hours=6)

    # 토큰 파일 관련 경로
    _token_file_name: str = None

    def __init__(
        self,
        api_config: Dict[str, Any],
        schema_file_path: str = None,
    ):
        """
        통합 API 클라이언트 생성자

        Args:
            api_config: API 설정 정보 (URL, 키 등)
            schema_file_path: API 스키마 파일 경로
        """
        self.api_config = api_config
        # 외부 접근을 위해 app_key와 app_secret 속성을 명시적으로 설정
        self.app_key = api_config.get("APP_KEY", "")
        self.app_secret = api_config.get("APP_SECRET", "")
        self.base_url = ""
        self.is_paper_trading = False
        self.user_agent = api_config.get("MY_AGENT", "KISsushiClient")
        self.cust_type = api_config.get("CUS_TYPE", "P")

        # 환경 설정
        self._configure_env()

        # 토큰 파일 경로 설정
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
        token_dir = os.path.join(project_root, "tokens")
        os.makedirs(token_dir, exist_ok=True)

        # 토큰 파일명 설정 (YYYYMMDD가 아니라 유효기간 기준으로 설정)
        # 한투 API 정책에 따라 토큰 유효기간은 1일이므로 날짜를 파일명에 사용
        token_validity_days = 1  # API 상수에서 가져올 수 있음
        expiry_date = (datetime.today() + timedelta(days=token_validity_days)).strftime(
            "%Y%m%d"
        )
        APIClient._token_file_name = os.path.join(
            token_dir, f"KIS_TOKEN_{expiry_date}.yaml"
        )

        # 토큰 파일 존재 확인
        if not os.path.exists(APIClient._token_file_name):
            # 기존 토큰 파일 찾기 (tokens 디렉토리 내 모든 KIS_TOKEN 파일)
            existing_token_files = [
                os.path.join(token_dir, f)
                for f in os.listdir(token_dir)
                if f.startswith("KIS_TOKEN_") and f.endswith(".yaml")
            ]

            # 기존 파일이 있다면 가장 최근 파일 사용
            if existing_token_files:
                newest_token_file = max(existing_token_files, key=os.path.getctime)
                logger.info(f"Found existing token file: {newest_token_file}")
                APIClient._token_file_name = newest_token_file
            else:
                # 파일이 없으면 새로 생성
                with open(APIClient._token_file_name, "w", encoding="utf-8") as f:
                    f.write("# KIS API Token File\n")
                    yaml.dump({"created_at": datetime.now()}, f)

        # API 스키마 파일 경로 설정
        if schema_file_path is None:
            schema_file_path = os.path.join(
                project_root, "hantu_api_docs", "response_api.json"
            )

        # API 스키마 로드
        self.api_schema = self._load_api_schema(schema_file_path)

        # 초기 인증
        if not self.app_key or not self.app_secret:
            logger.warning(
                "API Key or Secret is not configured. Running in mock mode if applicable."
            )
        else:
            # 저장된 토큰 확인 먼저 시도
            saved_token = self._read_token_from_file()
            if (
                saved_token
                and saved_token.get("valid_date")
                and saved_token["valid_date"] > datetime.now()
            ):
                logger.info(
                    f"Using saved token from file. Valid until: {saved_token['valid_date']}"
                )
                APIClient._access_token = saved_token["token"]
                APIClient._token_expired_at = saved_token["valid_date"]
                APIClient._last_auth_time = saved_token.get(
                    "auth_time", datetime.now() - timedelta(hours=5)
                )
            else:
                # 저장된 토큰이 없거나 만료된 경우 새로 발급
                logger.info("No valid token found. Requesting new token.")
                self._auth()

    def _configure_env(self):
        """api_config에 따라 운영/모의투자 환경 설정"""
        svr = self.api_config.get("SVR", "prod").lower()
        if svr == "prod":
            self.app_key = self.api_config.get("APP_KEY", "")
            self.app_secret = self.api_config.get("APP_SECRET", "")
            self.base_url = self.api_config.get(
                "PROD_URL", "https://openapi.koreainvestment.com:9443"
            )
            self.is_paper_trading = False
        elif svr == "vps":  # 모의투자
            self.app_key = self.api_config.get("PAPER_APP_KEY", "")
            self.app_secret = self.api_config.get("PAPER_APP_SECRET", "")
            self.base_url = self.api_config.get(
                "VPS_URL", "https://openapivts.koreainvestment.com:29443"
            )
            self.is_paper_trading = True
        else:
            logger.error(f"Invalid SVR configuration: {svr}. Defaulting to prod.")
            self.app_key = self.api_config.get("APP_KEY", "")
            self.app_secret = self.api_config.get("APP_SECRET", "")
            self.base_url = self.api_config.get(
                "PROD_URL", "https://openapi.koreainvestment.com:9443"
            )
            self.is_paper_trading = False

        logger.info(
            f"Environment configured for: {'Paper Trading' if self.is_paper_trading else 'Production'}"
        )
        logger.info(f"Base URL: {self.base_url}")

    def _save_token_to_file(self, token: str, expired_at: datetime):
        """토큰을 파일에 저장

        Args:
            token: 액세스 토큰
            expired_at: 만료 시간
        """
        try:
            logger.info(f"Saving token to file: {APIClient._token_file_name}")
            with open(APIClient._token_file_name, "w", encoding="utf-8") as f:
                yaml.dump(
                    {
                        "token": token,
                        "valid_date": expired_at,
                        "auth_time": datetime.now(),
                    },
                    f,
                )
            logger.info(
                f"Token saved successfully. Valid until: {expired_at.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            return True
        except Exception as e:
            logger.error(f"Error saving token to file: {e}")
            return False

    def _read_token_from_file(self):
        """파일에서 토큰 읽기

        Returns:
            Dict: 토큰 정보 (없으면 None)
        """
        try:
            if not os.path.exists(APIClient._token_file_name):
                logger.info(f"Token file not found: {APIClient._token_file_name}")
                return None

            with open(APIClient._token_file_name, "r", encoding="utf-8") as f:
                token_data = yaml.safe_load(f)

            if not token_data:
                logger.info("Token file is empty or invalid")
                return None

            # 현재 시간과 만료 시간 비교
            now = datetime.now()
            if token_data.get("valid_date") and token_data["valid_date"] > now:
                logger.info(
                    f"Found valid token. Expires at: {token_data['valid_date'].strftime('%Y-%m-%d %H:%M:%S')}"
                )
                return token_data
            else:
                logger.info("Token has expired or is invalid")
                return None
        except Exception as e:
            logger.error(f"Error reading token from file: {e}")
            return None

    def _load_api_schema(self, schema_file_path: str) -> Dict[str, Any]:
        """API 스키마 파일 로드"""
        try:
            if not os.path.exists(schema_file_path):
                # API 스키마 파일이 없어도 fallback mapping으로 동작하므로 debug 레벨로 변경
                logger.debug(f"API 스키마 파일이 존재하지 않습니다: {schema_file_path}")
                return {}

            with open(schema_file_path, "r", encoding="utf-8") as f:
                schema = json.load(f)
                logger.info(f"API 스키마 파일 로드 성공: {schema_file_path}")
                return schema
        except Exception as e:
            logger.error(f"스키마 파일 로드 실패: {e}")
            return {}

    def get_api_by_name(self, api_name: str) -> Optional[Dict[str, Any]]:
        """API 이름으로 API 정보 검색"""
        if not self.api_schema or "data_products" not in self.api_schema:
            return None

        for product in self.api_schema.get("data_products", []):
            for api in product.get("api_references", []):
                if api.get("api_name") == api_name:
                    return api
        return None

    def get_api_endpoint(self, api_name: str) -> Optional[Dict[str, Any]]:
        """API 이름으로 엔드포인트 정보 검색"""
        api_info = self.get_api_by_name(api_name)
        if api_info and "endpoint" in api_info:
            return api_info.get("endpoint")
        return None

    def get_request_params(self, api_name: str) -> Tuple[List[Dict], List[Dict]]:
        """API 이름으로 요청 파라미터 정보 검색

        Args:
            api_name: API 이름

        Returns:
            tuple: (header_params, query_params) 튜플
        """
        api_info = self.get_api_by_name(api_name)
        if not api_info or "request" not in api_info:
            return [], []

        request = api_info.get("request", {})
        header_params = request.get("header_params", [])
        query_params = request.get("query_params", [])
        return header_params, query_params

    def get_param_info(self, api_name: str, param_name: str) -> Optional[Dict]:
        """API 이름과 파라미터 이름으로 파라미터 정보 조회

        Args:
            api_name: API 이름
            param_name: 파라미터 이름

        Returns:
            Optional[Dict]: 파라미터 정보 딕셔너리 또는 None
        """
        header_params, query_params = self.get_request_params(api_name)

        # 헤더 파라미터 검색
        for param in header_params:
            if param.get("param_name", "").upper() == param_name.upper():
                return param

        # 쿼리 파라미터 검색
        for param in query_params:
            if param.get("param_name", "").upper() == param_name.upper():
                return param

        return None

    def get_field_info(self, api_name: str, field_name: str) -> Optional[Dict]:
        """API 이름과 응답 필드 이름으로 필드 정보 조회

        Args:
            api_name: API 이름
            field_name: 필드 이름

        Returns:
            Optional[Dict]: 필드 정보 딕셔너리 또는 None
        """
        api_info = self.get_api_by_name(api_name)
        if not api_info or "response" not in api_info:
            return None

        response = api_info.get("response", {})

        # output1_fields 검색
        for field in response.get("output1_fields", []):
            if field.get("field_name", "").upper() == field_name.upper():
                return field

        # output2_fields 검색
        for field in response.get("output2_fields", []):
            if field.get("field_name", "").upper() == field_name.upper():
                return field

        # fields 검색
        for field in response.get("fields", []):
            if field.get("field_name", "").upper() == field_name.upper():
                return field

        return None

    def get_all_api_names(self) -> List[str]:
        """모든 API 이름 목록 조회

        Returns:
            List[str]: API 이름 목록
        """
        api_names = []
        if not self.api_schema or "data_products" not in self.api_schema:
            return api_names

        for product in self.api_schema.get("data_products", []):
            for api in product.get("api_references", []):
                if "api_name" in api:
                    api_names.append(api["api_name"])

        return api_names

    def _auth(self) -> bool:
        """인증 토큰 발급 (한투 API 정책 준수)

        한국투자증권 API 정책:
        1. 접근토큰 유효기간: 24시간 (1일 1회 발급 원칙)
        2. 갱신발급주기: 6시간 (6시간 이내 재호출 시 기존 토큰 반환)
        """
        # 이미 인증 진행 중이면 대기
        if APIClient._auth_in_progress:
            logger.info("Authentication already in progress. Waiting...")
            # 대기 로직 - 실제로는 더 정교한 방식 필요
            for _ in range(30):  # 최대 3초 대기
                time.sleep(0.1)
                if not APIClient._auth_in_progress:
                    if APIClient._access_token:
                        logger.info("Using token from another thread.")
                        return True
                    break
            if APIClient._auth_in_progress:
                logger.warning("Authentication wait timeout.")
                return False

        # 먼저 파일에서 유효한 토큰이 있는지 확인
        token_data = self._read_token_from_file()
        if token_data:
            now = datetime.now()
            # 토큰이 아직 유효한지 확인
            if token_data.get("valid_date") and token_data["valid_date"] > now:
                # 마지막 인증 시도 후 6시간이 지났는지 확인 (한투 API 갱신발급주기)
                time_since_last_auth = now - token_data.get(
                    "auth_time", now - timedelta(hours=7)
                )

                # 6시간 이내 재발급 시도면 기존 토큰 사용
                if time_since_last_auth < APIClient._min_auth_interval:
                    logger.info(
                        "Using existing token from file (reuse period is still active)"
                    )
                    APIClient._access_token = token_data["token"]
                    APIClient._token_expired_at = token_data["valid_date"]
                    APIClient._last_auth_time = token_data.get(
                        "auth_time", now - timedelta(hours=5)
                    )
                    return True
                else:
                    logger.info(
                        "Token reuse period has passed, but token is still valid. Using it."
                    )
                    APIClient._access_token = token_data["token"]
                    APIClient._token_expired_at = token_data["valid_date"]
                    APIClient._last_auth_time = (
                        datetime.now()
                    )  # 마지막 인증 시간 업데이트
                    # 토큰 파일 업데이트 (auth_time만 업데이트)
                    token_data["auth_time"] = APIClient._last_auth_time
                    with open(APIClient._token_file_name, "w", encoding="utf-8") as f:
                        yaml.dump(token_data, f)
                    return True

        # 기존 토큰이 있고 유효한지 확인
        now = datetime.now()
        if APIClient._access_token and APIClient._token_expired_at > now:
            # 마지막 인증 시도 후 6시간이 지났는지 확인 (한투 API 갱신발급주기)
            time_since_last_auth = now - APIClient._last_auth_time
            if time_since_last_auth < APIClient._min_auth_interval:
                logger.info(
                    "Using existing token in memory (reuse period is still active)"
                )
                return True
            else:
                logger.info(
                    "Token reuse period has passed, but token is still valid. Using it."
                )
                APIClient._last_auth_time = now  # 마지막 인증 시간 업데이트
                # 토큰 파일 업데이트 (auth_time만 업데이트)
                self._save_token_to_file(
                    APIClient._access_token, APIClient._token_expired_at
                )
                return True

        # 인증 진행 중 표시
        APIClient._auth_in_progress = True

        if not self.app_key or not self.app_secret:
            logger.warning("Skipping authentication: API Key or Secret not provided.")
            APIClient._auth_in_progress = False
            return False

        # 토큰 발급 URL (개인 고객용) - 한투 가이드 참고
        auth_url = f"{self.base_url}/oauth2/tokenP"

        headers = {
            "content-type": "application/json; charset=utf-8",
            "User-Agent": self.user_agent,
        }
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        try:
            logger.info(f"Requesting access token from {auth_url}")
            response = requests.post(
                auth_url, headers=headers, data=json.dumps(body), timeout=5
            )
            response_data = response.json()

            # 응답 기록 (마지막 인증 시도 시각)
            APIClient._last_auth_time = datetime.now()

            if response.status_code == 200 and "access_token" in response_data:
                APIClient._access_token = response_data["access_token"]

                # expires_in이 초 단위로 제공됨 (일반적으로 86400초 = 24시간)
                expires_in = int(response_data.get("expires_in", 86400))

                # API에서 제공하는 만료 일시가 있으면 사용
                if "access_token_token_expired" in response_data:
                    try:
                        expires_at_str = response_data["access_token_token_expired"]
                        APIClient._token_expired_at = datetime.strptime(
                            expires_at_str, "%Y-%m-%d %H:%M:%S"
                        )
                        logger.info(f"Token will expire at: {expires_at_str}")
                    except ValueError:
                        logger.warning(
                            f"Could not parse expiration time: {response_data.get('access_token_token_expired')}"
                        )
                        # 파싱 실패 시 기본값 사용
                        APIClient._token_expired_at = datetime.now() + timedelta(
                            seconds=expires_in
                        )
                else:
                    # 만료 시간 정보가 없으면 기본값 사용
                    APIClient._token_expired_at = datetime.now() + timedelta(
                        seconds=expires_in
                    )

                # 토큰 파일에 저장
                self._save_token_to_file(
                    APIClient._access_token, APIClient._token_expired_at
                )

                logger.info("Access token issued successfully.")
                APIClient._auth_in_progress = False
                return True
            else:
                # 인증 실패 시 에러 로깅
                logger.error(
                    f"Failed to issue access token: {response.status_code} - {response_data}"
                )
                # 만약 '잠시 후 다시 시도' 관련 에러면 특별히 처리
                if (
                    "error_code" in response_data
                    and response_data.get("error_code") == "EGW00133"
                    and APIClient._access_token
                ):
                    logger.info(
                        "Rate limit error but we have an existing token. Using it."
                    )
                    APIClient._auth_in_progress = False
                    return True

                # 그 외의 경우는 토큰 없음으로 처리
                APIClient._access_token = None
                APIClient._auth_in_progress = False
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Error issuing access token: {str(e)}")
            APIClient._auth_in_progress = False
            return False

    def get_access_token(self) -> Optional[str]:
        """유효한 액세스 토큰 반환 (필요 시 갱신)"""
        # 토큰이 있고 만료되지 않았으면 기존 토큰 사용
        now = datetime.now()
        if APIClient._access_token and APIClient._token_expired_at > now:
            time_left = (APIClient._token_expired_at - now).total_seconds() / 60
            logger.debug(
                f"Using existing valid token (expires in {time_left:.1f} minutes)"
            )
            return APIClient._access_token

        # 토큰이 없거나 만료된 경우 인증 시도
        logger.info("No valid access token available. Authenticating...")
        if self._auth():
            return APIClient._access_token
        else:
            logger.error("Failed to get access token")
            return None

    def _prepare_base_headers(self) -> Dict[str, str]:
        """API 요청을 위한 기본 헤더 구성"""
        token = self.get_access_token()
        if not token:
            # 토큰이 없으면 API 호출 불가 (테스트 모드가 아닌 이상)
            raise ValueError("Access token is not available.")

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "custtype": self.cust_type,
            "User-Agent": self.user_agent,
        }
        return headers

    def _set_order_hash_key(
        self, headers: Dict[str, str], payload: Dict[str, Any]
    ) -> bool:
        """주문 API용 해시키 발급 및 헤더 설정"""
        if (
            not self.base_url
        ):  # base_url이 설정되지 않았다면 (예: 키 부재) 해시키 발급 불가
            logger.warning("Base URL not set, skipping hash key generation.")
            return False

        hash_url = f"{self.base_url}/uapi/hashkey"
        try:
            # 해시키 발급 요청 시 헤더는 기본 헤더에 appkey, appsecret, content-type만 포함
            hash_req_headers = {
                "Content-Type": "application/json; charset=utf-8",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "User-Agent": self.user_agent,
            }
            logger.debug(f"Requesting hashkey from {hash_url} with payload: {payload}")
            response = requests.post(
                hash_url, headers=hash_req_headers, data=json.dumps(payload), timeout=5
            )

            if response.status_code == 200:
                hash_data = response.json()
                if "HASH" in hash_data:
                    headers["hashkey"] = hash_data["HASH"]
                    logger.info("Order hashkey generated and set successfully.")
                    return True
                else:
                    logger.error(
                        f"Failed to get HASH from hashkey API response: {hash_data}"
                    )
                    return False
            else:
                logger.error(
                    f"Failed to generate order hashkey: {response.status_code} - {response.text}"
                )
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Error generating order hashkey: {str(e)}")
            return False

    def request(
        self,
        method: str,
        api_name: str,
        tr_id: str = None,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        is_order_api: bool = False,
        retry_count: int = 1,  # API 호출 재시도 횟수 (토큰 만료 시 1회 자동 재시도)
    ) -> Dict[str, Any]:
        """
        API 요청 수행

        Args:
            method (str): HTTP 메소드 ("GET" 또는 "POST").
            api_name (str): API 이름 (response_api.json에 정의된 이름).
            tr_id (str): 거래 ID.
            params (Optional[Dict]): GET 요청 시 URL 파라미터.
            body (Optional[Dict]): POST 요청 시 body 데이터.
            headers (Optional[Dict]): 요청 헤더 (기본 헤더 대체 또는 추가).
            is_order_api (bool): 주문 API 여부 (해시키 생성 필요).
            retry_count (int): 실패 시 재시도 횟수.

        Returns:
            Dict[str, Any]: API 응답 (JSON 파싱된 딕셔너리).
        """

        # API 키 검증 및 디버깅
        if not self.app_key or self.app_key.strip() == "":
            logger.error("API Key is missing or empty. Check api_config.yaml.")
            logger.warning(f"Mock API Call (No API Keys): {api_name}, TR_ID: {tr_id}")
            return {
                "rt_cd": "0",
                "msg1": f"Mock Success: {api_name}",
                "output": {},
                "output1": [],
                "output2": [],
                "_debug_info": "API Key is missing or empty",
            }

        if not self.app_secret or self.app_secret.strip() == "":
            logger.error("API Secret is missing or empty. Check api_config.yaml.")
            logger.warning(f"Mock API Call (No API Secret): {api_name}, TR_ID: {tr_id}")
            return {
                "rt_cd": "0",
                "msg1": f"Mock Success: {api_name}",
                "output": {},
                "output1": [],
                "output2": [],
                "_debug_info": "API Secret is missing or empty",
            }

        # API 엔드포인트 정보 가져오기
        api_endpoint = self.get_api_endpoint(api_name)
        if api_endpoint and "url_path" in api_endpoint:
            api_path = api_endpoint["url_path"]
        else:
            # API endpoint가 없어도 fallback mapping으로 동작하므로 debug 레벨로 변경
            logger.debug(
                f"API endpoint not found for: {api_name}. Using fallback mapping."
            )
            # 선물옵션 API를 위한 폴백 매핑 (구체적인 조건부터 체크)
            if "선물옵션 분봉" in api_name:
                api_path = "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice"
            elif "선물옵션기간별시세" in api_name or "선물옵션" in api_name:
                api_path = "/uapi/domestic-futureoption/v1/quotations/inquire-daily-fuopchartprice"
            elif "투자자시간대별매매동향" in api_name:
                api_path = (
                    "/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market"
                )
            else:
                api_path = "/uapi/domestic-stock/v1/quotations/inquire-price"  # 기본값

        url = f"{self.base_url}{api_path}"

        # tr_id가 없으면 API 엔드포인트에서 가져오기
        effective_tr_id = tr_id
        if not effective_tr_id and api_endpoint:
            effective_tr_id = api_endpoint.get("production_tr_id", "")
            if effective_tr_id:
                logger.info(f"Using TR_ID from schema: {effective_tr_id}")

        # TR_ID 모의투자용으로 변경
        if (
            self.is_paper_trading
            and effective_tr_id
            and effective_tr_id[0] in ("T", "J", "C", "F", "H")
        ):  # 국내주식, 해외주식, 파생 등
            effective_tr_id = "V" + effective_tr_id[1:]
            logger.info(
                f"Paper trading: TR_ID changed from {tr_id} to {effective_tr_id}"
            )

        # 헤더 설정
        if headers:
            # 사용자 제공 헤더가 있으면 사용
            req_headers = headers
        else:
            # 기본 헤더 생성
            req_headers = self._prepare_base_headers()
            # TR ID 설정
            if effective_tr_id:
                req_headers["tr_id"] = effective_tr_id
            # TR ID별 특수 헤더 추가
            specific_headers = self.api_config.get("tr_id_specific_headers", {}).get(
                effective_tr_id, {}
            )
            req_headers.update(specific_headers)

        # 주문 API인 경우 해시키 생성
        if is_order_api and body:
            self._set_order_hash_key(req_headers, body)

        current_retry = 0
        while current_retry <= retry_count:
            try:
                logger.info(f"Requesting {method} {url} (TR: {effective_tr_id})")
                logger.debug(f"Headers: {json.dumps(req_headers, indent=2)}")
                if params:
                    logger.debug(f"Params: {json.dumps(params, indent=2)}")
                if body:
                    logger.debug(f"Body: {json.dumps(body, indent=2)}")

                # API 키와 시크릿 키가 헤더에 제대로 들어갔는지 확인 (민감 정보 일부 마스킹)
                if "appkey" in req_headers:
                    key_value = req_headers["appkey"]
                    logger.info(
                        f"Using API Key: {key_value[:4]}...{key_value[-4:] if len(key_value) > 8 else ''}"
                    )
                else:
                    logger.error("API Key is missing from request headers!")

                if method.upper() == "GET":
                    response = requests.get(
                        url, headers=req_headers, params=params, timeout=10
                    )
                elif method.upper() == "POST":
                    response = requests.post(
                        url, headers=req_headers, json=body, timeout=10
                    )
                else:
                    logger.error(f"Unsupported HTTP method: {method}")
                    return {"rt_cd": "99", "msg1": f"Unsupported HTTP method: {method}"}

                logger.debug(f"Response Status: {response.status_code}")
                logger.debug(
                    f"Response Headers: {json.dumps(dict(response.headers), indent=2)}"
                )
                try:
                    response_data = response.json()
                    # 더 자세한 응답 디버깅
                    logger.info(
                        f"API Response Code: {response_data.get('rt_cd', 'N/A')}, Message: {response_data.get('msg1', 'N/A')}"
                    )

                    # 응답에 데이터가 있는지 확인하고 로그
                    has_output1 = (
                        "output1" in response_data and response_data["output1"]
                    )
                    has_output2 = (
                        "output2" in response_data and response_data["output2"]
                    )
                    has_output = "output" in response_data and response_data["output"]

                    if not (has_output1 or has_output2 or has_output):
                        # 데이터가 없는 경우는 정상적인 상황일 수 있으므로 debug 레벨로 변경
                        logger.debug(
                            f"API Response has no data in output fields for {api_name}!"
                        )

                    # 디버깅을 위해 전체 응답 로깅
                    logger.debug(
                        f"Response Body: {json.dumps(response_data, indent=2, ensure_ascii=False)}"
                    )
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON response: {response.text}")
                    return {
                        "rt_cd": "99",
                        "msg1": "JSON decode error",
                        "raw_response": response.text,
                    }

                # 토큰 만료 (HTTP 401) 또는 특정 에러 코드 시 재인증 후 재시도
                # KIS는 토큰 만료시 401 Unauthorized 와 함께 EGW00121, EGW00123 등의 에러 코드를 반환할 수 있음
                is_token_expired_error = (response.status_code == 401) or (
                    response_data.get("rt_cd") == "1"
                    and response_data.get("msg_cd") in ["EGW00121", "EGW00123"]
                )

                if is_token_expired_error and current_retry < retry_count:
                    logger.warning(
                        "Token expired or invalid. Attempting re-authentication and retry."
                    )
                    APIClient._access_token = None  # 기존 토큰 무효화
                    if self._auth():  # 재인증 성공 시
                        req_headers = (
                            self._prepare_base_headers()
                        )  # 헤더 다시 준비 (새 토큰으로)
                        if effective_tr_id:
                            req_headers["tr_id"] = effective_tr_id  # tr_id는 유지
                        if is_order_api and body:  # 주문 API면 해시키도 다시 (필요시)
                            self._set_order_hash_key(req_headers, body)
                        current_retry += 1
                        logger.info(
                            f"Retrying API request (attempt {current_retry}/{retry_count})"
                        )
                        time.sleep(0.1)  # 짧은 대기 후 재시도
                        continue
                    else:
                        logger.error(
                            "Re-authentication failed. Cannot retry API request."
                        )
                        return response_data  # 재인증 실패 시 현재 응답 반환

                return response_data

            except requests.exceptions.Timeout:
                logger.error(
                    f"API request timeout for {api_name} (attempt {current_retry+1})"
                )
                if current_retry < retry_count:
                    current_retry += 1
                    time.sleep(1)  # 타임아웃 시 잠시 대기 후 재시도
                    continue
                return {"rt_cd": "98", "msg1": "API request timeout"}
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"API request failed for {api_name} (attempt {current_retry+1}): {str(e)}"
                )
                # RequestException의 경우 재시도하지 않고 바로 반환할 수 있음, 혹은 특정 조건에서만 재시도
                return {"rt_cd": "99", "msg1": f"API request failed: {str(e)}"}

        logger.error(
            f"API request failed for {api_name} after {retry_count+1} attempts."
        )
        return {"rt_cd": "97", "msg1": "Max retries reached or unrecoverable error."}
