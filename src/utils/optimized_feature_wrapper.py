"""
Phase 3: 기존 피처 클래스를 위한 API 최적화 래퍼

이 모듈은 기존 피처 엔지니어링 클래스들이 새로운 API 최적화 기능을
투명하게 사용할 수 있도록 래핑합니다.
"""

import logging
from typing import Dict, List, Any, Optional, Type
from datetime import datetime, timedelta
import pandas as pd
from .api_optimizer import APIOptimizer, APIRequest, APIResponse

logger = logging.getLogger(__name__)


class OptimizedFeatureWrapper:
    """피처 클래스를 위한 API 최적화 래퍼"""

    def __init__(self, feature_instance, api_optimizer: APIOptimizer):
        """
        Args:
            feature_instance: 기존 피처 클래스 인스턴스
            api_optimizer: API 최적화 관리자
        """
        self.feature = feature_instance
        self.optimizer = api_optimizer
        self._original_api_client = None

        # 기존 API 클라이언트를 최적화된 버전으로 교체
        self._wrap_api_client()

        logger.info(
            f"🔧 Optimized wrapper applied to {type(feature_instance).__name__}"
        )

    def _wrap_api_client(self):
        """기존 API 클라이언트를 최적화된 버전으로 래핑"""
        if hasattr(self.feature, "api_client"):
            self._original_api_client = self.feature.api_client

            # API 클라이언트의 request 메소드를 최적화된 버전으로 교체
            original_request = self._original_api_client.request

            def optimized_request_wrapper(method: str, api_name: str, **kwargs):
                """최적화된 API 요청 래퍼"""
                try:
                    # API 최적화기를 통해 요청 수행
                    response = self.optimizer.optimized_request(
                        api_name=api_name,
                        method=method,
                        tr_id=kwargs.get("tr_id"),
                        params=kwargs.get("params"),
                        body=kwargs.get("body"),
                        headers=kwargs.get("headers"),
                    )

                    return response.data

                except Exception as e:
                    logger.warning(
                        f"Optimized request failed, falling back to original: {e}"
                    )
                    # 최적화 실패 시 원본 메소드로 폴백
                    return original_request(method, api_name, **kwargs)

            # 메소드 교체
            self.feature.api_client.request = optimized_request_wrapper

    def get_data(self, **kwargs) -> pd.DataFrame:
        """최적화된 데이터 수집"""
        try:
            return self.feature.get_data(**kwargs)
        except Exception as e:
            logger.error(f"Optimized data collection failed: {e}")
            raise

    def save_to_csv(self, **kwargs) -> bool:
        """최적화된 CSV 저장"""
        try:
            return self.feature.save_to_csv(**kwargs)
        except Exception as e:
            logger.error(f"Optimized CSV save failed: {e}")
            raise

    def get_performance_stats(self) -> Dict[str, Any]:
        """성능 통계 조회"""
        return self.optimizer.get_performance_report()

    def __getattr__(self, name):
        """기존 피처 클래스의 모든 메소드를 투명하게 전달"""
        return getattr(self.feature, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 원본 API 클라이언트 복원
        if self._original_api_client and hasattr(self.feature, "api_client"):
            self.feature.api_client = self._original_api_client


class OptimizedDateRangeCollector:
    """날짜 범위 기반 최적화된 데이터 수집기"""

    def __init__(self, api_optimizer: APIOptimizer):
        self.optimizer = api_optimizer

    def collect_futures_data_optimized(
        self,
        codes: List[str],
        start_date: datetime,
        end_date: datetime,
        api_name: str = "선물옵션기간별시세(일/주/월/년) [v1_국내선물-008]",
        tr_id: str = "FHKIF03020100",
    ) -> Dict[str, pd.DataFrame]:
        """
        선물 데이터를 최적화된 방식으로 수집

        Args:
            codes: 선물 코드 리스트
            start_date: 시작 날짜
            end_date: 종료 날짜
            api_name: API 이름
            tr_id: 거래 ID

        Returns:
            Dict[str, pd.DataFrame]: 코드별 데이터프레임
        """
        results = {}
        total_api_calls = 0

        logger.info(
            f"📊 Starting optimized futures data collection for {len(codes)} codes"
        )

        for code in codes:
            logger.info(f"🔍 Processing {code}...")

            # 기본 파라미터 설정
            base_params = {
                "FID_COND_MRKT_DIV_CODE": "F",
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": "",  # 시작일 (동적 설정)
                "FID_INPUT_DATE_2": "",  # 종료일 (동적 설정)
                "FID_PERIOD_DIV_CODE": "D",
            }

            try:
                # 최적화된 날짜 범위 요청
                responses = self.optimizer.optimize_date_range_requests(
                    api_name=api_name,
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    base_params=base_params,
                    date_param_name="FID_INPUT_DATE_1",
                    end_date_param_name="FID_INPUT_DATE_2",
                    tr_id=tr_id,
                )

                total_api_calls += len(responses)

                # 응답 데이터 병합
                all_data = []
                for response in responses:
                    if response.data.get("rt_cd") == "0":
                        output_data = response.data.get("output2", [])
                        if output_data:
                            all_data.extend(output_data)
                    else:
                        logger.warning(
                            f"API error for {code}: {response.data.get('msg1', 'Unknown error')}"
                        )

                if all_data:
                    # 데이터프레임 생성 및 정리
                    df = pd.DataFrame(all_data)
                    df = self._process_futures_dataframe(df)
                    results[code] = df

                    logger.info(f"✅ {code}: {len(df)} records collected")
                else:
                    logger.warning(f"⚠️ No data collected for {code}")

            except Exception as e:
                logger.error(f"❌ Failed to collect data for {code}: {e}")
                continue

        logger.info(
            f"🎯 Collection completed: {len(results)} codes, {total_api_calls} API calls"
        )
        return results

    def _process_futures_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """선물 데이터프레임 처리"""
        if df.empty:
            return df

        try:
            # 날짜 컬럼 처리
            date_columns = ["stck_bsop_date", "bsop_date", "date"]
            date_col = None

            for col in date_columns:
                if col in df.columns:
                    date_col = col
                    break

            if date_col:
                df[date_col] = pd.to_datetime(
                    df[date_col], format="%Y%m%d", errors="coerce"
                )
                df = df.sort_values(date_col)
                df = df.drop_duplicates()

            # 숫자형 컬럼 변환
            numeric_columns = [
                "futs_prpr",
                "futs_oprc",
                "futs_hgpr",
                "futs_lwpr",
                "futs_clpr",
                "acml_vol",
                "acml_tr_pbmn",
                "futs_prdy_vrss",
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

        except Exception as e:
            logger.error(f"DataFrame processing error: {e}")
            return df


class OptimizedBatchCollector:
    """배치 최적화된 데이터 수집기"""

    def __init__(self, api_optimizer: APIOptimizer):
        self.optimizer = api_optimizer

    def collect_multiple_features_optimized(
        self, feature_configs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        여러 피처를 배치로 최적화 수집

        Args:
            feature_configs: 피처 설정 리스트
                [
                    {
                        'feature_name': 'kospi200_futures_daily',
                        'api_name': '...',
                        'codes': ['101V06', '101V09'],
                        'params': {...}
                    },
                    ...
                ]

        Returns:
            Dict[str, Any]: 피처별 수집 결과
        """
        all_requests = []
        request_mapping = {}

        # 모든 요청 생성
        for config in feature_configs:
            feature_name = config["feature_name"]
            api_name = config["api_name"]
            codes = config.get("codes", [])
            base_params = config.get("params", {})

            for code in codes:
                params = base_params.copy()
                params.update(config.get("code_specific_params", {}).get(code, {}))

                request = APIRequest(
                    api_name=api_name, params=params, priority=config.get("priority", 5)
                )

                all_requests.append(request)
                request_mapping[id(request)] = {
                    "feature_name": feature_name,
                    "code": code,
                    "config": config,
                }

        logger.info(f"🚀 Starting batch collection: {len(all_requests)} requests")

        # 배치 실행
        responses = self.optimizer.batch_request(all_requests)

        # 결과 정리
        results = {}
        for response in responses:
            mapping_info = request_mapping.get(id(response.request))
            if not mapping_info:
                continue

            feature_name = mapping_info["feature_name"]
            code = mapping_info["code"]

            if feature_name not in results:
                results[feature_name] = {}

            if response.data.get("rt_cd") == "0":
                results[feature_name][code] = {
                    "data": response.data.get("output2", []),
                    "success": True,
                    "response_time": response.response_time,
                    "cached": response.cached,
                }
            else:
                results[feature_name][code] = {
                    "data": [],
                    "success": False,
                    "error": response.data.get("msg1", "Unknown error"),
                    "response_time": response.response_time,
                }

        logger.info(f"✅ Batch collection completed: {len(results)} features")
        return results


def apply_optimization_to_features(
    feature_instances: List[Any],
    api_client,
    optimization_config: Optional[Dict[str, Any]] = None,
) -> List[OptimizedFeatureWrapper]:
    """
    여러 피처 인스턴스에 최적화 적용

    Args:
        feature_instances: 피처 인스턴스 리스트
        api_client: API 클라이언트
        optimization_config: 최적화 설정

    Returns:
        List[OptimizedFeatureWrapper]: 최적화된 피처 래퍼 리스트
    """
    config = optimization_config or {}

    # API 최적화기 생성
    optimizer = APIOptimizer(
        api_client=api_client,
        max_requests_per_minute=config.get("max_requests_per_minute", 60),
        cache_ttl_seconds=config.get("cache_ttl_seconds", 300),
        max_workers=config.get("max_workers", 3),
    )

    # 피처들에 최적화 적용
    optimized_features = []
    for feature_instance in feature_instances:
        optimized_wrapper = OptimizedFeatureWrapper(feature_instance, optimizer)
        optimized_features.append(optimized_wrapper)

    logger.info(f"🎯 Applied optimization to {len(optimized_features)} features")
    return optimized_features
