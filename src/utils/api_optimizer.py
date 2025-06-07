"""
Phase 3: API 최적화 모듈

이 모듈은 다음 기능들을 제공합니다:
1. 지능형 요청 배치 처리
2. 적응형 속도 제한 및 재시도 메커니즘
3. API 응답 캐싱 시스템
4. 동적 날짜 범위 분할
5. 요청 성능 모니터링
"""

import asyncio
import time
import logging
import hashlib
import json
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import os

logger = logging.getLogger(__name__)


@dataclass
class APIRequest:
    """API 요청 정보를 담는 클래스"""

    api_name: str
    method: str = "GET"
    tr_id: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    body: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None
    priority: int = 5  # 1(최고) ~ 10(최저)
    timeout: int = 30
    retry_count: int = 3
    cache_key: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class APIResponse:
    """API 응답 정보를 담는 클래스"""

    request: APIRequest
    data: Dict[str, Any]
    status_code: int
    response_time: float
    cached: bool = False
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class RateLimiter:
    """적응형 속도 제한기"""

    def __init__(self, max_requests: int = 100, per_seconds: int = 60):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.requests = deque()
        self.lock = threading.Lock()
        self._adaptive_delay = 0.0
        self._last_error_time = None
        self._consecutive_errors = 0

    def acquire(self) -> float:
        """요청 허가를 얻고 대기 시간을 반환"""
        with self.lock:
            now = time.time()

            # 오래된 요청 기록 제거
            while self.requests and self.requests[0] <= now - self.per_seconds:
                self.requests.popleft()

            # 요청 한도 확인
            if len(self.requests) >= self.max_requests:
                wait_time = (
                    self.per_seconds - (now - self.requests[0]) + self._adaptive_delay
                )
                if wait_time > 0:
                    logger.info(f"Rate limit reached. Waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                    now = time.time()

            # 적응형 지연 적용
            if self._adaptive_delay > 0:
                time.sleep(self._adaptive_delay)

            self.requests.append(now)
            return self._adaptive_delay

    def report_error(self):
        """API 오류 보고 - 적응형 지연 증가"""
        with self.lock:
            self._consecutive_errors += 1
            self._last_error_time = time.time()

            # 연속 오류에 따른 지연 증가 (최대 10초)
            self._adaptive_delay = min(self._consecutive_errors * 0.5, 10.0)
            logger.warning(
                f"API error reported. Adaptive delay: {self._adaptive_delay:.2f}s"
            )

    def report_success(self):
        """API 성공 보고 - 적응형 지연 감소"""
        with self.lock:
            if self._consecutive_errors > 0:
                self._consecutive_errors = max(0, self._consecutive_errors - 1)
                self._adaptive_delay = max(0, self._adaptive_delay - 0.1)


class CircuitBreaker:
    """서킷 브레이커 패턴 구현"""

    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.lock = threading.Lock()

    def call(self, func: Callable, *args, **kwargs):
        """서킷 브레이커를 통한 함수 호출"""
        with self.lock:
            if self.state == "OPEN":
                if self._should_attempt_reset():
                    self.state = "HALF_OPEN"
                    logger.info("Circuit breaker: Attempting reset (HALF_OPEN)")
                else:
                    raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _should_attempt_reset(self) -> bool:
        """재시도 시도 여부 확인"""
        return (
            self.last_failure_time
            and time.time() - self.last_failure_time >= self.timeout
        )

    def _on_success(self):
        """성공 시 처리"""
        with self.lock:
            self.failure_count = 0
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                logger.info("Circuit breaker: Reset to CLOSED")

    def _on_failure(self):
        """실패 시 처리"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.warning(
                    f"Circuit breaker: OPEN (failures: {self.failure_count})"
                )


class ResponseCache:
    """API 응답 캐싱 시스템"""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache = {}
        self.access_times = {}
        self.lock = threading.Lock()

        # 캐시 파일 경로 설정
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
        cache_dir = os.path.join(project_root, "cache", "api_responses")
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_file = os.path.join(cache_dir, "response_cache.pkl")

        # 캐시 로드
        self._load_cache()

    def _generate_cache_key(self, request: APIRequest) -> str:
        """요청 정보로부터 캐시 키 생성"""
        key_data = {
            "api_name": request.api_name,
            "method": request.method,
            "tr_id": request.tr_id,
            "params": request.params,
            "body": request.body,
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, request: APIRequest) -> Optional[APIResponse]:
        """캐시에서 응답 조회"""
        cache_key = request.cache_key or self._generate_cache_key(request)

        with self.lock:
            if cache_key in self.cache:
                cached_response, cached_time = self.cache[cache_key]

                # TTL 확인
                if time.time() - cached_time <= self.ttl_seconds:
                    self.access_times[cache_key] = time.time()
                    cached_response.cached = True
                    logger.debug(f"Cache hit for key: {cache_key[:8]}...")
                    return cached_response
                else:
                    # 만료된 캐시 제거
                    del self.cache[cache_key]
                    if cache_key in self.access_times:
                        del self.access_times[cache_key]

        return None

    def put(self, request: APIRequest, response: APIResponse):
        """응답을 캐시에 저장"""
        cache_key = request.cache_key or self._generate_cache_key(request)

        with self.lock:
            # 캐시 크기 제한
            if len(self.cache) >= self.max_size:
                self._evict_oldest()

            self.cache[cache_key] = (response, time.time())
            self.access_times[cache_key] = time.time()
            logger.debug(f"Cached response for key: {cache_key[:8]}...")

            # 주기적으로 캐시 저장
            if len(self.cache) % 10 == 0:
                self._save_cache()

    def _evict_oldest(self):
        """가장 오래된 캐시 항목 제거 (LRU)"""
        if not self.access_times:
            return

        oldest_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])

        if oldest_key in self.cache:
            del self.cache[oldest_key]
        del self.access_times[oldest_key]

    def _save_cache(self):
        """캐시를 파일에 저장"""
        try:
            with open(self.cache_file, "wb") as f:
                pickle.dump({"cache": self.cache, "access_times": self.access_times}, f)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")

    def _load_cache(self):
        """파일에서 캐시 로드"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "rb") as f:
                    data = pickle.load(f)
                    self.cache = data.get("cache", {})
                    self.access_times = data.get("access_times", {})
                logger.info(f"Loaded {len(self.cache)} cached responses")
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
            self.cache = {}
            self.access_times = {}

    def clear(self):
        """캐시 초기화"""
        with self.lock:
            self.cache.clear()
            self.access_times.clear()
        try:
            if os.path.exists(self.cache_file):
                os.remove(self.cache_file)
        except Exception as e:
            logger.error(f"Failed to remove cache file: {e}")


class DateRangeSplitter:
    """동적 날짜 범위 분할기"""

    def __init__(self):
        self.code_statistics = defaultdict(dict)
        self._load_statistics()

    def calculate_optimal_splits(
        self,
        start_date: datetime,
        end_date: datetime,
        code: str,
        data_type: str = "daily",
    ) -> List[Tuple[datetime, datetime]]:
        """코드와 데이터 타입에 따른 최적 분할 계산"""

        total_days = (end_date - start_date).days

        # 코드별 통계 기반 분할 크기 결정
        avg_records_per_day = self._get_avg_records_per_day(code, data_type)

        if avg_records_per_day > 50:  # 고빈도 데이터
            split_days = 30  # 1개월 단위
        elif avg_records_per_day > 10:  # 중빈도 데이터
            split_days = 90  # 3개월 단위
        else:  # 저빈도 데이터
            split_days = 180  # 6개월 단위

        # 최대 분할 수 제한 (API 호출 최소화)
        max_splits = 10
        if total_days // split_days > max_splits:
            split_days = total_days // max_splits

        # 날짜 범위 분할
        splits = []
        current_date = start_date

        while current_date < end_date:
            split_end = min(current_date + timedelta(days=split_days), end_date)
            splits.append((current_date, split_end))
            current_date = split_end + timedelta(days=1)

        logger.info(
            f"Date range split for {code}: {len(splits)} segments "
            f"(avg {avg_records_per_day:.1f} records/day)"
        )

        return splits

    def _get_avg_records_per_day(self, code: str, data_type: str) -> float:
        """코드별 평균 일일 레코드 수 조회"""
        stats = self.code_statistics.get(code, {}).get(data_type, {})
        return stats.get("avg_records_per_day", 1.0)

    def update_statistics(
        self, code: str, data_type: str, date_range_days: int, record_count: int
    ):
        """코드별 통계 업데이트"""
        if code not in self.code_statistics:
            self.code_statistics[code] = {}
        if data_type not in self.code_statistics[code]:
            self.code_statistics[code][data_type] = {
                "total_records": 0,
                "total_days": 0,
                "avg_records_per_day": 1.0,
            }

        stats = self.code_statistics[code][data_type]
        stats["total_records"] += record_count
        stats["total_days"] += date_range_days
        stats["avg_records_per_day"] = stats["total_records"] / max(
            stats["total_days"], 1
        )

        # 주기적으로 통계 저장
        if stats["total_records"] % 100 == 0:
            self._save_statistics()

    def _save_statistics(self):
        """통계를 파일에 저장"""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
            cache_dir = os.path.join(project_root, "cache", "statistics")
            os.makedirs(cache_dir, exist_ok=True)

            stats_file = os.path.join(cache_dir, "code_statistics.json")
            with open(stats_file, "w", encoding="utf-8") as f:
                json.dump(self.code_statistics, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save statistics: {e}")

    def _load_statistics(self):
        """파일에서 통계 로드"""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
            stats_file = os.path.join(
                project_root, "cache", "statistics", "code_statistics.json"
            )

            if os.path.exists(stats_file):
                with open(stats_file, "r", encoding="utf-8") as f:
                    self.code_statistics = defaultdict(dict, json.load(f))
                logger.info(f"Loaded statistics for {len(self.code_statistics)} codes")
        except Exception as e:
            logger.error(f"Failed to load statistics: {e}")
            self.code_statistics = defaultdict(dict)


class PerformanceMonitor:
    """API 성능 모니터링"""

    def __init__(self):
        self.metrics = defaultdict(list)
        self.lock = threading.Lock()

    def record_request(
        self, api_name: str, response_time: float, success: bool, cached: bool = False
    ):
        """요청 성능 기록"""
        with self.lock:
            timestamp = time.time()
            self.metrics[api_name].append(
                {
                    "timestamp": timestamp,
                    "response_time": response_time,
                    "success": success,
                    "cached": cached,
                }
            )

            # 메모리 절약을 위해 최근 1000개만 유지
            if len(self.metrics[api_name]) > 1000:
                self.metrics[api_name] = self.metrics[api_name][-1000:]

    def get_api_stats(self, api_name: str, time_window: int = 3600) -> Dict[str, Any]:
        """API 통계 조회"""
        with self.lock:
            if api_name not in self.metrics:
                return {}

            now = time.time()
            recent_metrics = [
                m for m in self.metrics[api_name] if now - m["timestamp"] <= time_window
            ]

            if not recent_metrics:
                return {}

            response_times = [m["response_time"] for m in recent_metrics]
            success_count = sum(1 for m in recent_metrics if m["success"])
            cache_hit_count = sum(1 for m in recent_metrics if m["cached"])

            return {
                "total_requests": len(recent_metrics),
                "success_rate": success_count / len(recent_metrics),
                "cache_hit_rate": cache_hit_count / len(recent_metrics),
                "avg_response_time": sum(response_times) / len(response_times),
                "min_response_time": min(response_times),
                "max_response_time": max(response_times),
            }

    def get_overall_stats(self) -> Dict[str, Any]:
        """전체 성능 통계"""
        with self.lock:
            stats = {}
            for api_name in self.metrics.keys():
                stats[api_name] = self.get_api_stats(api_name)
            return stats


class APIOptimizer:
    """통합 API 최적화 관리자"""

    def __init__(
        self,
        api_client,
        max_requests_per_minute: int = 60,
        cache_ttl_seconds: int = 300,
        max_workers: int = 3,
    ):
        """
        API 최적화 관리자 초기화

        Args:
            api_client: 기존 API 클라이언트 인스턴스
            max_requests_per_minute: 분당 최대 요청 수
            cache_ttl_seconds: 캐시 TTL (초)
            max_workers: 최대 작업자 스레드 수
        """
        self.api_client = api_client
        self.max_workers = max_workers

        # 최적화 컴포넌트들 초기화
        self.rate_limiter = RateLimiter(max_requests_per_minute, 60)
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        self.response_cache = ResponseCache(
            max_size=1000, ttl_seconds=cache_ttl_seconds
        )
        self.date_splitter = DateRangeSplitter()
        self.performance_monitor = PerformanceMonitor()

        # 요청 큐 및 배치 처리
        self.request_queue = deque()
        self.batch_size = 10
        self.batch_timeout = 5.0
        self._last_batch_time = time.time()
        self._processing = False
        self._stop_event = threading.Event()

        # 배치 처리 스레드 시작
        self._batch_thread = threading.Thread(target=self._batch_processor, daemon=True)
        self._batch_thread.start()

        logger.info("🚀 API Optimizer initialized with advanced features")

    def optimized_request(
        self,
        api_name: str,
        method: str = "GET",
        tr_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        priority: int = 5,
        use_cache: bool = True,
    ) -> APIResponse:
        """
        최적화된 API 요청 수행

        Args:
            api_name: API 이름
            method: HTTP 메소드
            tr_id: 거래 ID
            params: URL 파라미터
            body: 요청 본문
            headers: 추가 헤더
            priority: 우선순위 (1=최고, 10=최저)
            use_cache: 캐시 사용 여부

        Returns:
            APIResponse: 최적화된 응답
        """
        # API 요청 객체 생성
        request = APIRequest(
            api_name=api_name,
            method=method,
            tr_id=tr_id,
            params=params,
            body=body,
            headers=headers,
            priority=priority,
        )

        # 캐시 확인 (사용하는 경우)
        if use_cache:
            cached_response = self.response_cache.get(request)
            if cached_response:
                self.performance_monitor.record_request(
                    api_name, 0.0, True, cached=True
                )
                logger.debug(f"🎯 Cache hit for {api_name}")
                return cached_response

        # 실제 API 호출 수행
        start_time = time.time()
        response = None
        success = False

        try:
            # 속도 제한 적용
            self.rate_limiter.acquire()

            # 서킷 브레이커를 통한 API 호출
            result = self.circuit_breaker.call(self._execute_api_request, request)

            # 응답 객체 생성
            response = APIResponse(
                request=request,
                data=result,
                status_code=200,
                response_time=time.time() - start_time,
                cached=False,
            )

            success = True
            self.rate_limiter.report_success()

            # 캐시에 저장 (성공한 경우만)
            if use_cache and result.get("rt_cd") == "0":
                self.response_cache.put(request, response)

            logger.debug(
                f"✅ API request completed: {api_name} "
                f"({response.response_time:.2f}s)"
            )

        except Exception as e:
            self.rate_limiter.report_error()

            # 오류 응답 생성
            response = APIResponse(
                request=request,
                data={
                    "rt_cd": "1",
                    "msg1": str(e),
                    "output": {},
                    "output1": [],
                    "output2": [],
                },
                status_code=500,
                response_time=time.time() - start_time,
                cached=False,
                error=str(e),
            )

            logger.error(f"❌ API request failed: {api_name} - {e}")

        # 성능 메트릭 기록
        self.performance_monitor.record_request(
            api_name, response.response_time, success, cached=False
        )

        return response

    def _execute_api_request(self, request: APIRequest) -> Dict[str, Any]:
        """실제 API 요청 실행"""
        return self.api_client.request(
            method=request.method,
            api_name=request.api_name,
            tr_id=request.tr_id,
            params=request.params,
            body=request.body,
            headers=request.headers,
        )

    def batch_request(
        self, requests: List[APIRequest], max_workers: Optional[int] = None
    ) -> List[APIResponse]:
        """배치 API 요청 처리"""
        max_workers = max_workers or self.max_workers
        responses = []

        # 우선순위별로 정렬
        sorted_requests = sorted(requests, key=lambda r: r.priority)

        logger.info(f"🔄 Processing batch of {len(requests)} requests")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 동시 실행
            future_to_request = {
                executor.submit(
                    self.optimized_request,
                    req.api_name,
                    req.method,
                    req.tr_id,
                    req.params,
                    req.body,
                    req.headers,
                    req.priority,
                ): req
                for req in sorted_requests
            }

            # 결과 수집
            for future in as_completed(future_to_request):
                request = future_to_request[future]
                try:
                    response = future.result()
                    responses.append(response)
                except Exception as e:
                    logger.error(f"Batch request failed for {request.api_name}: {e}")
                    error_response = APIResponse(
                        request=request,
                        data={"rt_cd": "1", "msg1": str(e)},
                        status_code=500,
                        response_time=0.0,
                        error=str(e),
                    )
                    responses.append(error_response)

        # 원래 순서대로 정렬
        request_order = {id(req): i for i, req in enumerate(requests)}
        responses.sort(key=lambda r: request_order.get(id(r.request), 999))

        logger.info(f"✅ Batch processing completed: {len(responses)} responses")
        return responses

    def optimize_date_range_requests(
        self,
        api_name: str,
        code: str,
        start_date: datetime,
        end_date: datetime,
        base_params: Dict[str, Any],
        date_param_name: str = "start_date",
        end_date_param_name: str = "end_date",
        tr_id: Optional[str] = None,
    ) -> List[APIResponse]:
        """날짜 범위 기반 최적화된 요청 처리"""

        # 최적 분할 계산
        date_ranges = self.date_splitter.calculate_optimal_splits(
            start_date, end_date, code, "daily"
        )

        # 요청 생성
        requests = []
        for range_start, range_end in date_ranges:
            params = base_params.copy()
            params[date_param_name] = range_start.strftime("%Y%m%d")
            params[end_date_param_name] = range_end.strftime("%Y%m%d")

            request = APIRequest(
                api_name=api_name, tr_id=tr_id, params=params, priority=5
            )
            requests.append(request)

        logger.info(f"📅 Date range optimized: {len(requests)} API calls for {code}")

        # 배치 처리
        responses = self.batch_request(requests)

        # 통계 업데이트
        total_records = 0
        for response in responses:
            if response.data.get("rt_cd") == "0":
                output_data = response.data.get("output2", [])
                total_records += len(output_data)

        total_days = (end_date - start_date).days
        self.date_splitter.update_statistics(code, "daily", total_days, total_records)

        return responses

    def _batch_processor(self):
        """배경에서 실행되는 배치 처리기"""
        while not self._stop_event.is_set():
            try:
                current_time = time.time()

                # 배치 처리 조건 확인
                if len(self.request_queue) >= self.batch_size or (
                    self.request_queue
                    and current_time - self._last_batch_time >= self.batch_timeout
                ):

                    # 큐에서 요청들 추출
                    batch_requests = []
                    while self.request_queue and len(batch_requests) < self.batch_size:
                        batch_requests.append(self.request_queue.popleft())

                    if batch_requests:
                        logger.debug(
                            f"🔄 Processing batch: {len(batch_requests)} requests"
                        )
                        self.batch_request(batch_requests)
                        self._last_batch_time = current_time

                time.sleep(0.1)  # CPU 사용량 조절

            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                time.sleep(1)

    def get_performance_report(self) -> Dict[str, Any]:
        """성능 리포트 생성"""
        stats = self.performance_monitor.get_overall_stats()

        # 전체 통계 계산
        total_requests = sum(s.get("total_requests", 0) for s in stats.values())
        avg_success_rate = sum(s.get("success_rate", 0) for s in stats.values()) / max(
            len(stats), 1
        )
        avg_cache_hit_rate = sum(
            s.get("cache_hit_rate", 0) for s in stats.values()
        ) / max(len(stats), 1)

        return {
            "summary": {
                "total_apis": len(stats),
                "total_requests": total_requests,
                "avg_success_rate": avg_success_rate,
                "avg_cache_hit_rate": avg_cache_hit_rate,
                "circuit_breaker_state": self.circuit_breaker.state,
                "current_rate_limit_delay": self.rate_limiter._adaptive_delay,
            },
            "api_details": stats,
            "timestamp": datetime.now().isoformat(),
        }

    def cleanup(self):
        """리소스 정리"""
        self._stop_event.set()
        if self._batch_thread.is_alive():
            self._batch_thread.join(timeout=5)

        # 캐시 저장
        self.response_cache._save_cache()
        self.date_splitter._save_statistics()

        logger.info("🧹 API Optimizer cleanup completed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
