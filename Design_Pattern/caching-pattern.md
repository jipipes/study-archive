# Caching Pattern

## 개념

**Caching Pattern**은 반복적으로 접근하는 데이터를 빠른 저장소(메모리)에 미리 올려두고 재활용하는 패턴이다.

머신러닝 학습에서는 동일한 데이터셋을 여러 에포크(epoch)에 걸쳐 반복 사용하기 때문에 매번 디스크에서 읽어오는 비용을 줄이기 위해 캐싱이 효과적이다.

*에포크(epoch): 전체 데이터셋을 최소 한 번씩 모두 사용해 학습하는 단위. 모델 수렴을 위해 보통 수십~수백 에포크 반복한다.

---

## 발생 배경 / 사용 조건

아래 상황에서 Caching Pattern이 필요해진다.

- **동일 데이터셋을 여러 에포크에 걸쳐 반복 학습할 때**: 매번 디스크에서 읽으면 I/O가 병목
- **전처리 비용이 클 때**: 이미지 리사이징, 정규화 등 전처리 결과를 캐시해두면 재연산 불필요
- **원격 저장소에서 데이터를 가져올 때**: 네트워크 I/O 비용이 매우 크므로 로컬 캐시 활용

---

## 발생하는 문제

```python
# 캐싱 없이 매 에포크마다 디스크에서 읽는 경우
for epoch in range(100):
    for batch in read_from_disk("dataset/"):  # 매번 디스크 I/O 발생
        preprocessed = preprocess(batch)      # 매번 전처리 재실행
        train(model, preprocessed)

# 문제: 100 에포크 = 디스크 읽기 100번 + 전처리 100번
# I/O와 전처리 시간이 실제 학습 시간보다 길어질 수 있음
```

**문제점**
- 에포크가 늘어날수록 디스크 I/O 비용이 선형으로 증가
- 전처리 결과가 매번 버려지고 동일한 연산이 반복됨
- 원격 저장소 접근 시 네트워크 지연까지 더해져 병목 심화

---

## 구조 및 코드 예시

```python
import numpy as np
import os
import pickle

class CachedDataset:
    def __init__(self, source_path, cache_path, batch_size=32):
        self.batch_size = batch_size
        self.cache_path = cache_path

        if os.path.exists(cache_path):
            print("Cache found. Loading from cache...")
            self.data = self._load_cache()
        else:
            print("No cache. Loading from source and building cache...")
            raw_data = self._load_from_source(source_path)
            self.data = self._preprocess(raw_data)
            self._save_cache(self.data)

    def _load_from_source(self, path):
        return np.load(path)  # 디스크 또는 원격에서 로드

    def _preprocess(self, data):
        # 전처리 (정규화, 리사이징 등) - 한 번만 실행
        return (data - data.mean()) / data.std()

    def _save_cache(self, data):
        with open(self.cache_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"Cache saved to {self.cache_path}")

    def _load_cache(self):
        with open(self.cache_path, 'rb') as f:
            return pickle.load(f)

    def __iter__(self):
        for i in range(0, len(self.data), self.batch_size):
            yield self.data[i:i + self.batch_size]

# --- 실행 ---
dataset = CachedDataset(
    source_path="s3://bucket/train-data.npy",
    cache_path="/tmp/train_cache.pkl"
)

for epoch in range(100):
    for batch in dataset:
        train(model, batch)   # 2번째 에포크부터는 캐시에서 읽음
    print(f"Epoch {epoch+1} done")
```

**동작 흐름**
```
1st Epoch:  디스크/원격 → 전처리 → [캐시 저장] → 학습
2nd Epoch~: [캐시 로드]             → 학습
                ↑ 전처리 생략, I/O 최소화
```

---

## 인메모리 캐시 vs 디스크 캐시

| 구분 | 인메모리 캐시 (RAM) | 디스크 캐시 |
|---|---|---|
| 속도 | 매우 빠름 (나노초 수준) | 상대적으로 느림 (밀리초 수준) |
| 영속성 | 프로세스 종료 시 유실 | 재시작 후에도 유지 |
| 용량 | 제한적 | 상대적으로 큼 |
| 적합한 상황 | 속도가 중요한 경우 | 안정성이 중요한 경우 |

> RAM은 디스크보다 최대 10만 배 빠를 수 있다. (랜덤 접근 기준)

---

## 캐시 무효화 (Cache Invalidation)

원본 데이터셋이 업데이트되면 캐시도 함께 갱신해야 한다.

```python
import hashlib

def get_source_hash(path):
    """원본 파일의 해시값으로 변경 여부 확인"""
    with open(path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def is_cache_valid(source_path, cache_meta_path):
    current_hash = get_source_hash(source_path)
    if not os.path.exists(cache_meta_path):
        return False
    with open(cache_meta_path) as f:
        cached_hash = f.read()
    return current_hash == cached_hash
```

---

## 응용

### DE 파이프라인에서의 적용

**API 응답 캐싱**

외부 API에서 데이터를 반복적으로 가져와야 하는 파이프라인에서 캐싱은 비용과 속도 모두에 영향을 미친다.

```python
import redis
import json
import requests

r = redis.Redis(host='localhost', port=6379)
TTL = 3600  # 1시간 캐시 유효

def fetch_with_cache(api_url, cache_key):
    # 캐시 확인
    cached = r.get(cache_key)
    if cached:
        print(f"Cache hit: {cache_key}")
        return json.loads(cached)

    # 캐시 미스 → API 호출
    print(f"Cache miss: {cache_key}. Fetching from API...")
    response = requests.get(api_url).json()

    # 캐시 저장 (TTL 설정)
    r.setex(cache_key, TTL, json.dumps(response))
    return response

# 사용 예시
data = fetch_with_cache(
    api_url="https://api.example.com/stock/AAPL",
    cache_key="stock:AAPL"
)
```

**실무 포인트**
- 캐시는 데이터가 자주 바뀌지 않고 읽기가 쓰기보다 훨씬 많을 때 효과적
- TTL(Time-To-Live) 설정으로 오래된 캐시가 자동 만료되도록 관리
- 파이프라인 장애 복구 시 캐시된 전처리 결과를 재활용하면 재시작 비용 절감

---

> Caching Pattern은 반복 접근하는 데이터를 빠른 저장소에 올려두고 재사용하는 패턴이다.
> "같은 걸 두 번 읽지 말고 한 번 읽고 기억해라."