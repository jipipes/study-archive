# Parameter Server Pattern (파라미터 서버 패턴)

## 개념

**Parameter Server Pattern**은 공유 상태(파라미터, 가중치, 설정값 등)를 전담 서버에 중앙화하고 여러 Worker가 이를 읽고(pull) 쓰는(push) 방식으로 분산 처리하는 설계 패턴이다.

분산 머신러닝 학습에서 시작된 패턴이지만 **"공유 상태를 여러 Worker가 동시에 읽고 써야 하는"** 모든 분산 시스템에 적용된다.

---

## 구성 요소

| 구성 요소 | 역할 |
|---|---|
| Parameter Server | 공유 파라미터를 저장하고 읽기/쓰기 요청을 처리 |
| Worker | 데이터 일부를 처리하고 결과(gradient 등)를 서버에 push, 최신 파라미터를 pull |

---

## 발생 배경 / 사용 조건

아래 상황에서 Parameter Server Pattern이 필요해진다.

- **공유 상태가 너무 커서 각 Worker가 모두 보유할 수 없을 때** (예: 수십억 개 파라미터)
- **여러 Worker가 동일한 상태를 동시에 업데이트해야 할 때**
- **동기화 비용 없이 비동기 업데이트를 허용하고 싶을 때**

---

## 패턴 없이 발생하는 문제

```python
# 모든 Worker가 각자 파라미터 복사본을 들고 처리하는 경우
worker_1_params = load_params()  # 복사본 A
worker_2_params = load_params()  # 복사본 B

worker_1_params = update(worker_1_params, grad_1)
worker_2_params = update(worker_2_params, grad_2)

# 문제: 어떤 복사본이 최신인가? 누가 최종 파라미터를 갖는가?
# → 상태 불일치(state inconsistency) 발생
```

**문제점**
- 각 Worker가 독립적으로 상태를 갱신하면 결과가 수렴하지 않음
- 파라미터가 너무 클 경우 각 Worker에 복사하는 것 자체가 불가능
- 동기화 방법(AllReduce 등)은 Worker 수 증가 시 통신 비용이 폭증

---

## 구조 및 코드 예시

```python
import threading

# --- Parameter Server ---
class ParameterServer:
    def __init__(self, initial_params):
        self.params = initial_params.copy()
        self.lock = threading.Lock()

    def pull(self):
        """Worker가 최신 파라미터를 가져감"""
        with self.lock:
            return self.params.copy()

    def push(self, gradient, learning_rate=0.01):
        """Worker가 계산한 gradient를 반영"""
        with self.lock:
            for key in self.params:
                self.params[key] -= learning_rate * gradient.get(key, 0)

# --- Worker ---
def worker(worker_id, ps, data_shard):
    # 1. 최신 파라미터 pull
    params = ps.pull()
    print(f"[Worker-{worker_id}] Pulled params: {params}")

    # 2. 로컬에서 gradient 계산 (간단한 예시)
    gradient = compute_gradient(params, data_shard)

    # 3. gradient push
    ps.push(gradient)
    print(f"[Worker-{worker_id}] Pushed gradient: {gradient}")

def compute_gradient(params, data):
    # 실제로는 loss function의 미분값
    return {"w": sum(data) * params["w"] * 0.1}

# --- 실행 ---
ps = ParameterServer(initial_params={"w": 1.0})

data_shards = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
threads = []
for i, shard in enumerate(data_shards):
    t = threading.Thread(target=worker, args=(i, ps, shard))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print(f"Final params: {ps.pull()}")
```

**동작 흐름**
```
           ┌──────────────────────────┐
           │     Parameter Server     │
           │    { w: 1.0, b: 0.5 }   │
           └─────────┬────────────────┘
         pull ↑↓ push│
    ┌──────────┼──────────┐
    │          │          │
 Worker-0  Worker-1  Worker-2
 (shard_0) (shard_1) (shard_2)
```

---

## 동기(Synchronous) vs 비동기(Asynchronous) 업데이트

| 방식 | 설명 | 특징 |
|---|---|---|
| **Synchronous** | 모든 Worker가 gradient를 push한 뒤 서버가 일괄 업데이트 | 정확하지만 느린 Worker가 병목이 됨 (straggler 문제) |
| **Asynchronous** | 각 Worker가 독립적으로 push | 빠르지만 stale gradient 문제 발생 가능 |

---

## 응용

### DE / ML 파이프라인에서의 적용

**분산 하이퍼파라미터 튜닝**

여러 실험 Worker가 동시에 모델을 학습하면서 공유 파라미터 서버(예: Ray의 GCS, MLflow)에서 최적 하이퍼파라미터를 공유하는 구조.

```python
# Ray를 활용한 Parameter Server 패턴 예시
import ray

@ray.remote
class ParameterServer:
    def __init__(self):
        self.params = {"lr": 0.01, "batch_size": 32}

    def get(self):
        return self.params

    def update(self, key, value):
        self.params[key] = value

@ray.remote
def worker(ps, worker_id):
    params = ray.get(ps.get.remote())
    print(f"Worker {worker_id} using params: {params}")
    # ... 학습 후 결과 반영
    ps.update.remote("lr", params["lr"] * 0.9)

ps = ParameterServer.remote()
ray.get([worker.remote(ps, i) for i in range(4)])
```

**전역 집계 카운터 / 상태 관리**

데이터 파이프라인에서 처리 진행 상황(processed count, error count)을 여러 Worker가 공유해야 할 때도 같은 패턴이 적용된다.

```python
# 예: 여러 Airflow Worker가 공유 카운터를 갱신하는 패턴
# Redis를 Parameter Server로 사용
import redis

r = redis.Redis()

def worker_task(file_key):
    # 처리 완료 후 카운터 증가 (atomic 연산)
    r.incr("processed_count")
    r.lpush("processed_files", file_key)
```

**실무 포인트**
- Parameter Server가 단일 장애점(SPOF)이 될 수 있으므로 **복제(replication)** 필요
- 파라미터 공간이 크면 **파라미터를 샤딩**해 여러 서버에 분산
- push 빈도 조절이 중요: 너무 자주 push하면 서버 병목, 너무 드물면 stale 문제

---

> Parameter Server Pattern은 공유 상태를 중앙 서버에 두고, Worker들이 pull-process-push 사이클을 반복하며 분산 처리를 구현하는 패턴이다.
> "상태는 서버가 기억하고, Worker는 계산에만 집중한다."