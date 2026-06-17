# Collective Communication Pattern

## 개념

**Collective Communication Pattern**은 파라미터 서버 없이 여러 Worker가 서로 직접 통신하며 그레이디언트를 집계하고 모델 파라미터를 동기화하는 분산 학습 패턴이다.

파라미터 서버 패턴에서 서버가 병목이 되는 문제를 해결하기 위해 등장했다.

---

## 발생 배경 / 사용 조건

아래 상황에서 Collective Communication Pattern이 필요해진다.

- **파라미터 서버가 병목이 될 때**: Worker 수가 늘어날수록 서버로 집중되는 통신량이 폭증
- **모델이 하나의 파라미터 서버에 들어가는 크기일 때**: 모델을 분할하지 않아도 될 만큼 적당한 크기
- **모든 Worker가 항상 최신 파라미터로 동기화되어야 할 때**

---

## 파라미터 서버 패턴의 한계

```
# 파라미터 서버 패턴에서의 통신 구조
Worker-0 ──┐
Worker-1 ──┤──→  Parameter Server  (단일 서버가 모든 통신 수용)
Worker-2 ──┘

# 문제: Worker 수가 늘어날수록 파라미터 서버로 향하는 트래픽 집중
# Worker-0과 Worker-1이 동시에 push하면 서버에서 블로킹 발생
```

**문제점**
- 파라미터 서버가 단일 장애점(SPOF)이자 성능 병목
- 여러 Worker가 동시에 모델을 업데이트할 수 없어 대기 발생
- Worker 수에 비례해 서버 부하가 선형 이상으로 증가

---

## 집합 통신의 핵심 연산

### Reduce
여러 Worker의 값을 하나로 집계 (합, 평균, 최댓값 등)

```
Worker-0:  [1, 2, 3]
Worker-1:  [4, 5, 6]   →  Reduce(sum)  →  [5, 7, 9]
Worker-2:  [0, 0, 0]
```

### Broadcast
하나의 값을 모든 Worker로 전송

```
Worker-0:  [5, 7, 9]   →  Broadcast  →  Worker-0: [5, 7, 9]
                                     →  Worker-1: [5, 7, 9]
                                     →  Worker-2: [5, 7, 9]
```

### AllReduce
Reduce + Broadcast를 합친 연산. 모든 Worker가 집계 결과를 동시에 가짐

```
Worker-0:  [1, 2, 3]                    Worker-0: [5, 7, 9]
Worker-1:  [4, 5, 6]  → AllReduce →    Worker-1: [5, 7, 9]
Worker-2:  [0, 0, 0]                    Worker-2: [5, 7, 9]
```

---

## 구조 및 코드 예시

```python
# Horovod를 활용한 AllReduce 기반 분산 학습 예시
import horovod.torch as hvd
import torch
import torch.nn as nn

# Horovod 초기화
hvd.init()

# 각 Worker는 자신의 GPU를 사용
torch.cuda.set_device(hvd.local_rank())

model = SimpleModel().cuda()
optimizer = torch.optim.SGD(model.parameters(), lr=0.01 * hvd.size())

# Horovod 분산 옵티마이저 래핑 (내부적으로 AllReduce 사용)
optimizer = hvd.DistributedOptimizer(
    optimizer,
    named_parameters=model.named_parameters()
)

# Worker-0의 초기 파라미터를 모든 Worker에 브로드캐스트
hvd.broadcast_parameters(model.state_dict(), root_rank=0)

for epoch in range(10):
    for batch in dataloader:
        optimizer.zero_grad()
        output = model(batch["input"])
        loss = criterion(output, batch["label"])
        loss.backward()

        # 각 Worker의 그레이디언트가 AllReduce로 집계됨
        optimizer.step()
```

**동작 흐름**
```
Worker-0  ──┐
Worker-1  ──┤──→  AllReduce (그레이디언트 합산) ──→  모든 Worker 동시 업데이트
Worker-2  ──┘
              파라미터 서버 없음, Worker 간 직접 통신
```

---

## 링 AllReduce (Ring AllReduce)

단순 AllReduce는 Worker 수가 늘어날수록 통신 비용이 증가한다. 이를 해결한 것이 **링 AllReduce**다.

```
일반 AllReduce:
  Worker-0이 모든 Worker와 직접 통신 → 통신량 O(N)

링 AllReduce:
  Worker들이 링 구조로 연결되어 순차적으로 값을 전달
  → 통신량 O(1) (Worker 수와 무관하게 일정)

  Worker-0 → Worker-1 → Worker-2 → Worker-0
      ↑                                ↓
      └────────────────────────────────┘
```

링 AllReduce는 네트워크 관점에서 가장 효율적인 AllReduce 알고리즘이다.

---

## 동기 업데이트 vs 비동기 업데이트

| 방식 | 설명 | 특징 |
|---|---|---|
| **동기** | 모든 Worker가 그레이디언트를 낼 때까지 대기 후 일괄 업데이트 | 정확하지만 느린 Worker(straggler)가 전체 블로킹 |
| **비동기** | 각 Worker가 준비되는 즉시 업데이트 | 빠르지만 stale gradient 발생 가능 |

집합 통신 패턴은 기본적으로 **동기** 방식이다.

---

## 응용

### DE / ML 파이프라인에서의 적용

**분산 집계 (Distributed Aggregation)**

여러 Worker가 처리한 결과를 하나로 합산해야 하는 파이프라인에서 동일한 개념이 적용된다.

```python
from multiprocessing import Pool
import numpy as np

def compute_partial_sum(data_shard):
    """각 Worker가 자신의 샤드 합산"""
    return np.sum(data_shard)

def allreduce_sum(shards, num_workers=4):
    """AllReduce: 모든 샤드의 합을 구해 전체 합 반환"""
    with Pool(num_workers) as pool:
        partial_sums = pool.map(compute_partial_sum, shards)
    # Reduce: 모든 부분합을 합산
    total = sum(partial_sums)
    # Broadcast: 모든 Worker에 결과 공유 (여기선 반환으로 대체)
    return total

dataset = np.random.rand(1_000_000)
shards = np.array_split(dataset, 4)
total_sum = allreduce_sum(shards)
print(f"Total sum: {total_sum}")
```

**포인트**
- PyTorch DDP(DistributedDataParallel), Horovod, JAX pmap 모두 집합 통신 패턴 구현체
- 모델이 너무 커서 단일 서버에 들어가지 않으면 파라미터 서버 패턴이 여전히 필요
- straggler 문제를 완화하려면 타임아웃 설정 또는 느린 Worker 제외 후 나머지로 재구성

---

> Collective Communication Pattern은 파라미터 서버 없이 Worker 간 AllReduce 연산으로 그레이디언트를 집계하고 모델을 동기화하는 패턴이다.
> "서버에 의존하지 말고, 다 같이 합쳐서 다 같이 업데이트하라."