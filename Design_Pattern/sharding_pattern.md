# Sharding Pattern

## 개념

**Sharding Pattern**은 큰 데이터셋을 여러 개의 작은 조각(샤드, shard)으로 나누어 여러 Worker에 분산시켜 병렬로 처리하는 패턴이다.

배치처리 패턴이 "순차적으로 나누어 처리"라면 샤딩 패턴은 "여러 Worker가 동시에 나누어 처리"하는 것이 핵심 차이다.

---

## 발생 배경 / 사용 조건

아래 상황에서 Sharding Pattern이 필요해진다.

- **배치처리가 너무 느릴 때**: 미니배치를 순차적으로 처리하면 전체 학습 시간이 매우 길어짐
- **여러 Worker(서버, GPU)를 보유하고 있을 때**: 유휴 자원을 병렬로 활용하고 싶을 때
- **데이터셋이 분산 파일 시스템에 저장되어 있을 때**: HDFS, S3 등에서 파티션 단위로 읽어올 수 있을 때

---

## 발생하는 문제

```python
# 배치처리만 사용하는 경우 (순차 처리)
for batch in dataset:
    train(model, batch)  # Worker 1개가 순서대로 처리

# 문제: Worker가 3대 있어도 1대만 일하고 2대는 놀고 있음
# 데이터셋이 클수록 학습 시간이 선형적으로 증가
```

**문제점**
- 전체 모델 학습이 단일 Worker 속도에 종속됨
- 보유한 연산 자원을 충분히 활용하지 못함
- 데이터셋 크기가 늘어날수록 학습 시간이 비례해서 늘어남

---

## 구조 및 코드 예시

```python
import math
import threading

def create_shards(dataset, num_workers):
    """데이터셋을 워커 수만큼 균등하게 샤딩"""
    shard_size = math.ceil(len(dataset) / num_workers)
    shards = []
    for i in range(num_workers):
        start = i * shard_size
        end = min(start + shard_size, len(dataset))
        shards.append(dataset[start:end])
    return shards

def worker(worker_id, shard, model):
    """각 워커가 자신의 샤드로 학습"""
    print(f"[Worker-{worker_id}] Start: {len(shard)} samples")
    for batch in get_batches(shard, batch_size=32):
        train(model, batch)
    print(f"[Worker-{worker_id}] Done")

def get_batches(shard, batch_size):
    for i in range(0, len(shard), batch_size):
        yield shard[i:i + batch_size]

# --- 실행 ---
NUM_WORKERS = 3
dataset = load_dataset()
shards = create_shards(dataset, NUM_WORKERS)

threads = []
for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(i, shards[i], model))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

print("All shards processed.")
```

**동작 흐름**
```
전체 데이터셋
    │
    ├── Shard 0  →  Worker-0  →  학습 (병렬)
    ├── Shard 1  →  Worker-1  →  학습 (병렬)
    └── Shard 2  →  Worker-2  →  학습 (병렬)
```

---

## 수평 분할 (행 기준 분할)

샤딩은 **수평 분할(가로 분할)**이다. 즉, 행(row) 기준으로 데이터를 나눈다.

```
[ 전체 데이터셋 ]         [ 샤딩 후 ]
┌──────────────┐          ┌──────────┐  Shard 0
│  row 0       │    →     │  row 0   │
│  row 1       │          │  row 1   │
│  row 2       │          └──────────┘
│  row 3       │          ┌──────────┐  Shard 1
│  row 4       │          │  row 2   │
│  row 5       │          │  row 3   │
└──────────────┘          └──────────┘
                          ┌──────────┐  Shard 2
                          │  row 4   │
                          │  row 5   │
                          └──────────┘
```

---

## 수동 샤딩의 한계와 자동 샤딩

**수동 샤딩 문제점**
- 데이터셋이 업데이트될 때마다 샤딩 작업을 다시 해야 함
- 특정 샤드에 데이터가 몰리면(데이터 스큐) 해당 Worker만 과부하
- 스키마 변경 시 모든 샤드에 동일하게 반영해야 함

**자동 샤딩: 해시 샤딩**

```python
def hash_sharding(data, num_shards):
    """해시 기반 자동 샤딩"""
    shards = [[] for _ in range(num_shards)]
    for record in data:
        shard_id = hash(record["id"]) % num_shards
        shards[shard_id].append(record)
    return shards

# 장점: 데이터가 추가되어도 자동으로 균등 분배
# 장점: 같은 키는 항상 같은 샤드로 → 일관성 보장
```

---

## 응용

### DE 파이프라인에서의 적용

**Spark의 파티셔닝**

Spark는 샤딩 패턴의 대표적인 구현체다. 데이터프레임을 파티션(샤드)으로 나누어 각 Executor(Worker)가 병렬로 처리한다.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sharding-example").getOrCreate()

df = spark.read.parquet("s3://bucket/large-dataset/")

# 파티션 수 조정 (샤드 수 조정과 동일한 개념)
df = df.repartition(100)  # 100개 샤드로 분산

# 각 파티션이 독립적으로 처리됨
result = df.groupBy("category").agg({"value": "sum"})
result.write.parquet("s3://bucket/output/")
```

**Airflow 동적 태스크 샤딩**

```python
from airflow.decorators import dag, task
import pendulum

@dag(start_date=pendulum.datetime(2024, 1, 1))
def sharded_pipeline():

    @task
    def get_file_list():
        # S3에서 처리할 파일 목록 조회
        return ["file_0.csv", "file_1.csv", "file_2.csv", "file_3.csv"]

    @task
    def process_shard(file_key):
        # 각 파일을 독립적으로 처리 (샤드 단위 처리)
        print(f"Processing {file_key}")

    files = get_file_list()
    process_shard.expand(file_key=files)  # 동적 병렬 실행

sharded_pipeline()
```

**실무 포인트**
- 샤드 수 = Worker 수에 맞추는 것이 기본. 단 I/O bound 작업은 더 잘게 쪼개도 됨
- 데이터 스큐(특정 샤드에 데이터 몰림) 주의 → 해시 샤딩 또는 랜덤 샘플링으로 방지
- 샤딩 후 결과를 다시 합쳐야 하는 경우 집합 통신 패턴 또는 리듀스 단계 필요

---

> Sharding Pattern은 큰 데이터셋을 여러 샤드로 나누어 여러 Worker가 병렬로 처리하도록 하는 패턴이다.
> "혼자 다 하려 하지 말고 나눠서 동시에 끝내라."