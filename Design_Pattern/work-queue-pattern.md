# Work-Queue Pattern

## 개념

**Work-Queue Pattern**은 생산자(Producer)가 작업을 큐에 넣고 하나 이상의 소비자(Worker/Consumer)가 큐에서 작업을 꺼내 처리하는 분산 처리 설계 패턴이다.

생산자와 소비자가 직접 연결되지 않고 큐를 매개로 **비동기적으로 분리(decoupling)**되는 것이 핵심이다.

---

## 구성 요소

| 구성 요소 | 역할 |
|---|---|
| Producer | 작업을 생성하여 큐에 삽입 |
| Queue | 작업을 임시 저장하는 버퍼 (FIFO) |
| Worker | 큐에서 작업을 꺼내 처리 |

---

## 발생 배경 / 사용 조건

아래 상황에서 Work Queue Pattern이 필요해진다.

- **처리 속도 불균형**: 생산자가 소비자보다 빠르게 작업을 만들어낼 때
- **작업량 급증(burst)**: 순간적으로 대량의 요청이 몰릴 때 (예: 이벤트, 배치 트리거)
- **작업 처리 시간이 길 때**: HTTP 요청처럼 즉각 응답이 필요한 맥락에서 무거운 작업을 분리하고 싶을 때
- **수평 확장 필요**: Worker 수를 늘려 처리량을 선형적으로 늘리고 싶을 때

---

## 발생하는 문제

```python
# 패턴 없이 직접 처리할 경우
def handle_request(data):
    # 무거운 작업을 요청 흐름 안에서 직접 처리
    result = run_heavy_job(data)  # 10초 소요
    return result

# 문제: 요청마다 10초 블로킹 → 동시 요청 폭증 시 서버 다운
```

**문제점**
- 생산자와 소비자가 강하게 결합(tight coupling)되어 있어 독립 확장이 불가능
- 순간 트래픽 급증 시 처리 불가 작업이 쌓이며 시스템 전체 지연 발생
- 한 작업 실패가 전체 요청 흐름에 영향을 미침

---

## 구조 및 코드 예시

```python
import queue
import threading
import time

# --- 큐 생성 ---
task_queue = queue.Queue()

# --- Producer ---
def producer(n):
    for i in range(n):
        task = {"id": i, "data": f"task_{i}"}
        task_queue.put(task)
        print(f"[Producer] Enqueued: {task['id']}")
        time.sleep(0.1)

# --- Worker ---
def worker(worker_id):
    while True:
        try:
            task = task_queue.get(timeout=3)  # 3초 대기 후 종료
            print(f"[Worker-{worker_id}] Processing: {task['id']}")
            time.sleep(0.5)  # 작업 처리 시뮬레이션
            task_queue.task_done()
        except queue.Empty:
            print(f"[Worker-{worker_id}] No more tasks. Exiting.")
            break

# --- 실행 ---
NUM_WORKERS = 3

# Worker 스레드 시작
threads = []
for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(i,))
    t.start()
    threads.append(t)

# Producer 실행
producer(10)

# 모든 Worker 완료 대기
for t in threads:
    t.join()

print("All tasks completed.")
```

**동작 흐름**
```
Producer  →  [ task_0, task_1, ..., task_9 ]  →  Worker-0
                         Queue                 →  Worker-1
                                               →  Worker-2
```

---

## 핵심 속성

### At-most-once vs At-least-once
| 속성 | 설명 |
|---|---|
| At-most-once | 작업이 유실될 수 있으나 중복 처리 없음 |
| At-least-once | 실패 시 재시도 → 중복 처리 가능성 있음 (→ 멱등성 필요) |

### Acknowledgement (ack)
Worker가 작업을 성공적으로 완료한 뒤 큐에 신호를 보내는 방식. ack 전에 Worker가 죽으면 큐는 해당 작업을 다시 다른 Worker에게 전달한다.

---

## 응용

### DE 파이프라인에서의 적용

**Airflow DAG 태스크 분배**

Airflow 자체가 Work-Queue Pattern의 구현체다. DAG에 정의된 태스크들이 큐(Celery/Redis/RabbitMQ)에 쌓이고 여러 Worker가 이를 병렬로 처리한다.

```
DAG Scheduler  →  Task Queue (Redis/RabbitMQ)  →  Celery Worker-1
                                                →  Celery Worker-2
                                                →  Celery Worker-3
```

**대용량 파일 수집 파이프라인**

```python
# 예: S3에 쌓인 파일 목록을 큐에 넣고 Worker가 처리
import boto3
import queue
import threading

s3 = boto3.client("s3")
file_queue = queue.Queue()

def producer(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get("Contents", []):
        file_queue.put(obj["Key"])

def worker(worker_id, bucket):
    while True:
        try:
            key = file_queue.get(timeout=5)
            # 파일 다운로드 → 파싱 → DB 적재
            process_file(bucket, key)
            file_queue.task_done()
        except queue.Empty:
            break

def process_file(bucket, key):
    print(f"Processing s3://{bucket}/{key}")
    # ... 실제 처리 로직
```

**실무 포인트**
- Worker 수 = CPU bound이면 CPU 코어 수, I/O bound이면 그 이상으로 조정
- 큐가 너무 오래 쌓이면 **Dead Letter Queue(DLQ)**로 실패 작업 격리
- 멱등성(idempotency) 보장: 같은 파일을 두 번 처리해도 결과가 동일해야 함

---

> Work Queue Pattern은 생산자-소비자 속도 불균형을 큐로 완충하고 Worker 수 조정으로 처리량을 탄력적으로 확장하는 패턴이다.
> "언제 처리할지는 Worker가 결정한다."