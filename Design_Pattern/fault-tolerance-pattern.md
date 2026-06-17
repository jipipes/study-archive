# Fault Tolerance Pattern (탄력성 및 내결함성 패턴)

## 개념

**Fault Tolerance Pattern**은 분산 학습 환경에서 Worker 장애, 네트워크 불안정, 데이터 손상 등 예기치 못한 실패가 발생해도 학습을 중단하지 않고 복구하거나 재개할 수 있도록 설계하는 패턴이다.

분산 시스템은 구성 요소가 많을수록 장애 가능성이 높아지기 때문에 "장애가 발생하지 않는 시스템"보다 "장애가 발생해도 회복할 수 있는 시스템"을 목표로 한다.

---

## 발생 배경 / 사용 조건

아래 상황에서 Fault Tolerance Pattern이 필요해진다.

- **학습 시간이 길어질수록**: 수십 시간~수일 걸리는 학습 중 장애 발생 시 처음부터 재시작하면 비용이 막대함
- **공유 클러스터 환경**: 다른 작업에 의해 Worker가 선점(preemption)되어 학습이 중단될 수 있음
- **원격 저장소에서 데이터를 가져올 때**: 네트워크 불안정으로 인한 학습 중단 가능성

---

## 장애 시나리오

| 시나리오 | 원인 | 영향 |
|---|---|---|
| 데이터 손상 | 파일 손상, 잘못된 포맷 | 해당 배치 처리 불가, 학습 중단 |
| 네트워크 불안정 | 인프라 장애, 기상 악화 | Worker ↔ 파라미터 서버 통신 끊김 |
| Worker 선점 | 높은 우선순위 작업 스케줄링 | 해당 Worker 강제 종료 |
| OOM | 배치 크기 과다, 메모리 누수 | Worker 프로세스 종료 |

---

## 발생하는 문제

```python
# 체크포인트 없이 분산 학습하는 경우
for epoch in range(100):
    for batch in dataloader:
        train(model, batch)
    print(f"Epoch {epoch+1} done")

# 90 에포크 학습 중 Worker 장애 발생 → 처음부터 재시작
# 90 에포크 분량의 학습 비용 전부 낭비
```

**문제점**
- 장애 발생 지점까지의 학습 결과가 전부 유실됨
- 분산 환경에서 한 Worker 장애가 전체 학습 프로세스를 블로킹
- 장애 원인 해결 후 처음부터 재시작해야 하는 높은 재시작 비용

---

## 해결: 체크포인트 (Checkpoint)

학습 중간 상태를 주기적으로 저장해두고 장애 발생 시 가장 최근 체크포인트에서 재개한다.

```python
import torch
import os

def save_checkpoint(model, optimizer, epoch, loss, path):
    """체크포인트 저장"""
    torch.save({
        'epoch': epoch,
        'model_state_dict': model.state_dict(),
        'optimizer_state_dict': optimizer.state_dict(),
        'loss': loss,
    }, path)
    print(f"Checkpoint saved: epoch {epoch}, loss {loss:.4f}")

def load_checkpoint(model, optimizer, path):
    """체크포인트 로드"""
    if not os.path.exists(path):
        print("No checkpoint found. Starting from scratch.")
        return 0

    checkpoint = torch.load(path)
    model.load_state_dict(checkpoint['model_state_dict'])
    optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
    start_epoch = checkpoint['epoch'] + 1
    print(f"Checkpoint loaded. Resuming from epoch {start_epoch}")
    return start_epoch

# --- 실행 ---
model = MyModel()
optimizer = torch.optim.Adam(model.parameters())
checkpoint_path = "/checkpoints/latest.pt"

start_epoch = load_checkpoint(model, optimizer, checkpoint_path)

for epoch in range(start_epoch, 100):
    total_loss = 0
    for batch in dataloader:
        loss = train_step(model, optimizer, batch)
        total_loss += loss

    avg_loss = total_loss / len(dataloader)

    # 5 에포크마다 체크포인트 저장
    if (epoch + 1) % 5 == 0:
        save_checkpoint(model, optimizer, epoch, avg_loss, checkpoint_path)
```

---

## 해결: 장애 Worker 제외 후 재구성

집합 통신 패턴에서 특정 Worker 통신이 불안정하면 해당 Worker를 제외하고 남은 Worker로 새 그룹을 구성해 학습을 재개한다.

```python
import torch.distributed as dist

def rebuild_process_group(active_ranks):
    """활성 Worker만으로 새 프로세스 그룹 생성"""
    new_group = dist.new_group(ranks=active_ranks)
    return new_group

def train_with_fault_tolerance(model, dataloader, max_retries=3):
    all_ranks = list(range(dist.get_world_size()))
    active_ranks = all_ranks.copy()

    for retry in range(max_retries):
        try:
            group = rebuild_process_group(active_ranks)
            for batch in dataloader:
                train_step(model, batch, group)
            break  # 성공 시 루프 탈출

        except RuntimeError as e:
            print(f"Worker failure detected: {e}")
            # 실패한 Worker 제외
            failed_rank = detect_failed_rank()
            active_ranks.remove(failed_rank)
            print(f"Excluding rank {failed_rank}. Active: {active_ranks}")

            if not active_ranks:
                raise RuntimeError("All workers failed.")
```

---

## 체크포인트 저장 위치

| 저장 위치 | 특징 |
|---|---|
| 로컬 디스크 | 빠르지만 Worker 장애 시 함께 유실 가능 |
| 공유 파일 시스템 (NFS, HDFS) | 여러 Worker가 접근 가능, 단일 장애점 아님 |
| 오브젝트 스토리지 (S3, GCS) | 높은 내구성, 네트워크 I/O 비용 존재 |

프로덕션 환경에서는 **오브젝트 스토리지**에 저장하는 것이 일반적이다.

---

## 응용

### DE 파이프라인에서의 적용

**Airflow 태스크 재시도**

Airflow는 태스크 단위 체크포인트 개념을 내장하고 있다. 태스크가 실패하면 DAG 전체를 재시작하지 않고 해당 태스크만 재시도한다.

```python
from airflow.decorators import dag, task
import pendulum

@dag(start_date=pendulum.datetime(2024, 1, 1), schedule="@daily")
def resilient_pipeline():

    @task(retries=3, retry_delay=pendulum.duration(minutes=5))
    def extract():
        # 실패 시 최대 3회 재시도, 5분 간격
        data = fetch_from_api()
        return data

    @task
    def transform(data):
        return clean_and_transform(data)

    @task
    def load(data):
        data.to_sql("target_table", con=engine, if_exists="append")

    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

resilient_pipeline()
```

**데이터 손상 처리**

```python
def safe_process_batch(batch, skip_on_error=True):
    """손상된 데이터가 포함된 배치 안전 처리"""
    valid_records = []
    error_records = []

    for record in batch:
        try:
            validated = validate_schema(record)
            valid_records.append(validated)
        except (ValueError, KeyError) as e:
            error_records.append({"record": record, "error": str(e)})

    if error_records:
        # 손상 데이터는 Dead Letter Queue로 격리
        send_to_dlq(error_records)
        print(f"{len(error_records)} records sent to DLQ")

    return valid_records
```

**포인트**
- 체크포인트 저장 주기는 "저장 비용 vs 장애 시 재작업 비용" 트레이드오프로 결정
- 파라미터 서버 장애 시 해당 서버의 모델 파티션 체크포인트를 다른 서버들이 나눠 로드해야 함
- Dead Letter Queue(DLQ)로 처리 불가 데이터를 격리해 파이프라인 전체가 멈추지 않도록 설계

---

> Fault Tolerance Pattern은 장애가 발생해도 체크포인트 복구와 Worker 재구성으로 학습을 이어가도록 설계하는 패턴이다.
> "장애를 막을 수 없다면, 장애가 와도 멈추지 않는 시스템을 만들어라."