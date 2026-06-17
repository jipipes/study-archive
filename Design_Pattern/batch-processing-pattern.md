# Batch Processing Pattern

## 개념

**Batch Processing Pattern**은 전체 데이터셋을 한 번에 메모리에 올리는 대신 일정 크기의 작은 조각(미니배치)으로 나누어 순차적으로 처리하는 패턴이다.

메모리 제약이 있는 환경에서 대용량 데이터셋을 다룰 수 있도록 한다.

---

## 발생 배경 / 사용 조건

아래 상황에서 Batch Processing Pattern이 필요해진다.

- **데이터셋이 메모리보다 클 때**: 전체 데이터를 한 번에 올리면 OOM(Out of Memory) 에러 발생
- **복잡한 수학 연산이 필요할 때**: 행렬 연산, 그레이디언트 계산 등은 메모리를 추가로 소비
- **데이터셋 크기가 가변적일 때**: 미니배치 크기만 조정하면 어떤 크기의 데이터셋도 처리 가능

---

## 발생하는 문제

```python
import numpy as np

# 전체 데이터셋을 한 번에 메모리에 올리는 경우
dataset = np.load("huge_dataset.npy")  # 100GB 데이터셋

# OOM 에러 발생 가능
result = heavy_computation(dataset)
```

**문제점**
- 데이터셋이 가용 메모리를 초과하면 프로세스 자체가 종료됨
- 연산에 필요한 중간 결과물까지 메모리에 올라가면 실제 한계는 더 낮아짐
- 데이터셋이 커질수록 서버 사양을 계속 올려야 하는 수직 확장 의존

---

## 구조 및 코드 예시

```python
import numpy as np

def load_dataset(path):
    """전체 데이터셋 로드 (파일에서 읽기)"""
    return np.load(path, mmap_mode='r')  # 메모리 맵으로 실제 로드는 배치 단위로

def get_mini_batch(dataset, batch_size, batch_idx):
    """배치 인덱스에 해당하는 미니배치 반환"""
    start = batch_idx * batch_size
    end = start + batch_size
    return dataset[start:end]

def train_one_batch(model, batch):
    """미니배치로 모델 학습 (그레이디언트 계산 및 업데이트)"""
    # 실제 학습 로직
    loss = compute_loss(model, batch)
    gradient = compute_gradient(loss)
    update_model(model, gradient)
    return loss

# --- 배치처리 실행 ---
dataset = load_dataset("huge_dataset.npy")
batch_size = 32
num_batches = len(dataset) // batch_size
num_epochs = 10

for epoch in range(num_epochs):
    total_loss = 0
    for batch_idx in range(num_batches):
        # 미니배치 단위로 메모리에 올림
        batch = get_mini_batch(dataset, batch_size, batch_idx)
        loss = train_one_batch(model, batch)
        total_loss += loss
        # 배치 처리 후 메모리에서 해제됨
    print(f"Epoch {epoch+1}, Loss: {total_loss / num_batches:.4f}")
```

**동작 흐름**
```
전체 데이터셋 (디스크)
    │
    ├── 미니배치 0  →  학습  →  파라미터 업데이트
    ├── 미니배치 1  →  학습  →  파라미터 업데이트
    ├── ...
    └── 미니배치 N  →  학습  →  파라미터 업데이트
              ↑ 1 Epoch 완료 후 반복
```

---

## 배치 크기 결정

배치 크기는 학습 성능과 메모리 사용량에 직접적인 영향을 미친다.

| 배치 크기 | 특징 |
|---|---|
| 작을수록 | 메모리 사용 ↓, 그레이디언트 노이즈 ↑, 수렴 불안정 |
| 클수록 | 메모리 사용 ↑, 그레이디언트 안정적, 병렬 처리 효율 ↑ |

배치 크기를 자동으로 최적화하는 프레임워크도 존재한다.
- **AdaptDL**: 학습 중 시스템 성능과 그레이디언트 노이즈 스케일을 측정해 최적 배치 크기를 동적으로 선택

---

## 제약 사항

배치처리 패턴은 모든 알고리즘에 적용 가능하지 않다.

- **적용 가능**: 데이터를 스트리밍 방식으로 순차 처리할 수 있는 알고리즘 (SGD, 딥러닝 등)
- **적용 불가**: 전체 데이터셋을 한 번에 봐야 하는 알고리즘 (일부 통계적 방법, 전체 데이터 기반 정규화 등)

---

## 응용

### DE 파이프라인에서의 적용

**대용량 CSV 파일 처리**

ETL 파이프라인에서 수십 GB의 로그 파일을 처리할 때 배치처리 패턴을 그대로 활용한다.

```python
import pandas as pd

def process_large_csv(filepath, batch_size=10_000):
    """청크 단위로 대용량 CSV 처리"""
    chunk_iter = pd.read_csv(filepath, chunksize=batch_size)

    results = []
    for i, chunk in enumerate(chunk_iter):
        # 전처리
        chunk = chunk.dropna()
        chunk["processed"] = chunk["value"].apply(transform)

        # DB 적재
        chunk.to_sql("target_table", con=engine, if_exists="append", index=False)
        print(f"Chunk {i+1} processed: {len(chunk)} rows")

    print("Done.")

process_large_csv("server_logs_2024.csv")
```

**실무 포인트**
- `pd.read_csv(chunksize=N)`, `spark.readStream` 모두 배치처리 패턴의 구현체
- 배치 크기는 가용 메모리의 약 50~70% 이내로 설정 (중간 연산용 여유 확보)
- 배치 처리는 기본적으로 순차적 → 병렬화가 필요하면 샤딩 패턴과 결합

---

> Batch Processing Pattern은 전체 데이터를 메모리에 올리는 대신 미니배치 단위로 쪼개어 순차적으로 처리하는 패턴이다.
> "한 번에 다 먹으려 하지 말고 한 입씩 씹어라."