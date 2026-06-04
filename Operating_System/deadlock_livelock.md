# Deadlock & Livelock
 
Race Condition이 동기화의 **부재**로 생기는 문제라면 
Deadlock과 Livelock은 동기화를 **잘못 사용**(과잉)해서 생기는 문제다.
 
---

## Deadlock (교착 상태)
 
두 개 이상의 프로세스가 서로가 점유한 자원을 기다리며 **영원히 진행되지 못하는 상태**.  
모두가 기다리기 때문에 아무것도 실행되지 않는다

### 발생 조건 (4가지 모두 충족 시 발생)
| 조건 | 설명 |
|------|------|
| **상호 배제** (Mutual Exclusion) | 자원은 한 번에 하나의 프로세스만 사용 가능 |
| **점유 대기** (Hold and Wait) | 자원을 점유한 채로 다른 자원을 기다림 |
| **비선점** (No Preemption) | 다른 프로세스의 자원을 강제로 빼앗을 수 없음 |
| **순환 대기** (Circular Wait) | 프로세스들이 원형으로 서로의 자원을 기다림 |
 
> 4가지 중 하나라도 깨면 Deadlock은 발생하지 않는다.

### 예시
 
```python
import threading
 
lock_a = threading.Lock()
lock_b = threading.Lock()
 
def task_1():
    with lock_a:          # A 획득
        with lock_b:      # B 기다림 → task_2가 B를 잡고 있으면 여기서 멈춤
            print("task_1 완료")
 
def task_2():
    with lock_b:          # B 획득
        with lock_a:      # A 기다림 → task_1이 A를 잡고 있으면 여기서 멈춤
            print("task_2 완료")
 
t1 = threading.Thread(target=task_1)
t2 = threading.Thread(target=task_2)
t1.start(); t2.start()
# 타이밍에 따라 두 스레드가 서로를 기다리며 영원히 멈출 수 있다
```
 
```
task_1: A 점유 → B 대기 ──┐
                           ↕  ← 순환 대기 → Deadlock
task_2: B 점유 → A 대기 ──┘
```

### 해결 방법
 
**1. 순환 대기 제거 — 자원 획득 순서 고정**
 
모든 스레드가 항상 같은 순서(A → B)로 락을 획득하면 순환이 생기지 않는다
 
```python
def task_1():
    with lock_a:
        with lock_b:
            print("task_1 완료")
 
def task_2():
    with lock_a:      # task_2도 A → B 순서로 통일
        with lock_b:
            print("task_2 완료")
```
 
**2. 타임아웃 설정**
 
일정 시간 내에 락을 획득하지 못하면 포기하고 재시도하도록 설정한다
 
```python
acquired = lock_a.acquire(timeout=1)  # 1초 안에 못 잡으면 False 반환
if acquired:
    try:
        # 작업 수행
    finally:
        lock_a.release()
else:
    # 재시도 or 다른 처리
```

---

## Livelock (라이브락)
 
프로세스들이 **실행은 되고 있지만** 서로를 배려하다가 결국 아무것도 진행되지 않는 상태  
Deadlock과 달리 멈춰 있지 않아서 겉으로 보면 정상처럼 보인다

> 좁은 복도에서 두 사람이 마주쳤을 때 서로 비켜주려고 같은 방향으로 동시에 움직이기를 반복하는 상황. 움직이고 있지만 아무도 지나가지 못한다.

### 예시
 
```python
import threading, time
 
resource_free = True
 
def process_a():
    global resource_free
    while True:
        if resource_free:
            resource_free = False   # 자원 점유 시도
            time.sleep(0.1)
            print("A: 작업 완료")
            resource_free = True
            break
        else:
            print("A: 양보")        # B를 위해 양보
            time.sleep(0.1)
 
def process_b():
    global resource_free
    while True:
        if resource_free:
            resource_free = False
            time.sleep(0.1)
            print("B: 작업 완료")
            resource_free = True
            break
        else:
            print("B: 양보")        # A를 위해 양보
            time.sleep(0.1)
 
# A와 B가 동시에 실행되면 서로 양보하다가 둘 다 진행을 못 할 수 있다
```

### 해결 방법
 
- 무작위 대기(Random Backoff): 재시도 전에 랜덤한 시간만큼 기다려 타이밍을 어긋나게 한다
```python
import random
 
time.sleep(random.uniform(0, 0.5))  # 재시도 전 랜덤 대기
```
- 우선순위 부여: 자원 접근 시 우선순위를 다르게 두어 특정 프로세스가 먼저 자원을 획득할 수 있도록 구조화
- 상태 체크 강화: 락을 획득하고 해제하는 단순 반복 패턴을 깨고 자원 사용의 성공 여부를 명확히 확인
 
---

## Deadlock vs Livelock 비교
 
| 구분 | Deadlock | Livelock |
|------|----------|----------|
| 프로세스 상태 | 완전히 멈춤 | 계속 실행 중 |
| 진행 여부 | 없음 | 없음 (하지만 바빠 보임) |
| 감지 난이도 | 비교적 쉬움 (CPU 사용률 0) | 어려움 (CPU 사용률 높음) |
| 원인 | 자원을 서로 기다림 | 서로를 배려하다 충돌 |
| 해결 방향 | 순환 제거, 타임아웃 | 랜덤 백오프, 우선순위 부여 |
 
---

## 💡 응용: 
 
Deadlock: 분산 파이프라인의 업스트림 대기
 
Airflow에서 DAG A가 DAG B의 결과를 기다리고, DAG B가 DAG A의 결과를 기다리도록 의존성을 잘못 설정하면 둘 다 영원히 대기 상태(queued)가 된다. 코드가 잘못된 게 아니라 의존성 설계가 잘못된 것이라 찾기 어렵다
 
Livelock: API 재시도 폭풍 (Retry Storm)
 
여러 서비스가 동시에 같은 외부 API에 실패하고 모두 같은 간격으로 재시도하면 계속 충돌하며 성공하지 못한다. AWS나 GCP SDK가 지수 백오프(Exponential Backoff) + 지터(Jitter)를 기본으로 사용하는 이유가 바로 이것
 
```
고정 재시도 간격 → 동시에 몰림 → 또 실패 → 반복 (Livelock)
랜덤 지터 추가   → 타이밍 분산 → 순차적으로 성공
```
 
> Deadlock은 "설계의 문제", Livelock은 "타이밍의 문제"로 기억해두면  
> 실제 시스템에서 어느 쪽인지 구분하는 데 도움이 된다.
