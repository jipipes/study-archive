# Race Condition
 
## 개념
 
두 개 이상의 프로세스(또는 스레드)가 **공유 자원에 동시에 접근**할 때, 실행 순서에 따라 결과가 달라지는 상태.
 
"누가 먼저 실행되느냐"에 따라 결과가 달라지기 때문에 **재현이 어렵고 디버깅이 까다롭다**.
 
---
 
## 발생 조건
 
Race Condition이 발생하려면 아래 세 가지가 동시에 충족되어야 한다.
 
1. **공유 자원** 이 존재한다 (메모리, 파일, DB 등)
2. **최소 하나의 쓰기(write) 작업** 이 발생한다
3. **동기화(synchronization) 메커니즘이 없다**
---

## 발생 시 문제점
 
| 문제 | 설명 |
|------|------|
| **데이터 불일치** | 두 스레드가 같은 값을 읽고 각자 수정하면 한쪽의 변경이 유실된다 |
| **비결정적 동작** | 실행할 때마다 결과가 달라져 테스트로 잡아내기 어렵다 |
| **디버깅 난이도** | 타이밍에 따라 발생하기 때문에 재현 자체가 불안정하다 |
| **시스템 신뢰성 저하** | 금융 트랜잭션, 재고 관리 등 정확성이 중요한 도메인에서는 치명적 결함이 된다 |

---
 
## 예시
 
```python
# 전역 변수를 두 스레드가 동시에 수정하는 경우
import threading
 
counter = 0
 
def increment():
    global counter
    for _ in range(100000):
        counter += 1  # read → modify → write 세 단계로 나뉨
 
t1 = threading.Thread(target=increment)
t2 = threading.Thread(target=increment)
 
t1.start(); t2.start()
t1.join();  t2.join()
 
print(counter)  # 200000이 되어야 하지만, 실행할 때마다 다른 값이 나온다
```
 
`counter += 1`은 원자적(atomic) 연산이 아니다.  
내부적으로 `읽기 → 더하기 → 쓰기` 세 단계로 쪼개지기 때문에, 두 스레드가 같은 값을 읽고 각각 더하면 하나의 증가가 유실된다.
 
---
 
## 해결 방법
 
### 1. Mutex (상호 배제)
 
한 번에 하나의 스레드만 임계 구역(Critical Section)에 진입하도록 잠금을 건다.
 
```python
import threading
 
counter = 0
lock = threading.Lock()
 
def increment():
    global counter
    for _ in range(100000):
        with lock:          # 잠금 획득
            counter += 1   # 임계 구역
                           # 잠금 자동 해제
```
 
### 2. Semaphore
 
Mutex의 일반화 버전. 동시에 접근할 수 있는 스레드 수를 N개로 제한한다.  
(Mutex = 허용 수가 1인 Semaphore)
 
```python
semaphore = threading.Semaphore(3)  # 최대 3개 스레드 동시 허용
 
def worker():
    with semaphore:
        # 공유 자원 접근
        pass
```
 
### 3. 원자적 연산 (Atomic Operation)
 
하드웨어 또는 언어 레벨에서 중단 없이 실행되도록 보장된 연산을 사용한다.
 
```python
import threading
 
counter = 0
lock = threading.Lock()
 
# Python의 GIL이 일부 보호하지만, 명시적 atomic 연산을 쓰는 것이 안전하다
```
 
---
 
## 핵심 개념: 임계 구역 (Critical Section)
 
공유 자원에 접근하는 코드 블록.  
이 구간에는 반드시 하나의 스레드만 진입해야 한다.
 
```
진입 구역 → [임계 구역] → 탈출 구역 → 나머지 구역
```
 
---
 
## 💡 응용: 이 개념이 실제로 쓰이는 곳
 
Race Condition은 OS 교과서 개념이지만, 분산 시스템이나 데이터 파이프라인으로 가면 다른 얼굴로 나타난다.
 
**파이프라인에서의 동시 쓰기 문제**
 
여러 Airflow Task나 Spark Job이 같은 S3 경로나 테이블 파티션에 동시에 쓰는 경우 어떤 데이터가 살아남을지 보장할 수 없다. 이것도 Race Condition이다.
 
```
Task A ──┐
         ├──▶ s3://bucket/output/date=2024-01-01/  ← 누가 마지막에 쓰냐에 따라 결과가 달라짐
Task B ──┘
```
 
해결 방식도 비슷하다. 테이블 단위 잠금(LOCK), 파티션 단위 원자적 덮어쓰기(atomic overwrite), 또는 쓰기 전에 임시 경로에 올리고 완료 후 rename하는 패턴이 같은 원리다.
 
> 부트캠프 프로젝트에서 Airflow DAG 여러 개가 같은 S3 prefix에 결과를 올릴 때  
> 실행 순서를 명시적으로 지정하지 않으면 데이터가 덮어씌워질 수 있다는 걸 경험했다.  
> 그게 바로 분산 환경의 Race Condition이었다고 지금와서 생각한다.