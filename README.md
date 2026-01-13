# IoT Project in Java

# IoT Sensor Simulation with Spring Boot

## Project Overview (프로젝트 개요)
Java & Spring Boot로 제조 센서 데이터 시뮬레이션 + AWS IoT 전송 구현. 웹 대시보드 추가로 실시간 모니터링.

## Tech Stack (기술 스택)
- Java 17
- Spring Boot 3.x (웹 API)
- AWS IoT Core (데이터 전송)
- Maven (의존성 관리)

## Problem Solving (문제 해결 과정)
1. 센서 데이터 랜덤 생성.
2. AWS Thing 등록 & 인증서 로드.
3. Spring Boot @Scheduled로 5초 자동 전송.
4. /data API로 JSON 리턴 – 제조 DX 다운타임 감소 어필.

## How to Run (실행 방법)
- mvn clean install
- Run IotSpringBootApplication – localhost:8080/data 확인.
- AWS Test에서 factory/sensor/data 토픽 구독.

## Appeal Point (어필 포인트)
제조 DX 기여 – AWS SAA 기반 Cloud 경험.

이후 [[[스크린샷 추가]]]
이후 [[[DB 추가]]]
