# Instorage Preprocess Operator 배포 가이드

## 배포 순서

### 1. CRD 설치
```bash
kubectl apply -f crd.yaml
```

### 2. Operator 배포
```bash
kubectl apply -f operator.yaml
```

### 또는 Kustomize 사용
```bash
kubectl apply -k .
```

## 사용 방법

### InstorageJob 생성
```bash
kubectl apply -f sample-job.yaml
```

### 상태 확인
```bash
# InstorageJob 목록 확인
kubectl get instoragejobs
kubectl get ij  # 단축 이름 사용

# 상세 정보 확인
kubectl describe instoragejob sample-preprocess-job

# 생성된 Kubernetes Job 확인
kubectl get jobs

# Operator 로그 확인
kubectl logs -n instorage-system deployment/instorage-preprocess-operator
```

## 경로 Prefix 동작

- **CSD 활성화** (`csd.enabled: true`):
  - dataPath: `/dataset/input` → `/home/ngd/storage/dataset/input`
  - outputPath: `/dataset/output` → `/home/ngd/storage/dataset/output`

- **CSD 비활성화** (`csd.enabled: false`):
  - dataPath: `/dataset/input` → `/mnt/dataset/input`
  - outputPath: `/dataset/output` → `/mnt/dataset/output`

## 삭제

```bash
# InstorageJob 삭제
kubectl delete instoragejob sample-preprocess-job

# Operator 삭제
kubectl delete -f operator.yaml

# CRD 삭제
kubectl delete -f crd.yaml

# 또는 Kustomize로 전체 삭제
kubectl delete -k .
```