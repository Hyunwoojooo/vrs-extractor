# aria-vrs-extractor

Python/CLI 도구로 Meta Project Aria Gen1 VRS 로그를 JPEG(이미지)/WAV(오디오)/JSONL(센서 메타)로 분해하고, 타임스탬프 정렬 `events.jsonl`과 추출 메타데이터 `manifest.json`까지 자동 생성합니다. 추출은 idempotent 하므로 이미 처리된 단계는 `_status/*.done` 마커로 건너뜁니다.

## 요구 사항

- Python 3.10+
- `projectaria_tools` 바이너리가 설치돼 있어 `_core_pybinds.*` 모듈을 불러올 수 있어야 합니다.
- (선택) `fsspec`을 설치하면 S3 등 원격 경로도 다룰 수 있습니다.

## 설치 / 환경 준비

이 저장소는 패키지로 빌드하지 않고 바로 사용하도록 구성했습니다. CLI를 실행할 때마다 `PYTHONPATH`에 패키지 루트를 포함시키면 됩니다.

```bash
export PYTHONPATH=$PWD/vrs_extract  # projectaria_tools 저장소 루트에서 실행
```

이후 `python -m aria_vrs_extractor ...` 형태로 명령을 호출합니다.

## 사용법

출력 루트는 스펙에 맞춰 다음과 같은 구조를 만듭니다.

```
/raw/aria/{recording_id}/
  rgb/frames/frame_*.jpg
  et/left/frame_*.jpg
  audio/chunk_*.wav
  sensors/{rgb,et,mic,imu,gps,wifi,bt,events}.jsonl
  manifest/manifest.json
  _status/*.done
```

### 1. 센서별 추출

각 명령은 단일 센서를 처리합니다. 이미 수행한 단계는 `_status/{step}.done`이 존재하면 자동으로 건너뜁니다. 강제로 재실행하려면 `--force` 옵션을 추가하세요.

```bash
# RGB → JPEG + sensors/rgb.jsonl
python -m aria_vrs_extractor extract-rgb \
  --vrs ~/Documents/vrs_files/rec01.vrs \
  --out ~/Documents/projectaria_sandbox/projectaria_tools/vrs_output/raw/aria/rec01 \
  --device-id devA --recording-id rec01

# Eye-tracking 프레임 → JPEG + sensors/et.jsonl
python -m aria_vrs_extractor extract-et --vrs ... --out ... --device-id devA --recording-id rec01

# 오디오 → chunk_*.wav + sensors/mic.jsonl
python -m aria_vrs_extractor extract-audio --vrs ... --out ... --device-id devA --recording-id rec01

# 기타 센서 JSONL
python -m aria_vrs_extractor extract-imu  --vrs ... --out ... --device-id devA --recording-id rec01
python -m aria_vrs_extractor extract-gps  --vrs ... --out ... --device-id devA --recording-id rec01
python -m aria_vrs_extractor extract-wifi --vrs ... --out ... --device-id devA --recording-id rec01
python -m aria_vrs_extractor extract-bt   --vrs ... --out ... --device-id devA --recording-id rec01
```

각 JSONL은 `ts_ns`, `stream_id`, 센서별 payload, `quality_flags`를 포함합니다. JPEG/WAV 파일 경로는 JSONL의 `uri` 필드를 통해 참조할 수 있습니다.

### 2. 이벤트 병합

센서 JSONL을 시간 순으로 하나의 `events.jsonl`로 합칩니다.

```bash
python -m aria_vrs_extractor merge-events \
  --root ~/Documents/projectaria_sandbox/projectaria_tools/vrs_output/raw/aria/rec01
```

### 3. 매니페스트 작성

파일별 체크섬·바이트 수·건수·타임스탬프 범위, lineage, 파티션 키를 담은 `manifest/manifest.json`을 생성합니다.

```bash
python -m aria_vrs_extractor write-manifest \
  --root ~/Documents/projectaria_sandbox/projectaria_tools/vrs_output/raw/aria/rec01 \
  --owner lab-x --tool-version aria_extractor:0.1.0 \
  --device-id devA --recording-id rec01 \
  --upstream vrs://rec01.vrs --transform aria_tools_extract
```

## 테스트

빠른 단위 테스트는 다음과 같이 실행합니다.

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=$PWD/vrs_extract pytest vrs_extract/tests
```

## 참고 사항

- `_status/` 디렉터리는 각 단계의 완료 요약(JSON)을 보관해 재실행 시 중복 작업을 방지합니다.
- 오디오 스트림은 VRS 레코드(2048/4096 샘플) 단위로 WAV 파일을 생성하며, 추후 필요 시 JSONL 메타데이터를 이용해 병합할 수 있습니다.
- Wi-Fi/BT 스트림이 활성화돼 있어도 해당 기록이 없으면 JSONL은 빈 파일로 남습니다.
- `--force` 옵션을 쓰면 특정 단계만 다시 생성할 수 있습니다.
