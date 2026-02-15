# CLAUDE.md

## Build & Deploy

Python 소스 코드를 수정한 후 앱 재시작이 필요한 경우, 반드시 `build_macos.sh` 스크립트를 실행하여 macOS 앱 번들을 빌드하고 `/Applications`에 설치해야 한다.

```bash
bash build_macos.sh
```

이 스크립트는 다음을 수행한다:
1. 실행 중인 TG Forwarder 앱 종료
2. PyInstaller로 앱 번들 빌드
3. `/Applications/TG Forwarder.app`에 설치

앱 재시작이 필요한 케이스:
- signal_trader/, core/, dashboard/ 등 Python 소스 수정
- requirements.txt 변경
- .spec 파일 변경
- 템플릿(templates/) 수정
