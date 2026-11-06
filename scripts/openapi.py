"""Write the OpenAPI document of the service for the types of the frontend.

The frontend generates its TypeScript types from `frontend/src/api/openapi.json`;
a test fails when the committed file differs from the document of the service.

```bash
uv run python scripts/openapi.py
```
"""

import json
from pathlib import Path

from simrun.service.app import openapi_document

TARGET = (
    Path(__file__).resolve().parent.parent / "frontend" / "src" / "api" / "openapi.json"
)


def main() -> None:
    """Write the document with sorted keys and a trailing newline."""
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(openapi_document(), indent=2, sort_keys=True)
    TARGET.write_text(text + "\n")
    print(f"wrote {TARGET}")


if __name__ == "__main__":
    main()
