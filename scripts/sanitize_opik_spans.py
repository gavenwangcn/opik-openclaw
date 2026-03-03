#!/usr/bin/env python3
"""Sanitize problematic media image references in existing Opik spans.

This script rewrites string occurrences like:
  media:https://example.com/image.jpg
  media:./image.jpg
  media:/tmp/image.png
to:
  media:<image-ref>

Dry-run is the default. Pass --apply to persist changes.
"""

from __future__ import annotations

import argparse
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

import opik

MEDIA_IMAGE_REFERENCE_RE = re.compile(
    r"""\bmedia:(?:https?://[^\s"'`]+|\.[/][^\s"'`]+|[/][^\s"'`]+|[^\s"'`]+)\.(?:jpe?g|png|webp|gif)(?=[\s"'`]|$)""",
    re.IGNORECASE,
)


def sanitize_string_for_opik(value: str) -> str:
    return MEDIA_IMAGE_REFERENCE_RE.sub("media:<image-ref>", value)


def sanitize_value_for_opik(value: Any) -> Any:
    if isinstance(value, str):
        return sanitize_string_for_opik(value)
    if isinstance(value, list):
        return [sanitize_value_for_opik(item) for item in value]
    if isinstance(value, dict):
        return {key: sanitize_value_for_opik(child) for key, child in value.items()}
    return value


@dataclass
class Credentials:
    host: Optional[str]
    api_key: Optional[str]


def load_opik_credentials(config_path: pathlib.Path) -> Credentials:
    host: Optional[str] = None
    api_key: Optional[str] = None

    if not config_path.exists():
        return Credentials(host=host, api_key=api_key)

    for raw in config_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = [part.strip() for part in line.split("=", 1)]
        if key == "url_override":
            host = value.rstrip("/")
        elif key == "api_key":
            api_key = value

    return Credentials(host=host, api_key=api_key)


def get_field(span: Any, field: str) -> Any:
    if hasattr(span, field):
        return getattr(span, field)
    data = span.model_dump() if hasattr(span, "model_dump") else span.to_dict()
    return data.get(field)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True, help="Opik project UUID")
    parser.add_argument("--workspace", required=True, help="Opik workspace")
    parser.add_argument(
        "--trace-id",
        help="Optional single trace UUID; when omitted, scans spans across the whole project.",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=1000,
        help="Maximum number of spans to scan (default: 1000)",
    )
    parser.add_argument(
        "--config",
        default=str(pathlib.Path.home() / ".opik.config"),
        help="Path to opik config file (default: ~/.opik.config)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist updates to Opik. Without this flag, only prints a dry-run summary.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    creds = load_opik_credentials(pathlib.Path(args.config))

    client = opik.Opik(
        workspace=args.workspace,
        host=creds.host,
        api_key=creds.api_key,
    )

    project = client.get_project(args.project_id)
    project_name = project.name
    spans = client.search_spans(
        project_name=project_name,
        trace_id=args.trace_id,
        max_results=args.max_results,
        truncate=False,
    )

    scanned = 0
    changed = 0
    updated = 0

    for span in spans:
        scanned += 1
        patch: Dict[str, Any] = {}
        for field in ("input", "output", "metadata"):
            original = get_field(span, field)
            sanitized = sanitize_value_for_opik(original)
            if sanitized != original:
                patch[field] = sanitized

        if not patch:
            continue

        changed += 1
        print(f"match span={span.id} trace={span.trace_id} fields={','.join(sorted(patch.keys()))}")

        if not args.apply:
            continue

        client.update_span(
            id=span.id,
            trace_id=span.trace_id,
            parent_span_id=span.parent_span_id,
            project_name=project_name,
            **patch,
        )
        updated += 1

    mode = "apply" if args.apply else "dry-run"
    print(
        f"done mode={mode} project={project_name} scanned={scanned} changed={changed} updated={updated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
