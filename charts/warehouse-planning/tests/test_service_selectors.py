#!/usr/bin/env python3
"""Assert this chart's Service selectors isolate each component.

Why this exists (fleet-wide): `app.kubernetes.io/name` + `instance` are identical
on every pod a release creates (OLTP api, MCP, ...). A Service that selects on
those two alone selects ALL of them -- verified live in other fleet charts,
where a Kong request for an OLTP /healthz was answered by the wrong pod.
Every Deployment here carries `app.kubernetes.io/component` in selector.matchLabels
and its pod labels, and every Service selects on it, so each Service selects
EXACTLY ONE Deployment. This test fails if that ever stops being true.

It mirrors process-path-management's charts/.../tests/test_service_selectors.py
(and warehouse-infra's scripts/check-chart-selectors.py), minus the
analytics/frontend components this chart does not ship.

Run: python3 charts/warehouse-planning/tests/test_service_selectors.py
Needs: helm, PyYAML.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
RELEASE = "warehouse-planning"

# Dummy DSN, no password: the chart refuses to render without a database source.
BASE = ["--set", "database.url=postgres://u@example.invalid:5432/db"]
ENABLE_EVERYTHING = BASE + [
    "--set", "mcp.enabled=true",
    "--set", "autoscaling.api.enabled=true",
    "--set", "config.eventPublisher=kafka",
    "--set", "kafka.enabled=true",
    "--set", "gatewayApi.enabled=true",
    "--set", "ingress.enabled=true",
]


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", RELEASE, str(CHART_DIR), *extra_args],
        capture_output=True, text=True, check=True,
    ).stdout
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert selectors", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def selector_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("selector") or {}


def pod_labels_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("template", {}).get("metadata", {}).get("labels") or {}


def matches(selector: dict, labels: dict) -> bool:
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def main() -> int:
    failures: list[str] = []

    docs = render(ENABLE_EVERYTHING)
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}

    if RELEASE not in services:
        failures.append("the OLTP Service was not rendered")
    elif selector_of(services[RELEASE]).get("app.kubernetes.io/component") != "api":
        failures.append("the OLTP Service selector must pin component=api")

    mcp_svc = f"{RELEASE}-mcp"
    if mcp_svc not in services:
        failures.append("the MCP Service was not rendered with mcp.enabled=true")
    elif selector_of(services[mcp_svc]).get("app.kubernetes.io/component") != "mcp":
        failures.append("the MCP Service selector must pin component=mcp")

    # Every Deployment's own selector must pin a component too, and be
    # satisfied by its pod labels.
    for name, dep in deployments.items():
        match_labels = dep["spec"]["selector"].get("matchLabels") or {}
        if "app.kubernetes.io/component" not in match_labels:
            failures.append(f"Deployment {name} selector.matchLabels lacks app.kubernetes.io/component")
        if not matches(match_labels, pod_labels_of(dep)):
            failures.append(f"Deployment {name} pod labels do not satisfy its own selector")

    # The real invariant: each Service selects exactly one Deployment.
    for svc_name, svc in services.items():
        sel = selector_of(svc)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(
                f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1"
            )

    # Default values must not deploy the MCP component at all.
    stray = [
        d["metadata"]["name"]
        for d in render(BASE)
        if d.get("metadata", {}).get("name", "").endswith("-mcp")
    ]
    if stray:
        failures.append(f"mcp resources rendered with default values: {stray}")

    # The chart must refuse to render without a database source.
    refused = subprocess.run(
        ["helm", "template", RELEASE, str(CHART_DIR)], capture_output=True, text=True
    )
    if refused.returncode == 0 or "requires database.url or database.existingSecret" not in refused.stderr:
        failures.append("chart rendered (or failed for another reason) without database.url/existingSecret")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print(f"PASS: {len(services)} Services each select exactly one Deployment; "
          "mcp is off by default; chart refuses to render without a database source")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
