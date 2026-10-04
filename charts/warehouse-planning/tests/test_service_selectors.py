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
(and warehouse-infra's scripts/check-chart-selectors.py), including the optional
analytics components (analytics-projector, analytics-reports; ADR 0005). The
optional frontend (the nginx pod that
serves the capacity_mfe remote) is checked too: it is its own workload,
component=frontend, a ClusterIP Service, never routed by this chart.

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
    "--set", "frontend.enabled=true",
    "--set", "autoscaling.api.enabled=true",
    "--set", "autoscaling.frontend.enabled=true",
    "--set", "autoscaling.projector.enabled=true",
    "--set", "autoscaling.reports.enabled=true",
    "--set", "config.eventPublisher=kafka",
    "--set", "kafka.enabled=true",
    "--set", "gatewayApi.enabled=true",
    "--set", "ingress.enabled=true",
    "--set", "analytics.enabled=true",
    "--set", "analytics.database.projectorUrl=postgres://p@example.invalid:5432/analytics",
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

    frontend_svc = f"{RELEASE}-frontend"
    if frontend_svc not in services:
        failures.append("the frontend Service was not rendered with frontend.enabled=true")
    else:
        if selector_of(services[frontend_svc]).get("app.kubernetes.io/component") != "frontend":
            failures.append("the frontend Service selector must pin component=frontend")
        if services[frontend_svc]["spec"].get("type") != "ClusterIP":
            failures.append("the frontend Service must be ClusterIP")
    if frontend_svc not in deployments:
        failures.append("the frontend Deployment was not rendered with frontend.enabled=true")

    # The analytics reports Service pins component=analytics-reports, is
    # ClusterIP, and the projector/reports Deployments carry their own components.
    reports_svc = f"{RELEASE}-reports"
    if reports_svc not in services:
        failures.append("the analytics reports Service was not rendered with analytics.enabled=true")
    else:
        if selector_of(services[reports_svc]).get("app.kubernetes.io/component") != "analytics-reports":
            failures.append("the reports Service selector must pin component=analytics-reports")
        if services[reports_svc]["spec"].get("type") != "ClusterIP":
            failures.append("the reports Service must be ClusterIP")
    for dep_name, component in ((f"{RELEASE}-projector", "analytics-projector"), (reports_svc, "analytics-reports")):
        dep = deployments.get(dep_name)
        if dep is None:
            failures.append(f"Deployment {dep_name} was not rendered with analytics.enabled=true")
        elif (dep["spec"]["selector"].get("matchLabels") or {}).get("app.kubernetes.io/component") != component \
                or pod_labels_of(dep).get("app.kubernetes.io/component") != component:
            failures.append(f"Deployment {dep_name} must carry component={component} in selector.matchLabels and its pod labels")
    if f"{RELEASE}-projector" in services:
        failures.append("the projector exposes no Service: it serves only its admin port to the kubelet")
    for hpa_name in (f"{RELEASE}-projector", reports_svc):
        hpa = next((d for d in docs if d.get("kind") == "HorizontalPodAutoscaler" and d["metadata"]["name"] == hpa_name), None)
        if hpa is None:
            failures.append(f"the {hpa_name} HPA was not rendered")
        elif hpa["spec"]["scaleTargetRef"]["name"] != hpa_name:
            failures.append(f"the {hpa_name} HPA must scale its own Deployment")
        elif "replicas" in deployments[hpa_name]["spec"]:
            failures.append(f"Deployment {hpa_name} must omit replicas when its HPA owns them")

    # Frontend routing belongs to warehouse-infra's Nginx web gateway, not this chart.
    for d in docs:
        if d.get("kind") in {"Ingress", "HTTPRoute"} and "frontend" in d["metadata"]["name"]:
            failures.append(f"{d['kind']} {d['metadata']['name']}: frontend routing must not live in this chart")

    # The frontend HPA (autoscaling.frontend.enabled) must target the frontend Deployment.
    for d in docs:
        if d.get("kind") == "HorizontalPodAutoscaler" and d["metadata"]["name"] == frontend_svc:
            if d["spec"]["scaleTargetRef"]["name"] != frontend_svc:
                failures.append("the frontend HPA must scale the frontend Deployment")
            break
    else:
        failures.append("the frontend HPA was not rendered with autoscaling.frontend.enabled=true")

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

    # Default values must not deploy the MCP, frontend or analytics components at all.
    stray = [
        d["metadata"]["name"]
        for d in render(BASE)
        if d.get("metadata", {}).get("name", "").endswith(("-mcp", "-frontend", "-projector", "-reports", "-analytics"))
    ]
    if stray:
        failures.append(f"optional components rendered with default values: {stray}")

    # analytics.enabled needs a DSN source and kafka: refuse to render otherwise.
    for label, args, needle in (
        ("without an analytical DSN", BASE + ["--set", "analytics.enabled=true", "--set", "kafka.enabled=true"], "analytics.database.projectorUrl"),
        ("without kafka", BASE + ["--set", "analytics.enabled=true", "--set", "analytics.database.existingSecret=x"], "kafka.enabled is false"),
    ):
        refused_analytics = subprocess.run(
            ["helm", "template", RELEASE, str(CHART_DIR), *args], capture_output=True, text=True
        )
        if refused_analytics.returncode == 0 or needle not in refused_analytics.stderr:
            failures.append(f"analytics.enabled rendered (or failed for another reason) {label}")

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
          "mcp, frontend and analytics are off by default; chart refuses to render without a database source "
          "and refuses analytics without a DSN or kafka")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
