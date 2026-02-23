# Routing & Role Contract

This document defines the intended audience split between the public investor experience and operator diagnostics.

## Public default (End-user)
- Entry: `/` (no query parameter required)
- Renders: End-user investor workspace (`src/enduser/app.py`)
- Audience: Investors consuming macro/sector/portfolio insights
- UX rule: No role/workspace toggle exposed in the public default flow

## Operator surface (explicit)
- Entry: `/?view=operator`
- Required flag: `ENABLE_OPERATOR_VIEW=true`
- Renders: Operator diagnostics dashboard (`src/dashboard/app.py`)
- Audience: Internal operator/maintainer workflows only

If `view=operator` is requested while `ENABLE_OPERATOR_VIEW` is disabled, the app falls back to End-user view and shows a warning.

## Environment knobs
- `ENABLE_OPERATOR_VIEW` (default: disabled)
  - truthy values: `1`, `true`, `yes`, `on`
- `STREAMLIT_PUBLIC_URL`
  - optional; when set, access status banner checks deployed accessibility

## Design intent
- Keep end-user surface simple and decision-focused.
- Keep operator diagnostics available, but only through intentional routing and deployment-time control.
- Reduce accidental exposure of operator-only complexity in public sessions.
