# Streamlit View Routing

Primary deployment entrypoint is `streamlit_app.py` and supports two explicit views:

- `?view=enduser` (default): investor-facing workspace (`Portfolio`, `Signals`)
- `?view=operator`: ingestion/policy operator dashboard (**only when `ENABLE_OPERATOR_VIEW=true`**)

## Behavior

1. If `view` query param is provided and valid, it is used.
2. If `view=operator` is requested while `ENABLE_OPERATOR_VIEW` is disabled, app falls back to `enduser` and shows an operator-only warning.
3. If `view` is missing, the app restores the last view stored in Streamlit session state (operator session is also downgraded to enduser when operator mode is disabled).
4. If `view` is invalid, app falls back to `enduser` and shows a warning.
5. In-app workspace radio toggle is rendered only when `ENABLE_OPERATOR_VIEW=true`.

## Access status banner

If `STREAMLIT_PUBLIC_URL` env is configured, both views render the same access health banner using `check_streamlit_access`.
This keeps auth-wall/degraded signals visible regardless of active view.

## Deploy/CI smoke coverage

- `scripts/post_deploy_verify.sh` now checks three routes: default landing, `?view=enduser`, `?view=operator`.
- `scripts/deploy_access_gate_ci.sh` enforces deploy-access gate on the same three routes and fails CI if any route regresses to auth-wall/degraded blocker state.
