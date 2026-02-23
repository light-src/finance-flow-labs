from __future__ import annotations

import importlib

from src.enduser.macro_signal_reader import read_latest_macro_regime_signal
from src.enduser.signals import render_macro_regime_card


def run_enduser_app(dsn: str, *, configure_page: bool = True) -> None:
    st = importlib.import_module("streamlit")

    if configure_page:
        st.set_page_config(page_title="finance-flow-labs · End-user", layout="wide")
    st.title("finance-flow-labs · End-user")
    st.caption("Investor workspace (paper-trade intelligence).")

    portfolio_tab, signals_tab = st.tabs(["Portfolio", "Signals"])

    with portfolio_tab:
        st.info("Coming soon")

    with signals_tab:
        regime_signal = read_latest_macro_regime_signal(dsn)
        render_macro_regime_card(regime_signal=regime_signal, dsn=dsn)
        st.info("More signal cards coming soon")
