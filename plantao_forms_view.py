# -*- coding: utf-8 -*-
"""
Visualização somente leitura das respostas do Google Forms (planilha ligada).

Colunas esperadas: Carimbo de data/hora, Dia do Plantão, Turno, Nome Completo

Execução:
  streamlit run plantao_forms_view.py
"""
from __future__ import annotations

import datetime as dt
import html
import os
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from simulador_dv.config.constants import (
    COR_AZUL_ESC,
    COR_BORDA,
    COR_TEXTO_MUTED,
    COR_VERMELHO,
    URL_FAVICON_RESERVA,
)
from simulador_dv.services.sistema_data import _credential_path, _normalize_service_account_dict, _service_account_info_from_env

try:
    import gspread
except Exception:  # pragma: no cover
    gspread = None

FORMS_SHEET_ID = os.environ.get("PLANTAO_FORMS_SHEET_ID", "11E-BbbUANvtIickxYXsMnkPNQpSt4HWNs9qVNiOuEXc")
FORMS_WORKSHEET_GID = int(os.environ.get("PLANTAO_FORMS_GID", "330542744"))

MES_PT_ABBR = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]

CANON_COLS = {
    "carimbo": "Carimbo de data/hora",
    "dia_plantao": "Dia do Plantão",
    "turno": "Turno",
    "nome": "Nome Completo",
}


def _norm_header(s: str) -> str:
    t = str(s or "").strip().lower()
    return re.sub(r"\s+", " ", t)


def _match_column(df: pd.DataFrame, targets: Tuple[str, ...]) -> Optional[str]:
    cols_map = {_norm_header(c): c for c in df.columns}
    for t in targets:
        k = _norm_header(t)
        if k in cols_map:
            return cols_map[k]
    for k, orig in cols_map.items():
        for t in targets:
            if _norm_header(t) in k or k in _norm_header(t):
                return orig
    return None


def _service_account_from_streamlit_secrets() -> Optional[Dict[str, Any]]:
    if not hasattr(st, "secrets"):
        return None
    try:
        s = st.secrets
        if "type" in s and str(s.get("type")) == "service_account":
            return _normalize_service_account_dict(dict(s))
        for key in ("google_service_account", "gcp_service_account", "service_account", "gsheets"):
            if key in s:
                block = s[key]
                if hasattr(block, "keys") and block.get("type") == "service_account":
                    return _normalize_service_account_dict(dict(block))
    except Exception:
        return None
    return None


def _open_gspread_client():
    if gspread is None:
        return None
    info = _service_account_info_from_env()
    if info is not None:
        return gspread.service_account_from_dict(info)
    info = _service_account_from_streamlit_secrets()
    if info is not None:
        return gspread.service_account_from_dict(info)
    cred_path = _credential_path()
    if cred_path:
        return gspread.service_account(filename=cred_path)
    return None


def _monday_of(d: dt.date) -> dt.date:
    return d - dt.timedelta(days=d.weekday())


def _format_day(d: dt.date) -> str:
    return f"{d.day:02d}/{MES_PT_ABBR[d.month - 1]}"


def _format_week_label(monday: dt.date) -> str:
    sunday = monday + dt.timedelta(days=6)
    return f"{_format_day(monday)} - {_format_day(sunday)}"


def _weekday_pt(d: dt.date) -> str:
    nomes = ["SEGUNDA", "TERÇA", "QUARTA", "QUINTA", "SEXTA", "SÁBADO", "DOMINGO"]
    return nomes[d.weekday()]


def _strip_accents(s: str) -> str:
    t = unicodedata.normalize("NFD", s)
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


def _turno_periodo_bucket(turno_raw: Any) -> str:
    """Classifica o valor da coluna Turno (ex.: MANHÃ / TARDE do Form) em manhã ou tarde."""
    s = _strip_accents(str(turno_raw or "").strip().lower())
    s = re.sub(r"\s+", " ", s)
    if not s:
        return "outro"
    if "tarde" in s or "vespertino" in s:
        return "tarde"
    if "manha" in s or "matutino" in s:
        return "manha"
    return "outro"


def _coerce_sheets_datetime(val: Any) -> pd.Timestamp:
    """
    Converte células da planilha (Forms) para timestamp.

    Com `ValueRenderOption.unformatted`, o Google devolve **número serial** (tipo Excel):
    dias desde 1899-12-30 + parte decimal = hora. Se passar esse float a `pd.to_datetime`
    sem `origin`, o pandas trata como nanossegundos desde 1970 → **01/01/1970**.
    """
    if val is None:
        return pd.NaT
    if isinstance(val, float) and pd.isna(val):
        return pd.NaT
    if isinstance(val, (pd.Timestamp, dt.datetime)):
        return pd.Timestamp(val)

    # Numérico: serial Sheets/Excel ou Unix
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        try:
            x = float(val)
        except (TypeError, ValueError):
            return pd.NaT
        if x == 0:
            return pd.NaT
        # Unix (segundos / ms) — típico > 1e9
        if 1e9 <= abs(x) < 1e12:
            return pd.to_datetime(x, unit="s", errors="coerce")
        if abs(x) >= 1e12:
            return pd.to_datetime(x, unit="ms", errors="coerce")
        # Serial Google Sheets / Excel (dias desde 1899-12-30), ex.: ~46106.x = 25/03/2026 19:22
        if abs(x) < 1e6:
            return pd.to_datetime(x, unit="D", origin="1899-12-30", errors="coerce")
        return pd.NaT

    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "-"):
        return pd.NaT
    # Texto só com dígitos/ponto → serial (ex.: "46106.807488")
    if not any(c in s for c in "/-:") and s.replace(".", "").replace(",", "").isdigit():
        try:
            x = float(s.replace(",", "."))
            if 1000 < abs(x) < 1e6:
                return pd.to_datetime(x, unit="D", origin="1899-12-30", errors="coerce")
        except ValueError:
            pass
    t = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(t):
        t = pd.to_datetime(s, errors="coerce", format="%d/%m/%Y %H:%M:%S")
    if pd.isna(t):
        t = pd.to_datetime(s, errors="coerce", format="%d/%m/%Y")
    return t if not pd.isna(t) else pd.NaT


def _fmt_ts(ts: Any) -> str:
    if pd.isna(ts):
        return ""
    t = pd.Timestamp(ts)
    if pd.isna(t):
        return ""
    return t.strftime("%d/%m/%Y %H:%M:%S")


def inject_plantao_layout_css() -> None:
    """Inspiração: streamlit_monolith / simulador (Montserrat, cores Direcional, cards)."""
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@600;700;800;900&family=Inter:wght@400;500;600;700&display=swap');
        html, body, [data-testid="stAppViewContainer"] {{
            font-family: 'Inter', sans-serif;
            color: {COR_AZUL_ESC};
            background-color: #fcfdfe;
        }}
        [data-testid="stSidebar"],
        [data-testid="stSidebarNav"],
        [data-testid="collapsedControl"] {{
            display: none !important;
        }}
        section[data-testid="stSidebar"] {{
            display: none !important;
        }}
        div[data-testid="stAppViewContainer"] > .main {{
            margin-left: 0 !important;
        }}
        [data-testid="stAppViewContainer"] .main .block-container,
        [data-testid="stAppViewContainer"] div[data-testid="stMainBlockContainer"] {{
            max-width: 100% !important;
            padding-left: 2rem !important;
            padding-right: 2rem !important;
        }}
        h1, h2, h3, h4 {{
            font-family: 'Montserrat', sans-serif !important;
            color: {COR_AZUL_ESC} !important;
        }}
        .main .block-container,
        div[data-testid="stMainBlockContainer"] {{
            padding-top: 0.75rem !important;
        }}
        .plantao-header-wrap {{
            text-align: center;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            width: 100%;
            max-width: 100%;
            box-sizing: border-box;
            padding: 0.45rem 1rem 0.5rem;
            background: #ffffff;
            margin: 0 0 0.65rem 0;
            border-radius: 0 0 20px 20px;
            border-bottom: 1px solid {COR_BORDA};
            box-shadow: 0 8px 20px -14px rgba(0,44,93,0.16);
        }}
        .plantao-header-wrap .plantao-header-logo {{
            height: 32px;
            margin: 0 0 0.35rem 0;
            display: block;
        }}
        .plantao-title {{
            font-family: 'Montserrat', sans-serif;
            color: {COR_AZUL_ESC};
            font-size: 1.65rem;
            font-weight: 900;
            margin: 0;
            line-height: 1.15;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            text-align: center;
            width: 100%;
            max-width: 100%;
            box-sizing: border-box;
        }}
        .plantao-sub {{
            color: {COR_AZUL_ESC};
            font-size: 0.85rem;
            font-weight: 600;
            margin: 0.3rem 0 0 0;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            opacity: 0.85;
            line-height: 1.2;
            text-align: center;
            width: 100%;
            max-width: 100%;
            box-sizing: border-box;
        }}
        [data-testid="stMarkdownContainer"] .plantao-header-wrap {{
            margin-left: auto;
            margin-right: auto;
        }}
        .plantao-scrolling {{
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 16px;
            padding: 8px 4px 16px;
            width: 100%;
            margin: 0 auto;
        }}
        .plantao-scrolling-stack {{
            flex-direction: column;
            flex-wrap: nowrap;
            align-items: stretch;
            justify-content: flex-start;
            gap: 12px;
            max-width: 720px;
            margin-left: auto;
            margin-right: auto;
        }}
        .plantao-scrolling-stack .plantao-card-item {{
            flex: 0 0 auto;
            width: 100%;
            max-width: 100%;
            min-width: 0;
        }}
        .plantao-scrolling-stack .plantao-card {{
            height: auto;
            min-height: 96px;
        }}
        .plantao-scrolling-stack.plantao-scrolling-stack--col {{
            max-width: 100%;
            margin-left: 0;
            margin-right: 0;
            padding-left: 0;
            padding-right: 0;
        }}
        .plantao-card-item {{
            flex: 1 1 280px;
            min-width: 280px;
            max-width: 100%;
        }}
        .plantao-card {{
            background: #ffffff;
            padding: 18px 16px;
            border-radius: 16px;
            border: 1px solid {COR_BORDA};
            text-align: center;
            height: 160px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
            border-top: 4px solid {COR_AZUL_ESC};
        }}
        .plantao-card:hover {{
            transform: translateY(-3px);
            border-color: {COR_VERMELHO};
            box-shadow: 0 10px 24px -12px rgba(227, 6, 19, 0.18);
        }}
        .plantao-nome {{
            font-family: 'Montserrat', sans-serif;
            font-weight: 800;
            font-size: 1.05rem;
            color: {COR_AZUL_ESC};
            margin: 0 0 8px 0;
            line-height: 1.25;
            text-align: center;
            width: 100%;
            align-self: center;
        }}
        .plantao-turno {{
            font-size: 0.72rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            color: {COR_VERMELHO};
            margin-bottom: 6px;
            text-align: center;
            width: 100%;
        }}
        .plantao-meta {{
            font-size: 0.78rem;
            color: {COR_TEXTO_MUTED};
            margin: 0;
            text-align: center;
            width: 100%;
        }}
        .plantao-dia-section {{
            margin-bottom: 2rem;
            text-align: center;
            width: 100%;
            max-width: 100%;
            box-sizing: border-box;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .plantao-dia-title {{
            font-family: 'Montserrat', sans-serif;
            font-size: 1.15rem;
            font-weight: 800;
            color: {COR_AZUL_ESC};
            margin: 0 auto 0.75rem auto;
            padding-bottom: 0.35rem;
            border-bottom: 2px solid {COR_VERMELHO};
            display: inline-block;
            text-align: center;
        }}
        [data-testid="stMarkdownContainer"] .plantao-dia-section {{
            margin-left: auto;
            margin-right: auto;
        }}
        .plantao-subsec-turno {{
            margin-top: 1.1rem;
            margin-bottom: 0.35rem;
            text-align: center;
        }}
        .plantao-subsec-turno .plantao-dia-title {{
            font-size: 1.02rem;
            margin-bottom: 0.5rem;
        }}
        div[data-baseweb="tab-list"] {{ justify-content: center !important; gap: 32px; margin-bottom: 24px; }}
        button[data-baseweb="tab"] p {{
            color: {COR_AZUL_ESC} !important;
            font-weight: 700 !important;
            font-family: 'Montserrat', sans-serif !important;
            font-size: 0.85rem !important;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }}
        div[data-baseweb="tab-highlight"] {{ background-color: {COR_VERMELHO} !important; height: 3px !important; }}
        section.main [data-testid="stSelectbox"] [data-baseweb="select"] {{
            width: 100% !important;
            min-width: 100% !important;
            max-width: 100% !important;
        }}
        section.main [data-testid="stSelectbox"] > div > div {{
            width: 100% !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    st.markdown(
        f"""
        <div class="plantao-header-wrap">
            <img class="plantao-header-logo" src="{URL_FAVICON_RESERVA}" alt="" />
            <div class="plantao-title">Plantão — respostas do Form</div>
            <div class="plantao-sub">Visualização apenas leitura (sem edição)</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_day_cards(
    rows: pd.DataFrame,
    *,
    stacked: bool = False,
    stack_fill_column: bool = False,
) -> None:
    """Cards em grelha (wrap) ou um por linha (`stacked=True`, aba Diário / colunas Semanal)."""
    if rows.empty:
        st.caption("Sem registros para este dia.")
        return
    if stacked:
        extra = " plantao-scrolling-stack--col" if stack_fill_column else ""
        wrap_cls = f"plantao-scrolling plantao-scrolling-stack{extra}"
    else:
        wrap_cls = "plantao-scrolling"
    parts: List[str] = [f'<div class="{wrap_cls}">']
    for _, r in rows.iterrows():
        nome = html.escape(str(r.get("nome", "")))
        turno = html.escape(str(r.get("turno", "")))
        carimbo = html.escape(str(r.get("carimbo_fmt", "")))
        parts.append('<div class="plantao-card-item">')
        parts.append('<div class="plantao-card">')
        parts.append(f'<div class="plantao-turno">{turno}</div>')
        parts.append(f'<div class="plantao-nome">{nome}</div>')
        parts.append(f'<p class="plantao-meta">Carimbo: {carimbo}</p>')
        parts.append("</div></div>")
    parts.append("</div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


def _card_df_from_plantao_rows(sub: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "nome": sub[CANON_COLS["nome"]].astype(str),
            "turno": sub[CANON_COLS["turno"]].astype(str),
            "carimbo_fmt": sub["carimbo_fmt"].astype(str),
        }
    )


def load_forms_responses() -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    client = _open_gspread_client()
    if client is None:
        return None, (
            "Sem credenciais Google. Configure SIMULADOR_GSHEETS_JSON (ou B64), "
            "credentials.json, ou no Streamlit Secrets um bloco service_account."
        )
    try:
        sh = client.open_by_key(FORMS_SHEET_ID)
    except PermissionError:
        return None, (
            "Sem permissão para abrir a planilha. Partilhe com o client_email da conta de serviço."
        )
    except Exception as e:
        return None, f"Erro ao abrir planilha: {e}"

    try:
        ws = sh.get_worksheet_by_id(FORMS_WORKSHEET_GID)
    except Exception:
        try:
            ws = sh.sheet1
        except Exception as e2:
            return None, f"Aba gid={FORMS_WORKSHEET_GID} não encontrada: {e2}"

    try:
        from gspread.utils import ValueRenderOption

        records = ws.get_all_records(value_render_option=ValueRenderOption.unformatted)
    except Exception:
        records = ws.get_all_records()

    if not records:
        return pd.DataFrame(), None

    df = pd.DataFrame(records)
    if df.empty:
        return df, None

    c_carimbo = _match_column(
        df,
        ("Carimbo de data/hora", "Carimbo de data hora", "Timestamp"),
    )
    c_dia = _match_column(
        df,
        ("Dia do Plantão", "Dia do Plantao", "Dia plantão", "Dia"),
    )
    c_turno = _match_column(df, ("Turno",))
    c_nome = _match_column(df, ("Nome Completo", "Nome completo", "Nome"))

    if not all([c_carimbo, c_dia, c_turno, c_nome]):
        return None, (
            "Cabeçalhos não reconhecidos. Esperado: Carimbo, Dia do Plantão, Turno, Nome Completo. "
            f"Encontrado: {list(df.columns)}"
        )

    s_carimbo = df[c_carimbo].map(_coerce_sheets_datetime)
    s_dia = df[c_dia].map(_coerce_sheets_datetime)
    # Dia do plantão: só a data (normaliza meia-noite local do serial)
    s_dia = s_dia.dt.normalize()

    out = pd.DataFrame(
        {
            CANON_COLS["carimbo"]: s_carimbo,
            CANON_COLS["dia_plantao"]: s_dia,
            CANON_COLS["turno"]: df[c_turno].astype(str).str.strip(),
            CANON_COLS["nome"]: df[c_nome].astype(str).str.strip(),
        }
    )
    out = out.dropna(subset=[CANON_COLS["dia_plantao"]])

    d_series = out[CANON_COLS["dia_plantao"]].dt.date
    out["_data"] = d_series
    out["_segunda_semana"] = d_series.map(_monday_of)
    out["_dia_semana"] = d_series.map(_weekday_pt)
    out["_semana_label"] = out["_segunda_semana"].map(_format_week_label)
    out["carimbo_fmt"] = out[CANON_COLS["carimbo"]].map(_fmt_ts)

    out = out.sort_values([CANON_COLS["dia_plantao"], CANON_COLS["carimbo"]], ascending=[True, True])
    return out, None


@st.cache_data(ttl=60)
def load_forms_responses_cached() -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    return load_forms_responses()


def main() -> None:
    st.set_page_config(page_title="Plantão (Form)", layout="wide", initial_sidebar_state="collapsed")
    inject_plantao_layout_css()
    render_header()

    df, err = load_forms_responses_cached()
    if err:
        st.error(err)
        st.stop()
    if df is None or df.empty:
        st.info("Nenhum registro na planilha.")
        st.stop()

    _pad, btn_col, _pad2 = st.columns([2, 1, 2])
    with btn_col:
        if st.button("Atualizar dados", use_container_width=True, key="plantao_atualizar"):
            load_forms_responses_cached.clear()
            st.rerun()

    hoje = dt.date.today()
    tab_diario, tab_semanal = st.tabs(["Diário", "Semanal"])

    # --- Diário: apenas plantão de hoje, uma caixa por linha ---
    with tab_diario:
        sub_hoje = df[df["_data"] == hoje]
        titulo = f"Hoje — {_format_day(hoje)} — {_weekday_pt(hoje)}"
        st.markdown(
            f'<div class="plantao-dia-section"><h3 class="plantao-dia-title">{html.escape(titulo)}</h3></div>',
            unsafe_allow_html=True,
        )
        if sub_hoje.empty:
            st.markdown(
                "<div style='text-align: center; color: gray; margin-bottom: 2rem;'>"
                "Sem registros para o plantão de hoje.</div>",
                unsafe_allow_html=True,
            )
        else:
            c_turno = CANON_COLS["turno"]
            sub_hoje = sub_hoje.copy()
            sub_hoje["_periodo"] = sub_hoje[c_turno].map(_turno_periodo_bucket)

            for periodo, titulo_turno in (
                ("manha", "Turno da manhã"),
                ("tarde", "Turno da tarde"),
            ):
                chunk = sub_hoje[sub_hoje["_periodo"] == periodo]
                st.markdown(
                    f'<div class="plantao-dia-section plantao-subsec-turno">'
                    f'<h4 class="plantao-dia-title">{html.escape(titulo_turno)}</h4></div>',
                    unsafe_allow_html=True,
                )
                if chunk.empty:
                    st.markdown(
                        "<div style='text-align: center; color: gray; margin-bottom: 1.25rem;'>"
                        f"Sem registros no {titulo_turno.lower()}.</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    render_day_cards(_card_df_from_plantao_rows(chunk), stacked=True)

            outros = sub_hoje[sub_hoje["_periodo"] == "outro"]
            if not outros.empty:
                st.markdown(
                    '<div class="plantao-dia-section plantao-subsec-turno">'
                    '<h4 class="plantao-dia-title">Outros turnos</h4></div>',
                    unsafe_allow_html=True,
                )
                render_day_cards(_card_df_from_plantao_rows(outros), stacked=True)

    # --- Semanal: por semana (segunda–domingo); mesmo estilo de cards por dia ---
    with tab_semanal:
        semanas = sorted(df["_segunda_semana"].dropna().unique())
        if not semanas:
            st.warning("Sem dados na planilha para a visão semanal.")
        else:
            st.markdown(
                f"<div style='text-align: center; margin: 0 0 0.5rem 0; font-family: Montserrat, sans-serif; font-weight: 800; font-size: 0.95rem; color: {COR_AZUL_ESC}; text-transform: uppercase; letter-spacing: 0.08em;'>Semana</div>",
                unsafe_allow_html=True,
            )
            pick = st.selectbox(
                "Escolha a semana do plantão",
                options=semanas,
                index=len(semanas) - 1,
                format_func=lambda m: f"Semana {_format_week_label(m)}",
                label_visibility="collapsed",
                key="plantao_semana_select",
            )
            sub_w = df[df["_segunda_semana"] == pick].copy()
            c_turno = CANON_COLS["turno"]
            sub_w["_periodo"] = sub_w[c_turno].map(_turno_periodo_bucket)
            st.markdown(
                f"<div style='text-align: center; margin-bottom: 1rem;'><strong>Semana:</strong> {_format_week_label(pick)} · <strong>Registros:</strong> {len(sub_w)}</div>",
                unsafe_allow_html=True,
            )

            d0 = pick
            for i in range(7):
                d = d0 + dt.timedelta(days=i)
                day_rows = sub_w[sub_w["_data"] == d]
                titulo = f"Dia {_format_day(d)} — {_weekday_pt(d)}"
                st.markdown(
                    f'<div class="plantao-dia-section"><h3 class="plantao-dia-title">{html.escape(titulo)}</h3></div>',
                    unsafe_allow_html=True,
                )
                if day_rows.empty:
                    st.markdown("<div style='text-align: center; color: gray; margin-bottom: 2rem;'>Sem registros.</div>", unsafe_allow_html=True)
                else:
                    manha_d = day_rows[day_rows["_periodo"] == "manha"]
                    tarde_d = day_rows[day_rows["_periodo"] == "tarde"]
                    outros_d = day_rows[day_rows["_periodo"] == "outro"]
                    col_m, col_t = st.columns(2)
                    with col_m:
                        st.markdown(
                            '<div class="plantao-dia-section plantao-subsec-turno">'
                            '<h4 class="plantao-dia-title">Turno da manhã</h4></div>',
                            unsafe_allow_html=True,
                        )
                        if manha_d.empty:
                            st.markdown(
                                "<div style='text-align: center; color: gray; font-size: 0.85rem; margin-bottom: 1rem;'>"
                                "Sem registros.</div>",
                                unsafe_allow_html=True,
                            )
                        else:
                            render_day_cards(
                                _card_df_from_plantao_rows(manha_d),
                                stacked=True,
                                stack_fill_column=True,
                            )
                    with col_t:
                        st.markdown(
                            '<div class="plantao-dia-section plantao-subsec-turno">'
                            '<h4 class="plantao-dia-title">Turno da tarde</h4></div>',
                            unsafe_allow_html=True,
                        )
                        if tarde_d.empty:
                            st.markdown(
                                "<div style='text-align: center; color: gray; font-size: 0.85rem; margin-bottom: 1rem;'>"
                                "Sem registros.</div>",
                                unsafe_allow_html=True,
                            )
                        else:
                            render_day_cards(
                                _card_df_from_plantao_rows(tarde_d),
                                stacked=True,
                                stack_fill_column=True,
                            )
                    if not outros_d.empty:
                        st.markdown(
                            '<div class="plantao-dia-section plantao-subsec-turno">'
                            '<h4 class="plantao-dia-title">Outros turnos</h4></div>',
                            unsafe_allow_html=True,
                        )
                        render_day_cards(_card_df_from_plantao_rows(outros_d), stacked=True)


if __name__ == "__main__":
    main()
