# -*- coding: utf-8 -*-
"""
Visualização somente leitura das respostas do Google Forms (planilha ligada).

Colunas esperadas: Carimbo de data/hora, Dia do Plantão, Turno, Nome Completo

Execução:
  streamlit run plantao_forms_view.py
"""
from __future__ import annotations

import base64
import binascii
import datetime as dt
import html
import json
import os
import re
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# --- Standalone: sem pacote simulador_dv (ex.: deploy Streamlit Cloud só com este ficheiro) ---
URL_FAVICON_RESERVA = "https://direcional.com.br/wp-content/uploads/2021/04/cropped-favicon-direcional-32x32.png"
COR_AZUL_ESC = "#002c5d"
COR_VERMELHO = "#e30613"
COR_BORDA = "#eef2f6"
COR_TEXTO_MUTED = "#64748b"

_PLANTAO_APP_ROOT = Path(__file__).resolve().parent


def _normalize_service_account_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Corrige private_key quando o JSON tem \\n literal em vez de newlines reais."""
    pk = data.get("private_key")
    if isinstance(pk, str) and "\\n" in pk and pk.count("\n") < 3:
        data = dict(data)
        data["private_key"] = pk.replace("\\n", "\n")
    return data


def _parse_service_account_json_string(text: str) -> Optional[Dict[str, Any]]:
    text = (text or "").strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    if data.get("type") != "service_account":
        return None
    if not data.get("private_key") or not data.get("client_email"):
        return None
    return _normalize_service_account_dict(data)


def _service_account_info_from_env() -> Optional[Dict[str, Any]]:
    for key in ("SIMULADOR_GSHEETS_JSON", "GOOGLE_SERVICE_ACCOUNT_JSON"):
        raw = os.environ.get(key)
        if not raw:
            continue
        parsed = _parse_service_account_json_string(str(raw))
        if parsed is not None:
            return parsed

    b64 = os.environ.get("SIMULADOR_GSHEETS_JSON_B64") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_B64")
    if b64:
        try:
            raw_b64 = re.sub(r"\s+", "", str(b64).strip())
            decoded = base64.b64decode(raw_b64, validate=False)
            text = decoded.decode("utf-8")
            parsed = _parse_service_account_json_string(text)
            if parsed is not None:
                return parsed
        except (binascii.Error, UnicodeDecodeError):
            pass

    for key in ("SIMULADOR_GSHEETS_CREDENTIALS", "GOOGLE_APPLICATION_CREDENTIALS"):
        raw = os.environ.get(key)
        if not raw:
            continue
        text = str(raw).strip()
        if Path(text).is_file():
            continue
        if not text.startswith("{"):
            continue
        parsed = _parse_service_account_json_string(text)
        if parsed is not None:
            return parsed

    return None


def _credential_path() -> Optional[str]:
    for p in (
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        os.environ.get("SIMULADOR_GSHEETS_CREDENTIALS"),
        str(_PLANTAO_APP_ROOT / "credentials.json"),
    ):
        if p and Path(p).is_file():
            return p
    return None

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


def _secrets_mapping_to_dict(obj: Any) -> Dict[str, Any]:
    """Converte bloco Streamlit secrets (dict-like / TOMLSection) em dict simples."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, "keys"):
        return {str(k): obj[k] for k in obj.keys()}
    return {}


def _service_account_dict_from_gsheets_block(block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Aceita o formato [connections.gsheets] do secrets.toml (st-gsheets-connection).
    Exige private_key e client_email; força type=service_account para o gspread.
    """
    if not block.get("private_key") or not block.get("client_email"):
        return None
    data = dict(block)
    data["type"] = "service_account"
    return _normalize_service_account_dict(data)


def _service_account_from_streamlit_secrets() -> Optional[Dict[str, Any]]:
    if not hasattr(st, "secrets"):
        return None
    try:
        s = st.secrets
        if "type" in s and str(s.get("type")) == "service_account":
            return _normalize_service_account_dict(_secrets_mapping_to_dict(s))
        for key in ("google_service_account", "gcp_service_account", "service_account", "gsheets"):
            if key in s:
                block = _secrets_mapping_to_dict(s[key])
                if block.get("type") == "service_account" or (
                    block.get("private_key") and block.get("client_email")
                ):
                    got = _service_account_dict_from_gsheets_block(block)
                    if got is not None:
                        return got
        # secrets.toml: [connections.gsheets]
        if "connections" in s:
            conn = _secrets_mapping_to_dict(s["connections"])
            for gs_key in ("gsheets", "google_sheets", "GSheets"):
                if gs_key in conn:
                    block = _secrets_mapping_to_dict(conn[gs_key])
                    got = _service_account_dict_from_gsheets_block(block)
                    if got is not None:
                        return got
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
            display: block;
            width: fit-content;
            max-width: 100%;
            margin-left: auto;
            margin-right: auto;
            text-align: center;
            box-sizing: border-box;
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
            margin-left: auto;
            margin-right: auto;
            text-align: center;
        }}
        .plantao-dia-section h3,
        .plantao-dia-section h4 {{
            text-align: center !important;
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
            <div class="plantao-title">Plantão</div>
            <div class="plantao-sub">Visualização apenas leitura</div>
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
            "Sem credenciais Google. Configure SIMULADOR_GSHEETS_JSON (ou B64), credentials.json, "
            "ou no Streamlit Secrets: [connections.gsheets] (private_key, client_email, …) ou um bloco service_account na raiz."
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


def _pdf_safe_str(val: Any) -> str:
    """Garante texto compatível com fontes PDF core (Latin-1)."""
    s = str(val if val is not None else "")
    for bad, good in (
        ("\u2014", "-"),  # em dash
        ("\u2013", "-"),  # en dash
        ("\u2010", "-"),
        ("\u00a0", " "),
    ):
        s = s.replace(bad, good)
    try:
        return s.encode("latin-1").decode("latin-1")
    except UnicodeEncodeError:
        return s.encode("latin-1", errors="replace").decode("latin-1")


def _week_days_from_monday(monday: dt.date) -> List[dt.date]:
    return [monday + dt.timedelta(days=i) for i in range(7)]


def _mondays_spanning_range(d0: dt.date, d1: dt.date):
    """Cada segunda-feira cujo intervalo Seg–Dom intersecta [d0, d1]."""
    m = _monday_of(d0)
    end_m = _monday_of(d1)
    while m <= end_m:
        yield m
        m += dt.timedelta(days=7)


# Cabeçalhos de coluna no PDF (Segunda = coluna 0 … Domingo = 6)
NOMES_DIAS_PDF = ("Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo")


def _pdf_nome_uma_linha(nome: Any) -> str:
    """
    Um nome completo numa única linha na célula: evita quebra por espaços
    (ex.: 'Lucas Felipe Santana Maia' não vira duas linhas no PDF).
    Vários nomes na mesma célula continuam separados por newline em _pdf_cell_nomes_turno.
    """
    t = _pdf_safe_str(nome)
    # NBSP: Latin-1; a tabela do fpdf2 não quebra linha no meio do nome.
    return t.replace(" ", "\u00a0")


def _pdf_cell_nomes_turno(
    sw: pd.DataFrame,
    day: dt.date,
    periodo: str,
    d0: dt.date,
    d1: dt.date,
) -> str:
    if day < d0 or day > d1:
        return "-"
    chunk = sw[(sw["_data"] == day) & (sw["_periodo"] == periodo)]
    if chunk.empty:
        return "-"
    return "\n".join(_pdf_nome_uma_linha(x) for x in chunk[CANON_COLS["nome"]].astype(str).tolist())


def _pdf_truncate_line_to_width(pdf: Any, line: str, max_w: float) -> str:
    """Encurta com reticências para não ultrapassar max_w (largura já na fonte atual)."""
    if not line or pdf.get_string_width(line) <= max_w:
        return line
    suf = "..."
    for n in range(len(line), 0, -1):
        if pdf.get_string_width(line[:n] + suf) <= max_w:
            return line[:n] + suf
    return suf if pdf.get_string_width(suf) <= max_w else ""


def _pdf_fit_cell_nomes_text(
    pdf: Any,
    raw: str,
    inner_w_mm: float,
    base_pt: float = 9.0,
    min_pt: float = 4.5,
) -> tuple[str, float]:
    """
    Garante uma linha por nome na célula: reduz o tamanho da fonte até caber na largura;
    se ainda não couber no mínimo, trunca com reticências (sem transbordo).
    """
    if raw == "-":
        return raw, base_pt
    lines = raw.split("\n")
    # Margem interna para bordas da célula e arredondamentos do fpdf2
    avail = max(3.5, float(inner_w_mm) - 1.0)
    pdf.set_font("Helvetica", "", base_pt)
    max_w_line = 0.0
    for line in lines:
        s = line.strip()
        if not s:
            continue
        max_w_line = max(max_w_line, pdf.get_string_width(line))
    if max_w_line <= 0:
        return raw, base_pt
    pt = base_pt
    if max_w_line > avail:
        pt = max(min_pt, min(base_pt, base_pt * (avail / max_w_line)))
    pdf.set_font("Helvetica", "", pt)
    out_lines: List[str] = []
    for line in lines:
        if not line.strip():
            out_lines.append(line)
            continue
        if pdf.get_string_width(line) <= avail:
            out_lines.append(line)
        else:
            out_lines.append(_pdf_truncate_line_to_width(pdf, line, avail))
    return "\n".join(out_lines), pt


def build_plantao_periodo_pdf_bytes(
    sub_w: pd.DataFrame,
    data_inicio: dt.date,
    data_fim: dt.date,
) -> bytes:
    """
    PDF A4 horizontal: por semana (segunda a domingo), título "Semana de X a Y",
    duas tabelas (manhã e tarde) na mesma página, colunas Segunda … Domingo.
    """
    from fpdf import FPDF
    from fpdf.fonts import FontFace
    from fpdf.enums import Align, TableBordersLayout, TextEmphasis, WrapMode

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.set_margins(10, 10, 10)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 9, _pdf_safe_str("Plantão"), ln=1, align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(
        0,
        5.5,
        _pdf_safe_str(
            f"Intervalo: {_format_day(data_inicio)} ({_weekday_pt(data_inicio).title()}) "
            f"a {_format_day(data_fim)} ({_weekday_pt(data_fim).title()})"
        ),
        align="C",
    )
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 6, _pdf_safe_str(f"Total de registros: {len(sub_w)}"), ln=1, align="C")
    pdf.ln(3)

    if sub_w.empty:
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 7, _pdf_safe_str("Sem registros neste período."), align="C")
        buf = BytesIO()
        pdf.output(buf)
        return buf.getvalue()

    sw = sub_w.copy()
    if "_periodo" not in sw.columns:
        sw["_periodo"] = sw[CANON_COLS["turno"]].map(_turno_periodo_bucket)

    page_w = float(pdf.epw)
    w_col = page_w / 7.0
    col_widths = (w_col,) * 7

    style_sec_manha = FontFace(
        family="Helvetica",
        emphasis="BOLD",
        fill_color=(255, 236, 236),
        color=(227, 6, 19),
        size_pt=10,
    )
    style_sec_tarde = FontFace(
        family="Helvetica",
        emphasis="BOLD",
        fill_color=(232, 242, 252),
        color=(0, 44, 93),
        size_pt=10,
    )
    style_sec_outros = FontFace(
        family="Helvetica",
        emphasis="BOLD",
        fill_color=(255, 248, 230),
        color=(100, 80, 0),
        size_pt=10,
    )
    style_cab_dias = FontFace(
        family="Helvetica",
        emphasis="BOLD",
        fill_color=(0, 60, 110),
        color=(255, 255, 255),
        size_pt=9,
    )

    cab_nomes = [_pdf_safe_str(n) for n in NOMES_DIAS_PDF]

    # Largura útil por coluna (7 dias) para encaixar texto numa linha sem transbordo
    _inner_col_mm = max(8.0, w_col - 1.5)

    def _render_tabela_turno(
        titulo_secao: str,
        periodo: str,
        style_sec: FontFace,
        days: List[dt.date],
    ) -> None:
        with pdf.table(
            col_widths=col_widths,
            width=page_w,
            line_height=6.2,
            first_row_as_headings=False,
            borders_layout=TableBordersLayout.ALL,
            text_align=Align.C,
            wrapmode=WrapMode.WORD,
        ) as table:
            table.row(
                [
                    {
                        "text": _pdf_safe_str(titulo_secao),
                        "colspan": 7,
                        "align": Align.C,
                        "style": style_sec,
                    }
                ]
            )
            table.row(cab_nomes, style=style_cab_dias)
            linha_dados = []
            for d in days:
                raw = _pdf_cell_nomes_turno(sw, d, periodo, data_inicio, data_fim)
                txt, pt = _pdf_fit_cell_nomes_text(pdf, raw, _inner_col_mm)
                linha_dados.append(
                    {
                        "text": txt,
                        "align": Align.C,
                        "style": FontFace(
                            family="Helvetica",
                            emphasis=TextEmphasis.NONE,
                            size_pt=pt,
                        ),
                    }
                )
            table.row(linha_dados)

    for monday in _mondays_spanning_range(data_inicio, data_fim):
        sunday = monday + dt.timedelta(days=6)
        # Espaço para título + 2 tabelas (+ opcional) na mesma página
        if pdf.get_y() > 145:
            pdf.add_page()
            pdf.set_font("Helvetica", "", 10)

        week_days = _week_days_from_monday(monday)
        titulo_sem = _pdf_safe_str(f"Semana de {_format_day(monday)} a {_format_day(sunday)}")

        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, titulo_sem, ln=1, align="C")
        pdf.ln(3)

        _render_tabela_turno("Turno da manhã", "manha", style_sec_manha, week_days)
        pdf.ln(4)
        _render_tabela_turno("Turno da tarde", "tarde", style_sec_tarde, week_days)

        dias_sem = [d for d in week_days if data_inicio <= d <= data_fim]
        tem_outros = not sw[
            (sw["_data"].isin(dias_sem)) & (sw["_periodo"] == "outro")
        ].empty
        if tem_outros:
            pdf.ln(4)
            _render_tabela_turno("Outros turnos", "outro", style_sec_outros, week_days)

        pdf.ln(6)

    buf = BytesIO()
    pdf.output(buf)
    return buf.getvalue()


def main() -> None:
    st.set_page_config(page_title="Plantão", layout="wide", initial_sidebar_state="collapsed")
    inject_plantao_layout_css()
    render_header()

    df, err = load_forms_responses_cached()
    if err:
        st.error(err)
        st.stop()
    if df is None or df.empty:
        st.info("Nenhum registro na planilha.")
        st.stop()

    dias_todos = sorted(df["_data"].dropna().unique())
    d_plan_min, d_plan_max = dias_todos[0], dias_todos[-1]

    _pad, btn_col, _pad2 = st.columns([2, 1, 2])
    with btn_col:
        if st.button("Atualizar dados", use_container_width=True, key="plantao_atualizar"):
            load_forms_responses_cached.clear()
            st.rerun()

    hoje = dt.date.today()
    tab_diario, tab_semanal = st.tabs(["Diário", "Semanal"])

    # --- Diário: apenas plantão de hoje, uma caixa por linha ---
    with tab_diario:
        monday_sem = _monday_of(hoje)
        sunday_sem = monday_sem + dt.timedelta(days=6)
        sub_semana = df[(df["_data"] >= monday_sem) & (df["_data"] <= sunday_sem)].copy()
        c_turno_d = CANON_COLS["turno"]
        if not sub_semana.empty:
            sub_semana["_periodo"] = sub_semana[c_turno_d].map(_turno_periodo_bucket)

        sub_hoje = df[df["_data"] == hoje]
        titulo = f"Hoje — {_format_day(hoje)} — {_weekday_pt(hoje)}"
        st.markdown(
            f'<div class="plantao-dia-section"><h3 class="plantao-dia-title">{html.escape(titulo)}</h3></div>',
            unsafe_allow_html=True,
        )
        _dl_d_l, _dl_d_c, _dl_d_r = st.columns([2, 1, 2])
        with _dl_d_c:
            try:
                _pdf_sem_dia = build_plantao_periodo_pdf_bytes(sub_semana, monday_sem, sunday_sem)
                st.download_button(
                    label="Baixar PDF da semana",
                    data=_pdf_sem_dia,
                    file_name=f"plantao_semana_{monday_sem.isoformat()}_{sunday_sem.isoformat()}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="plantao_pdf_semana_diario",
                )
            except ImportError:
                st.caption("Instale **fpdf2** para exportar PDF (`pip install fpdf2`).")
        st.caption(
            f"PDF da semana de {_format_day(monday_sem)} a {_format_day(sunday_sem)} "
            f"(segunda a domingo)."
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

    # --- Semanal: intervalo livre (dia inicial → dia final), manhã | tarde por dia ---
    with tab_semanal:
        st.markdown(
            f"<div style='text-align: center; margin: 0 0 0.5rem 0; font-family: Montserrat, sans-serif; font-weight: 800; font-size: 0.95rem; color: {COR_AZUL_ESC}; text-transform: uppercase; letter-spacing: 0.08em;'>Período</div>",
            unsafe_allow_html=True,
        )
        c_ini, c_fim = st.columns(2)
        with c_ini:
            data_inicio_sem = st.date_input(
                "Dia inicial",
                value=d_plan_min,
                key="plantao_sem_inicio",
                help="Primeiro dia do intervalo (ex.: segunda dia 1). Pode cruzar várias semanas.",
            )
        with c_fim:
            data_fim_sem = st.date_input(
                "Dia final",
                value=d_plan_max,
                key="plantao_sem_fim",
                help="Último dia do intervalo (ex.: domingo dia 17).",
            )

        if data_inicio_sem > data_fim_sem:
            st.warning("O dia inicial é depois do final — invertendo o intervalo.")
            data_inicio_sem, data_fim_sem = data_fim_sem, data_inicio_sem

        sub_w = df[(df["_data"] >= data_inicio_sem) & (df["_data"] <= data_fim_sem)].copy()
        c_turno = CANON_COLS["turno"]
        if not sub_w.empty:
            sub_w["_periodo"] = sub_w[c_turno].map(_turno_periodo_bucket)

        st.markdown(
            f"<div style='text-align: center; margin-bottom: 1rem;'>"
            f"<strong>Intervalo:</strong> {_format_day(data_inicio_sem)} — {_weekday_pt(data_inicio_sem)} "
            f"a {_format_day(data_fim_sem)} — {_weekday_pt(data_fim_sem)} · "
            f"<strong>Registros:</strong> {len(sub_w)}</div>",
            unsafe_allow_html=True,
        )

        _dl_l, _dl_c, _dl_r = st.columns([2, 1, 2])
        with _dl_c:
            try:
                _pdf_bytes = build_plantao_periodo_pdf_bytes(sub_w, data_inicio_sem, data_fim_sem)
                st.download_button(
                    label="Baixar PDF do período",
                    data=_pdf_bytes,
                    file_name=f"plantao_{data_inicio_sem.isoformat()}_{data_fim_sem.isoformat()}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="plantao_pdf_periodo",
                )
            except ImportError:
                st.caption("Instale **fpdf2** para exportar PDF (`pip install fpdf2`).")

        d_cur = data_inicio_sem
        while d_cur <= data_fim_sem:
            day_rows = sub_w[sub_w["_data"] == d_cur]
            titulo = f"Dia {_format_day(d_cur)} — {_weekday_pt(d_cur)}"
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
            d_cur += dt.timedelta(days=1)


if __name__ == "__main__":
    main()
