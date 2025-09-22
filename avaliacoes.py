# -*- coding: utf-8 -*-
# -------------------------------------------------------------
# Vavivê — Visualizador de Atendimentos + Carteirinhas (Streamlit)
# -------------------------------------------------------------
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import re

st.set_page_config(page_title="Vavivê — Atendimentos + Carteirinhas", layout="wide")

# =========================
# Utilitários
# =========================

def slugify_col(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    trocas = {
        "á":"a","à":"a","â":"a","ã":"a","ä":"a",
        "é":"e","è":"e","ê":"e","ë":"e",
        "í":"i","ì":"i","î":"i","ï":"i",
        "ó":"o","ò":"o","ô":"o","õ":"o","ö":"o",
        "ú":"u","ù":"u","û":"u","ü":"u",
        "ç":"c","ñ":"n",
        "#":" ", "+":" ", "/":" ", "-":" ", ".":" ", "º":" ", "°":" ",
    }
    for k, v in trocas.items():
        s = s.replace(k, v)
    s = " ".join(s.split()).replace(" ", "_")
    s = "".join(ch for ch in s if ch.isalnum() or ch == "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns={c: slugify_col(c) for c in df.columns}, inplace=True)
    return df

def parse_datetime_col(serie):
    def parse_one(x):
        if pd.isna(x):
            return pd.NaT
        if isinstance(x, (pd.Timestamp, datetime)):
            return pd.to_datetime(x)
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            try:
                return datetime(1899, 12, 30) + timedelta(days=float(x))
            except Exception:
                return pd.NaT
        for fmt in [
            "%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M",
            "%d/%m/%Y","%Y-%m-%d",
            "%d-%m-%Y %H:%M","%d-%m-%Y",
        ]:
            try:
                return datetime.strptime(str(x).strip(), fmt)
            except Exception:
                pass
        try:
            return pd.to_datetime(x, errors="coerce", dayfirst=True)
        except Exception:
            return pd.NaT
    return serie.apply(parse_one)

def parse_time_hhmm(serie):
    dt = parse_datetime_col(serie)
    return dt.dt.strftime("%H:%M"), dt

def ensure_numeric_hours(serie):
    def to_hours(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            v = float(x)
            return v if v <= 12 else v * 24.0
        if isinstance(x, timedelta):
            return x.total_seconds() / 3600.0
        s = str(x).strip()
        if ":" in s:
            try:
                h, m = s.split(":", 1)
                return float(h) + float(m)/60.0
            except Exception:
                pass
        s2 = s.replace(",", ".")
        try:
            return float(s2)
        except Exception:
            return np.nan
    return serie.apply(to_hours)

def _ensure_series(df: pd.DataFrame, colname: str) -> pd.Series:
    obj = df[colname]
    return obj.iloc[:, 0] if isinstance(obj, pd.DataFrame) else obj

def _s(v):
    return "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)

def normalize_id_string(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    return s.apply(lambda x: "".join(re.findall(r"[0-9A-Za-z]+", x)))

PT_WEEKDAYS = ["segunda-feira","terça-feira","quarta-feira","quinta-feira","sexta-feira","sábado","domingo"]
def format_date_br(d):
    if pd.isna(d) or d is None:
        return ""
    if isinstance(d, (pd.Timestamp, datetime)):
        d = d.date()
    try:
        wd = PT_WEEKDAYS[d.weekday()].capitalize()
        return f"{wd}, {d:%d/%m/%Y}"
    except Exception:
        return str(d)

# =========================
# Mapeamentos
# =========================

ATEND_COLS = {
    "data": ["data", "data_1", "dt", "dt_atendimento", "data_atendimento"],
    "cliente": ["cliente", "nome_cliente", "cliente_nome"],
    "servico": ["servico", "tipo_servico", "descricao_servico"],
    "endereco": ["endereco","endereço","endereco_completo","endereco_cliente","logradouro","rua","address"],
    "hora_entrada": ["hora_entrada","entrada","hora_inicio","inicio","horario","hora","hora_de_entrada"],
    "duracao_horas": ["duracao","duracao_horas","horas","carga_horaria","tempo","horas_de_servico"],
    "profissional_nome": ["nome_do_profissional","profissional","nome_profissional","prof_nome","prestador"],
    "profissional_id": ["num_prestador","num_prestadora","id_profissional","numero_do_profissional","num_profissional","num"],
    "status": ["status","situacao","status_servico","situacao_servico","status_atendimento","situacao_atendimento","andamento","etapa"],
    "observacoes": ["obs","observacoes","observações","observacao","observação"],
}

CART_COLS = {
    "profissional_id": ["matricula","num_prestador","id_profissional","numero_do_profissional","num_profissional","num"],
    "profissional_nome": ["profissional","nome","nome_profissional","prof_nome","prestador"],
    "foto_url": ["carteirinha","carteirinhas","foto_url","url","link","image","foto","photo","photo_url"],
}

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# =========================
# Coerções
# =========================

def coerce_atendimentos(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    cols = {k: pick_col(df, v) for k, v in ATEND_COLS.items()}

    out = pd.DataFrame()
    out["data"] = parse_datetime_col(_ensure_series(df, cols["data"])).dt.date if cols["data"] else pd.NaT
    out["cliente"] = _ensure_series(df, cols["cliente"]).astype(str) if cols["cliente"] else ""
    out["servico"] = _ensure_series(df, cols["servico"]).astype(str) if cols["servico"] else ""
    out["endereco"] = _ensure_series(df, cols["endereco"]).astype(str) if cols.get("endereco") else ""
    if cols["hora_entrada"]:
        hhmm, dt_full = parse_time_hhmm(_ensure_series(df, cols["hora_entrada"]))
        out["hora_entrada"] = hhmm
        out["_hora_entrada_dt"] = dt_full
    else:
        out["hora_entrada"] = ""
        out["_hora_entrada_dt"] = pd.NaT
    if cols["duracao_horas"]:
        out["duracao_horas"] = ensure_numeric_hours(_ensure_series(df, cols["duracao_horas"]))
    else:
        out["duracao_horas"] = np.nan
    out["profissional_nome"] = _ensure_series(df, cols["profissional_nome"]).astype(str) if cols["profissional_nome"] else ""
    out["profissional_id"] = _ensure_series(df, cols["profissional_id"]).astype(str) if cols["profissional_id"] else ""
    out["status"] = _ensure_series(df, cols["status"]).astype(str) if cols["status"] else ""
    out["observacoes"] = _ensure_series(df, cols["observacoes"]).astype(str) if cols.get("observacoes") else ""
    out["__nome_norm"] = (
        out["profissional_nome"].fillna("").str.strip().str.lower()
        .str.normalize("NFKD").str.encode("ascii","ignore").str.decode("utf-8")
    )
    out["duracao_horas"] = out["duracao_horas"].round(2)
    return out

def coerce_carteirinhas(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    cols = {k: pick_col(df, v) for k, v in CART_COLS.items()}
    out = pd.DataFrame()
    out["profissional_id"] = _ensure_series(df, cols["profissional_id"]).astype(str) if cols["profissional_id"] else ""
    out["profissional_nome"] = _ensure_series(df, cols["profissional_nome"]).astype(str) if cols["profissional_nome"] else ""
    out["foto_url"] = _ensure_series(df, cols["foto_url"]).astype(str) if cols["foto_url"] else ""
    out["__nome_norm"] = (
        out["profissional_nome"].fillna("").str.strip().str.lower()
        .str.normalize("NFKD").str.encode("ascii","ignore").str.decode("utf-8")
    )
    out = (out.sort_values(by=["foto_url"], ascending=[False])
              .drop_duplicates(subset=["profissional_id","__nome_norm"], keep="first"))
    return out

# =========================
# Upload e leitura
# =========================

st.title("📸 Vavivê — Atendimentos + Carteirinhas")

c1, c2 = st.columns(2)
with c1:
    f_atend = st.file_uploader("Arquivo de Atendimentos (Excel)", type=["xlsx","xls"], key="up_atend")
with c2:
    f_cart = st.file_uploader("Arquivo de Carteirinhas (Excel)", type=["xlsx","xls"], key="up_cart")

if not f_atend or not f_cart:
    st.info("⬆️ Carregue os dois arquivos para continuar.")
    st.stop()

df_atend_raw = pd.read_excel(pd.ExcelFile(f_atend), sheet_name=0)
df_cart_raw = pd.read_excel(pd.ExcelFile(f_cart), sheet_name=0)

at = coerce_atendimentos(df_atend_raw)
ct = coerce_carteirinhas(df_cart_raw)
at["profissional_id"] = normalize_id_string(at["profissional_id"])
ct["profissional_id"] = normalize_id_string(ct["profissional_id"])

merged = at.merge(ct[["profissional_id","foto_url"]], on="profissional_id", how="left")
faltam = merged["foto_url"].isna() | (merged["foto_url"].astype(str).str.strip()=="")
if faltam.any():
    aux = ct[["__nome_norm","foto_url"]].rename(columns={"foto_url":"foto_url_byname"})
    merged = merged.merge(aux, on="__nome_norm", how="left")
    merged["foto_url"] = np.where(
        (merged["foto_url"].astype(str).str.strip()=="") | merged["foto_url"].isna(),
        merged["foto_url_byname"],
        merged["foto_url"]
    )
    merged.drop(columns=["foto_url_byname"], inplace=True, errors="ignore")

final_cols = ["data","cliente","servico","endereco","hora_entrada","duracao_horas",
              "profissional_nome","profissional_id","status","observacoes","foto_url"]
for c in final_cols:
    if c not in merged.columns:
        merged[c] = ""
merged_view = merged[final_cols].sort_values(by=["data","cliente","profissional_nome"])

# =========================
# Cartões com st.html
# =========================
st.subheader("🖼️ Cartões")
if merged_view.empty:
    st.info("Nenhum atendimento para exibir.")
else:
    for _, row in merged_view.iterrows():
        html = f"""
        <div style="display:flex; gap:16px; align-items:flex-start;
                    border:1px solid #e5e7eb; padding:12px 14px; border-radius:14px;
                    background:#ffffff; box-shadow:0 1px 2px rgba(0,0,0,0.03); margin-bottom:14px;">
          <div style="flex:1;">
            <div style="font-weight:700; font-size:1.05rem; margin-bottom:2px; color:#0f172a;">{_s(row['cliente'])}</div>
            <div style="color:#64748b; margin-bottom:8px;">{_s(row['servico'])}</div>
            <div style="display:flex; gap:12px; flex-wrap:wrap; font-size:0.92rem; margin-bottom:8px; color:#334155;">
              <span>📅 {format_date_br(row['data'])}</span>
              <span>⏱️ {_s(row['hora_entrada'])} • {_s(row['duracao_horas'])}h</span>
              {f"<span>🔖 {_s(row['status'])}</span>" if _s(row['status']) else ""}
            </div>
            <div style="font-size:0.92rem; margin-bottom:6px; color:#334155;">
              👤 {_s(row['profissional_nome'])} | ID: {_s(row['profissional_id'])}
            </div>
            <div style="font-size:0.92rem; color:#1f2937; line-height:1.35;">
              📍 {_s(row['endereco'])}
            </div>
            {f"<div style='margin-top:8px; padding:10px 12px; background:#f1f5f9; border-radius:10px; font-size:0.9rem;'>Obs: {_s(row['observacoes'])}</div>" if _s(row['observacoes']) else ""}
          </div>
          <div style="width:130px; text-align:center;">
            {f'<img src="{_s(row["foto_url"])}" style="width:100%; border-radius:12px;" />' if _s(row['foto_url']) else '<div style="background:#eaf2ff; color:#1e40af; padding:12px; border-radius:12px; font-size:0.9rem;">Sem foto</div>'}
          </div>
        </div>
        """
        st.html(html)
