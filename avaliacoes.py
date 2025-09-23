# -*- coding: utf-8 -*-
# -------------------------------------------------------------
# Vavivê — Visualizador de Atendimentos + Carteirinhas (Streamlit)
# -------------------------------------------------------------
# Upload de 1 arquivo Excel (Atendimentos) + leitura direta de "Carteirinhas" a partir do disco
# Cruzamento PRIORITÁRIO por ID/Matrícula (#Num Prestador ↔ Matricula)
# Cartões com layout: texto à esquerda e foto à direita
# -------------------------------------------------------------

import os
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import re
import streamlit.components.v1 as components

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
# Mapeamentos de colunas
# =========================

ATEND_COLS = {
    "data": ["data", "data_1", "dt", "dt_atendimento", "data_atendimento"],
    "cliente": ["cliente", "nome_cliente", "cliente_nome"],
    "servico": ["servico", "tipo_servico", "descricao_servico"],
    "endereco": ["endereco", "endereço", "endereco_completo", "endereco_cliente", "logradouro", "rua", "address"],
    "hora_entrada": ["hora_entrada", "entrada", "hora_inicio", "inicio", "horario", "hora", "hora_de_entrada"],
    "duracao_horas": ["duracao", "duracao_horas", "horas", "carga_horaria", "tempo", "horas_de_servico"],
    "profissional_nome": ["nome_do_profissional", "profissional", "nome_profissional", "prof_nome", "prestador"],
    "profissional_id": ["num_prestador", "num_prestadora", "id_profissional", "numero_do_profissional", "num_profissional", "num"],
    "status": ["status", "situacao", "status_servico", "situacao_servico", "status_atendimento", "situacao_atendimento", "andamento", "etapa"],
    "observacoes": ["obs","observacoes","observações","observacao","observação"],
    "observacoes_prestador": [
        "obs_prestador","observacoes_prestador","observações_prestador",
        "observacao_prestador","observação_prestador","obs_profissional",
        "comentario_prestador","comentarios_prestador"
    ],
}

CART_COLS = {
    "profissional_id": ["matricula", "num_prestador", "id_profissional", "numero_do_profissional", "num_profissional", "num"],
    "profissional_nome": ["profissional", "nome", "nome_profissional", "prof_nome", "prestador"],
    # aceita "Carteirinha"
    "foto_url": ["carteirinha", "carteirinhas", "foto_url", "url", "link", "image", "foto", "photo", "photo_url"],
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
        possiveis_fim = ["hora_fim", "saida", "hora_termino", "fim", "horario_fim"]
        fim_col = None
        for c in possiveis_fim:
            c_norm = slugify_col(c)
            if c_norm in df.columns:
                fim_col = c_norm
                break
        if fim_col and cols["hora_entrada"]:
            _, dt_fim = parse_time_hhmm(_ensure_series(df, fim_col))
            out["duracao_horas"] = (dt_fim - out["_hora_entrada_dt"]).dt.total_seconds() / 3600.0
        else:
            out["duracao_horas"] = np.nan

    out["profissional_nome"] = _ensure_series(df, cols["profissional_nome"]).astype(str) if cols["profissional_nome"] else ""
    out["profissional_id"] = _ensure_series(df, cols["profissional_id"]).astype(str) if cols["profissional_id"] else ""
    out["status"] = _ensure_series(df, cols["status"]).astype(str) if cols["status"] else ""
    out["observacoes"] = _ensure_series(df, cols["observacoes"]).astype(str) if cols.get("observacoes") else ""
    out["observacoes_prestador"] = (
        _ensure_series(df, cols["observacoes_prestador"]).astype(str) if cols.get("observacoes_prestador") else ""
    )

    out["__nome_norm"] = (
        out["profissional_nome"].fillna("").str.strip().str.lower()
        .str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("utf-8")
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
        .str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("utf-8")
    )
    out = (
        out.sort_values(by=["foto_url"], ascending=[False])
           .drop_duplicates(subset=["profissional_id", "__nome_norm"], keep="first")
    )
    return out

# =========================
# UI e Leitura de arquivos
# =========================

st.title("📸 Vavivê — Atendimentos + Carteirinhas")
st.caption("Atendimentos por upload; Carteirinhas lidas direto do disco (raiz/subpasta).")

# 👉 Caminho fixo para Carteirinhas (no diretório do app)
cart_default = "carteirinhas.xlsx"  # mude aqui se preferir outra pasta: "data/carteirinhas.xlsx"
cart_path = st.text_input("Caminho do arquivo de Carteirinhas (no diretório da aplicação)", value=cart_default,
                          help="Ex.: 'carteirinhas.xlsx' ou 'data/carteirinhas.xlsx'")

if not os.path.exists(cart_path):
    st.error(f"Arquivo de Carteirinhas não encontrado em: {cart_path}\n"
             "Coloque o arquivo no diretório do app ou ajuste o caminho acima.")
    st.stop()

# Upload apenas de Atendimentos
f_atend = st.file_uploader("Arquivo de Atendimentos (Excel)", type=["xlsx", "xls"], key="up_atend")
if not f_atend:
    st.info("⬆️ Envie o arquivo de Atendimentos para continuar.")
    st.stop()

def pick_sheet(xls: pd.ExcelFile, prefer: str | None = None):
    if prefer and prefer in xls.sheet_names:
        return prefer
    for s in xls.sheet_names:
        tmp = pd.read_excel(xls, sheet_name=s, nrows=5)
        if not tmp.empty and tmp.dropna(how="all", axis=1).shape[1] > 0:
            return s
    return xls.sheet_names[0]  # fallback

# Lê Atendimentos (preferindo "Clientes")
try:
    xls_a = pd.ExcelFile(f_atend)
    sheet_a_default = "Clientes" if "Clientes" in xls_a.sheet_names else pick_sheet(xls_a)
    st.caption(":file_folder: Aba detectada no arquivo de Atendimentos")
    sheet_a = st.selectbox("Aba dos Atendimentos", options=xls_a.sheet_names,
                           index=xls_a.sheet_names.index(sheet_a_default))
    df_atend_raw = pd.read_excel(xls_a, sheet_name=sheet_a)
except Exception as e:
    st.error(f"Erro ao ler Atendimentos: {e}")
    st.stop()

# Lê Carteirinhas do disco
try:
    xls_c = pd.ExcelFile(cart_path)
    sheet_c_default = pick_sheet(xls_c)  # primeira com dados
    st.caption(f":file_folder: Carteirinhas carregadas de **{cart_path}**")
    sheet_c = st.selectbox("Aba das Carteirinhas (arquivo local)", options=xls_c.sheet_names,
                           index=xls_c.sheet_names.index(sheet_c_default))
    df_cart_raw = pd.read_excel(xls_c, sheet_name=sheet_c)
except Exception as e:
    st.error(f"Erro ao ler Carteirinhas ({cart_path}): {e}")
    st.stop()

# =========================
# Normalização + Merge
# =========================

at = coerce_atendimentos(df_atend_raw)
ct = coerce_carteirinhas(df_cart_raw)

# Normalização forte do ID
at["profissional_id"] = normalize_id_string(at["profissional_id"])
ct["profissional_id"] = normalize_id_string(ct["profissional_id"])

# Merge por ID (left)
merged = at.merge(ct[["profissional_id", "foto_url"]], on="profissional_id", how="left")

# Fallback por nome (se necessário)
faltam = merged["foto_url"].isna() | (merged["foto_url"].astype(str).str.strip() == "")
if faltam.any():
    aux = ct[["__nome_norm", "foto_url"]].rename(columns={"foto_url": "foto_url_byname"})
    merged = merged.merge(aux, on="__nome_norm", how="left")
    merged["foto_url"] = np.where(
        (merged["foto_url"].astype(str).str.strip() == "") | merged["foto_url"].isna(),
        merged["foto_url_byname"],
        merged["foto_url"]
    )
    merged.drop(columns=["foto_url_byname"], inplace=True, errors="ignore")

# =========================
# Visão final + filtros
# =========================

final_cols = [
    "data","cliente","servico","endereco","hora_entrada","duracao_horas",
    "profissional_nome","profissional_id","status",
    "observacoes","observacoes_prestador","foto_url"
]
for c in final_cols:
    if c not in merged.columns:
        merged[c] = np.nan if c.endswith("_horas") else ""

merged_view = merged[final_cols].sort_values(by=["data", "cliente", "profissional_nome"])
merged_view["foto_url"] = merged_view["foto_url"].fillna("")
merged_view["status"] = merged_view["status"].fillna("")
merged_view["observacoes"] = merged_view["observacoes"].fillna("")
merged_view["observacoes_prestador"] = merged_view["observacoes_prestador"].fillna("")

with st.expander("🔎 Filtros"):
    cA, cB, cC = st.columns([1, 1, 2])
    datas = sorted([d for d in merged_view["data"].dropna().unique() if pd.notna(d)])
    data_sel = cA.selectbox("Filtrar por Data", options=["(todas)"] + datas, index=0)
    txt_cliente = cB.text_input("Cliente contém", "")
    txt_prof = cC.text_input("Profissional contém", "")
    status_opts = sorted([s for s in merged_view["status"].dropna().unique() if str(s).strip() != ""])
    status_sel = st.multiselect("Status do serviço", options=status_opts, default=status_opts)

    mask = pd.Series(True, index=merged_view.index)
    if data_sel != "(todas)":
        mask &= (merged_view["data"] == data_sel)
    if txt_cliente.strip():
        mask &= merged_view["cliente"].str.contains(txt_cliente.strip(), case=False, na=False)
    if txt_prof.strip():
        mask &= merged_view["profissional_nome"].str.contains(txt_prof.strip(), case=False, na=False)
    if status_sel:
        mask &= merged_view["status"].isin(status_sel)

    merged_view = merged_view[mask]

# =========================
# Tabela
# =========================
st.subheader("📄 Tabela de Atendimentos")
st.dataframe(merged_view.drop(columns=["foto_url"]), use_container_width=True, hide_index=True)

# =========================
# Cartões (foto à direita)
# =========================
st.subheader("🖼️ Cartões")
if merged_view.empty:
    st.info("Nenhum atendimento para exibir.")
else:
    n_cols = st.slider("Colunas", 1, 4, 2, help="Quantidade de cartões por linha")
    rows = [merged_view.iloc[i:i+n_cols] for i in range(0, len(merged_view), n_cols)]
    for r in rows:
        cols = st.columns(len(r))
        for col, (_, row) in zip(cols, r.iterrows()):
            with col:
                cliente   = _s(row.get("cliente"))
                servico   = _s(row.get("servico"))
                data_br   = format_date_br(row.get("data"))
                hora      = _s(row.get("hora_entrada"))
                dur       = _s(row.get("duracao_horas"))
                status    = _s(row.get("status"))
                prof      = _s(row.get("profissional_nome"))
                pid       = _s(row.get("profissional_id"))
                endereco  = _s(row.get("endereco"))
                obs       = _s(row.get("observacoes")).strip()
                obs_prestador = _s(row.get("observacoes_prestador")).strip()

                obs_html = f"""
                    <div style="margin-top:8px; padding:10px 12px; background:#f1f5f9;
                                border-radius:10px; font-size:0.9rem; color:#0f172a;">
                        <strong>Obs:</strong> {obs}
                    </div>
                """ if obs else ""

                obs_prestador_html = f"""
                    <div style="margin-top:8px; padding:10px 12px; background:#fef9c3;
                                border-radius:10px; font-size:0.9rem; color:#713f12;">
                        <strong>Obs Prestador:</strong> {obs_prestador}
                    </div>
                """ if obs_prestador else ""

                val = row.get("foto_url", None)
                url = "" if (val is None or (isinstance(val, float) and pd.isna(val))) else str(val).strip()

                html = f"""
                <div style="display:flex; gap:16px; align-items:flex-start;
                            border:1px solid #e5e7eb; padding:12px 14px; border-radius:14px;
                            background:#ffffff; box-shadow:0 1px 2px rgba(0,0,0,0.03);">
                  <div style="flex:1; min-width:0;">
                    <div style="font-weight:700; font-size:1.05rem; margin-bottom:2px; color:#0f172a;">{cliente}</div>
                    <div style="color:#64748b; margin-bottom:8px;">{servico}</div>

                    <div style="display:flex; gap:12px; flex-wrap:wrap; font-size:0.92rem; margin-bottom:8px; color:#334155;">
                      <span>📅 {data_br}</span>
                      <span>⏱️ {hora} • {dur}h</span>
                      {f'<span>🔖 {status}</span>' if status else ''}
                    </div>

                    <div style="font-size:0.92rem; margin-bottom:6px; color:#334155;">
                      👤 {prof} &nbsp;|&nbsp; ID: {pid}
                    </div>

                    <div style="font-size:0.92rem; color:#1f2937; line-height:1.35;">
                      📍 {endereco}
                    </div>

                    {obs_html}
                    {obs_prestador_html}
                  </div>

                  <div style="width:130px; text-align:center;">
                    {(
                      f'<img src="{url}" alt="foto" style="width:100%; height:auto; border-radius:12px; object-fit:cover;" />'
                      if url else
                      '<div style="background:#eaf2ff; color:#1e40af; padding:12px; border-radius:12px; font-size:0.9rem;">Sem foto</div>'
                    )}
                  </div>
                </div>
                """
                components.html(html, height=260, scrolling=False)

# =========================
# Exportar
# =========================
st.subheader("⬇️ Exportar")
csv_bytes = merged_view.to_csv(index=False).encode("utf-8-sig")
st.download_button("Baixar CSV", data=csv_bytes, file_name="atendimentos_fotos.csv", mime="text/csv")

out = io.BytesIO()
with pd.ExcelWriter(out, engine="xlsxwriter") as wr:
    merged_view.to_excel(wr, index=False, sheet_name="Atendimentos")
    ws = wr.sheets["Atendimentos"]
    for i, col in enumerate(merged_view.columns):
        try:
            maxlen = int(min(60, max(10, merged_view[col].astype(str).str.len().max())))
        except ValueError:
            maxlen = 20
        ws.set_column(i, i, maxlen + 2)

st.download_button(
    "Baixar Excel",
    data=out.getvalue(),
    file_name="atendimentos_fotos.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.caption("Coloque o arquivo de Carteirinhas no diretório do app (ex.: 'carteirinhas.xlsx' ou 'data/carteirinhas.xlsx') e ajuste o caminho acima.")
