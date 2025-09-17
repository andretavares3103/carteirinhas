# -*- coding: utf-8 -*-
# -------------------------------------------------------------
# Vavivê — Visualizador de Atendimentos + Carteirinhas (Streamlit)
# -------------------------------------------------------------
# Upload independente de:
# - Atendimentos (ex.: 202509.xlsx)  -> ler aba "Clientes" (ou primeira com dados)
# - Carteirinhas (fotos/links)
# Exibe: Data, Cliente, Serviço, Hora de entrada, Duração (h),
#        Profissional, Foto (URL)
# -------------------------------------------------------------

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io

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
    }
    for k,v in trocas.items():
        s = s.replace(k, v)
    # Remove símbolos que costumam atrapalhar (#, +, /, -, .) e normaliza espaços
    for ch in ["#", "+", "/", "-", ".", "º", "°"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    s = s.replace(" ", "_")
    s = "".join(ch for ch in s if ch.isalnum() or ch == "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mapping = {c: slugify_col(c) for c in df.columns}
    df.rename(columns=mapping, inplace=True)
    return df

def parse_datetime_col(serie):
    def parse_one(x):
        if pd.isna(x):
            return pd.NaT
        if isinstance(x, (pd.Timestamp, datetime)):
            return pd.to_datetime(x)
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            # Excel serial (dias desde 1899-12-30)
            try:
                base = datetime(1899, 12, 30)
                return base + timedelta(days=float(x))
            except Exception:
                return pd.NaT
        # string
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
    hhmm = dt.dt.strftime("%H:%M")
    return hhmm, dt

def ensure_numeric_hours(serie):
    def to_hours(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            val = float(x)
            # Heurística: <= 12 tratamos como horas; > 12 pode ser fração de dia (Excel)
            if val <= 12:
                return val
            return val * 24.0
        if isinstance(x, timedelta):
            return x.total_seconds()/3600.0
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

# =========================
# Mapeamentos
# =========================

ATEND_COLS = {
    # alvo -> candidatos normalizados (inclui variantes do seu cabeçalho)
    "data": ["data", "data_1", "dt", "dt_atendimento", "data_atendimento"],
    "cliente": ["cliente", "nome_cliente", "cliente_nome"],
    "servico": ["servico", "tipo_servico", "descricao_servico"],
    "hora_entrada": ["hora_entrada", "entrada", "hora_inicio", "inicio", "horario", "hora", "hora_de_entrada"],
    "duracao_horas": ["duracao", "duracao_horas", "horas", "carga_horaria", "tempo", "horas_de_servico"],
    "profissional_nome": ["nome_do_profissional", "profissional", "nome_profissional", "prof_nome", "prestador"],
    # ID do profissional: cobre "#Num Prestador" e "#num+Prestador" após slugify -> "num_prestador", "num_prestador" etc.
    "profissional_id": [
        "num_prestador", "num_prestadora", "id_profissional", "numero_do_profissional",
        "num_profissional", "num", "num_prestador", "num_prestador"
    ],
}

CART_COLS = {
    "profissional_id": ["num_prestador", "id_profissional", "numero_do_profissional", "num_profissional", "num"],
    "profissional_nome": ["profissional", "nome", "nome_profissional", "prof_nome", "prestador"],
    "foto_url": ["foto_url", "url", "link", "image", "foto", "photo", "photo_url"],
}

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def coerce_atendimentos(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    cols = {k: pick_col(df, v) for k, v in ATEND_COLS.items()}

    data_col = cols["data"]
    cliente_col = cols["cliente"]
    servico_col = cols["servico"]
    entrada_col = cols["hora_entrada"]
    dur_col = cols["duracao_horas"]
    prof_nome_col = cols["profissional_nome"]
    prof_id_col = cols["profissional_id"]

    out = pd.DataFrame()

    if data_col is not None:
        out["data"] = parse_datetime_col(df[data_col]).dt.date
    else:
        out["data"] = pd.NaT

    out["cliente"] = df[cliente_col].astype(str) if cliente_col else ""
    out["servico"] = df[servico_col].astype(str) if servico_col else ""

    if entrada_col:
        hhmm, dt_full = parse_time_hhmm(df[entrada_col])
        out["hora_entrada"] = hhmm
        out["_hora_entrada_dt"] = dt_full
    else:
        out["hora_entrada"] = ""
        out["_hora_entrada_dt"] = pd.NaT

    if dur_col:
        out["duracao_horas"] = ensure_numeric_hours(df[dur_col])
    else:
        # tenta calcular por hora fim - inicio se alguma coluna fim existir
        possiveis_fim = ["hora_fim", "saida", "hora_termino", "fim", "horario_fim"]
        fim_col = None
        for c in possiveis_fim:
            c_norm = slugify_col(c)
            if c_norm in df.columns:
                fim_col = c_norm
                break
        if fim_col and entrada_col:
            _, dt_fim = parse_time_hhmm(df[fim_col])
            dur = (dt_fim - out["_hora_entrada_dt"]).dt.total_seconds() / 3600.0
            out["duracao_horas"] = dur
        else:
            out["duracao_horas"] = np.nan

    out["profissional_nome"] = df[prof_nome_col].astype(str) if prof_nome_col else ""
    out["profissional_id"] = df[prof_id_col].astype(str) if prof_id_col else ""

    # chave auxiliar para merge por nome
    out["__nome_norm"] = (
        out["profissional_nome"]
        .fillna("").str.strip().str.lower()
        .str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    )

    out["duracao_horas"] = out["duracao_horas"].round(2)
    return out

def coerce_carteirinhas(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    cols = {k: pick_col(df, v) for k, v in CART_COLS.items()}

    out = pd.DataFrame()
    out["profissional_id"] = df[cols["profissional_id"]].astype(str) if cols["profissional_id"] else ""
    out["profissional_nome"] = df[cols["profissional_nome"]].astype(str) if cols["profissional_nome"] else ""
    out["foto_url"] = df[cols["foto_url"]].astype(str) if cols["foto_url"] else ""

    out["__nome_norm"] = (
        out["profissional_nome"]
        .fillna("").str.strip().str.lower()
        .str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    )

    # prioriza quem tem foto
    out = (
        out.sort_values(by=["foto_url"], ascending=[False])
           .drop_duplicates(subset=["profissional_id", "__nome_norm"], keep="first")
    )
    return out

# =========================
# UI
# =========================

st.title("📸 Vavivê — Atendimentos + Carteirinhas")
st.caption("Envie os dois arquivos (nomes livres). O app detecta a aba 'Clientes' no arquivo de atendimentos.")

col1, col2 = st.columns(2)
with col1:
    f_atend = st.file_uploader("Arquivo de Atendimentos (Excel)", type=["xlsx", "xls"], key="up_atend")
with col2:
    f_cart = st.file_uploader("Arquivo de Carteirinhas (Excel) — fotos/links", type=["xlsx", "xls"], key="up_cart")

# Fallbacks locais (opcional)
if not f_atend:
    try:
        f_atend = open("202509.xlsx", "rb")
    except Exception:
        pass
if not f_cart:
    try:
        f_cart = open("Carteirinhas.xlsx", "rb")
    except Exception:
        pass

if not f_atend or not f_cart:
    st.info("⬆️ Carregue os dois arquivos para continuar.")
    st.stop()

# Leitura inteligente das abas
try:
    xls_a = pd.ExcelFile(f_atend)
    # usa "Clientes" se existir; senão, primeira aba com dados
    default_sheet = "Clientes" if "Clientes" in xls_a.sheet_names else None
    if default_sheet is None:
        chosen = None
        for s in xls_a.sheet_names:
            tmp = pd.read_excel(xls_a, sheet_name=s, nrows=5)
            if not tmp.empty and tmp.dropna(how="all", axis=1).shape[1] > 0:
                chosen = s
                break
        default_sheet = chosen or xls_a.sheet_names[0]
    st.caption(":file_folder: Aba detectada no arquivo de Atendimentos")
    sheet_sel = st.selectbox("Aba dos Atendimentos", options=xls_a.sheet_names,
                             index=xls_a.sheet_names.index(default_sheet))
    df_atend_raw = pd.read_excel(xls_a, sheet_name=sheet_sel)
except Exception as e:
    st.error(f"Erro ao ler Atendimentos: {e}")
    st.stop()

try:
    xls_c = pd.ExcelFile(f_cart)
    chosen_c = None
    for s in xls_c.sheet_names:
        tmp = pd.read_excel(xls_c, sheet_name=s, nrows=5)
        if not tmp.empty and tmp.dropna(how="all", axis=1).shape[1] > 0:
            chosen_c = s
            break
    chosen_c = chosen_c or xls_c.sheet_names[0]
    st.caption(":file_folder: Aba detectada no arquivo de Carteirinhas")
    sheet_cart = st.selectbox("Aba das Carteirinhas", options=xls_c.sheet_names,
                              index=xls_c.sheet_names.index(chosen_c))
    df_cart_raw = pd.read_excel(xls_c, sheet_name=sheet_cart)
except Exception as e:
    st.error(f"Erro ao ler Carteirinhas: {e}")
    st.stop()

# Normalização
at = coerce_atendimentos(df_atend_raw)
ct = coerce_carteirinhas(df_cart_raw)

# Merge por ID se existir; senão por nome
merged = at.copy()
has_id_at = merged["profissional_id"].astype(str).str.len() > 0
has_id_ct = ct["profissional_id"].astype(str).str.len() > 0

if has_id_at.any() and has_id_ct.any():
    merged = merged.merge(ct[["profissional_id", "foto_url"]], on="profissional_id", how="left")
else:
    merged = merged.merge(ct[["__nome_norm", "foto_url"]], on="__nome_norm", how="left")

# Colunas finais
final_cols = [
    "data", "cliente", "servico", "hora_entrada", "duracao_horas",
    "profissional_nome", "profissional_id", "foto_url"
]
for c in final_cols:
    if c not in merged.columns:
        merged[c] = np.nan if c.endswith("_horas") else ""

merged_view = merged[final_cols].sort_values(
    by=["data", "cliente", "profissional_nome"], ascending=[True, True, True]
)

# Filtros
with st.expander("🔎 Filtros"):
    c1, c2, c3 = st.columns([1,1,2])
    datas = [d for d in merged_view["data"].dropna().unique()]
    datas = sorted([d for d in datas if pd.notna(d)])
    data_sel = c1.selectbox("Filtrar por Data", options=["(todas)"] + datas, index=0)
    txt_cliente = c2.text_input("Cliente contém", "")
    txt_prof = c3.text_input("Profissional contém", "")

    mask = pd.Series([True]*len(merged_view))
    if data_sel != "(todas)":
        mask &= (merged_view["data"] == data_sel)
    if txt_cliente.strip():
        mask &= merged_view["cliente"].str.contains(txt_cliente.strip(), case=False, na=False)
    if txt_prof.strip():
        mask &= merged_view["profissional_nome"].str.contains(txt_prof.strip(), case=False, na=False)
    merged_view = merged_view[mask]

st.subheader("📄 Tabela de Atendimentos")
st.dataframe(merged_view, use_container_width=True, hide_index=True)

# Cartões com foto
st.subheader("🖼️ Cartões com Foto")
if merged_view.empty:
    st.info("Nenhum atendimento para exibir.")
else:
    n_cols = st.slider("Colunas", 2, 6, 4, help="Quantidade de cartões por linha")
    rows = [merged_view.iloc[i:i+n_cols] for i in range(0, len(merged_view), n_cols)]
    for r in rows:
        cols = st.columns(len(r))
        for col, (_, row) in zip(cols, r.iterrows()):
            with col:
                st.markdown(f"**{row['cliente']}**")
                st.caption(f"{row['servico'] or ''}")
                st.write(f"📅 {row['data']}  ⏱️ {row['hora_entrada'] or ''}  •  {row['duracao_horas']}h")
                st.write(f"👤 {row['profissional_nome']}  |  ID: {row['profissional_id']}")
                url = (row.get('foto_url', '') or '').strip()
                if url:
                    try:
                        st.image(url, use_column_width=True, caption=row['profissional_nome'])
                    except Exception:
                        st.warning("Não foi possível carregar a imagem desta URL.")
                else:
                    st.info("Sem foto cadastrada.")

# Exportar
st.subheader("⬇️ Exportar")
csv_bytes = merged_view.to_csv(index=False).encode("utf-8-sig")
st.download_button("Baixar CSV", data=csv_bytes, file_name="atendimentos_fotos.csv", mime="text/csv")

out = io.BytesIO()
with pd.ExcelWriter(out, engine="xlsxwriter") as wr:
    merged_view.to_excel(wr, index=False, sheet_name="Atendimentos")
    ws = wr.sheets["Atendimentos"]
    for i, col in enumerate(merged_view.columns):
        # largura simples com limite
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

st.caption("Dica: o app tenta a aba 'Clientes' primeiro; se não houver, escolhe a primeira com dados. Você pode trocar a aba pelos seletores acima.")
