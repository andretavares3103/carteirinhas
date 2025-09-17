# -*- coding: utf-8 -*-
# -------------------------------------------------------------
# Vavivê — Visualizador de Atendimentos + Carteirinhas (Streamlit)
# -------------------------------------------------------------
# Funções:
# - Upload independente de 2 arquivos Excel: "Atendimentos" (ex.: 202509.xlsx)
#   e "Carteirinhas" (fotos/links de profissionais)
# - Mapeamento flexível de colunas (nomes variantes)
# - Merge por ID do profissional e/ou nome
# - Exibição em tabela com: Data, Cliente, Serviço, Hora de entrada, Duração (h),
#   Profissional, Foto (URL)
# - Cartões com foto por atendimento (opcional)
# - Download do CSV/Excel resultante
# -------------------------------------------------------------

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io

st.set_page_config(page_title="Vavivê — Atendimentos + Carteirinhas", layout="wide")

# =========================
# Utilitários de parsing
# =========================

def slugify_col(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    trocas = {
        "á":"a", "à":"a", "â":"a", "ã":"a", "ä":"a",
        "é":"e", "è":"e", "ê":"e", "ë":"e",
        "í":"i", "ì":"i", "î":"i", "ï":"i",
        "ó":"o", "ò":"o", "ô":"o", "õ":"o", "ö":"o",
        "ú":"u", "ù":"u", "û":"u", "ü":"u",
        "ç":"c", "ñ":"n",
        "#":"num", "º":"", "°":"",
        "  ":" ", " ":"_", "/":"_", "-":"_", ".":"_",
    }
    for k,v in trocas.items():
        s = s.replace(k, v)
    s = "".join(ch for ch in s if ch.isalnum() or ch=='_')
    s = "_".join([p for p in s.split("_") if p != ""])  # remove duplos
    return s


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mapping = {c: slugify_col(c) for c in df.columns}
    df.rename(columns=mapping, inplace=True)
    return df


def parse_datetime_col(serie):
    # Aceita string, datetime, excel serial (número) e retorna datetime (naive)
    def parse_one(x):
        if pd.isna(x):
            return pd.NaT
        if isinstance(x, (pd.Timestamp, datetime)):
            return pd.to_datetime(x)
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            # Tenta Excel serial date/time (com ou sem parte inteira)
            try:
                base = datetime(1899, 12, 30)  # convenção Excel
                return base + timedelta(days=float(x))
            except Exception:
                return pd.NaT
        # string
        for fmt in [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%Y %H:%M",
            "%d-%m-%Y",
        ]:
            try:
                return datetime.strptime(str(x).strip(), fmt)
            except Exception:
                pass
        try:
            return pd.to_datetime(x, errors='coerce', dayfirst=True)
        except Exception:
            return pd.NaT
    return serie.apply(parse_one)


def parse_time_hhmm(serie):
    # Converte uma coluna de hora (string/serial) para HH:MM (string) + auxiliar datetime
    dt = parse_datetime_col(serie)
    hhmm = dt.dt.strftime("%H:%M")
    return hhmm, dt


def ensure_numeric_hours(serie):
    # Converte duração para horas (float). Aceita '2', '2,5', '2:30', serial Excel, timedelta etc.
    def to_hours(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            # Pode ser horas já, ou fração de dia (Excel). Heurística: se <= 12 -> trata como horas; > 12 -> dias
            val = float(x)
            if val <= 12:
                return val
            # assume dias -> horas
            return val * 24.0
        if isinstance(x, timedelta):
            return x.total_seconds() / 3600.0
        s = str(x).strip()
        # '2:30' -> 2.5h
        if ":" in s:
            try:
                h, m = s.split(":", 1)
                return float(h) + float(m)/60.0
            except Exception:
                pass
        # '2,5' -> 2.5
        s2 = s.replace(',', '.')
        try:
            return float(s2)
        except Exception:
            return np.nan
    return serie.apply(to_hours)


# =========================
# Mapeamento de colunas
# =========================

ATEND_COLS = {
    # alvo: lista de possíveis nomes normalizados (inclui variantes exatas do seu cabeçalho)
    "data": ["data", "data_1", "dt", "dt_atendimento", "data_atendimento"],
    "cliente": ["cliente", "nome_cliente", "cliente_nome"],
    "servico": ["servico", "tipo_servico", "descricao_servico"],
    "hora_entrada": ["hora_entrada", "entrada", "hora_inicio", "inicio", "horario", "hora", "hora_de_entrada"],
    "duracao_horas": ["duracao", "duracao_horas", "horas", "carga_horaria", "tempo", "horas_de_servico"],
    "profissional_nome": ["nome_do_profissional", "profissional", "nome_profissional", "prof_nome", "prestador"],
    # ID do profissional pode vir como "#Num Prestador" ou "#num+Prestador"
    "profissional_id": [
        "num_prestador", "num_prestadora", "id_profissional", "numero_do_profissional", "num_profissional", "num",
        # variantes geradas pelo slugify para colunas com '#'
        "numnum_prestador", "numnumprestador", "num_num_prestador",
    ],
}

CART_COLS = {
    "profissional_id": ["num_prestador", "id_profissional", "numero_do_profissional", "num_profissional", "num", "numnum_prestador", "numnumprestador", "num_num_prestador"],
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

    # Trata colunas essenciais
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

    if cliente_col is not None:
        out["cliente"] = df[cliente_col].astype(str)
    else:
        out["cliente"] = ""

    if servico_col is not None:
        out["servico"] = df[servico_col].astype(str)
    else:
        out["servico"] = ""

    if entrada_col is not None:
        hhmm, dt_full = parse_time_hhmm(df[entrada_col])
        out["hora_entrada"] = hhmm
        out["_hora_entrada_dt"] = dt_full
    else:
        out["hora_entrada"] = ""
        out["_hora_entrada_dt"] = pd.NaT

    if dur_col is not None:
        out["duracao_horas"] = ensure_numeric_hours(df[dur_col])
    else:
        # Tenta calcular por hora fim - início se existir algo como "hora_fim"
        possiveis_fim = ["hora_fim", "saida", "hora_termino", "fim", "horario_fim"]
        fim_col = None
        for c in possiveis_fim:
            c_norm = slugify_col(c)
            if c_norm in df.columns:
                fim_col = c_norm
                break
        if fim_col is not None and entrada_col is not None:
            _, dt_fim = parse_time_hhmm(df[fim_col])
            dur = (dt_fim - out["_hora_entrada_dt"]).dt.total_seconds() / 3600.0
            out["duracao_horas"] = dur
        else:
            out["duracao_horas"] = np.nan

    if prof_nome_col is not None:
        out["profissional_nome"] = df[prof_nome_col].astype(str)
    else:
        out["profissional_nome"] = ""

    if prof_id_col is not None:
        out["profissional_id"] = df[prof_id_col].astype(str)
    else:
        out["profissional_id"] = ""

    # Normaliza chaves auxiliares para merge por nome
    out["__nome_norm"] = (
        out["profissional_nome"].fillna("").str.strip().str.lower()
        .str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    )

    # Arredonda duração para 2 casas
    out["duracao_horas"] = out["duracao_horas"].round(2)

    return out


def coerce_carteirinhas(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    cols = {k: pick_col(df, v) for k, v in CART_COLS.items()}

    out = pd.DataFrame()
    if cols["profissional_id"] is not None:
        out["profissional_id"] = df[cols["profissional_id"]].astype(str)
    else:
        out["profissional_id"] = ""

    if cols["profissional_nome"] is not None:
        out["profissional_nome"] = df[cols["profissional_nome"]].astype(str)
    else:
        out["profissional_nome"] = ""

    if cols["foto_url"] is not None:
        out["foto_url"] = df[cols["foto_url"]].astype(str)
    else:
        out["foto_url"] = ""

    out["__nome_norm"] = (
        out["profissional_nome"].fillna("").str.strip().str.lower()
        .str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    )

    # Remove duplicados priorizando quem tem foto_url não vazia
    out = out.sort_values(by=["foto_url"], ascending=[False]).drop_duplicates(subset=["profissional_id", "__nome_norm"], keep="first")
    return out


# =========================
# UI
# =========================

st.title("📸 Vavivê — Atendimentos + Carteirinhas")
st.caption("Envie os dois arquivos abaixo (nomes livres). O app tenta mapear as colunas automaticamente.")

col1, col2 = st.columns(2)
with col1:
    f_atend = st.file_uploader("Arquivo de Atendimentos (Excel)", type=["xlsx", "xls"], key="up_atend")
with col2:
    f_cart = st.file_uploader("Arquivo de Carteirinhas (Excel) — fotos/links", type=["xlsx", "xls"], key="up_cart")

# Fallback opcional (útil em dev local): tenta ler caminhos padrão se nada foi enviado
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

# Leitura dos excels (tenta primeira aba automaticamente)
try:
    df_atend_raw = pd.read_excel(f_atend)
except Exception as e:
    st.error(f"Erro ao ler Atendimentos: {e}")
    st.stop()

try:
    df_cart_raw = pd.read_excel(f_cart)
except Exception as e:
    st.error(f"Erro ao ler Carteirinhas: {e}")
    st.stop()

# Normalização
at = coerce_atendimentos(df_atend_raw)
ct = coerce_carteirinhas(df_cart_raw)

# Merge por profissional_id (se existir) e fallback por nome
merged = at.copy()

if (merged["profissional_id"].astype(str).str.len() > 0).any() and (ct["profissional_id"].astype(str).str.len() > 0).any():
    merged = merged.merge(ct[["profissional_id", "foto_url"]], on="profissional_id", how="left")
else:
    merged = merged.merge(ct[["__nome_norm", "foto_url"]], on="__nome_norm", how="left")

# Seleção e ordenação de colunas finais
final_cols = [
    "data", "cliente", "servico", "hora_entrada", "duracao_horas",
    "profissional_nome", "profissional_id", "foto_url"
]
for c in final_cols:
    if c not in merged.columns:
        merged[c] = np.nan if c.endswith("_horas") else ""

merged_view = merged[final_cols].sort_values(by=["data", "cliente", "profissional_nome"], ascending=[True, True, True])

# Filtros rápidos
with st.expander("🔎 Filtros"):
    c1, c2, c3 = st.columns([1,1,2])
    datas_disponiveis = [d for d in merged_view["data"].dropna().unique()]
    datas_disponiveis = sorted([d for d in datas_disponiveis if pd.notna(d)])
    data_sel = c1.selectbox("Filtrar por Data", options=["(todas)"] + datas_disponiveis, index=0)
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

# Cartões com foto (opcional)
st.subheader("🖼️ Cartões com Foto")
if merged_view.empty:
    st.info("Nenhum atendimento para exibir.")
else:
    # grade responsiva simples
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
                url = row.get('foto_url', '') or ''
                if isinstance(url, str) and url.strip():
                    try:
                        st.image(url, use_column_width=True, caption=row['profissional_nome'])
                    except Exception:
                        st.warning("Não foi possível carregar a imagem desta URL.")
                else:
                    st.info("Sem foto cadastrada.")

# Downloads
st.subheader("⬇️ Exportar")
# CSV
csv_bytes = merged_view.to_csv(index=False).encode('utf-8-sig')
st.download_button("Baixar CSV", data=csv_bytes, file_name="atendimentos_fotos.csv", mime="text/csv")

# Excel
out = io.BytesIO()
with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
    merged_view.to_excel(wr, index=False, sheet_name='Atendimentos')
    # Auto-ajuste simples de largura
    ws = wr.sheets['Atendimentos']
    for i, col in enumerate(merged_view.columns):
        maxlen = max(10, min(60, merged_view[col].astype(str).str.len().max() if len(merged_view) else 10))
        ws.set_column(i, i, maxlen + 2)

st.download_button("Baixar Excel", data=out.getvalue(), file_name="atendimentos_fotos.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.caption("Dica: se a coluna do ID do profissional vier como '#Num Prestador' no Excel, o app reconhece automaticamente.")
