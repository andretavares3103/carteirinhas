# -*- coding: utf-8 -*-
# Vavivê — Visualizador de Atendimentos + Carteirinhas (Streamlit)

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import re

st.set_page_config(page_title="Vavivê — Atendimentos + Carteirinhas", layout="wide")

def slugify_col(s: str) -> str:
    if s is None: return ""
    s = str(s).strip().lower()
    trocas = {"á":"a","à":"a","â":"a","ã":"a","ä":"a","é":"e","è":"e","ê":"e","ë":"e",
              "í":"i","ì":"i","î":"i","ï":"i","ó":"o","ò":"o","ô":"o","õ":"o","ö":"o",
              "ú":"u","ù":"u","û":"u","ü":"u","ç":"c","ñ":"n",
              "#":" ", "+":" ", "/":" ", "-":" ", ".":" ", "º":" ", "°":" "}
    for k,v in trocas.items(): s = s.replace(k,v)
    s = " ".join(s.split()).replace(" ","_")
    s = "".join(ch for ch in s if ch.isalnum() or ch=="_")
    while "__" in s: s = s.replace("__","_")
    return s.strip("_")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns={c: slugify_col(c) for c in df.columns}, inplace=True)
    return df

def parse_datetime_col(serie):
    def parse_one(x):
        if pd.isna(x): return pd.NaT
        if isinstance(x,(pd.Timestamp,datetime)): return pd.to_datetime(x)
        if isinstance(x,(int,float)) and not isinstance(x,bool):
            try: return datetime(1899,12,30)+timedelta(days=float(x))
            except: return pd.NaT
        for fmt in ["%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%d/%m/%Y %H:%M:%S",
                    "%d/%m/%Y %H:%M","%d/%m/%Y","%Y-%m-%d","%d-%m-%Y %H:%M","%d-%m-%Y"]:
            try: return datetime.strptime(str(x).strip(),fmt)
            except: pass
        try: return pd.to_datetime(x, errors="coerce", dayfirst=True)
        except: return pd.NaT
    return serie.apply(parse_one)

def parse_time_hhmm(serie):
    dt = parse_datetime_col(serie); return dt.dt.strftime("%H:%M"), dt

def ensure_numeric_hours(serie):
    def to_hours(x):
        if pd.isna(x): return np.nan
        if isinstance(x,(int,float)) and not isinstance(x,bool):
            v=float(x); return v if v<=12 else v*24.0
        if isinstance(x,timedelta): return x.total_seconds()/3600.0
        s=str(x).strip()
        if ":" in s:
            try: h,m=s.split(":",1); return float(h)+float(m)/60.0
            except: pass
        s2=s.replace(",",".")
        try: return float(s2)
        except: return np.nan
    return serie.apply(to_hours)

def _ensure_series(df: pd.DataFrame, colname: str) -> pd.Series:
    obj = df[colname]
    return obj.iloc[:,0] if isinstance(obj,pd.DataFrame) else obj

def _s(v): return "" if (v is None or (isinstance(v,float) and pd.isna(v))) else str(v)

def normalize_id_string(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.replace(r"\.0$","",regex=True)
    return s.apply(lambda x: "".join(re.findall(r"[0-9A-Za-z]+", x)))

ATEND_COLS = {
    "data": ["data","data_1","dt","dt_atendimento","data_atendimento"],
    "cliente": ["cliente","nome_cliente","cliente_nome"],
    "servico": ["servico","tipo_servico","descricao_servico"],
    "hora_entrada": ["hora_entrada","entrada","hora_inicio","inicio","horario","hora","hora_de_entrada"],
    "duracao_horas": ["duracao","duracao_horas","horas","carga_horaria","tempo","horas_de_servico"],
    "profissional_nome": ["nome_do_profissional","profissional","nome_profissional","prof_nome","prestador"],
    "profissional_id": ["num_prestador","num_prestadora","id_profissional","numero_do_profissional","num_profissional","num"],
    "status": ["status","situacao","status_servico","situacao_servico","status_atendimento","situacao_atendimento","andamento","etapa"],
}

# 👇 AQUI o ajuste: inclui "carteirinha" e "carteirinhas" como candidatos de foto_url
CART_COLS = {
    "profissional_id": ["matricula","num_prestador","id_profissional","numero_do_profissional","num_profissional","num"],
    "profissional_nome": ["profissional","nome","nome_profissional","prof_nome","prestador"],
    "foto_url": ["carteirinha","carteirinhas","foto_url","url","link","image","foto","photo","photo_url"],
}

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def coerce_atendimentos(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    cols = {k: pick_col(df,v) for k,v in ATEND_COLS.items()}
    out = pd.DataFrame()
    out["data"] = parse_datetime_col(_ensure_series(df, cols["data"])).dt.date if cols["data"] else pd.NaT
    out["cliente"] = _ensure_series(df, cols["cliente"]).astype(str) if cols["cliente"] else ""
    out["servico"] = _ensure_series(df, cols["servico"]).astype(str) if cols["servico"] else ""
    if cols["hora_entrada"]:
        hhmm, dtf = parse_time_hhmm(_ensure_series(df, cols["hora_entrada"])); out["hora_entrada"]=hhmm; out["_hora_entrada_dt"]=dtf
    else:
        out["hora_entrada"]=""; out["_hora_entrada_dt"]=pd.NaT
    if cols["duracao_horas"]:
        out["duracao_horas"]=ensure_numeric_hours(_ensure_series(df, cols["duracao_horas"]))
    else:
        possiveis_fim=["hora_fim","saida","hora_termino","fim","horario_fim"]; fim_col=None
        for c in possiveis_fim:
            c_norm=slugify_col(c)
            if c_norm in df.columns: fim_col=c_norm; break
        if fim_col and cols["hora_entrada"]:
            _,dtf=parse_time_hhmm(_ensure_series(df,fim_col))
            out["duracao_horas"]=(dtf-out["_hora_entrada_dt"]).dt.total_seconds()/3600.0
        else:
            out["duracao_horas"]=np.nan
    out["profissional_nome"]=_ensure_series(df, cols["profissional_nome"]).astype(str) if cols["profissional_nome"] else ""
    out["profissional_id"]=_ensure_series(df, cols["profissional_id"]).astype(str) if cols["profissional_id"] else ""
    out["status"]=_ensure_series(df, cols["status"]).astype(str) if cols["status"] else ""
    out["__nome_norm"]=(
        out["profissional_nome"].fillna("").str.strip().str.lower()
          .str.normalize('NFKD').str.encode('ascii','ignore').str.decode('utf-8')
    )
    out["duracao_horas"]=out["duracao_horas"].round(2)
    return out

def coerce_carteirinhas(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    cols = {k: pick_col(df,v) for k,v in CART_COLS.items()}
    out = pd.DataFrame()
    out["profissional_id"]=_ensure_series(df, cols["profissional_id"]).astype(str) if cols["profissional_id"] else ""
    out["profissional_nome"]=_ensure_series(df, cols["profissional_nome"]).astype(str) if cols["profissional_nome"] else ""
    out["foto_url"]=_ensure_series(df, cols["foto_url"]).astype(str) if cols["foto_url"] else ""
    out["__nome_norm"]=(
        out["profissional_nome"].fillna("").str.strip().str.lower()
          .str.normalize('NFKD').str.encode('ascii','ignore').str.decode('utf-8')
    )
    out = (out.sort_values(by=["foto_url"], ascending=[False])
              .drop_duplicates(subset=["profissional_id","__nome_norm"], keep="first"))
    return out

# ---------------- UI / Leitura ----------------

st.title("📸 Vavivê — Atendimentos + Carteirinhas")
st.caption("Cruzamento PRIORITÁRIO por ID/Matrícula (#Num Prestador ↔ Matricula).")

c1,c2 = st.columns(2)
with c1: f_atend = st.file_uploader("Arquivo de Atendimentos (Excel)", type=["xlsx","xls"], key="up_atend")
with c2: f_cart  = st.file_uploader("Arquivo de Carteirinhas (Excel) — fotos/links", type=["xlsx","xls"], key="up_cart")
if not f_atend or not f_cart:
    st.info("⬆️ Carregue os dois arquivos para continuar."); st.stop()

def pick_sheet(excel_file, prefer="Clientes"):
    xls = pd.ExcelFile(excel_file)
    if prefer in xls.sheet_names: return prefer
    for s in xls.sheet_names:
        tmp = pd.read_excel(xls, sheet_name=s, nrows=5)
        if not tmp.empty and tmp.dropna(how="all", axis=1).shape[1] > 0: return s
    return xls.sheet_names[0]

try:
    sa = pick_sheet(f_atend, "Clientes")
    df_atend_raw = pd.read_excel(pd.ExcelFile(f_atend), sheet_name=sa)
except Exception as e:
    st.error(f"Erro ao ler Atendimentos: {e}"); st.stop()

try:
    sc = pick_sheet(f_cart)
    df_cart_raw = pd.read_excel(pd.ExcelFile(f_cart), sheet_name=sc)
except Exception as e:
    st.error(f"Erro ao ler Carteirinhas: {e}"); st.stop()

at = coerce_atendimentos(df_atend_raw)
ct = coerce_carteirinhas(df_cart_raw)

# Normalização forte do ID
at["profissional_id"] = normalize_id_string(at["profissional_id"])
ct["profissional_id"] = normalize_id_string(ct["profissional_id"])

# Merge por ID (left)
merged = at.merge(ct[["profissional_id","foto_url"]], on="profissional_id", how="left")

# Fallback por nome (se ainda sem foto)
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

final_cols = ["data","cliente","servico","hora_entrada","duracao_horas",
              "profissional_nome","profissional_id","status","foto_url"]
for c in final_cols:
    if c not in merged.columns:
        merged[c] = np.nan if c.endswith("_horas") else ""
merged_view = merged[final_cols].sort_values(by=["data","cliente","profissional_nome"])

merged_view["foto_url"] = merged_view["foto_url"].fillna("")
merged_view["status"]   = merged_view["status"].fillna("")

with st.expander("🔎 Filtros"):
    c1,c2,c3 = st.columns([1,1,2])
    datas = sorted([d for d in merged_view["data"].dropna().unique() if pd.notna(d)])
    data_sel = c1.selectbox("Filtrar por Data", options=["(todas)"]+datas, index=0)
    txt_cliente = c2.text_input("Cliente contém","")
    txt_prof    = c3.text_input("Profissional contém","")
    status_opts = sorted([s for s in merged_view["status"].dropna().unique() if str(s).strip()!=""])
    status_sel  = st.multiselect("Status do serviço", options=status_opts, default=status_opts)
    mask = pd.Series(True, index=merged_view.index)
    if data_sel!="(todas)": mask &= (merged_view["data"]==data_sel)
    if txt_cliente.strip(): mask &= merged_view["cliente"].str.contains(txt_cliente.strip(), case=False, na=False)
    if txt_prof.strip():    mask &= merged_view["profissional_nome"].str.contains(txt_prof.strip(), case=False, na=False)
    if status_sel:          mask &= merged_view["status"].isin(status_sel)
    merged_view = merged_view[mask]

st.subheader("📄 Tabela de Atendimentos")
st.dataframe(merged_view, use_container_width=True, hide_index=True)

st.subheader("🖼️ Cartões com Foto")
if merged_view.empty:
    st.info("Nenhum atendimento para exibir.")
else:
    n_cols = st.slider("Colunas", 2, 6, 4)
    rows = [merged_view.iloc[i:i+n_cols] for i in range(0,len(merged_view),n_cols)]
    for r in rows:
        cols = st.columns(len(r))
        for col,(_,row) in zip(cols, r.iterrows()):
            with col:
                st.markdown(f"**{_s(row.get('cliente'))}**")
                st.caption(f"{_s(row.get('servico'))}")
                status_txt = _s(row.get('status'))
                st.write(f"📅 {_s(row.get('data'))}  ⏱️ {_s(row.get('hora_entrada'))} • {_s(row.get('duracao_horas'))}h"
                         + (f" • 🔖 {status_txt}" if status_txt else ""))
                st.write(f"👤 {_s(row.get('profissional_nome'))} | ID: {_s(row.get('profissional_id'))}")
                val = row.get("foto_url", None)
                url = "" if (val is None or (isinstance(val,float) and pd.isna(val))) else str(val).strip()
                if url:
                    try:
                        st.image(url, width=480, caption=_s(row.get('profissional_nome')))
                    except Exception:
                        st.warning("Não foi possível carregar a imagem desta URL.")
                else:
                    st.info("Sem foto cadastrada.")


st.subheader("⬇️ Exportar")
csv_bytes = merged_view.to_csv(index=False).encode("utf-8-sig")
st.download_button("Baixar CSV", data=csv_bytes, file_name="atendimentos_fotos.csv", mime="text/csv")

out = io.BytesIO()
with pd.ExcelWriter(out, engine="xlsxwriter") as wr:
    merged_view.to_excel(wr, index=False, sheet_name="Atendimentos")
    ws = wr.sheets["Atendimentos"]
    for i,col in enumerate(merged_view.columns):
        try: maxlen = int(min(60, max(10, merged_view[col].astype(str).str.len().max())))
        except ValueError: maxlen = 20
        ws.set_column(i, i, maxlen+2)

st.download_button("Baixar Excel", data=out.getvalue(),
                   file_name="atendimentos_fotos.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.caption("Agora a coluna **Carteirinha** é reconhecida como fonte de foto. Cruzamento por ID é prioridade.")



