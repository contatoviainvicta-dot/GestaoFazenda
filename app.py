import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from pathlib import Path
from datetime import date

st.set_page_config(
    page_title="AgroGestão - Gestão de Fazenda",
    page_icon="🐄",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #f5f0e8; }
    [data-testid="stSidebar"] { background-color: #2d4a22; }
    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] p { color: #e8dcc8 !important; }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 { color: #f0e6cc !important; }
    .stButton > button { background-color: #5a8a3c; color: white; border: none; border-radius: 4px; font-weight: 600; }
    .stButton > button:hover { background-color: #2d4a22; }
    div[data-testid="metric-container"] { background-color: white; border: 1px solid #d4c5a9; border-radius: 8px; padding: 1rem; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# BANCO DE DADOS
# ─────────────────────────────────────────────

DB_PATH = Path("fazenda.db")

def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS lotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE NOT NULL,
            nome TEXT NOT NULL,
            data_entrada DATE NOT NULL,
            raca TEXT, sexo TEXT, categoria TEXT,
            quantidade INTEGER,
            peso_entrada_total REAL, preco_total REAL, preco_arroba REAL,
            fornecedor TEXT, origem TEXT, observacoes TEXT,
            status TEXT DEFAULT 'Ativo',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS animais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lote_id INTEGER NOT NULL,
            brinco TEXT UNIQUE NOT NULL,
            nome TEXT, sexo TEXT, raca TEXT,
            data_nascimento DATE, peso_entrada REAL,
            status TEXT DEFAULT 'Ativo', observacoes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (lote_id) REFERENCES lotes(id)
        );
        CREATE TABLE IF NOT EXISTS pesagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            animal_id INTEGER, lote_id INTEGER NOT NULL,
            data_pesagem DATE NOT NULL, peso REAL NOT NULL,
            tipo TEXT DEFAULT 'Rotina', responsavel TEXT, observacoes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (animal_id) REFERENCES animais(id),
            FOREIGN KEY (lote_id) REFERENCES lotes(id)
        );
        CREATE TABLE IF NOT EXISTS ocorrencias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lote_id INTEGER, animal_id INTEGER,
            tipo TEXT NOT NULL, descricao TEXT,
            data_ocorrencia DATE NOT NULL,
            responsavel TEXT, custo REAL DEFAULT 0,
            tratamento TEXT, status TEXT DEFAULT 'Aberta',
            data_resolucao DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (lote_id) REFERENCES lotes(id),
            FOREIGN KEY (animal_id) REFERENCES animais(id)
        );
    """)
    conn.commit()
    conn.close()

init_db()

# ── Lotes ──
def inserir_lote(d):
    conn = get_conn(); c = conn.cursor()
    c.execute("""INSERT INTO lotes (codigo,nome,data_entrada,raca,sexo,categoria,quantidade,
        peso_entrada_total,preco_total,preco_arroba,fornecedor,origem,observacoes)
        VALUES (:codigo,:nome,:data_entrada,:raca,:sexo,:categoria,:quantidade,
        :peso_entrada_total,:preco_total,:preco_arroba,:fornecedor,:origem,:observacoes)""", d)
    conn.commit(); lid = c.lastrowid; conn.close(); return lid

def listar_lotes():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM lotes ORDER BY data_entrada DESC", conn)
    conn.close(); return df

def atualizar_status_lote(lote_id, status):
    conn = get_conn(); c = conn.cursor()
    c.execute("UPDATE lotes SET status=? WHERE id=?", (status, lote_id))
    conn.commit(); conn.close()

# ── Animais ──
def inserir_animal(d):
    conn = get_conn(); c = conn.cursor()
    c.execute("""INSERT INTO animais (lote_id,brinco,nome,sexo,raca,data_nascimento,peso_entrada,observacoes)
        VALUES (:lote_id,:brinco,:nome,:sexo,:raca,:data_nascimento,:peso_entrada,:observacoes)""", d)
    conn.commit(); aid = c.lastrowid; conn.close(); return aid

def listar_animais(lote_id=None):
    conn = get_conn()
    if lote_id:
        df = pd.read_sql("""SELECT a.*, l.codigo as lote_codigo FROM animais a
            JOIN lotes l ON a.lote_id=l.id WHERE a.lote_id=? ORDER BY a.brinco""", conn, params=(lote_id,))
    else:
        df = pd.read_sql("""SELECT a.*, l.codigo as lote_codigo FROM animais a
            JOIN lotes l ON a.lote_id=l.id ORDER BY a.brinco""", conn)
    conn.close(); return df

# ── Pesagens ──
def inserir_pesagem(d):
    conn = get_conn(); c = conn.cursor()
    c.execute("""INSERT INTO pesagens (animal_id,lote_id,data_pesagem,peso,tipo,responsavel,observacoes)
        VALUES (:animal_id,:lote_id,:data_pesagem,:peso,:tipo,:responsavel,:observacoes)""", d)
    conn.commit(); conn.close()

def listar_pesagens(lote_id=None, animal_id=None):
    conn = get_conn()
    base = """SELECT p.*, a.brinco, l.codigo as lote_codigo FROM pesagens p
        LEFT JOIN animais a ON p.animal_id=a.id
        LEFT JOIN lotes l ON p.lote_id=l.id"""
    if animal_id:
        df = pd.read_sql(base + " WHERE p.animal_id=? ORDER BY p.data_pesagem", conn, params=(animal_id,))
    elif lote_id:
        df = pd.read_sql(base + " WHERE p.lote_id=? ORDER BY p.data_pesagem", conn, params=(lote_id,))
    else:
        df = pd.read_sql(base + " ORDER BY p.data_pesagem DESC", conn)
    conn.close(); return df

# ── Ocorrências ──
def inserir_ocorrencia(d):
    conn = get_conn(); c = conn.cursor()
    c.execute("""INSERT INTO ocorrencias (lote_id,animal_id,tipo,descricao,data_ocorrencia,
        responsavel,custo,tratamento,status)
        VALUES (:lote_id,:animal_id,:tipo,:descricao,:data_ocorrencia,
        :responsavel,:custo,:tratamento,:status)""", d)
    conn.commit(); conn.close()

def listar_ocorrencias(lote_id=None, animal_id=None):
    conn = get_conn()
    base = """SELECT o.*, l.codigo as lote_codigo, a.brinco FROM ocorrencias o
        LEFT JOIN lotes l ON o.lote_id=l.id
        LEFT JOIN animais a ON o.animal_id=a.id"""
    if animal_id:
        df = pd.read_sql(base + " WHERE o.animal_id=? ORDER BY o.data_ocorrencia DESC", conn, params=(animal_id,))
    elif lote_id:
        df = pd.read_sql(base + " WHERE o.lote_id=? ORDER BY o.data_ocorrencia DESC", conn, params=(lote_id,))
    else:
        df = pd.read_sql(base + " ORDER BY o.data_ocorrencia DESC", conn)
    conn.close(); return df

def resolver_ocorrencia(oc_id, data_res):
    conn = get_conn(); c = conn.cursor()
    c.execute("UPDATE ocorrencias SET status='Resolvida', data_resolucao=? WHERE id=?", (data_res, oc_id))
    conn.commit(); conn.close()

# ── Cálculo GMD ──
def calcular_gmd(animal_id):
    df = listar_pesagens(animal_id=animal_id)
    if len(df) < 2:
        return None
    df["data_pesagem"] = pd.to_datetime(df["data_pesagem"])
    df = df.sort_values("data_pesagem")
    p0, p1 = df.iloc[0], df.iloc[-1]
    dias = (p1["data_pesagem"] - p0["data_pesagem"]).days
    if dias == 0:
        return None
    return {"gmd": round((p1["peso"] - p0["peso"]) / dias, 3),
            "peso_inicial": p0["peso"], "peso_final": p1["peso"],
            "ganho_total": round(p1["peso"] - p0["peso"], 2),
            "dias": dias, "n_pesagens": len(df)}

def calcular_gmd_lote(lote_id):
    animais = listar_animais(lote_id)
    if animais.empty:
        return None
    rows = []
    for _, a in animais.iterrows():
        g = calcular_gmd(a["id"])
        if g:
            g["brinco"] = a["brinco"]; g["animal_id"] = a["id"]
            rows.append(g)
    return pd.DataFrame(rows) if rows else None

def resumo_dashboard():
    conn = get_conn()
    lotes  = pd.read_sql("SELECT * FROM lotes WHERE status='Ativo'", conn)
    animais = pd.read_sql("SELECT * FROM animais WHERE status='Ativo'", conn)
    ocs    = pd.read_sql("SELECT * FROM ocorrencias WHERE status='Aberta'", conn)
    pesagens = pd.read_sql("SELECT * FROM pesagens", conn)
    conn.close()
    return {"total_lotes": len(lotes), "total_animais": len(animais),
            "ocorrencias_abertas": len(ocs), "total_pesagens": len(pesagens),
            "valor_total_investido": lotes["preco_total"].sum() if not lotes.empty else 0}

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
RACAS = ["Nelore","Angus","Brahman","Simmental","Hereford","Brangus","Gir","Guzerá","Tabapuã","Cruzado","Outra"]
CATEGORIAS = ["Bezerro","Garrote","Novilho","Boi","Vaca","Novilha","Touro"]
SEXOS = ["Macho","Fêmea","Misto"]
TIPOS_OC = ["Doença Respiratória","Doença Digestiva","Lesão/Trauma","Parasitismo (carrapato, berne)",
            "Brucelose/Vacinação","Morte","Fuga/Extravio","Problema Reprodutivo",
            "Deficiência Nutricional","Outro"]

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("# 🐄 AgroGestão")
    st.markdown("---")
    st.markdown("### Navegação")
    page = st.radio("", ["🏠 Dashboard","📦 Lotes","⚖️ Pesagens",
                         "🏥 Ocorrências","📊 Análises & GMD","🔍 Comparativos"],
                    label_visibility="collapsed")
    st.markdown("---")
    st.markdown("**Fazenda:** Rancho Belo")
    st.markdown("**Responsável:** Admin")

# ─────────────────────────────────────────────
# PÁGINAS
# ─────────────────────────────────────────────

# ════════════ DASHBOARD ════════════
if page == "🏠 Dashboard":
    st.title("🏠 Dashboard — Visão Geral da Fazenda")
    st.markdown("---")
    r = resumo_dashboard()
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("🗂️ Lotes Ativos", r["total_lotes"])
    c2.metric("🐄 Animais Ativos", r["total_animais"])
    c3.metric("⚠️ Ocorrências Abertas", r["ocorrencias_abertas"])
    c4.metric("⚖️ Total Pesagens", r["total_pesagens"])
    c5.metric("💰 Investimento Total", f"R$ {r['valor_total_investido']:,.2f}")
    st.markdown("---")

    ca, cb = st.columns(2)
    lotes = listar_lotes()
    with ca:
        st.subheader("📦 Lotes por Raça")
        if not lotes.empty:
            ativos = lotes[lotes["status"]=="Ativo"]
            if not ativos.empty:
                rc = ativos.groupby("raca")["quantidade"].sum().reset_index()
                fig = px.pie(rc, values="quantidade", names="raca",
                             color_discrete_sequence=["#5a8a3c","#8ab05a","#2d4a22","#a8c87a","#d4e8a0"], hole=0.4)
                fig.update_layout(margin=dict(t=20,b=20,l=20,r=20), height=280)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nenhum lote cadastrado ainda.")

    with cb:
        st.subheader("💰 Investimento por Lote")
        if not lotes.empty:
            ativos = lotes[lotes["status"]=="Ativo"].head(8)
            if not ativos.empty:
                fig = px.bar(ativos, x="codigo", y="preco_total",
                             labels={"codigo":"Lote","preco_total":"R$"},
                             color_discrete_sequence=["#5a8a3c"])
                fig.update_layout(margin=dict(t=20,b=20,l=20,r=20), height=280)
                st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    cc, cd = st.columns(2)
    with cc:
        st.subheader("⚠️ Ocorrências Recentes")
        ocs = listar_ocorrencias()
        if not ocs.empty:
            exib = ocs.head(5)[["data_ocorrencia","tipo","lote_codigo","brinco","status"]].copy()
            exib.columns = ["Data","Tipo","Lote","Animal","Status"]
            st.dataframe(exib, use_container_width=True, hide_index=True)
        else:
            st.success("✅ Nenhuma ocorrência registrada.")

    with cd:
        st.subheader("⚖️ Evolução de Pesagens")
        pes = listar_pesagens()
        if not pes.empty:
            pes["data_pesagem"] = pd.to_datetime(pes["data_pesagem"])
            ps = pes.sort_values("data_pesagem").tail(30)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=ps["data_pesagem"], y=ps["peso"],
                mode="markers+lines", marker=dict(color="#5a8a3c",size=6),
                line=dict(color="#5a8a3c",width=2)))
            fig.update_layout(xaxis_title="Data", yaxis_title="Peso (kg)",
                              margin=dict(t=10,b=20,l=20,r=20), height=280)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nenhuma pesagem registrada ainda.")

    st.markdown("---")
    st.subheader("🔔 Alertas")
    alertas = []
    if r["ocorrencias_abertas"] > 0:
        alertas.append(f"⚠️ **{r['ocorrencias_abertas']} ocorrência(s) em aberto** requerem atenção.")
    if r["total_animais"] == 0:
        alertas.append("📋 Nenhum animal cadastrado. Cadastre um lote para começar.")
    if not alertas:
        st.success("✅ Tudo em ordem! Nenhum alerta no momento.")
    else:
        for a in alertas:
            st.warning(a)

# ════════════ LOTES ════════════
elif page == "📦 Lotes":
    st.title("📦 Gestão de Lotes")
    tab1, tab2, tab3 = st.tabs(["📋 Listar Lotes","➕ Cadastrar Lote","🐄 Animais do Lote"])

    with tab1:
        lotes = listar_lotes()
        if lotes.empty:
            st.info("Nenhum lote cadastrado. Vá para 'Cadastrar Lote' para começar.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                fs = st.selectbox("Filtrar por status", ["Todos","Ativo","Encerrado","Vendido"])
            with col2:
                fr = st.selectbox("Filtrar por raça", ["Todas"]+RACAS)
            df = lotes.copy()
            if fs != "Todos": df = df[df["status"]==fs]
            if fr != "Todas": df = df[df["raca"]==fr]
            cols = ["codigo","nome","data_entrada","raca","categoria","quantidade",
                    "peso_entrada_total","preco_total","preco_arroba","fornecedor","status"]
            nomes = ["Código","Nome","Data Entrada","Raça","Categoria","Qtd",
                     "Peso Total (kg)","Preço Total (R$)","R$/@","Fornecedor","Status"]
            st.dataframe(df[cols].rename(columns=dict(zip(cols,nomes))),
                         use_container_width=True, hide_index=True)
            st.markdown("---")
            st.subheader("✏️ Atualizar Status")
            if not df.empty:
                sel = st.selectbox("Lote", df["codigo"].tolist(), key="upd_lote")
                row = lotes[lotes["codigo"]==sel].iloc[0]
                opts = ["Ativo","Encerrado","Vendido"]
                novo = st.selectbox("Novo status", opts,
                                    index=opts.index(row["status"]) if row["status"] in opts else 0)
                if st.button("💾 Atualizar"):
                    atualizar_status_lote(row["id"], novo)
                    st.success(f"Status de {sel} atualizado para {novo}!")
                    st.rerun()

    with tab2:
        st.subheader("➕ Cadastrar Novo Lote")
        with st.form("form_lote", clear_on_submit=True):
            c1,c2,c3 = st.columns(3)
            with c1:
                codigo = st.text_input("Código *", placeholder="L2024-001")
                nome   = st.text_input("Nome *", placeholder="Nelore Engorda Jan/24")
                dt_ent = st.date_input("Data de Entrada *", value=date.today())
                forn   = st.text_input("Fornecedor")
            with c2:
                raca   = st.selectbox("Raça", RACAS)
                sexo   = st.selectbox("Sexo", SEXOS)
                categ  = st.selectbox("Categoria", CATEGORIAS)
                orig   = st.text_input("Origem", placeholder="Uberaba/MG")
            with c3:
                qtd    = st.number_input("Quantidade *", min_value=1, value=1)
                peso_t = st.number_input("Peso Total Entrada (kg)", min_value=0.0, step=0.5)
                preco  = st.number_input("Preço Total (R$)", min_value=0.0, step=100.0)
                arroba = st.number_input("Preço por @ (R$)", min_value=0.0, step=1.0)
            obs = st.text_area("Observações")
            if st.form_submit_button("💾 Cadastrar Lote", use_container_width=True):
                if not codigo or not nome:
                    st.error("Código e Nome são obrigatórios.")
                else:
                    try:
                        inserir_lote({"codigo":codigo,"nome":nome,"data_entrada":str(dt_ent),
                            "raca":raca,"sexo":sexo,"categoria":categ,"quantidade":qtd,
                            "peso_entrada_total":peso_t,"preco_total":preco,"preco_arroba":arroba,
                            "fornecedor":forn,"origem":orig,"observacoes":obs})
                        st.success(f"✅ Lote {codigo} cadastrado!")
                    except Exception as e:
                        st.error(f"Erro: {e}")

    with tab3:
        st.subheader("🐄 Animais por Lote")
        lotes = listar_lotes()
        if lotes.empty:
            st.info("Cadastre um lote primeiro.")
        else:
            opts = (lotes["codigo"] + " — " + lotes["nome"]).tolist()
            sel  = st.selectbox("Lote", opts)
            cod  = sel.split(" — ")[0]
            lid  = lotes[lotes["codigo"]==cod].iloc[0]["id"]
            anis = listar_animais(lid)
            if not anis.empty:
                cols = ["brinco","nome","sexo","raca","data_nascimento","peso_entrada","status"]
                nc   = ["Brinco","Nome","Sexo","Raça","Nasc.","Peso Entrada (kg)","Status"]
                st.dataframe(anis[cols].rename(columns=dict(zip(cols,nc))),
                             use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum animal individual cadastrado neste lote.")
            st.markdown("---")
            st.subheader("➕ Cadastrar Animal Individual")
            with st.form("form_animal", clear_on_submit=True):
                c1,c2 = st.columns(2)
                with c1:
                    brinco  = st.text_input("Brinco *", placeholder="0042")
                    nome_a  = st.text_input("Nome/Apelido")
                    sexo_a  = st.selectbox("Sexo", ["Macho","Fêmea"])
                    raca_a  = st.selectbox("Raça", RACAS)
                with c2:
                    nasc    = st.date_input("Data Nascimento", value=None)
                    peso_e  = st.number_input("Peso Entrada (kg)", min_value=0.0, step=0.5)
                    obs_a   = st.text_area("Observações")
                if st.form_submit_button("💾 Cadastrar Animal", use_container_width=True):
                    if not brinco:
                        st.error("Brinco obrigatório.")
                    else:
                        try:
                            inserir_animal({"lote_id":lid,"brinco":brinco,"nome":nome_a,
                                "sexo":sexo_a,"raca":raca_a,
                                "data_nascimento":str(nasc) if nasc else None,
                                "peso_entrada":peso_e,"observacoes":obs_a})
                            st.success(f"✅ Animal brinco {brinco} cadastrado!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro: {e}")

# ════════════ PESAGENS ════════════
elif page == "⚖️ Pesagens":
    st.title("⚖️ Controle de Pesagens")
    tab1, tab2, tab3 = st.tabs(["📋 Histórico","➕ Registrar Pesagem","📈 Evolução por Animal"])

    with tab1:
        lotes = listar_lotes()
        c1,c2 = st.columns(2)
        with c1:
            opts = ["Todos"] + (lotes["codigo"] + " — " + lotes["nome"]).tolist() if not lotes.empty else ["Todos"]
            fl = st.selectbox("Filtrar por Lote", opts)
        with c2:
            ft = st.selectbox("Tipo", ["Todos","Entrada","Rotina","Saída","Veterinária"])
        if fl != "Todos" and not lotes.empty:
            cod = fl.split(" — ")[0]
            lid = lotes[lotes["codigo"]==cod].iloc[0]["id"]
            pes = listar_pesagens(lote_id=lid)
        else:
            pes = listar_pesagens()
        if ft != "Todos":
            pes = pes[pes["tipo"]==ft]
        if not pes.empty:
            cols = ["data_pesagem","lote_codigo","brinco","peso","tipo","responsavel"]
            nc   = ["Data","Lote","Brinco","Peso (kg)","Tipo","Responsável"]
            exib = pes[[c for c in cols if c in pes.columns]].copy()
            exib.columns = nc[:len(exib.columns)]
            st.dataframe(exib, use_container_width=True, hide_index=True)
            c1,c2,c3 = st.columns(3)
            c1.metric("Total Registros", len(pes))
            c2.metric("Peso Médio (kg)", f"{pes['peso'].mean():.1f}")
            c3.metric("Peso Máximo (kg)", f"{pes['peso'].max():.1f}")
        else:
            st.info("Nenhuma pesagem encontrada.")

    with tab2:
        lotes = listar_lotes()
        if lotes.empty:
            st.warning("Cadastre um lote primeiro.")
        else:
            with st.form("form_pes", clear_on_submit=True):
                c1,c2 = st.columns(2)
                with c1:
                    lsel = st.selectbox("Lote *", (lotes["codigo"]+" — "+lotes["nome"]).tolist())
                    cod  = lsel.split(" — ")[0]
                    lid  = lotes[lotes["codigo"]==cod].iloc[0]["id"]
                    anis = listar_animais(lid)
                    opca = ["Lote inteiro"] + anis["brinco"].tolist() if not anis.empty else ["Lote inteiro"]
                    asel = st.selectbox("Animal (Brinco)", opca)
                    dt_p = st.date_input("Data *", value=date.today())
                    tipo = st.selectbox("Tipo", ["Rotina","Entrada","Saída","Veterinária"])
                with c2:
                    peso = st.number_input("Peso (kg) *", min_value=0.1, step=0.5, value=300.0)
                    resp = st.text_input("Responsável")
                    obs  = st.text_area("Observações")
                if st.form_submit_button("💾 Registrar", use_container_width=True):
                    aid = None
                    if asel != "Lote inteiro" and not anis.empty:
                        aid = anis[anis["brinco"]==asel].iloc[0]["id"]
                    inserir_pesagem({"lote_id":lid,"animal_id":aid,"data_pesagem":str(dt_p),
                        "peso":peso,"tipo":tipo,"responsavel":resp,"observacoes":obs})
                    st.success(f"✅ Pesagem de {peso} kg registrada!")
                    st.rerun()

    with tab3:
        lotes = listar_lotes()
        if lotes.empty:
            st.info("Nenhum lote cadastrado.")
        else:
            lsel = st.selectbox("Lote", (lotes["codigo"]+" — "+lotes["nome"]).tolist(), key="ev_l")
            cod  = lsel.split(" — ")[0]
            lid  = lotes[lotes["codigo"]==cod].iloc[0]["id"]
            anis = listar_animais(lid)
            if anis.empty:
                pes = listar_pesagens(lote_id=lid)
                if not pes.empty:
                    pes["data_pesagem"] = pd.to_datetime(pes["data_pesagem"])
                    fig = px.line(pes.sort_values("data_pesagem"), x="data_pesagem", y="peso",
                                  title="Evolução — Lote Inteiro", markers=True,
                                  color_discrete_sequence=["#5a8a3c"])
                    st.plotly_chart(fig, use_container_width=True)
            else:
                asel = st.multiselect("Animais (Brincos)", anis["brinco"].tolist(),
                                       default=anis["brinco"].head(5).tolist())
                if asel:
                    all_d = []
                    for b in asel:
                        a = anis[anis["brinco"]==b].iloc[0]
                        p = listar_pesagens(animal_id=a["id"])
                        if not p.empty:
                            p["brinco"] = b; all_d.append(p)
                    if all_d:
                        df_all = pd.concat(all_d)
                        df_all["data_pesagem"] = pd.to_datetime(df_all["data_pesagem"])
                        fig = px.line(df_all.sort_values("data_pesagem"), x="data_pesagem", y="peso",
                                      color="brinco", markers=True,
                                      title="Evolução por Animal",
                                      color_discrete_sequence=px.colors.qualitative.Set2)
                        st.plotly_chart(fig, use_container_width=True)

# ════════════ OCORRÊNCIAS ════════════
elif page == "🏥 Ocorrências":
    st.title("🏥 Ocorrências Adversas")
    tab1, tab2, tab3 = st.tabs(["📋 Listar","➕ Registrar","📊 Relatório"])

    with tab1:
        c1,c2,c3 = st.columns(3)
        with c1: fs = st.selectbox("Status", ["Todos","Aberta","Resolvida"])
        with c2: ft = st.selectbox("Tipo", ["Todos"]+TIPOS_OC)
        lotes = listar_lotes()
        with c3:
            opl = ["Todos"] + (lotes["codigo"]+" — "+lotes["nome"]).tolist() if not lotes.empty else ["Todos"]
            fl = st.selectbox("Lote", opl)
        ocs = listar_ocorrencias()
        if fs != "Todos": ocs = ocs[ocs["status"]==fs]
        if ft != "Todos": ocs = ocs[ocs["tipo"]==ft]
        if fl != "Todos" and not lotes.empty:
            cod = fl.split(" — ")[0]
            lid = lotes[lotes["codigo"]==cod].iloc[0]["id"]
            ocs = ocs[ocs["lote_id"]==lid]
        if not ocs.empty:
            for _, oc in ocs.iterrows():
                ico = "🔴" if oc["status"]=="Aberta" else "✅"
                with st.expander(f"{ico} [{oc['data_ocorrencia']}] {oc['tipo']} — Lote: {oc.get('lote_codigo','N/A')} | Animal: {oc.get('brinco','Lote inteiro') or 'Lote inteiro'}"):
                    ca,cb,cc = st.columns(3)
                    ca.markdown(f"**Tipo:** {oc['tipo']}\n\n**Data:** {oc['data_ocorrencia']}\n\n**Status:** {oc['status']}")
                    cb.markdown(f"**Responsável:** {oc.get('responsavel','—')}\n\n**Custo:** R$ {oc.get('custo',0):.2f}")
                    cc.markdown(f"**Descrição:** {oc.get('descricao','—')}\n\n**Tratamento:** {oc.get('tratamento','—')}")
                    if oc["status"] == "Aberta":
                        dr = st.date_input("Data resolução", value=date.today(), key=f"dr_{oc['id']}")
                        if st.button("✅ Marcar Resolvida", key=f"res_{oc['id']}"):
                            resolver_ocorrencia(oc["id"], str(dr))
                            st.success("Resolvida!"); st.rerun()
        else:
            st.success("✅ Nenhuma ocorrência com esses filtros.")

    with tab2:
        lotes = listar_lotes()
        if lotes.empty:
            st.warning("Cadastre um lote primeiro.")
        else:
            with st.form("form_oc", clear_on_submit=True):
                c1,c2 = st.columns(2)
                with c1:
                    lsel = st.selectbox("Lote *", (lotes["codigo"]+" — "+lotes["nome"]).tolist())
                    cod  = lsel.split(" — ")[0]
                    lid  = lotes[lotes["codigo"]==cod].iloc[0]["id"]
                    anis = listar_animais(lid)
                    opca = ["Lote inteiro"] + anis["brinco"].tolist() if not anis.empty else ["Lote inteiro"]
                    asel = st.selectbox("Animal", opca)
                    tipo = st.selectbox("Tipo *", TIPOS_OC)
                    dt_o = st.date_input("Data *", value=date.today())
                with c2:
                    resp = st.text_input("Responsável")
                    custo = st.number_input("Custo (R$)", min_value=0.0, step=10.0)
                    stat = st.selectbox("Status", ["Aberta","Resolvida"])
                desc  = st.text_area("Descrição *")
                trat  = st.text_area("Tratamento Aplicado")
                if st.form_submit_button("💾 Registrar", use_container_width=True):
                    if not desc:
                        st.error("Descrição obrigatória.")
                    else:
                        aid = None
                        if asel != "Lote inteiro" and not anis.empty:
                            aid = anis[anis["brinco"]==asel].iloc[0]["id"]
                        inserir_ocorrencia({"lote_id":lid,"animal_id":aid,"tipo":tipo,
                            "descricao":desc,"data_ocorrencia":str(dt_o),"responsavel":resp,
                            "custo":custo,"tratamento":trat,"status":stat})
                        st.success("✅ Ocorrência registrada!"); st.rerun()

    with tab3:
        ocs = listar_ocorrencias()
        if ocs.empty:
            st.info("Nenhuma ocorrência registrada.")
        else:
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Total", len(ocs))
            c2.metric("Abertas", len(ocs[ocs["status"]=="Aberta"]))
            c3.metric("Resolvidas", len(ocs[ocs["status"]=="Resolvida"]))
            c4.metric("Custo Total", f"R$ {ocs['custo'].sum():,.2f}")
            ca,cb = st.columns(2)
            with ca:
                tc = ocs.groupby("tipo").size().reset_index(name="count")
                fig = px.bar(tc, x="count", y="tipo", orientation="h",
                             title="Ocorrências por Tipo", color_discrete_sequence=["#5a8a3c"])
                fig.update_layout(yaxis_title="", xaxis_title="Qtd", height=350)
                st.plotly_chart(fig, use_container_width=True)
            with cb:
                ct = ocs.groupby("tipo")["custo"].sum().reset_index()
                ct = ct[ct["custo"]>0]
                if not ct.empty:
                    fig2 = px.pie(ct, values="custo", names="tipo", title="Custo por Tipo",
                                  color_discrete_sequence=["#5a8a3c","#8ab05a","#2d4a22","#a8c87a","#c8401c","#e8784c"])
                    fig2.update_layout(height=350)
                    st.plotly_chart(fig2, use_container_width=True)

# ════════════ ANÁLISES & GMD ════════════
elif page == "📊 Análises & GMD":
    st.title("📊 Análises & Cálculo de GMD")
    st.info("**GMD — Ganho de Peso Médio Diário** = (Peso Final − Peso Inicial) ÷ Número de Dias")

    lotes = listar_lotes()
    if lotes.empty:
        st.warning("Cadastre lotes e pesagens para ver análises.")
    else:
        tab1, tab2, tab3 = st.tabs(["🐄 GMD por Animal","📦 GMD por Lote","💹 Custo de Produção"])

        with tab1:
            lsel = st.selectbox("Lote", (lotes["codigo"]+" — "+lotes["nome"]).tolist(), key="an_l")
            cod  = lsel.split(" — ")[0]
            lid  = lotes[lotes["codigo"]==cod].iloc[0]["id"]
            anis = listar_animais(lid)
            lote_info = lotes[lotes["codigo"]==cod].iloc[0]

            if anis.empty:
                pes = listar_pesagens(lote_id=lid)
                if not pes.empty and len(pes) >= 2:
                    pes["data_pesagem"] = pd.to_datetime(pes["data_pesagem"])
                    pes = pes.sort_values("data_pesagem")
                    p0,p1 = pes.iloc[0], pes.iloc[-1]
                    dias = (p1["data_pesagem"] - p0["data_pesagem"]).days
                    if dias > 0:
                        gmd = (p1["peso"] - p0["peso"]) / dias
                        c1,c2,c3,c4 = st.columns(4)
                        c1.metric("Peso Inicial", f"{p0['peso']:.1f} kg")
                        c2.metric("Peso Atual", f"{p1['peso']:.1f} kg")
                        c3.metric("Ganho Total", f"{p1['peso']-p0['peso']:.1f} kg")
                        c4.metric("GMD", f"{gmd:.3f} kg/dia", delta=f"{dias} dias")
                        fig = px.line(pes, x="data_pesagem", y="peso", markers=True,
                                      title="Evolução — Lote", color_discrete_sequence=["#5a8a3c"])
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Sem dados suficientes. Adicione pelo menos 2 pesagens.")
            else:
                asel = st.selectbox("Animal (Brinco)", anis["brinco"].tolist())
                aid  = anis[anis["brinco"]==asel].iloc[0]["id"]
                gd   = calcular_gmd(aid)
                pes  = listar_pesagens(animal_id=aid)
                if gd is None:
                    st.info("São necessárias pelo menos 2 pesagens para calcular o GMD.")
                else:
                    c1,c2,c3,c4,c5 = st.columns(5)
                    c1.metric("Peso Inicial", f"{gd['peso_inicial']:.1f} kg")
                    c2.metric("Peso Final", f"{gd['peso_final']:.1f} kg")
                    c3.metric("Ganho Total", f"{gd['ganho_total']:.1f} kg")
                    c4.metric("GMD", f"{gd['gmd']:.3f} kg/dia")
                    c5.metric("Dias", gd["dias"])
                    gv = gd["gmd"]
                    if gv >= 1.2:   st.success(f"🏆 Desempenho Excelente! GMD {gv:.3f} kg/dia")
                    elif gv >= 0.8: st.info(f"👍 Bom Desempenho. GMD {gv:.3f} kg/dia")
                    elif gv >= 0.5: st.warning(f"⚠️ Desempenho Moderado. GMD {gv:.3f} kg/dia")
                    else:           st.error(f"🔴 Desempenho Baixo! GMD {gv:.3f} kg/dia — verificar alimentação e saúde.")
                    if not pes.empty:
                        pes["data_pesagem"] = pd.to_datetime(pes["data_pesagem"])
                        pes = pes.sort_values("data_pesagem")
                        ult = pes["data_pesagem"].max()
                        proj_d = pd.date_range(start=ult, periods=31, freq="D")
                        proj_p = [pes["peso"].iloc[-1] + gv*i for i in range(31)]
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=pes["data_pesagem"], y=pes["peso"],
                            mode="lines+markers", name="Peso Real",
                            line=dict(color="#5a8a3c",width=2), marker=dict(size=8)))
                        fig.add_trace(go.Scatter(x=proj_d, y=proj_p,
                            mode="lines", name="Projeção 30 dias",
                            line=dict(color="#e8784c",width=2,dash="dash")))
                        fig.update_layout(title=f"Evolução — Brinco {asel}",
                            xaxis_title="Data", yaxis_title="Peso (kg)")
                        st.plotly_chart(fig, use_container_width=True)

        with tab2:
            lsel2 = st.selectbox("Lote", (lotes["codigo"]+" — "+lotes["nome"]).tolist(), key="gmd_l")
            cod2  = lsel2.split(" — ")[0]
            lid2  = lotes[lotes["codigo"]==cod2].iloc[0]["id"]
            dfg   = calcular_gmd_lote(lid2)
            if dfg is None or dfg.empty:
                st.info("Sem dados suficientes (mín. 2 pesagens por animal).")
            else:
                c1,c2,c3 = st.columns(3)
                c1.metric("GMD Médio", f"{dfg['gmd'].mean():.3f} kg/dia")
                c2.metric("Melhor GMD", f"{dfg['gmd'].max():.3f} — Brinco {dfg.loc[dfg['gmd'].idxmax(),'brinco']}")
                c3.metric("Pior GMD", f"{dfg['gmd'].min():.3f} — Brinco {dfg.loc[dfg['gmd'].idxmin(),'brinco']}")
                fig = px.bar(dfg.sort_values("gmd",ascending=False), x="brinco", y="gmd",
                             color="gmd", color_continuous_scale=["#c8401c","#ffc107","#5a8a3c"],
                             title="GMD por Animal", labels={"brinco":"Brinco","gmd":"GMD (kg/dia)"})
                fig.add_hline(y=dfg["gmd"].mean(), line_dash="dash", line_color="navy",
                              annotation_text="Média")
                st.plotly_chart(fig, use_container_width=True)
                dfr = dfg.sort_values("gmd",ascending=False).reset_index(drop=True)
                dfr.index += 1
                st.dataframe(dfr[["brinco","gmd","peso_inicial","peso_final","ganho_total","dias"]].rename(
                    columns={"brinco":"Brinco","gmd":"GMD","peso_inicial":"Peso Inicial",
                             "peso_final":"Peso Final","ganho_total":"Ganho (kg)","dias":"Dias"}),
                    use_container_width=True)

        with tab3:
            lsel3 = st.selectbox("Lote", (lotes["codigo"]+" — "+lotes["nome"]).tolist(), key="ct_l")
            cod3  = lsel3.split(" — ")[0]
            li3   = lotes[lotes["codigo"]==cod3].iloc[0]
            c1,c2 = st.columns(2)
            with c1:
                st.markdown("**Dados de Entrada**")
                custo_extra  = st.number_input("Custos extras acumulados (R$)", min_value=0.0, step=100.0)
                preco_venda  = st.number_input("Preço esperado de venda (R$/@)", min_value=0.0, step=1.0, value=280.0)
                rend_carc    = st.number_input("Rendimento de carcaça (%)", min_value=40.0, max_value=65.0, value=52.0)
            with c2:
                qtd = li3["quantidade"] or 1
                custo_tot = (li3["preco_total"] or 0) + custo_extra
                custo_cab = custo_tot / qtd
                pes_lote  = listar_pesagens(lote_id=li3["id"])
                if not pes_lote.empty:
                    peso_med = pes_lote.groupby("animal_id")["peso"].last().mean() if "animal_id" in pes_lote.columns else pes_lote["peso"].iloc[-1]
                else:
                    peso_med = (li3["peso_entrada_total"] or 0) / qtd
                arrobas   = (peso_med * (rend_carc/100)) / 15
                receita   = arrobas * preco_venda
                lucro_cab = receita - custo_cab
                st.markdown("**Resultado Estimado**")
                st.metric("Custo Total", f"R$ {custo_tot:,.2f}")
                st.metric("Custo por Cabeça", f"R$ {custo_cab:,.2f}")
                st.metric("Receita por Cabeça", f"R$ {receita:,.2f}")
                st.metric("Lucro por Cabeça", f"R$ {lucro_cab:,.2f}",
                          delta=f"Total lote: R$ {lucro_cab*qtd:,.2f}")
            if li3["preco_total"] and arrobas > 0:
                peq = custo_cab / arrobas
                st.info(f"💡 **Ponto de equilíbrio:** venda mínima de **R$ {peq:.2f}/@** para cobrir os custos.")

# ════════════ COMPARATIVOS ════════════
elif page == "🔍 Comparativos":
    st.title("🔍 Comparativos entre Lotes")
    lotes = listar_lotes()
    if lotes.empty:
        st.warning("Cadastre pelo menos 1 lote.")
    else:
        tab1, tab2, tab3 = st.tabs(["💰 Preços","🏆 Desempenho GMD","🏥 Saúde"])

        with tab1:
            ativos = lotes[lotes["status"]=="Ativo"] if "Ativo" in lotes["status"].values else lotes
            if ativos.empty:
                st.info("Nenhum lote ativo.")
            else:
                fig = px.bar(ativos, x="codigo", y="preco_total", color="raca",
                             title="Investimento Total por Lote",
                             labels={"codigo":"Lote","preco_total":"R$","raca":"Raça"},
                             color_discrete_sequence=px.colors.qualitative.Set2, text_auto=True)
                st.plotly_chart(fig, use_container_width=True)
                ca,cb = st.columns(2)
                with ca:
                    df_a = ativos[ativos["preco_arroba"]>0]
                    if not df_a.empty:
                        fig2 = px.bar(df_a, x="codigo", y="preco_arroba",
                                      title="Preço de Compra por @",
                                      color_discrete_sequence=["#5a8a3c"], text_auto=True)
                        st.plotly_chart(fig2, use_container_width=True)
                with cb:
                    ativos = ativos.copy()
                    ativos["custo_cab"] = ativos.apply(
                        lambda r: r["preco_total"]/r["quantidade"] if r["quantidade"]>0 else 0, axis=1)
                    df_cc = ativos[ativos["custo_cab"]>0]
                    if not df_cc.empty:
                        fig3 = px.bar(df_cc, x="codigo", y="custo_cab",
                                      title="Custo por Cabeça",
                                      color_discrete_sequence=["#2d4a22"], text_auto=True)
                        st.plotly_chart(fig3, use_container_width=True)
                tab_cols = ["codigo","nome","raca","categoria","quantidade",
                            "peso_entrada_total","preco_total","preco_arroba"]
                tab_nc   = ["Código","Nome","Raça","Categoria","Qtd",
                            "Peso Total (kg)","Preço Total (R$)","R$/@"]
                st.dataframe(ativos[tab_cols].rename(columns=dict(zip(tab_cols,tab_nc))),
                             use_container_width=True, hide_index=True)

        with tab2:
            opts = (lotes["codigo"]+" — "+lotes["nome"]).tolist()
            sels = st.multiselect("Lotes para comparar", opts, default=opts[:3])
            if sels:
                rows = []
                for s in sels:
                    cod = s.split(" — ")[0]
                    lid = lotes[lotes["codigo"]==cod].iloc[0]["id"]
                    li  = lotes[lotes["codigo"]==cod].iloc[0]
                    dg  = calcular_gmd_lote(lid)
                    if dg is not None and not dg.empty:
                        rows.append({"lote":cod,"nome":li["nome"],"raca":li["raca"],
                                     "gmd_medio":dg["gmd"].mean(),"gmd_max":dg["gmd"].max(),
                                     "gmd_min":dg["gmd"].min(),"n_animais":len(dg),
                                     "ganho_medio":dg["ganho_total"].mean()})
                if rows:
                    dc = pd.DataFrame(rows)
                    c1,c2,c3 = st.columns(3)
                    best = dc.loc[dc["gmd_medio"].idxmax()]
                    c1.metric("🥇 Melhor Lote", best["lote"], f"GMD: {best['gmd_medio']:.3f}")
                    c2.metric("Média Geral", f"{dc['gmd_medio'].mean():.3f} kg/dia")
                    c3.metric("Maior Ganho Médio", f"{dc['ganho_medio'].max():.1f} kg")
                    fig = px.bar(dc, x="lote", y="gmd_medio",
                                 error_y=dc["gmd_max"]-dc["gmd_medio"],
                                 error_y_minus=dc["gmd_medio"]-dc["gmd_min"],
                                 title="GMD Médio por Lote (com variação)",
                                 color_discrete_sequence=["#5a8a3c"])
                    fig.add_hline(y=dc["gmd_medio"].mean(), line_dash="dash",
                                  annotation_text="Média geral")
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(dc.rename(columns={"lote":"Lote","nome":"Nome","raca":"Raça",
                        "gmd_medio":"GMD Médio","gmd_max":"GMD Máx","gmd_min":"GMD Mín",
                        "n_animais":"Animais","ganho_medio":"Ganho Médio (kg)"}),
                        use_container_width=True, hide_index=True)
                else:
                    st.info("Sem dados de GMD nos lotes selecionados. Adicione pesagens.")

        with tab3:
            ocs = listar_ocorrencias()
            if ocs.empty:
                st.success("✅ Nenhuma ocorrência em nenhum lote.")
            else:
                rs = ocs.groupby("lote_codigo").agg(
                    total=("id","count"),
                    abertas=("status", lambda x: (x=="Aberta").sum()),
                    custo=("custo","sum")
                ).reset_index()
                li_info = lotes[["codigo","quantidade"]].rename(columns={"codigo":"lote_codigo"})
                rs = rs.merge(li_info, on="lote_codigo", how="left")
                ca,cb = st.columns(2)
                with ca:
                    fig = px.bar(rs, x="lote_codigo", y="total", color="abertas",
                                 title="Ocorrências por Lote",
                                 color_continuous_scale=["#5a8a3c","#ffc107","#dc3545"])
                    st.plotly_chart(fig, use_container_width=True)
                with cb:
                    rs_c = rs[rs["custo"]>0]
                    if not rs_c.empty:
                        fig2 = px.bar(rs_c, x="lote_codigo", y="custo",
                                      title="Custo com Saúde por Lote",
                                      color_discrete_sequence=["#c8401c"], text_auto=True)
                        st.plotly_chart(fig2, use_container_width=True)
                tl = ocs.groupby(["lote_codigo","tipo"]).size().reset_index(name="count")
                fig3 = px.density_heatmap(tl, x="lote_codigo", y="tipo", z="count",
                                           color_continuous_scale="YlOrRd",
                                           title="Ocorrências por Tipo e Lote")
                st.plotly_chart(fig3, use_container_width=True)
