import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf

# ==========================================
# 1. FUNÇÕES MATEMÁTICAS
# ==========================================
def calcular_ifr(series, periodos):
    delta = series.diff()
    ganho = delta.where(delta > 0, 0).ewm(alpha=1/periodos, adjust=False).mean()
    perda = (-delta.where(delta < 0, 0)).ewm(alpha=1/periodos, adjust=False).mean()
    
    rs = ganho / perda.replace(0, np.nan)
    ifr = 100 - (100 / (1 + rs))
    return ifr.fillna(100)

# ==========================================
# 2. MOTOR QUANTITATIVO (NUVEM)
# ==========================================
@st.cache_data(ttl="1d")
def varrer_mercado_ao_vivo():
    tickers = [
        "A1MD34", "AALR3", "AAPL34", "ABBV34", "ABCB4", "ABEV3", "ABTT34", "ADBE34", "AERI3", "AGRO3",
        "AGXY3", "ALLD3", "ALOS3", "ALPA4", "ALPK3", "ALUP11", "AMAR3", "AMBP3", "AMER3", "AMGN34",
        "AMOB3", "AMZO34", "ANIM3", "ARML3", "ASAI3", "ASML34", "ATMP3", "ATTB34", "AUAU3", "AURA33",
        "AURE3", "AVGO34", "AXIA3", "AXIA6", "AXIA7", "AXPB34", "AZEV3", "AZEV4", "AZUL53", "AZZA3",
        "B1IL34", "B3SA3", "BABA34", "BBAS3", "BBDC3", "BBDC4", "BBSE3", "BCSA34", "BEEF3", "BEES3",
        "BEES4", "BERK34", "BHIA3", "BIDU34", "BIOM3", "BKNG34", "BLAK34", "BLAU3", "BMEB4", "BMGB4",
        "BMOB3", "BOAC34", "BOBR4", "BOVA11", "BPAC11", "BRAP3", "BRAP4", "BRAV3", "BRBI11",
        "BRKM5", "BRSR3", "BRSR6", "C2OI34", "C2RW34", "CAMB3", "CAML3", "CASH3", "CATP34", "CBAV3",
        "CCRO3", "CEAB3", "CEBR3", "CGRA4", "CHCM34", "CHVX34", "CLSA3", "CMCS34", "CMIG3", "CMIG4",
        "CMIN3", "COCA34", "COCE5", "COGN3", "COPH34", "COWC34", "CPFE3", "CPLE3", "CSAN3", "CSCO34",
        "CSED3", "CSMG3", "CSNA3", "CSUD3", "CTGP34", "CURY3", "CVCB3", "CXSE3", "CYRE3", "DASA3",
        "DEEC34", "DESK3", "DEXP3", "DHER34", "DIRR3", "DISB34", "DIVO11", "DMVF3", "DOTZ3", "DXCO3",
        "E1CO34", "EALT4", "EBAY34", "ECOR3", "EGIE3", "EMBJ3", "ENEV3", "ENGI11", "ENJU3", "EQTL3",
        "ESPA3", "ETER3", "EUCA4", "EVEN3", "EXXO34", "EZTC3", "FESA4", "FHER3", "FIEI3", "FIQE3",
        "FLRY3", "FRAS3", "GEOO34", "GFSA3", "GGBR3", "GGBR4", "GGPS3", "GMAT3", "GMCO34", "GOAU3",
        "GOAU4", "GOGL34", "GOGL35", "GOLL54", "GOLD11", "GRND3", "GSGI34", "GUAR3", "HAGA4", "HAPV3",
        "HBOR3", "HBRE3", "HBSA3", "HYPE3", "IFCM3", "IGTI11", "IGTI3", "INEP3", "INEP4", "INTB3",
        "INTU34", "IRBR3", "ISAE4", "ITLC34", "ITSA3", "ITSA4", "ITUB3", "ITUB4", "IVVB11", "JALL3",
        "JBSS32", "JHSF3", "JNJB34", "JPMC34", "JSLG3", "K2CG34", "KEPL3", "KLBN11", "KLBN3", "KLBN4",
        "KRSA3", "L1MN34", "LAND3", "LAVV3", "LEVE3", "LIGT3", "LILY34", "LJQQ3", "LOGG3", "LOGN3",
        "LPSB3", "LREN3", "LUPA3", "LVTC3", "LWSA3", "M1RN34", "M1TA34", "MATD3", "MBLY3", "MCDC34",
        "MDIA3", "MDNE3", "MEAL3", "MELI34", "MELK3", "MGLU3", "MILS3", "MLAS3", "MOOO34", "MOTV3",
        "MOVI3", "MRCK34", "MBRF3", "MRVE3", "MSBR34", "MSCD34", "MSFT34", "MTRE3", "MULT3", "MUTC34",
        "MYPK3", "N1EM34", "N1OW34", "N1VO34", "NASD11", "NATU3", "NDIV11", "NEOE3", "NEXT34", "NFLX34",
        "NGRD3", "NIKE34", "NSDV11", "NVDC34", "ODPV3", "OFSA3", "OIBR3", "OIBR4", "ONCO3", "OPCT3",
        "ORCL34", "ORVR3", "OXYP34", "P1DD34", "P2LT34", "PAGS34", "PCAR3", "PDGR3", "PDTC3", "PETR3",
        "PETR4", "PETZ3", "PFIZ34", "PFRM3", "PGCO34", "PGMN3", "PINE4", "PLPL3", "PMAM3", "PNVL3",
        "POMO3", "POMO4", "POSI3", "PRIO3", "PRNR3", "PSSA3", "PTBL3", "PTNT4", "QCOM34", "QUAL3",
        "RADL3", "RAIL3", "RAIZ4", "RANI3", "RAPT4", "RCSL3", "RCSL4", "RDOR3", "RECV3", "RENT3",
        "RIAA3", "RNEW11", "RNEW3", "ROMI3", "ROXO34", "RSID3", "S1PO34", "S2EA34", "S2HO34", "S2NW34",
        "SANB11", "SANB3", "SANB4", "SAPR11", "SAPR3", "SAPR4", "SBFG3", "SBSP3", "SCAR3", "SCHW34",
        "SEER3", "SEQL3", "SHOW3", "SHUL4", "SIMH3", "SIMN34", "SLCE3", "SMFT3", "SMTO3", "SOJA3",
        "SPGI34", "SSFO34", "STBP3", "SUZB3", "SYNE3", "TAEE11", "TAEE3", "TAEE4", "TASA3", "TASA4",
        "TCSA3", "TECN3", "TEND3", "TFCO4", "TGMA3", "TIMS3", "TOTS3", "TPIS3", "TRAD3", "TRIS3",
        "TSLA34", "TSMC34", "TTEN3", "TUPY3", "U1BE34", "U1RI34", "UCAS3", "UGPA3", "UNHH34", "UNIP6",
        "UPSS34", "USIM3", "USIM5", "USTK11", "VALE3", "VAMO3", "VBBR3", "VERZ34", "VGIA11", "VISA34",
        "VITT3", "VIVA3", "VIVR3", "VIVT3", "VLID3", "VSTE3", "VTRU3", "VULC3", "VVEO3", "WALM34",
        "WEB311", "WEGE3", "WEST3", "WFCO34", "WHRL4", "WIZC3", "XFIX11", "XPBR31", "YDUQ3"
    ]
    tickers_sa = [t + ".SA" for t in tickers]
    benchmarks = ["BOVA11.SA", "IVVB11.SA"]
    lista_completa = list(set(tickers_sa + benchmarks))

    dados_brutos = yf.download(lista_completa, period="5y", progress=False, ignore_tz=True)

    lista_rastreador = []
    retorno_12m_ibov = 0
    retorno_12m_ivvb = 0

    for bench in benchmarks:
        try:
            df_b = pd.DataFrame({'Fechamento': dados_brutos['Close'][bench]}).ffill().dropna()
            if len(df_b) > 252:
                data_atual = df_b.index[-1]
                data_12m_atras = data_atual - pd.Timedelta(days=365)
                df_passado = df_b[df_b.index <= data_12m_atras]
                
                if not df_passado.empty:
                    ret_12m = (df_b['Fechamento'].iloc[-1] / df_passado['Fechamento'].iloc[-1]) - 1
                    if 'BOVA' in bench: retorno_12m_ibov = ret_12m
                    if 'IVVB' in bench: retorno_12m_ivvb = ret_12m
        except: pass

    for t_sa in tickers_sa:
        try:
            df = pd.DataFrame({
                'Fechamento': dados_brutos['Close'][t_sa],
                'Máximo': dados_brutos['High'][t_sa],
                'Mínimo': dados_brutos['Low'][t_sa],
                'Quantidade': dados_brutos['Volume'][t_sa]
            })
            
            # AUDITORIA: Preços preenchem vazios com o último valor (ffill). 
            # Quantidade/Volume preenche vazios com ZERO (fillna(0)).
            df[['Fechamento', 'Máximo', 'Mínimo']] = df[['Fechamento', 'Máximo', 'Mínimo']].ffill()
            df['Quantidade'] = df['Quantidade'].fillna(0)
            df = df.dropna() # Agora sim, remove a "sujeira" do IPO

            if len(df) < 252: continue 
            # ... (o resto continua igual, ticker_puro, etc) 
        
            ticker_puro = t_sa.replace(".SA", "")
            
            df['MM20'] = df['Fechamento'].rolling(window=20).mean()
            df['MM50'] = df['Fechamento'].rolling(window=50).mean()
            df['MM80'] = df['Fechamento'].rolling(window=80).mean()
            df['MM100'] = df['Fechamento'].rolling(window=100).mean()
            df['MM150'] = df['Fechamento'].rolling(window=150).mean()
            df['QtdMedia_20d'] = df['Quantidade'].rolling(window=20).mean()
            df['QtdMedia_60d'] = df['Quantidade'].rolling(window=60).mean()
            df['QtdMedia_100d'] = df['Quantidade'].rolling(window=100).mean()
            df['IFR40'] = calcular_ifr(df['Fechamento'], periodos=40)
            df['IFR3'] = calcular_ifr(df['Fechamento'], periodos=3)
            df['Max_52W'] = df['Máximo'].rolling(window=252).max()
            df['Min_52W'] = df['Mínimo'].rolling(window=252).min()
            df['Topo_Historico'] = df['Máximo'].cummax()

            min_8 = df['Mínimo'].rolling(window=8).min()
            max_8 = df['Máximo'].rolling(window=8).max()
            denominador = (max_8 - min_8).replace(0, np.nan)
            k_rapido = 100 * ((df['Fechamento'] - min_8) / denominador)
            df['Estocastico_Lento'] = k_rapido.rolling(window=3).mean().fillna(50)

            ema_curta = df['Fechamento'].ewm(span=3, adjust=False).mean()
            ema_longa = df['Fechamento'].ewm(span=10, adjust=False).mean()
            df['MACD_Linha'] = ema_curta - ema_longa
            df['MACD_Media_36'] = df['MACD_Linha'].ewm(span=36, adjust=False).mean()
            
            hoje = df.iloc[-1]
            data_atual = df.index[-1]
            preco_atual = hoje['Fechamento']
            
            data_12m_atras = data_atual - pd.Timedelta(days=365)
            df_passado = df[df.index <= data_12m_atras]
            retorno_12m = (preco_atual / df_passado.iloc[-1]['Fechamento']) - 1 if not df_passado.empty else 0
            
            ano_atual = data_atual.year
            df_ano_atual = df[df.index.year == ano_atual]
            retorno_ano = (preco_atual / df_ano_atual.iloc[0]['Fechamento']) - 1 if not df_ano_atual.empty else 0
            
            fr_ibov = ((1 + retorno_12m) / (1 + retorno_12m_ibov)) - 1 if retorno_12m_ibov != -1 else 0
            fr_ivvb = ((1 + retorno_12m) / (1 + retorno_12m_ivvb)) - 1 if retorno_12m_ivvb != -1 else 0

            lista_rastreador.append({
                'Ticker': ticker_puro, 'Fechamento': preco_atual, 'Retorno_Ano': retorno_ano,
                'Retorno_12M': retorno_12m, 'FR_IBOV': fr_ibov, 'FR_IVVB': fr_ivvb,
                'IFR3': hoje['IFR3'], 'IFR40': hoje['IFR40'], 'Estocastico_Lento': hoje['Estocastico_Lento'],
                'MACD_Linha_10_3': hoje['MACD_Linha'], 'MACD_Media_36': hoje['MACD_Media_36'],
                'Max_52W': hoje['Max_52W'], 'Min_52W': hoje['Min_52W'], 'Topo_Historico': hoje['Topo_Historico'],
                'MM20': hoje['MM20'], 'MM50': hoje['MM50'], 'MM80': hoje['MM80'],
                'MM100': hoje['MM100'], 'MM150': hoje['MM150'], 'Qtd_Acoes': hoje['Quantidade'],
                'QtdMedia_20d': hoje['QtdMedia_20d'], 'QtdMedia_60d': hoje['QtdMedia_60d'], 'QtdMedia_100d': hoje['QtdMedia_100d']
            })
        except Exception as e:
            pass
            
    return pd.DataFrame(lista_rastreador)

# ==========================================
# 3. INTERFACE DE USUÁRIO E FILTROS
# ==========================================
st.set_page_config(page_title="Terminal Quantitativo B3", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
           .block-container { padding-top: 1.5rem; padding-bottom: 0rem; }
           div[data-testid="stExpanderDetails"] { padding-top: 0px; padding-bottom: 0.5rem; }
           label[data-baseweb="checkbox"] { margin-bottom: -8px; }
           
           [data-testid="stSidebar"][aria-expanded="true"] {
               min-width: 320px !important;
               max-width: 320px !important;
           }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 🔐 SISTEMA DE LOGIN
# ==========================================
if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    st.markdown("<br><br><br>", unsafe_allow_html=True) 
    col1, col2, col3 = st.columns([1, 1, 1]) 
    
    with col2:
        st.markdown("<h2 style='text-align: center;'>🔐 Acesso Restrito</h2>", unsafe_allow_html=True)
        usuario = st.text_input("Usuário")
        senha = st.text_input("Senha", type="password")
        btn_login = st.button("Entrar no Terminal", type="primary", use_container_width=True)
        
        usuarios_permitidos = st.secrets["senhas"]
        
        if btn_login:
            if usuario in usuarios_permitidos and usuarios_permitidos[usuario] == senha: 
                st.session_state['autenticado'] = True
                st.rerun() 
            else:
                st.error("❌ Usuário ou senha incorretos.")
                
    st.stop()

# ==========================================
# PAINEL DE CONTROLE (PÓS-LOGIN)
# ==========================================
st.title("🌐 Terminal Quantitativo B3")

btn_varredura = st.button("🚀 Executar Varredura Ao Vivo (Forçar Atualização de Dados)")
if btn_varredura:
    varrer_mercado_ao_vivo.clear()

st.markdown("---")
st.sidebar.header("🎛️ Painel de Controle")

with st.sidebar.expander("📈 Filtros de Tendência", expanded=True):
    preco_minimo = st.number_input("Preço Mínimo (R$)", value=2.00, step=0.50)
    st.markdown("---")
    direcao_tendencia = st.radio("Selecione a Direção Buscada:", ["Alta (>)", "Baixa (<)"], horizontal=True)
    is_alta = (direcao_tendencia == "Alta (>)")
    simbolo = ">" if is_alta else "<"

    st.markdown("**1. Preço vs Médias Móveis:**")
    tend_p_mm20 = st.checkbox(f"Preço {simbolo} MM20", value=False)
    tend_p_mm50 = st.checkbox(f"Preço {simbolo} MM50", value=False)
    tend_p_mm80 = st.checkbox(f"Preço {simbolo} MM80", value=False)
    tend_p_mm150 = st.checkbox(f"Preço {simbolo} MM150", value=False)
    
    st.markdown("**2. Cruzamento de Médias Móveis:**")
    tend_mm20_50 = st.checkbox(f"MM20 {simbolo} MM50", value=False)
    tend_mm20_80 = st.checkbox(f"MM20 {simbolo} MM80", value=False)
    tend_mm50_80 = st.checkbox(f"MM50 {simbolo} MM80", value=False)
    tend_mm50_150 = st.checkbox(f"MM50 {simbolo} MM150", value=False)
    tend_mm80_150 = st.checkbox(f"MM80 {simbolo} MM150", value=False)

    st.markdown("---")
    texto_52w = "Próxima à Máxima de 52W (Até 5%)" if is_alta else "Próxima à Mínima de 52W (Até 5%)"
    rompimento_52w = st.checkbox(texto_52w, value=False)

with st.sidebar.expander("⚡ Filtros de Momento"):
    usar_ifr40 = st.checkbox("Filtro IFR40", value=True)
    if usar_ifr40:
        filtro_ifr40 = st.slider("IFR40 Mínimo (Força Longa)", 0, 100, 50)
        
    st.markdown("---")
    usar_ifr3 = st.checkbox("Filtro IFR3", value=True)
    if usar_ifr3:
        filtro_ifr3 = st.slider("IFR3 Máximo (Buscar Sobrevenda)", 0, 100, 100)
        
    st.markdown("---")
    usar_estocastico = st.checkbox("Filtro Estocástico", value=True)
    if usar_estocastico:
        filtro_estocastico = st.slider("Estocástico Lento Máximo", 0, 100, 100)
        
    st.markdown("---")
    st.markdown("**MACD vs Linha Zero (0)**")
    usar_macd_linha = st.checkbox("Filtro MACD Linha")
    if usar_macd_linha:
        dir_macd_linha = st.radio("A MACD Linha deve ser:", ["Maior que 0 (>)", "Menor que 0 (<)"], key="rad_macd_linha")
        
    usar_macd_media = st.checkbox("Filtro MACD Média 36")
    if usar_macd_media:
        dir_macd_media = st.radio("A Média 36 deve ser:", ["Maior que 0 (>)", "Menor que 0 (<)"], key="rad_macd_media")

with st.sidebar.expander("🏆 Filtros de Performance"):
    fr_ibov_minimo = st.number_input("FR_IBOV Mínimo (%)", value=0.0, step=5.0)
    fr_ivvb_minimo = st.number_input("FR_IVVB Mínimo (%)", value=-100.0, step=5.0)
    retorno_12m_minimo = st.number_input("Retorno 12M Mínimo (%)", value=-100.0, step=10.0)

with st.sidebar.expander("💰 Filtros de Liquidez"):
    # Atualizado para manter consistência total com o nome
    filtro_qtd_hoje = st.number_input("Qtd. de Ações Hoje (Mínimo)", value=10000, step=10000)
    filtro_qtdmm20 = st.number_input("Qtd. Média 20d (Mínimo)", value=0, step=10000)
    volume_crescente = st.checkbox("Liquidez Crescente (Qtd. 20d > Qtd. 60d)", value=False)

st.sidebar.markdown("---")
ordenar_por = st.sidebar.selectbox("Ordenar Resultados por:", 
    ["FR_IBOV", "Retorno_12M", "IFR40", "MACD_Linha_10_3", "Qtd_Acoes"])

# ==========================================
# 4. EXECUÇÃO E APRESENTAÇÃO
# ==========================================
with st.spinner("Analisando o mercado e aplicando os filtros em tempo real..."):
    tabela_completa = varrer_mercado_ao_vivo()
    
    if tabela_completa.empty:
        st.error("Erro ao puxar dados da internet. Clique no botão de Varredura acima.")
    else:
        t = tabela_completa.copy()

        t = t[
            (t['Fechamento'] >= preco_minimo) &
            (t['Qtd_Acoes'] >= filtro_qtd_hoje) & # A variável atualizada aqui
            (t['QtdMedia_20d'] >= filtro_qtdmm20) &
            (t['Retorno_12M'] >= (retorno_12m_minimo / 100)) &
            (t['FR_IBOV'] >= (fr_ibov_minimo / 100)) &
            (t['FR_IVVB'] >= (fr_ivvb_minimo / 100))
        ]

        if usar_ifr40: t = t[t['IFR40'] >= filtro_ifr40]
        if usar_ifr3: t = t[t['IFR3'] <= filtro_ifr3]
        if usar_estocastico: t = t[t['Estocastico_Lento'] <= filtro_estocastico]
        if usar_macd_linha:
            t = t[t['MACD_Linha_10_3'] > 0] if dir_macd_linha == "Maior que 0 (>)" else t[t['MACD_Linha_10_3'] < 0]
        if usar_macd_media:
            t = t[t['MACD_Media_36'] > 0] if dir_macd_media == "Maior que 0 (>)" else t[t['MACD_Media_36'] < 0]

        if is_alta: 
            if tend_p_mm20: t = t[t['Fechamento'] > t['MM20']]
            if tend_p_mm50: t = t[t['Fechamento'] > t['MM50']]
            if tend_p_mm80: t = t[t['Fechamento'] > t['MM80']]
            if tend_p_mm150: t = t[t['Fechamento'] > t['MM150']]
            if tend_mm20_50: t = t[t['MM20'] > t['MM50']]
            if tend_mm20_80: t = t[t['MM20'] > t['MM80']]
            if tend_mm50_80: t = t[t['MM50'] > t['MM80']]
            if tend_mm50_150: t = t[t['MM50'] > t['MM150']]
            if tend_mm80_150: t = t[t['MM80'] > t['MM150']]
            if rompimento_52w: t = t[t['Fechamento'] >= (t['Max_52W'] * 0.95)]
        else:       
            if tend_p_mm20: t = t[t['Fechamento'] < t['MM20']]
            if tend_p_mm50: t = t[t['Fechamento'] < t['MM50']]
            if tend_p_mm80: t = t[t['Fechamento'] < t['MM80']]
            if tend_p_mm150: t = t[t['Fechamento'] < t['MM150']]
            if tend_mm20_50: t = t[t['MM20'] < t['MM50']]
            if tend_mm20_80: t = t[t['MM20'] < t['MM80']]
            if tend_mm50_80: t = t[t['MM50'] < t['MM80']]
            if tend_mm50_150: t = t[t['MM50'] < t['MM150']]
            if tend_mm80_150: t = t[t['MM80'] < t['MM150']]
            if rompimento_52w: t = t[t['Fechamento'] <= (t['Min_52W'] * 1.05)]

        if volume_crescente: t = t[t['QtdMedia_20d'] > t['QtdMedia_60d']]

        t = t.sort_values(by=ordenar_por, ascending=False).reset_index(drop=True)

        # REORGANIZAÇÃO LOGÍSTICA DAS COLUNAS
        ordem_colunas = [
            'Ticker', 'Fechamento', 'Topo_Historico', 'Max_52W', 'Min_52W', 
            'Retorno_Ano', 'Retorno_12M', 'FR_IBOV', 'FR_IVVB', 
            'IFR3', 'IFR40', 'Estocastico_Lento', 'MACD_Linha_10_3', 'MACD_Media_36', 
            'MM20', 'MM50', 'MM80', 'MM100', 'MM150', 
            'Qtd_Acoes', 'QtdMedia_20d', 'QtdMedia_60d', 'QtdMedia_100d'
        ]
        t = t[ordem_colunas] 

        # FORMATAÇÃO VISUAL 
        colunas_dinheiro = ['Fechamento', 'Max_52W', 'Min_52W', 'Topo_Historico', 'MM20', 'MM50', 'MM80', 'MM100', 'MM150']
        for col in colunas_dinheiro: t[col] = t[col].apply(lambda x: f"R$ {x:.2f}")
        
        colunas_perc = ['Retorno_Ano', 'Retorno_12M', 'FR_IBOV', 'FR_IVVB']
        for col in colunas_perc: t[col] = t[col].apply(lambda x: f"{x:.2%}")

        colunas_3dec = ['MACD_Linha_10_3', 'MACD_Media_36']
        for col in colunas_3dec: t[col] = t[col].apply(lambda x: f"{x:.3f}")

        colunas_2dec = ['IFR3', 'IFR40', 'Estocastico_Lento']
        for col in colunas_2dec: t[col] = t[col].apply(lambda x: f"{x:.2f}")

        colunas_int = ['Qtd_Acoes', 'QtdMedia_20d', 'QtdMedia_60d', 'QtdMedia_100d']
        for col in colunas_int: t[col] = t[col].apply(lambda x: f"{x:,.0f}".replace(',', '.'))

        # VISUALIZAÇÃO FOCADA (CLEAN UI)
        st.success(f"Sincronizado! {len(t)} ações passaram nos filtros.")
        st.dataframe(t, use_container_width=True, height=750)