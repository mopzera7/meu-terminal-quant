import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf

# ==========================================
# 1. FUNÇÕES MATEMÁTICAS (CORRIGIDAS)
# ==========================================
def calcular_ifr(series, periodos):
    delta = series.diff()
    # Usa Média Exponencial com o Alpha de Wilder (Padrão Profissional)
    ganho = delta.where(delta > 0, 0).ewm(alpha=1/periodos, adjust=False).mean()
    perda = (-delta.where(delta < 0, 0)).ewm(alpha=1/periodos, adjust=False).mean()
    
    rs = ganho / perda.replace(0, np.nan)
    ifr = 100 - (100 / (1 + rs))
    return ifr.fillna(100)

# ==========================================
# 2. MOTOR QUANTITATIVO (AGORA NA NUVEM!)
# ==========================================
@st.cache_data(ttl="1d")
def varrer_mercado_ao_vivo():
    tickers = [
        "AALR3", "ABCB4", "ABEV3", "AERI3", "AGRO3", "AGXY3", "ALLD3", "ALOS3", "ALPA4", "ALPK3",
        "ALUP11", "AMAR3", "AMBP3", "AMER3", "AMOB3", "ANIM3", "ARML3", "ASAI3", "AURA33", "AURE3",
        "AXIA3", "AXIA6", "AXIA7", "AZEV3", "AZEV4", "AZUL4", "AZZA3", "B3SA3", "BBAS3", "BBDC3",
        "BBDC4", "BBSE3", "BEEF3", "BEES3", "BEES4", "BHIA3", "BIOM3", "BLAU3", "BMEB4", "BMGB4",
        "BMOB3", "BOBR4", "BPAC11", "BPAN4", "BRAP3", "BRAP4", "BRAV3", "BRBI11", "BRKM5", "BRSR3",
        "BRSR6", "CAMB3", "CAML3", "CASH3", "CBAV3", "CEAB3", "CGRA4", "CMIG3", "CMIG4", "CMIN3",
        "COCE5", "COGN3", "CPFE3", "CPLE3", "CSAN3", "CSED3", "CSMG3", "CSNA3", "CSUD3", "CURY3",
        "CVCB3", "CXSE3", "CYRE3", "DASA3", "DESK3", "DEXP3", "DIRR3", "DIVO11", "DMVF3", "DOTZ3",
        "DXCO3", "EALT4", "ECOR3", "EGIE3", "EMBR3", "ENEV3", "ENGI11", "ENJU3", "EQTL3", "ESPA3",
        "ETER3", "EUCA4", "EVEN3", "EZTC3", "FESA4", "FHER3", "FIQE3", "FLRY3", "FRAS3", "GFSA3",
        "GGBR3", "GGBR4", "GGPS3", "GMAT3", "GOAU3", "GOAU4", "GOLD11", "GRND3", "GUAR3", "HAGA4",
        "HAPV3", "HBOR3", "HBRE3", "HBSA3", "HYPE3", "IFCM3", "IGTI11", "IGTI3", "INEP3", "INEP4",
        "INTB3", "IRBR3", "ISAE4", "ITSA3", "ITSA4", "ITUB3", "ITUB4", "JALL3", "JBSS32", "JHSF3",
        "JSLG3", "KEPL3", "KLBN11", "KLBN3", "KLBN4", "LAND3", "LAVV3", "LEVE3", "LIGT3", "LJQQ3",
        "LOGG3", "LOGN3", "LPSB3", "LREN3", "LUPA3", "LWSA3", "MATD3", "MBRF3", "MDIA3", "MDNE3",
        "MEAL3", "MELK3", "MGLU3", "MILS3", "MLAS3", "MOTV3", "MOVI3", "MRVE3", "MTRE3", "MULT3",
        "MYPK3", "NDIV11", "NEOE3", "NGRD3", "NSDV11", "ODPV3", "OFSA3", "OIBR3", "OIBR4", "ONCO3",
        "OPCT3", "ORVR3", "PCAR3", "PDGR3", "PDTC3", "PETR3", "PETR4", "PETZ3", "PFRM3", "PGMN3",
        "PINE4", "PLPL3", "PMAM3", "PNVL3", "POMO3", "POMO4", "PORT3", "POSI3", "PRIO3", "PRNR3",
        "PSSA3", "PTBL3", "PTNT4", "QUAL3", "RADL3", "RAIL3", "RAIZ4", "RANI3", "RAPT4", "RCSL3",
        "RCSL4", "RDOR3", "RECV3", "RENT3", "RIAA3", "RNEW11", "RNEW3", "ROMI3", "RSID3", "SANB11",
        "SANB3", "SANB4", "SAPR11", "SAPR3", "SAPR4", "SBFG3", "SBSP3", "SCAR3", "SEER3", "SEQL3",
        "SHOW3", "SHUL4", "SIMH3", "SLCE3", "SMFT3", "SMTO3", "SOJA3", "SRNA3", "SUZB3", "SYNE3",
        "TAEE11", "TAEE3", "TAEE4", "TASA3", "TASA4", "TCSA3", "TECN3", "TEND3", "TFCO4", "TGMA3",
        "TIMS3", "TOTS3", "TPIS3", "TRAD3", "TRIS3", "TTEN3", "TUPY3", "UCAS3", "UGPA3", "UNIP6",
        "USIM3", "USIM5", "USTK11", "VALE3", "VAMO3", "VBBR3", "VGIA11", "VITT3", "VIVA3", "VIVR3",
        "VIVT3", "VLID3", "VSTE3", "VULC3", "VVEO3", "WEB311", "WEGE3", "WEST3", "WHRL4", "WIZC3",
        "XPBR31", "YDUQ3",
    ]

    tickers_sa = [t + ".SA" for t in tickers]
    benchmarks = ["BOVA11.SA", "IVVB11.SA"]
    lista_completa = tickers_sa + benchmarks

    dados_brutos = yf.download(lista_completa, period="2y", progress=False)

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
            # CORREÇÃO: ffill() adicionado para lidar com dias sem volume no Yahoo Finance
            df = pd.DataFrame({
                'Fechamento': dados_brutos['Close'][t_sa],
                'Máximo': dados_brutos['High'][t_sa],
                'Mínimo': dados_brutos['Low'][t_sa],
                'Quantidade': dados_brutos['Volume'][t_sa]
            }).ffill().dropna()

            if len(df) < 200: continue 
            
            ticker_puro = t_sa.replace(".SA", "")
            
            df['MM20'] = df['Fechamento'].rolling(window=20).mean()
            df['MM50'] = df['Fechamento'].rolling(window=50).mean()
            df['MM80'] = df['Fechamento'].rolling(window=80).mean()
            df['MM100'] = df['Fechamento'].rolling(window=100).mean()
            df['MM150'] = df['Fechamento'].rolling(window=150).mean()
            df['QtdMM20'] = df['Quantidade'].rolling(window=20).mean()
            df['QtdMM60'] = df['Quantidade'].rolling(window=60).mean()
            df['QtdMM100'] = df['Quantidade'].rolling(window=100).mean()
            df['IFR40'] = calcular_ifr(df['Fechamento'], periodos=40)
            df['IFR3'] = calcular_ifr(df['Fechamento'], periodos=3)
            df['Max_52W'] = df['Máximo'].rolling(window=252).max()
            df['Min_52W'] = df['Mínimo'].rolling(window=252).min()
            df['Topo_Historico'] = df['Máximo'].cummax()

            min_8 = df['Mínimo'].rolling(window=8).min()
            max_8 = df['Máximo'].rolling(window=8).max()
            k_rapido = 100 * ((df['Fechamento'] - min_8) / (max_8 - min_8))
            df['Estocastico_Lento'] = k_rapido.rolling(window=3).mean()

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
                
            fr_ibov = ((1 + retorno_12m) / (1 + retorno_12m_ibov)) - 1 if retorno_12m_ibov != 0 else 0
            fr_ivvb = ((1 + retorno_12m) / (1 + retorno_12m_ivvb)) - 1 if retorno_12m_ivvb != 0 else 0

            lista_rastreador.append({
                'Ticker': ticker_puro, 'Fechamento': preco_atual, 'Retorno_Ano': retorno_ano,
                'Retorno_12M': retorno_12m, 'FR_IBOV': fr_ibov, 'FR_IVVB': fr_ivvb,
                'IFR3': hoje['IFR3'], 'IFR40': hoje['IFR40'], 'Estocastico_Lento': hoje['Estocastico_Lento'],
                'MACD_Linha_10_3': hoje['MACD_Linha'], 'MACD_Media_36': hoje['MACD_Media_36'],
                'Max_52W': hoje['Max_52W'], 'Min_52W': hoje['Min_52W'], 'Topo_Historico': hoje['Topo_Historico'],
                'MM20': hoje['MM20'], 'MM50': hoje['MM50'], 'MM80': hoje['MM80'],
                'MM100': hoje['MM100'], 'MM150': hoje['MM150'], 'Negocios_Hoje': hoje['Quantidade'],
                'QtdMM20': hoje['QtdMM20'], 'QtdMM60': hoje['QtdMM60'], 'QtdMM100': hoje['QtdMM100']
            })
        except Exception as e:
            pass
            
    return pd.DataFrame(lista_rastreador)

# ==========================================
# 3. INTERFACE DE USUÁRIO E FILTROS
# ==========================================
st.set_page_config(page_title="Terminal Quantitativo B3", layout="wide")

# O CSS para remover as margens brancas gigantes do Streamlit
st.markdown("""
    <style>
           .block-container { padding-top: 1rem; padding-bottom: 0rem; }
    </style>
    """, unsafe_allow_html=True)

st.title("🌐 Terminal Quantitativo B3")

# O Botão volta para o centro do palco, logo abaixo do título!
btn_varredura = st.button("🚀 Executar Varredura Ao Vivo (Forçar Atualização de Dados)", use_container_width=True)
if btn_varredura:
    varrer_mercado_ao_vivo.clear()

st.markdown("---") # Uma linha charmosa para separar o botão da tabela

st.sidebar.header("🎛️ Painel de Controle")

# (Seus filtros de tendência continuam aqui para baixo...)

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
    usar_ifr40 = st.checkbox(" Filtro IFR40", value=True)
    if usar_ifr40:
        filtro_ifr40 = st.slider("IFR40 Mínimo (Força Longa)", 0, 100, 50)
        
    st.markdown("---")
    usar_ifr3 = st.checkbox(" Filtro IFR3", value=True)
    if usar_ifr3:
        filtro_ifr3 = st.slider("IFR3 Máximo (Buscar Sobrevenda)", 0, 100, 100)
        
    st.markdown("---")
    usar_estocastico = st.checkbox(" Filtro Estocástico", value=True)
    if usar_estocastico:
        filtro_estocastico = st.slider("Estocástico Lento Máximo", 0, 100, 100)
        
    st.markdown("---")
    st.markdown("**MACD vs Linha Zero (0)**")
    usar_macd_linha = st.checkbox(" Filtro MACD Linha")
    if usar_macd_linha:
        dir_macd_linha = st.radio("A MACD Linha deve ser:", ["Maior que 0 (>)", "Menor que 0 (<)"], key="rad_macd_linha")
        
    usar_macd_media = st.checkbox(" Filtro MACD Média 36")
    if usar_macd_media:
        dir_macd_media = st.radio("A Média 36 deve ser:", ["Maior que 0 (>)", "Menor que 0 (<)"], key="rad_macd_media")

with st.sidebar.expander("🏆 Filtros de Performance"):
    fr_ibov_minimo = st.number_input("FR_IBOV Mínimo (%)", value=0.0, step=5.0)
    fr_ivvb_minimo = st.number_input("FR_IVVB Mínimo (%)", value=-100.0, step=5.0)
    retorno_12m_minimo = st.number_input("Retorno 12M Mínimo (%)", value=-100.0, step=10.0)

with st.sidebar.expander("💰 Filtros de Liquidez"):
    filtro_negocios = st.number_input("Negócios Hoje (Mínimo)", value=100, step=100)
    filtro_qtdmm20 = st.number_input("Média de Negócios 20d (Mínimo)", value=0, step=100)
    volume_crescente = st.checkbox("Liquidez Crescente (QtdMM20 > QtdMM60)", value=False)

st.sidebar.markdown("---")
ordenar_por = st.sidebar.selectbox("Ordenar Resultados por:", 
    ["FR_IBOV", "Retorno_12M", "IFR40", "MACD_Linha_10_3", "Negocios_Hoje"])

# ==========================================
# 4. EXECUÇÃO E APRESENTAÇÃO (GATILHO REATIVO)
# ==========================================

with st.spinner("Analisando o mercado e aplicando os filtros em tempo real..."):
    tabela_completa = varrer_mercado_ao_vivo()
    
    if tabela_completa.empty:
        st.error("Erro ao puxar dados da internet. Clique no botão de Varredura acima.")
    else:
        t = tabela_completa.copy()

        t = t[
            (t['Fechamento'] >= preco_minimo) &
            (t['Negocios_Hoje'] >= filtro_negocios) &
            (t['QtdMM20'] >= filtro_qtdmm20) &
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

        if volume_crescente: t = t[t['QtdMM20'] > t['QtdMM60']]

        t = t.sort_values(by=ordenar_por, ascending=False).reset_index(drop=True)

        colunas_dinheiro = ['Fechamento', 'Max_52W', 'Min_52W', 'Topo_Historico', 'MM20', 'MM50', 'MM80', 'MM100', 'MM150']
        for col in colunas_dinheiro: t[col] = t[col].apply(lambda x: f"R$ {x:.2f}")
        
        colunas_perc = ['Retorno_Ano', 'Retorno_12M', 'FR_IBOV', 'FR_IVVB']
        for col in colunas_perc: t[col] = t[col].apply(lambda x: f"{x:.2%}")

        colunas_3dec = ['MACD_Linha_10_3', 'MACD_Media_36']
        for col in colunas_3dec: t[col] = t[col].apply(lambda x: f"{x:.3f}")

        colunas_2dec = ['IFR3', 'IFR40', 'Estocastico_Lento']
        for col in colunas_2dec: t[col] = t[col].apply(lambda x: f"{x:.2f}")

        colunas_int = ['Negocios_Hoje', 'QtdMM20', 'QtdMM60', 'QtdMM100']
        for col in colunas_int: t[col] = t[col].apply(lambda x: f"{x:,.0f}".replace(',', '.'))

        st.success(f"Nuvem Sincronizada! {len(t)} ações passaram nos seus filtros.")
        
      # (Seu código de formatação das colunas de dinheiro, perc, etc, continua acima disto...)

        # ==========================================
        # 5. DASHBOARD COMPACTO E TABELA
        # ==========================================
        
        # Mensagem fina e elegante no lugar da caixa verde gigante
        st.markdown(f"** Nuvem Sincronizada! | {len(t)} ações passaram nos seus filtros.**")

        # Pódio Top 3 compactado (a 4ª coluna invisível empurra eles para a esquerda)
        if len(t) >= 3:
            col1, col2, col3, col4 = st.columns([1, 1, 1, 2]) 
            with col1: st.metric(label=f"🥇 {t.iloc[0]['Ticker']}", value=t.iloc[0]['Fechamento'], delta=t.iloc[0]['Retorno_12M'])
            with col2: st.metric(label=f"🥈 {t.iloc[1]['Ticker']}", value=t.iloc[1]['Fechamento'], delta=t.iloc[1]['Retorno_12M'])
            with col3: st.metric(label=f"🥉 {t.iloc[2]['Ticker']}", value=t.iloc[2]['Fechamento'], delta=t.iloc[2]['Retorno_12M'])

        # Tabela com altura controlada (400px) para não passar do limite da tela
        st.dataframe(t, use_container_width=True, height=400)




