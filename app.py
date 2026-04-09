import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# ==========================================
# 1. FUNÇÕES MATEMÁTICAS
# ==========================================
def calcular_ifr(series, periodos):
    delta = series.diff()
    ganho = (delta.where(delta > 0, 0)).rolling(window=periodos).mean()
    perda = (-delta.where(delta < 0, 0)).rolling(window=periodos).mean()
    rs = ganho / perda.replace(0, np.nan)
    ifr = 100 - (100 / (1 + rs))
    ifr = ifr.fillna(100)
    ifr = ifr.where((ganho != 0) | (perda != 0), 50)
    return ifr

# ==========================================
# 2. MOTOR QUANTITATIVO (AGORA NA NUVEM!)
# ==========================================
# MÁGICA: O comando ttl="1d" diz para o robô baixar os dados só 1 vez por dia. 
# Ele salva na memória. No dia seguinte, ele baixa os novos dados sozinho!
@st.cache_data(ttl="1d")
def varrer_mercado_ao_vivo():
    # Lista atualizada 
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

    
    # O Yahoo Finance exige o sufixo ".SA" para ações brasileiras
    tickers_sa = [t + ".SA" for t in tickers]
    benchmarks = ["BOVA11.SA", "IVVB11.SA"]
    lista_completa = tickers_sa + benchmarks

    # O robô vai na internet e baixa 2 anos de histórico de TODAS as ações de uma vez
    dados_brutos = yf.download(lista_completa, period="2y", progress=False)

    lista_rastreador = []
    retorno_12m_ibov = 0
    retorno_12m_ivvb = 0

    # 1. Calculando os Benchmarks primeiro
    for bench in benchmarks:
        try:
            df_b = pd.DataFrame({'Fechamento': dados_brutos['Close'][bench]}).dropna()
            if len(df_b) > 252:
                data_atual = df_b.index[-1]
                data_12m_atras = data_atual - pd.Timedelta(days=365)
                df_passado = df_b[df_b.index <= data_12m_atras]
                
                if not df_passado.empty:
                    ret_12m = (df_b['Fechamento'].iloc[-1] / df_passado['Fechamento'].iloc[-1]) - 1
                    if 'BOVA' in bench: retorno_12m_ibov = ret_12m
                    if 'IVVB' in bench: retorno_12m_ivvb = ret_12m
        except: pass

    # 2. Calculando as Ações
    for t_sa in tickers_sa:
        try:
            # Extraindo a ação específica do pacote que veio da internet
            df = pd.DataFrame({
                'Fechamento': dados_brutos['Close'][t_sa],
                'Máximo': dados_brutos['High'][t_sa],
                'Mínimo': dados_brutos['Low'][t_sa],
                'Quantidade': dados_brutos['Volume'][t_sa]
            }).dropna()

            if len(df) < 200: continue # Pula ações muito novas
            
            ticker_puro = t_sa.replace(".SA", "")
            
            # Cálculo dos Indicadores (Exatamente como era no Excel/Colab)
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
            
            # Retornos de Calendário
            data_12m_atras = data_atual - pd.Timedelta(days=365)
            df_passado = df[df.index <= data_12m_atras]
            if not df_passado.empty: retorno_12m = (preco_atual / df_passado.iloc[-1]['Fechamento']) - 1
            else: retorno_12m = 0
            
            ano_atual = data_atual.year
            df_ano_atual = df[df.index.year == ano_atual]
            if not df_ano_atual.empty: retorno_ano = (preco_atual / df_ano_atual.iloc[0]['Fechamento']) - 1
            else: retorno_ano = 0
                
            fr_ibov = ((1 + retorno_12m) / (1 + retorno_12m_ibov)) - 1 if retorno_12m_ibov != 0 else 0
            fr_ivvb = ((1 + retorno_12m) / (1 + retorno_12m_ivvb)) - 1 if retorno_12m_ivvb != 0 else 0

            lista_rastreador.append({
                'Ticker': ticker_puro, 'Fechamento': preco_atual, 'Retorno_Ano': retorno_ano,
                'Retorno_12M': retorno_12m, 'FR_IBOV': fr_ibov, 'FR_IVVB': fr_ivvb,
                'IFR3': hoje['IFR3'], 'IFR40': hoje['IFR40'], 'Estocastico_Lento': hoje['Estocastico_Lento'],
                'MACD_Linha_10_3': hoje['MACD_Linha'], 'MACD_Media_36': hoje['MACD_Media_36'],
                'Max_52W': hoje['Max_52W'], 'Topo_Historico': hoje['Topo_Historico'],
                'MM20': hoje['MM20'], 'MM50': hoje['MM50'], 'MM80': hoje['MM80'],
                'MM100': hoje['MM100'], 'MM150': hoje['MM150'], 'Negocios_Hoje': hoje['Quantidade'],
                'QtdMM20': hoje['QtdMM20'], 'QtdMM60': hoje['QtdMM60'], 'QtdMM100': hoje['QtdMM100']
            })
        except Exception as e:
            pass
            
    return pd.DataFrame(lista_rastreador)

# ==========================================
# 3. INTERFACE DE USUÁRIO (FRONT-END)
# ==========================================
st.set_page_config(page_title="Terminal Quantitativo B3", layout="wide")
st.title("🌐 Terminal Quantitativo B3 (Live Sync)")

st.sidebar.header("🎛️ Painel de Controle")

with st.sidebar.expander("📈 Filtros de Tendência"):
    preco_minimo = st.number_input("Preço Mínimo (R$)", value=2.00, step=0.50)
    tend_mm150 = st.checkbox("Preço > MM150 (Tendência Primária)", value=False)
    tend_mm20_50 = st.checkbox("MM20 > MM50 (Tendência Curta)", value=False)
    alinhamento_medias = st.checkbox("Alinhamento Total (P > M20 > M50 > M100 > M150)", value=False)
    rompimento_52w = st.checkbox("Próxima à Máxima de 52W (Distância < 5%)", value=False)

with st.sidebar.expander("⚡ Filtros de Momento"):
    filtro_ifr40 = st.slider("IFR40 Mínimo (Força Longa)", 0, 100, 50)
    filtro_ifr3 = st.slider("IFR3 Máximo (Buscar Sobrevenda)", 0, 100, 100)
    filtro_estocastico = st.slider("Estocástico Lento Máximo", 0, 100, 100)
    macd_comprado = st.checkbox("MACD Linha > Média 36", value=False)

with st.sidebar.expander("🏆 Filtros de Performance"):
    fr_ibov_minimo = st.number_input("FR_IBOV Mínimo (%)", value=0.0, step=5.0)
    fr_ivvb_minimo = st.number_input("FR_IVVB Mínimo (%)", value=-100.0, step=5.0)
    retorno_12m_minimo = st.number_input("Retorno 12M Mínimo (%)", value=-100.0, step=10.0)

with st.sidebar.expander("💰 Filtros de Liquidez"):
    filtro_negocios = st.number_input("Negócios Hoje (Mínimo)", value=100, step=100)
    filtro_qtdmm20 = st.number_input("Média de Negócios 20d (Mínimo)", value=0, step=100)
    volume_crescente = st.checkbox("Liquidez Crescente (QtdMM20 > QtdMM60)", value=False)

ordenar_por = st.sidebar.selectbox("Ordenar Resultados por:", 
    ["FR_IBOV", "Retorno_12M", "IFR40", "MACD_Linha_10_3", "Negocios_Hoje"])

# ==========================================
# 4. EXECUÇÃO E FILTRAGEM (O GATILHO)
# ==========================================
if st.button("🚀 Executar Varredura Ao Vivo"):
    with st.spinner("Baixando dados da B3 pelo Yahoo Finance... Isso leva uns 15 segundos."):
        
        tabela_completa = varrer_mercado_ao_vivo()
        
        if tabela_completa.empty:
            st.error("Erro ao puxar dados da internet.")
        else:
            tabela_filtrada = tabela_completa.copy()

            tabela_filtrada = tabela_filtrada[
                (tabela_filtrada['Fechamento'] >= preco_minimo) &
                (tabela_filtrada['IFR40'] >= filtro_ifr40) & 
                (tabela_filtrada['IFR3'] <= filtro_ifr3) &
                (tabela_filtrada['Estocastico_Lento'] <= filtro_estocastico) &
                (tabela_filtrada['Negocios_Hoje'] >= filtro_negocios) &
                (tabela_filtrada['QtdMM20'] >= filtro_qtdmm20) &
                (tabela_filtrada['Retorno_12M'] >= (retorno_12m_minimo / 100)) &
                (tabela_filtrada['FR_IBOV'] >= (fr_ibov_minimo / 100)) &
                (tabela_filtrada['FR_IVVB'] >= (fr_ivvb_minimo / 100))
            ]

            if tend_mm150: tabela_filtrada = tabela_filtrada[tabela_filtrada['Fechamento'] > tabela_filtrada['MM150']]
            if tend_mm20_50: tabela_filtrada = tabela_filtrada[tabela_filtrada['MM20'] > tabela_filtrada['MM50']]
            if alinhamento_medias: 
                tabela_filtrada = tabela_filtrada[
                    (tabela_filtrada['Fechamento'] > tabela_filtrada['MM20']) &
                    (tabela_filtrada['MM20'] > tabela_filtrada['MM50']) &
                    (tabela_filtrada['MM50'] > tabela_filtrada['MM100']) &
                    (tabela_filtrada['MM100'] > tabela_filtrada['MM150'])
                ]
            if rompimento_52w: tabela_filtrada = tabela_filtrada[tabela_filtrada['Fechamento'] >= (tabela_filtrada['Max_52W'] * 0.95)]
            if macd_comprado: tabela_filtrada = tabela_filtrada[tabela_filtrada['MACD_Linha_10_3'] > tabela_filtrada['MACD_Media_36']]
            if volume_crescente: tabela_filtrada = tabela_filtrada[tabela_filtrada['QtdMM20'] > tabela_filtrada['QtdMM60']]

            tabela_filtrada = tabela_filtrada.sort_values(by=ordenar_por, ascending=False).reset_index(drop=True)

            colunas_dinheiro = ['Fechamento', 'Max_52W', 'Topo_Historico', 'MM20', 'MM50', 'MM80', 'MM100', 'MM150']
            for col in colunas_dinheiro: tabela_filtrada[col] = tabela_filtrada[col].apply(lambda x: f"R$ {x:.2f}")
            
            colunas_perc = ['Retorno_Ano', 'Retorno_12M', 'FR_IBOV', 'FR_IVVB']
            for col in colunas_perc: tabela_filtrada[col] = tabela_filtrada[col].apply(lambda x: f"{x:.2%}")

            colunas_3dec = ['MACD_Linha_10_3', 'MACD_Media_36']
            for col in colunas_3dec: tabela_filtrada[col] = tabela_filtrada[col].apply(lambda x: f"{x:.3f}")

            colunas_2dec = ['IFR3', 'IFR40', 'Estocastico_Lento']
            for col in colunas_2dec: tabela_filtrada[col] = tabela_filtrada[col].apply(lambda x: f"{x:.2f}")

            colunas_int = ['Negocios_Hoje', 'QtdMM20', 'QtdMM60', 'QtdMM100']
            for col in colunas_int: tabela_filtrada[col] = tabela_filtrada[col].apply(lambda x: f"{x:,.0f}".replace(',', '.'))

            st.success(f"Nuvem Sincronizada! {len(tabela_filtrada)} ações encontradas.")
            st.dataframe(tabela_filtrada, width='stretch')






