import pandas as pd
import numpy as np
from haversine import haversine, Unit
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import folium
import time
import requests
import os
from datetime import date, timedelta

# ==============================================================================
# CONFIGURAÇÕES GLOBAIS
CHAVE_API_GOOGLE = "AIzaSyBldtILdvj5UAy_sYCSPrAL637DbclAE3k"
FATOR_CUSTO_DISTANCIA = 50
ARQUIVO_HISTORICO_ROTAS = "historico_rotas_k.csv"
ARQUIVO_ANALISE_GERAL_K = "analise_fator_k_geral.csv"
ARQUIVO_HISTORICO_TRECHOS = "historico_trechos_k.csv"
ARQUIVO_ANALISE_GRANULAR_K = "analise_k_por_distancia.csv"
# ==============================================================================

def obter_distancia_real_google(origem_coords, destino_coords, waypoints_coords, chave_api):
    if chave_api == "COLE_SUA_CHAVE_DE_API_AQUI": return None
    base_url = "https://maps.googleapis.com/maps/api/directions/json"
    origin_str, destination_str = f"{origem_coords[0]},{origem_coords[1]}", f"{destino_coords[0]},{destino_coords[1]}"
    if len(waypoints_coords) > 25:
        print(f"  - AVISO GOOGLE API: Rota com {len(waypoints_coords)} pontos excede o limite de 25.")
        return None
    waypoints_str = "|".join([f"{lat},{lon}" for lat, lon in waypoints_coords])
    params = {"origin": origin_str, "destination": destination_str, "waypoints": f"optimize:true|{waypoints_str}", "key": chave_api}
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        if data['status'] == 'OK': return data['routes'][0]['legs']
        else: print(f"  - AVISO GOOGLE API: {data.get('error_message', data['status'])}"); return None
    except requests.exceptions.RequestException as e:
        print(f"  - ERRO DE CONEXÃO COM GOOGLE API: {e}"); return None

def analisar_k_geral_por_polo(df_historico, df_polos_info):
    print("\n--- ANÁLISE GERAL DE FATOR K (POR ROTA) ---")
    dados_analise = []
    for _, polo_info in df_polos_info.iterrows():
        polo, fator_k_atual = polo_info['Centro Operativo'], polo_info['fator_k']
        df_historico_polo = df_historico[df_historico['Polo'] == polo].copy()
        qtd_rotas = len(df_historico_polo)
        novo_k_sugerido_str, observacao, nivel_confianca = "N/A", "", ""
        if qtd_rotas < 10:
            nivel_confianca, observacao = "Dados Insuficientes", f"São necessárias no mínimo 10 rotas ({qtd_rotas} existentes)."
        else:
            if 10 <= qtd_rotas < 30: nivel_confianca = "Inicial"
            elif 30 <= qtd_rotas < 100: nivel_confianca = "Confiável"
            else: nivel_confianca = "Alta Precisão"
            df_historico_polo['KM_Estimado_K'] = pd.to_numeric(df_historico_polo['KM_Estimado_K'], errors='coerce')
            df_historico_polo['KM_Real_Google'] = pd.to_numeric(df_historico_polo['KM_Real_Google'], errors='coerce')
            df_historico_polo.dropna(subset=['KM_Estimado_K', 'KM_Real_Google'], inplace=True)
            df_valido = df_historico_polo[(df_historico_polo['Fator_K_Usado'] > 0) & (df_historico_polo['KM_Estimado_K'] > 0)]
            if not df_valido.empty:
                df_valido['Dist_Linha_Reta_km'] = df_valido['KM_Estimado_K'] / df_valido['Fator_K_Usado']
                df_valido_sem_zero = df_valido[df_valido['Dist_Linha_Reta_km'] > 0]
                if not df_valido_sem_zero.empty:
                    df_valido_sem_zero['K_Real_Individual'] = df_valido_sem_zero['KM_Real_Google'] / df_valido_sem_zero['Dist_Linha_Reta_km']
                    novo_k_sugerido = df_valido_sem_zero['K_Real_Individual'].mean()
                    novo_k_sugerido_str = f"{novo_k_sugerido:.2f}"
                    observacao = "Sugestão calculada"
        dados_analise.append({'Polo': polo, 'Fator_K_Atual': f"{fator_k_atual:.2f}", 'Fator_K_Sugerido': novo_k_sugerido_str, 'Qtd_Rotas_Analisadas': qtd_rotas, 'Nivel_Confianca': nivel_confianca, 'Observacao': observacao})
    if dados_analise:
        print(f"\nSalvando a análise geral do Fator K em '{ARQUIVO_ANALISE_GERAL_K}'...")
        pd.DataFrame(dados_analise).to_csv(ARQUIVO_ANALISE_GERAL_K, index=False, sep=';', encoding='utf-8-sig')
        print(">>> Arquivo de análise geral salvo com sucesso.")

def analisar_k_por_distancia(df_historico_trechos):
    print("\n--- ANÁLISE GRANULAR DE FATOR K (POR TRECHO) ---")
    if df_historico_trechos.empty or len(df_historico_trechos) < 10: return
    df_analise = df_historico_trechos.copy()
    df_analise.dropna(subset=['Distancia_Reta_m', 'Distancia_Real_m'], inplace=True)
    df_analise = df_analise[(df_analise['Distancia_Reta_m'] > 0) & (df_analise['Distancia_Real_m'] > 0)]
    if df_analise.empty: return
    df_analise['K_Individual'] = df_analise['Distancia_Real_m'] / df_analise['Distancia_Reta_m']
    max_dist = df_analise['Distancia_Reta_m'].max()
    bins = np.arange(0, max_dist + 500, 500)
    labels = [f"{int(b)}-{int(b+500)} m" for b in bins[:-1]]
    if not labels: return
    df_analise['Faixa_Distancia'] = pd.cut(df_analise['Distancia_Reta_m'], bins=bins, labels=labels, right=False)
    resultado_analise = df_analise.groupby(['Polo', 'Faixa_Distancia'], observed=False).agg(
        K_Sugerido=('K_Individual', 'mean'), Qtd_Amostras=('K_Individual', 'count')
    ).reset_index()
    resultado_analise['K_Sugerido'] = resultado_analise['K_Sugerido'].round(2)
    print(f"Salvando a análise granular do Fator K em '{ARQUIVO_ANALISE_GRANULAR_K}'...")
    resultado_analise.to_csv(ARQUIVO_ANALISE_GRANULAR_K, index=False, sep=';', encoding='utf-8-sig')
    print(">>> Arquivo de análise granular salvo com sucesso.")

def gerar_mapa_de_rotas(df_rotas, df_polos_info, df_servicos_info, polos_processados):
    if df_rotas.empty: return
    polos_filtrados = df_polos_info[df_polos_info['Centro Operativo'].isin(polos_processados)]
    if polos_filtrados.empty: return
    mapa = folium.Map(location=[polos_filtrados['latitude'].mean(), polos_filtrados['longitude'].mean()], zoom_start=10)
    df_rotas_com_coords = pd.merge(df_rotas, df_servicos_info[['ID_Servico', 'Latitude', 'Longitude']], on='ID_Servico')
    df_rotas_com_coords.rename(columns={'Latitude': 'LATITUD', 'Longitude': 'LONGITUD'}, inplace=True)
    cores = ['blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'red', 'lightblue', 'lightgreen', 'gray', 'pink', 'lightgray']
    mapa_cores_equipe = {}
    df_rotas_com_coords['Equipe_Unica'] = df_rotas_com_coords['Polo'] + " - " + df_rotas_com_coords['Equipe']
    equipes_unicas = df_rotas_com_coords['Equipe_Unica'].unique()
    for i, equipe_unica in enumerate(equipes_unicas): mapa_cores_equipe[equipe_unica] = cores[i % len(cores)]
    for _, polo in polos_filtrados.iterrows():
        folium.Marker(location=[polo['latitude'], polo['longitude']], popup=f"<strong>Polo: {polo['Centro Operativo']}</strong>", icon=folium.Icon(color='black', icon='industry', prefix='fa')).add_to(mapa)
    for equipe_unica in equipes_unicas:
        rota_da_equipe = df_rotas_com_coords[df_rotas_com_coords['Equipe_Unica'] == equipe_unica].sort_values(by='Ordem_Visita')
        polo_da_rota = rota_da_equipe['Polo'].iloc[0]
        polo_coords = df_polos_info[df_polos_info['Centro Operativo'] == polo_da_rota][['latitude', 'longitude']].iloc[0]
        pontos_da_rota = [tuple(polo_coords)] + list(zip(rota_da_equipe['LATITUD'], rota_da_equipe['LONGITUD'])) + [tuple(polo_coords)]
        cor_da_rota = mapa_cores_equipe[equipe_unica]
        folium.PolyLine(pontos_da_rota, color=cor_da_rota, weight=3, opacity=0.8, tooltip=f"<strong>{equipe_unica}</strong>").add_to(mapa)
        for _, servico in rota_da_equipe.iterrows():
            popup_html = (f"<strong>Equipe:</strong> {servico['Equipe']}<br><strong>Polo:</strong> {servico['Polo']}<br><strong>Ordem:</strong> {servico['Ordem_Visita']}<br>"
                          f"<strong>ID Serviço:</strong> {servico['ID_Servico']}<br><strong>Dívida:</strong> R$ {servico['Valor_Divida']:.2f}")
            folium.Marker(location=[servico['LATITUD'], servico['LONGITUD']], popup=folium.Popup(popup_html, max_width=300), icon=folium.Icon(color=cor_da_rota, icon='info-sign')).add_to(mapa)
    nome_arquivo = f"mapa_rotas.html"
    mapa.save(nome_arquivo)
    print(f"\n>>> Mapa interativo salvo com sucesso em '{nome_arquivo}'! <<<")

def verificar_dia_restrito(data_atual, municipios_do_polo, df_feriados):
    df_feriados.columns = [str(col).strip() for col in df_feriados.columns]
    if data_atual.weekday() in [4, 5, 6]:
        dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
        return True, f"{dias_semana[data_atual.weekday()]}"
    
    feriados_gerais = set(df_feriados[df_feriados['COD_MUNICIPIO'] == 0]['FECHA'])
    feriados_municipais = set(df_feriados[df_feriados['Municipio'].isin(municipios_do_polo)]['FECHA'])
    todos_os_feriados = feriados_gerais.union(feriados_municipais)
    datas_restritas = set()
    for feriado in todos_os_feriados:
        if pd.notna(feriado):
            datas_restritas.add(feriado.date())
            datas_restritas.add(feriado.date() - timedelta(days=1))
    if data_atual in datas_restritas:
        return True, "Feriado ou Véspera de Feriado"
    return False, ""
# Adicione esta constante no início do seu arquivo, junto com as outras configurações
MINUTOS_POR_KM = 3 # Premissa: Velocidade média de 20 km/h (60 min / 20 km = 3 min/km)

def main():
    """
    Roteirizador VRP v14.4 - API Google como pós-processamento para enriquecimento de dados
    """
    print("Carregando todos os dados...")
    try:
        # Leitura de cabeçalhos dinâmica
        colunas_equipes = [
            'Centro Operativo', 'Quantidade_equipes_Leves', 'Capacidade_maxima_Leves',
            'Quantidades_equipes_Cesto', 'Capacidade_maxima_Cesto'
        ]
        colunas_servicos = [
            'CODIGO_EXTERNO', 'TDC', 'FECHA_CREACION_ORDEN_SIS_EXT', 'FECHA_CREACION_TDC_EORDER', 'ESTADO',
            'CODIGO_PROCESO', 'DESCRIPCION_PROCESO', 'CODIGO_ORDEN', 'DESCRIPCION_ORDEN', 'CICLO',
            'FECHA_ACTUALIZACION', 'ETL_TIME', 'CODIGO_CENTRO_OPERATIVO', 'CENTRO_OPERATIVO_TDC',
            'LONGITUD', 'LATITUD', 'CODIGO_CLIENTE', 'LOCALIDAD', 'CALLE', 'NUMERO_CALLE', 'MUNICIPIO',
            'BARRIO', 'CODIGO_ZIP', 'COMPLEMENTO', 'MOTIVO_INSPECION', 'CODE_NOTA', 'DESCRIPCION_CODE_NOTA',
            'MEDIDA', 'DESCIPCION_MEDIDA', 'TEXTO_DIRECCION_COMPLETA', 'ALOC_RECURSOS', 'RESIDUAL',
            'TDC_FIM', 'ANS_LEGAL', 'ANS_LEGAL_CALCULADO', 'CODIGO_EXTERNO_SAP_CONCAT', 'TIPO_REMESSA',
            'NUMERO_PROTOCOLO', 'valor_factura_sum', 'TIPO_CORTE', 'Trâmite_Solicitado',
            'Executor_Solicitado', 'Polo', 'Centro Operativo', 'UT', 'tipo_servico'
        ]

        df_polos = pd.read_csv("polos.csv", encoding='utf-8', sep=';')
        df_equipes = pd.read_csv("equipes.csv", encoding='utf-8', sep=';', header=None, names=colunas_equipes, skiprows=1, on_bad_lines='skip')
        df_servicos_raw = pd.read_csv("servicos.csv", encoding='utf-8', sep=';', header=None, names=colunas_servicos, skiprows=1, low_memory=False, on_bad_lines='skip')
        df_feriados = pd.read_excel("feriados.xlsx")
        df_tempos = pd.read_csv("Tempos.csv", encoding='utf-8', sep=';')
        df_fator_k = pd.read_csv("fator_k.csv", encoding='utf-8', sep=';')

    except Exception as e:
        print(f"ERRO CRÍTICO ao ler os arquivos. Verifique os nomes/separadores dos arquivos e a ORDEM das colunas. Detalhe: {e}")
        return

    print("Preparando e padronizando os dados...")
    try:
        # Limpeza dos nomes de colunas lidos dos arquivos
        df_polos.columns = [str(col).strip() for col in df_polos.columns]
        df_fator_k.columns = [str(col).strip() for col in df_fator_k.columns]
        df_fator_k.rename(columns={'Fator K Estimado': 'Fator_K_Estimado'}, inplace=True)

        # Preparação dos dados de tempo
        df_tempos_execucao = df_tempos[['Equipe', 'Serviço', 'Mix_solic', 'Tempo Execução']].copy().dropna()
        df_tempos_execucao['Tempo_Execucao_Min'] = pd.to_timedelta(df_tempos_execucao['Tempo Execução']).dt.total_seconds() / 60
        tempo_total_str = df_tempos.loc[0, 'Total']
        tempo_total_timedelta = pd.to_timedelta(tempo_total_str + ':00' if len(tempo_total_str) <= 5 else tempo_total_str)
        JORNADA_TRABALHO_MIN = tempo_total_timedelta.total_seconds() / 60
        SERVICOS_EXTRAS_IMPRODUTIVIDADE = float(str(df_tempos.loc[0, 'Improdutividade_serviços_extras']).replace(',', '.'))

        # Preparação dos dados de Fator K
        df_fator_k['Centro Operativo'] = df_fator_k['Centro Operativo'].astype(str).str.strip().str.upper()
        df_fator_k['Fator_K_Estimado'] = pd.to_numeric(df_fator_k['Fator_K_Estimado'].astype(str).str.replace(',', '.'), errors='coerce')

        # Preparação dos dados de Polos
        df_polos['Centro Operativo'] = df_polos['Centro Operativo'].astype(str).str.strip().str.upper()
        df_polos = pd.merge(df_polos, df_fator_k[['Centro Operativo', 'Fator_K_Estimado']], on='Centro Operativo', how='left')
        
        polos_sem_k = df_polos[df_polos['Fator_K_Estimado'].isnull()]
        if not polos_sem_k.empty:
            print("\nAVISO: Os seguintes polos não foram encontrados em 'fator_k.csv' e usarão um Fator K padrão de 1.4:")
            for polo_sem_k in polos_sem_k['Centro Operativo']:
                print(f"  - {polo_sem_k}")
            df_polos['Fator_K_Estimado'] = df_polos['Fator_K_Estimado'].fillna(1.4)

        # Preparação dos dados de Serviços
        colunas_necessarias = {
            'TDC': 'ID_Servico', 'Centro Operativo': 'Polo', 'LATITUD': 'Latitude',
            'LONGITUD': 'Longitude', 'valor_factura_sum': 'Valor_Divida',
            'tipo_servico': 'Tipo_Servico', 'MUNICIPIO': 'Municipio',
            'Executor_Solicitado': 'Tipo_Equipe_Requerida',
            'Trâmite_Solicitado': 'Mix_Solic'
        }
        df_servicos = df_servicos_raw[list(colunas_necessarias.keys())].rename(columns=colunas_necessarias)

        for col in ['Polo', 'Municipio', 'Tipo_Servico', 'Tipo_Equipe_Requerida', 'Mix_Solic']:
            df_servicos[col] = df_servicos[col].astype(str).str.strip().str.upper()
        for col_tempo in ['Equipe', 'Serviço', 'Mix_solic']:
            df_tempos_execucao[col_tempo] = df_tempos_execucao[col_tempo].astype(str).str.strip().str.upper()
        
        df_servicos = df_servicos[df_servicos['Polo'] != 'NAN'].copy()

        df_servicos = pd.merge(
            df_servicos,
            df_tempos_execucao[['Equipe', 'Serviço', 'Mix_solic', 'Tempo_Execucao_Min']],
            left_on=['Tipo_Equipe_Requerida', 'Tipo_Servico', 'Mix_Solic'],
            right_on=['Equipe', 'Serviço', 'Mix_solic'],
            how='left'
        )

        servicos_sem_tempo = df_servicos[df_servicos['Tempo_Execucao_Min'].isnull()]
        if not servicos_sem_tempo.empty:
            print(f"\nAVISO: {len(servicos_sem_tempo)} serviços não encontraram tempo de execução em Tempos.csv e foram removidos da roteirização.")
            print("Verifique as combinações de Equipe/Serviço/Mix_solic em ambos os arquivos.")
            df_servicos.dropna(subset=['Tempo_Execucao_Min'], inplace=True)
        
        # Padronizações Finais
        df_equipes['Centro Operativo'] = df_equipes['Centro Operativo'].astype(str).str.strip().str.upper()
        df_feriados.columns = [str(col).strip() for col in df_feriados.columns]
        df_feriados['Municipio'] = df_feriados['Municipio'].astype(str).str.strip().str.upper()
        df_feriados['FECHA'] = pd.to_datetime(df_feriados['FECHA'], errors='coerce')
        df_servicos.dropna(subset=['Polo', 'Municipio', 'Latitude', 'Longitude'], inplace=True)

        df_polos['latitude'] = pd.to_numeric(df_polos['latitude'].astype(str).str.replace(',', '.'), errors='coerce')
        df_polos['longitude'] = pd.to_numeric(df_polos['longitude'].astype(str).str.replace(',', '.'), errors='coerce')
        df_servicos['Latitude'] = pd.to_numeric(df_servicos['Latitude'].astype(str).str.replace(',', '.'), errors='coerce')
        df_servicos['Longitude'] = pd.to_numeric(df_servicos['Longitude'].astype(str).str.replace(',', '.'), errors='coerce')
        df_servicos['Valor_Divida'] = pd.to_numeric(df_servicos['Valor_Divida'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

    except KeyError as e:
        print(f"\nERRO DE COLUNA: Uma coluna esperada não foi encontrada. Verifique se o nome da coluna '{e}' está correto nos seus arquivos CSV.")
        return
    except Exception as e:
        print(f"ERRO ao preparar ou padronizar os dados: {e}")
        return

    df_polos_completo = pd.merge(df_polos, df_equipes, on="Centro Operativo")
    
    # Lógica de Datas e Menus
    hoje = date.today()
    dia_semana = hoje.weekday()
    if dia_semana in [0, 1, 2, 3]: data_despacho = hoje + timedelta(days=1)
    elif dia_semana == 4: data_despacho = hoje + timedelta(days=3)
    elif dia_semana == 5: data_despacho = hoje + timedelta(days=2)
    else: data_despacho = hoje + timedelta(days=1)
    
    dias_pt = ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"]
    
    polos_disponiveis = sorted(df_servicos['Polo'].unique())
    
    print("\n--- AGENDAMENTO DE ROTEIRIZAÇÃO ---")
    print(f"  - Data de Hoje....: {hoje.strftime('%d/%m/%Y')} ({dias_pt[hoje.weekday()]})")
    print(f"  - Despacho para...: {data_despacho.strftime('%d/%m/%Y')} ({dias_pt[data_despacho.weekday()]})")
    
    print("\n--- MENU DE ROTEIRIZAÇÃO ---")
    for i, polo in enumerate(polos_disponiveis): print(f"  {i+1}: {polo}")
    print("  T: Processar TODOS os polos")
    escolha_polo = input("\n> Digite o número do polo desejado (ou 'T' para todos): ").strip().upper()
    
    # (O restante do código dos menus continua igual)
    # ...
    # (O restante do código, incluindo o loop de otimização e a geração de arquivos, permanece idêntico à versão anterior)
    
    # ... (código dos menus e filtros) ...
    polos_para_processar = []
    if escolha_polo == 'T': polos_para_processar = polos_disponiveis
    else:
        try:
            indice_escolhido = int(escolha_polo) - 1
            if 0 <= indice_escolhido < len(polos_disponiveis): polos_para_processar.append(polos_disponiveis[indice_escolhido])
        except (ValueError, IndexError): print("Erro: Escolha de polo inválida."); return
    if not polos_para_processar: print("Nenhum polo selecionado."); return

    servicos_a_processar = df_servicos[df_servicos['Polo'].isin(polos_para_processar)]
    municipios_a_processar = servicos_a_processar['Municipio'].unique()

    restrito, motivo = verificar_dia_restrito(data_despacho, municipios_a_processar, df_feriados)

    print("\n--- FILTRAR TIPO DE SERVIÇO ---")
    print("  1: Apenas Cortes")
    print("  2: Apenas Recortes")
    print("  3: Cortes + Recortes (Todos)")
    escolha_tipo = input("> Digite a opção desejada: ").strip()

    if restrito and escolha_tipo in ['1', '3']:
        print(f"\nERRO: Roteirização de CORTES não é permitida para o dia {data_despacho.strftime('%d/%m/%Y')} ({motivo}).")
        print("Apenas 'Recortes' são permitidos. O programa será encerrado.")
        return

    df_servicos_filtrado = pd.DataFrame()
    try:
        if 'Tipo_Servico' not in servicos_a_processar.columns: raise ValueError("Coluna 'Tipo_Servico' não encontrada")
        tipo_servico_upper = servicos_a_processar['Tipo_Servico'].astype(str).str.strip().str.upper()
        if escolha_tipo == '1': df_servicos_filtrado = servicos_a_processar[tipo_servico_upper == 'CORTE'].copy()
        elif escolha_tipo == '2': df_servicos_filtrado = servicos_a_processar[tipo_servico_upper == 'RECORTE'].copy()
        elif escolha_tipo == '3':
            if restrito:
                print(f"AVISO: O dia {data_despacho.strftime('%d/%m/%Y')} é restrito. Roteirizando apenas 'Recortes'.")
                df_servicos_filtrado = servicos_a_processar[tipo_servico_upper == 'RECORTE'].copy()
            else:
                df_servicos_filtrado = servicos_a_processar.copy()
        else: print("Opção inválida."); return
        if df_servicos_filtrado.empty: print("Nenhum serviço encontrado para o filtro."); return
    except Exception as e: print(f"ERRO ao filtrar serviços: {e}"); return

    print("\n--- ESCOLHA A ESTRATÉGIA DE ROTEIRIZAÇÃO ---")
    print("  1: Rota mais CURTA (maximizar serviços)")
    print("  2: Rota mais VALIOSA (maximizar valor, equilibrado pela distância)")
    print("  3: Rota mais EFICIENTE (equilíbrio)")
    escolha_estrategia = input("> Digite a estratégia desejada: ").strip()
    if escolha_estrategia not in ['1', '2', '3']: print("Opção inválida."); return

    print("\n--- ESCOLHA O CRITÉRIO DE RESTRIÇÃO PRINCIPAL ---")
    print("  1: Por CAPACIDADE de serviços (otimiza a quantidade de visitas)")
    print("  2: Por TEMPO de trabalho (otimiza respeitando a jornada de 8h)")
    escolha_restricao = input("> Digite o critério desejado (1/2): ").strip()
    if escolha_restricao not in ['1', '2']: print("Opção de restrição inválida."); return

    print("\n--- ENRIQUECER DADOS COM API GOOGLE MAPS? ---")
    print("  1: SIM (Calcula distância/tempo real para as rotas e gera link)")
    print("  2: NÃO (Usa apenas estimativas locais)")
    consultar_google_api = input("> Deseja consultar a API do Google? (1/2): ").strip()

    todas_as_rotas_df = pd.DataFrame()
    servicos_nao_atendidos_df = pd.DataFrame()
    dados_relatorio = []

    for nome_polo_atual in polos_para_processar:
        polo_filtrado = df_polos_completo[df_polos_completo['Centro Operativo'] == nome_polo_atual]
        if polo_filtrado.empty:
            servicos_do_polo = df_servicos_filtrado[df_servicos_filtrado['Polo'] == nome_polo_atual]
            if not servicos_do_polo.empty:
                servicos_nao_atendidos_df = pd.concat([servicos_nao_atendidos_df, servicos_do_polo])
            continue

        info_polo = polo_filtrado.iloc[0]
        fator_k_polo = info_polo['Fator_K_Estimado']
        
        for tipo_equipe in ["LEVE", "CESTO"]:
            if tipo_equipe == "LEVE":
                num_equipes = int(info_polo.get('Quantidade_equipes_Leves', 0))
                capacidade_base = int(info_polo.get('Capacidade_maxima_Leves', 0))
            else: # CESTO
                num_equipes = int(info_polo.get('Quantidades_equipes_Cesto', 0))
                capacidade_base = int(info_polo.get('Capacidade_maxima_Cesto', 0))

            grupo_servicos = df_servicos_filtrado[
                (df_servicos_filtrado['Polo'] == nome_polo_atual) &
                (df_servicos_filtrado['Tipo_Equipe_Requerida'] == tipo_equipe)
            ].copy()

            if grupo_servicos.empty or num_equipes == 0:
                if not grupo_servicos.empty:
                     servicos_nao_atendidos_df = pd.concat([servicos_nao_atendidos_df, grupo_servicos])
                continue

            print(f"\n--- OTIMizando ROTAS PARA: {nome_polo_atual} - EQUIPES {tipo_equipe} (usando Fator K: {fator_k_polo:.2f}) ---")
            
            locations = [(info_polo['latitude'], info_polo['longitude'])] + list(zip(grupo_servicos['Latitude'], grupo_servicos['Longitude']))
            manager = pywrapcp.RoutingIndexManager(len(locations), num_equipes, 0)
            routing = pywrapcp.RoutingModel(manager)

            def distance_callback(from_index, to_index):
                from_node = manager.IndexToNode(from_index)
                to_node = manager.IndexToNode(to_index)
                distancia = int(haversine(locations[from_node], locations[to_node], unit=Unit.METERS) * fator_k_polo)
                if escolha_estrategia == '2':
                    return distancia * FATOR_CUSTO_DISTANCIA
                return distancia

            transit_callback_index = routing.RegisterTransitCallback(distance_callback)
            routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

            if escolha_restricao == '1':
                print(f"  - Usando restrição por CAPACIDADE DE SERVIÇOS.")
                capacidade_servicos_ajustada = int(capacidade_base + SERVICOS_EXTRAS_IMPRODUTIVIDADE)
                routing.AddDimensionWithVehicleCapacity(
                    routing.RegisterUnaryTransitCallback(lambda from_index: 1),
                    0, [capacidade_servicos_ajustada] * num_equipes, True, 'Capacity'
                )
            elif escolha_restricao == '2':
                print(f"  - Usando restrição por TEMPO DE TRABALHO ({int(JORNADA_TRABALHO_MIN)} min).")
                def time_callback(from_index, to_index):
                    from_node = manager.IndexToNode(from_index)
                    to_node = manager.IndexToNode(to_index)
                    dist_metros = haversine(locations[from_node], locations[to_node], unit=Unit.METERS) * fator_k_polo
                    tempo_deslocamento = (dist_metros / 1000) * MINUTOS_POR_KM
                    tempo_execucao = 0
                    if from_node > 0:
                        tempo_execucao = grupo_servicos.iloc[from_node - 1]['Tempo_Execucao_Min']
                    return int(tempo_deslocamento + tempo_execucao)

                time_callback_index = routing.RegisterTransitCallback(time_callback)
                routing.AddDimension(
                    time_callback_index, 0, int(JORNADA_TRABALHO_MIN), True, 'Time'
                )
            
            for node_idx in range(1, len(locations)):
                penalty = 0
                valor_divida_atual = grupo_servicos.iloc[node_idx - 1]['Valor_Divida']
                if escolha_estrategia == '1': penalty = 15000
                elif escolha_estrategia == '2': penalty = int(valor_divida_atual * 100)
                elif escolha_estrategia == '3':
                    dist_do_polo = int(haversine(locations[0], locations[node_idx], unit=Unit.METERS) * fator_k_polo)
                    if dist_do_polo == 0: dist_do_polo = 1
                    penalty = int((valor_divida_atual * 10000) / dist_do_polo)
                if penalty <= 0: penalty = 1
                routing.AddDisjunction([manager.NodeToIndex(node_idx)], penalty)

            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
            search_parameters.local_search_metaheuristic = (routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
            search_parameters.time_limit.FromSeconds(30)
            solution = routing.SolveWithParameters(search_parameters)

            if solution:
                polo_rotas_list, servicos_atendidos_indices = [], set()
                valor_total_polo, equipes_usadas = 0, 0

                for vehicle_id in range(num_equipes):
                    index = routing.Start(vehicle_id)
                    pontos_da_rota_indices = []
                    while not routing.IsEnd(index):
                        node_index = manager.IndexToNode(index)
                        if node_index > 0:
                            pontos_da_rota_indices.append(node_index - 1)
                            servicos_atendidos_indices.add(node_index - 1)
                        index = solution.Value(routing.NextVar(index))

                    if pontos_da_rota_indices:
                        equipes_usadas += 1
                        
                        # ATUALIZAÇÃO: Lógica de pós-processamento com a API Google
                        gmaps_url = "N/A"
                        legs_info = None

                        if consultar_google_api == '1' and CHAVE_API_GOOGLE != "COLE_SUA_CHAVE_DE_API_AQUI":
                            print(f"  - Consultando Google Maps para a rota da Equipe {tipo_equipe.capitalize()} {vehicle_id + 1}...")
                            pontos_coords = [locations[idx + 1] for idx in pontos_da_rota_indices]
                            depot_coords = locations[0]
                            legs_info = obter_distancia_real_google(depot_coords, depot_coords, pontos_coords, CHAVE_API_GOOGLE)
                            if legs_info:
                                origin_url = f"{depot_coords[0]},{depot_coords[1]}"
                                waypoints_url = "/".join([f"{lat},{lon}" for lat,lon in pontos_coords])
                                gmaps_url = f"https://www.google.com/maps/dir/{origin_url}/{waypoints_url}/{origin_url}"
                            else:
                                print(f"  - AVISO: Falha na consulta à API do Google para a Equipe {tipo_equipe.capitalize()} {vehicle_id + 1}. Usando apenas estimativas locais.")
                        
                        # Processa os trechos da rota (serviço a serviço)
                        ponto_anterior_loc = locations[0]
                        for i, serv_idx in enumerate(pontos_da_rota_indices):
                            servico_atual = grupo_servicos.iloc[serv_idx]
                            ponto_atual_loc = locations[serv_idx + 1]
                            
                            dist_trecho_metros = haversine(ponto_anterior_loc, ponto_atual_loc, unit=Unit.METERS) * fator_k_polo
                            km_trecho_estimado = dist_trecho_metros / 1000
                            tempo_deslocamento_estimado = km_trecho_estimado * MINUTOS_POR_KM
                            
                            km_trecho_google, tempo_trecho_google = "N/A", "N/A"
                            if legs_info and i < len(legs_info):
                                leg = legs_info[i]
                                km_trecho_google = round(leg['distance']['value'] / 1000, 2)
                                tempo_trecho_google = round(leg['duration']['value'] / 60, 2)
                            
                            polo_rotas_list.append({
                                'Polo': nome_polo_atual, 'Equipe': f"Equipe {tipo_equipe.capitalize()} {vehicle_id + 1}",
                                'Tipo_Equipe': tipo_equipe.capitalize(), 'Ordem_Visita': i + 1,
                                'ID_Servico': servico_atual['ID_Servico'], 'Valor_Divida': servico_atual['Valor_Divida'],
                                'Tempo_Execucao_Min': servico_atual['Tempo_Execucao_Min'],
                                'KM_Trecho_Estimado': round(km_trecho_estimado, 2),
                                'Tempo_Trecho_Estimado_Min': round(tempo_deslocamento_estimado, 2),
                                'KM_Trecho_Google': km_trecho_google, 'Tempo_Trecho_Google_Min': tempo_trecho_google,
                                'Link_Google_Maps': gmaps_url
                            })
                            ponto_anterior_loc = ponto_atual_loc
                        
                        # Processa o trecho de retorno ao depósito
                        dist_retorno_metros = haversine(ponto_anterior_loc, locations[0], unit=Unit.METERS) * fator_k_polo
                        km_retorno_estimado = dist_retorno_metros / 1000
                        tempo_retorno_estimado = km_retorno_estimado * MINUTOS_POR_KM

                        km_retorno_google, tempo_retorno_google = "N/A", "N/A"
                        if legs_info and len(legs_info) == len(pontos_da_rota_indices) + 1:
                            leg = legs_info[-1]
                            km_retorno_google = round(leg['distance']['value'] / 1000, 2)
                            tempo_retorno_google = round(leg['duration']['value'] / 60, 2)

                        polo_rotas_list.append({
                            'Polo': nome_polo_atual, 'Equipe': f"Equipe {tipo_equipe.capitalize()} {vehicle_id + 1}",
                            'Tipo_Equipe': tipo_equipe.capitalize(), 'Ordem_Visita': len(pontos_da_rota_indices) + 1,
                            'ID_Servico': 'RETORNO_AO_DEPOSITO', 'Valor_Divida': 0, 'Tempo_Execucao_Min': 0,
                            'KM_Trecho_Estimado': round(km_retorno_estimado, 2),
                            'Tempo_Trecho_Estimado_Min': round(tempo_retorno_estimado, 2),
                            'KM_Trecho_Google': km_retorno_google, 'Tempo_Trecho_Google_Min': tempo_retorno_google,
                            'Link_Google_Maps': gmaps_url
                        })
                # ... (resto da lógica de processamento da solução)
                print(f"Solução encontrada! Serviços atendidos: {len(servicos_atendidos_indices)} de {len(grupo_servicos)}. Equipes usadas: {equipes_usadas} de {num_equipes}")
                if polo_rotas_list:
                    todas_as_rotas_df = pd.concat([todas_as_rotas_df, pd.DataFrame(polo_rotas_list)])
                nao_atendidos_indices = set(range(len(grupo_servicos))) - servicos_atendidos_indices
                if nao_atendidos_indices:
                    servicos_nao_atendidos_df = pd.concat([servicos_nao_atendidos_df, grupo_servicos.iloc[list(nao_atendidos_indices)]])
                dados_relatorio.append({'Polo': f"{nome_polo_atual} - {tipo_equipe}", 'Data': time.strftime("%Y-%m-%d"), 'Total_Servicos_Disponiveis': len(grupo_servicos), 'Servicos_Roteirizados': len(servicos_atendidos_indices), 'Servicos_Nao_Roteirizados': len(nao_atendidos_indices), 'Aproveitamento_%': f"{(len(servicos_atendidos_indices) / len(grupo_servicos) * 100):.2f}" if len(grupo_servicos) > 0 else "0.00", 'Valor_Total_Roteirizado_R$': valor_total_polo})

            else:
                print(f"NÃO FOI ENCONTRADA NENHUMA SOLUÇÃO VIÁVEL para {nome_polo_atual} - EQUIPES {tipo_equipe}.")
                if not grupo_servicos.empty:
                    servicos_nao_atendidos_df = pd.concat([servicos_nao_atendidos_df, grupo_servicos])

    if not todas_as_rotas_df.empty:
        print("\nSalvando o resultado em 'rotas_otimizadas.csv'...")
        rotas_sem_retorno = todas_as_rotas_df[todas_as_rotas_df['ID_Servico'] != 'RETORNO_AO_DEPOSITO'].copy()
        
        colunas_saida = [
            'Polo', 'Equipe', 'Tipo_Equipe', 'Ordem_Visita', 'ID_Servico', 'Valor_Divida',
            'Tempo_Execucao_Min', 'KM_Trecho_Estimado', 'Tempo_Trecho_Estimado_Min',
            'KM_Trecho_Google', 'Tempo_Trecho_Google_Min', 'Link_Google_Maps'
        ]
        rotas_sem_retorno.to_csv("rotas_otimizadas.csv", columns=colunas_saida, index=False, sep=';', encoding='utf-8-sig')

        print("Criando o resumo por equipes em 'resumo_equipes.csv'...")
        
        df_resumo = todas_as_rotas_df.groupby('Equipe').agg(
            Quantidade_servicos_alocados=('ID_Servico', lambda x: (x != 'RETORNO_AO_DEPOSITO').sum()),
            KM_percorridos=('KM_Trecho_Estimado', 'sum'),
            Tempo_total_deslocamento=('Tempo_Trecho_Estimado_Min', 'sum'),
            Tempo_total_servicos=('Tempo_Execucao_Min', 'sum')
        ).reset_index()

        df_resumo['Tempo_total_rota'] = df_resumo['Tempo_total_deslocamento'] + df_resumo['Tempo_total_servicos']
        df_resumo = df_resumo.round(2)
        
        colunas_resumo = [
            'Equipe', 'Quantidade_servicos_alocados', 'KM_percorridos',
            'Tempo_total_deslocamento', 'Tempo_total_servicos', 'Tempo_total_rota'
        ]
        df_resumo.to_csv("resumo_equipes.csv", columns=colunas_resumo, index=False, sep=';', encoding='utf-8-sig')

        gerar_mapa_de_rotas(rotas_sem_retorno, df_polos_completo, df_servicos, polos_para_processar)
    else:
        print("\nNenhuma rota foi gerada.")

    if not servicos_nao_atendidos_df.empty:
        print("Salvando a lista de serviços não roteirizados em 'servicos_nao_roteirizados.csv'...")
        servicos_nao_atendidos_df.to_csv("servicos_nao_roteirizados.csv", index=False, sep=';', encoding='utf-8-sig')
    if dados_relatorio:
        print("Salvando o relatório gerencial em 'resumo_do_dia.csv'...")
        df_relatorio = pd.DataFrame(dados_relatorio)
        df_relatorio.to_csv("resumo_do_dia.csv", index=False, sep=';', encoding='utf-8-sig')

    print("\nProcesso concluído!")

if __name__ == "__main__":
    main()