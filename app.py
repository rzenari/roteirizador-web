import streamlit as st
import pandas as pd
import numpy as np
from haversine import haversine, Unit
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import folium
from streamlit_folium import st_folium
import time
from datetime import date, timedelta
import requests
import io

# ==============================================================================
# CONFIGURAÇÕES GLOBAIS
# ==============================================================================
# ⚠️ INSIRA SUA CHAVE DA API DO GOOGLE AQUI ⚠️
CHAVE_API_GOOGLE = "COLE_SUA_CHAVE_DE_API_AQUI" 
FATOR_CUSTO_DISTANCIA = 50
MINUTOS_POR_KM = 3 # Premissa: Velocidade média de 20 km/h

# ==============================================================================
# FUNÇÕES AUXILIARES DE PROCESSAMENTO
# ==============================================================================

@st.cache_data
def carregar_dados_config():
    """Carrega todos os arquivos de configuração estáticos."""
    try:
        df_polos = pd.read_csv("polos.csv", encoding='utf-8', sep=';')
        colunas_equipes = ['Centro Operativo', 'Quantidade_equipes_Leves', 'Capacidade_maxima_Leves', 'Quantidades_equipes_Cesto', 'Capacidade_maxima_Cesto']
        df_equipes = pd.read_csv("equipes.csv", encoding='utf-8', sep=';', header=None, names=colunas_equipes, skiprows=1, on_bad_lines='skip')
        df_feriados = pd.read_excel("feriados.xlsx")
        df_tempos = pd.read_csv("Tempos.csv", encoding='utf-8', sep=';')
        df_fator_k = pd.read_csv("fator_k.csv", encoding='utf-8', sep=';')
        return df_polos, df_equipes, df_feriados, df_tempos, df_fator_k
    except FileNotFoundError as e:
        st.error(f"ERRO CRÍTICO: Arquivo de configuração não encontrado: {e.filename}. Verifique se todos os arquivos de base estão no repositório do GitHub.")
        return None, None, None, None, None
    except Exception as e:
        st.error(f"ERRO ao carregar os dados de configuração: {e}")
        return None, None, None, None, None

def carregar_dados_servicos(uploaded_file):
    """Carrega o arquivo de serviços enviado pelo usuário."""
    if uploaded_file is None:
        return None
    try:
        colunas_servicos = ['CODIGO_EXTERNO', 'TDC', 'FECHA_CREACION_ORDEN_SIS_EXT', 'FECHA_CREACION_TDC_EORDER', 'ESTADO', 'CODIGO_PROCESO', 'DESCRIPCION_PROCESO', 'CODIGO_ORDEN', 'DESCRIPCION_ORDEN', 'CICLO', 'FECHA_ACTUALIZACION', 'ETL_TIME', 'CODIGO_CENTRO_OPERATIVO', 'CENTRO_OPERATIVO_TDC', 'LONGITUD', 'LATITUD', 'CODIGO_CLIENTE', 'LOCALIDAD', 'CALLE', 'NUMERO_CALLE', 'MUNICIPIO', 'BARRIO', 'CODIGO_ZIP', 'COMPLEMENTO', 'MOTIVO_INSPECION', 'CODE_NOTA', 'DESCRIPCION_CODE_NOTA', 'MEDIDA', 'DESCIPCION_MEDIDA', 'TEXTO_DIRECCION_COMPLETA', 'ALOC_RECURSOS', 'RESIDUAL', 'TDC_FIM', 'ANS_LEGAL', 'ANS_LEGAL_CALCULADO', 'CODIGO_EXTERNO_SAP_CONCAT', 'TIPO_REMESSA', 'NUMERO_PROTOCOLO', 'valor_factura_sum', 'TIPO_CORTE', 'Trâmite_Solicitado', 'Executor_Solicitado', 'Polo', 'Centro Operativo', 'UT', 'tipo_servico']
        df_servicos_raw = pd.read_csv(uploaded_file, encoding='utf-8', sep=';', header=None, names=colunas_servicos, skiprows=1, low_memory=False, on_bad_lines='skip')
        return df_servicos_raw
    except Exception as e:
        st.error(f"ERRO ao ler o arquivo 'servicos.csv' enviado. Verifique o formato e o separador (deve ser ';'). Detalhe: {e}")
        return None

@st.cache_data
def preparar_dados(df_polos, df_equipes, df_servicos_raw, df_feriados, df_tempos, df_fator_k):
    """Prepara e padroniza os dataframes para a roteirização."""
    try:
        df_polos.columns = [str(col).strip() for col in df_polos.columns]
        df_fator_k.columns = [str(col).strip() for col in df_fator_k.columns]
        df_fator_k.rename(columns={'Fator K Estimado': 'Fator_K_Estimado'}, inplace=True)

        df_tempos_execucao = df_tempos[['Equipe', 'Serviço', 'Mix_solic', 'Tempo Execução']].copy().dropna()
        df_tempos_execucao['Tempo_Execucao_Min'] = pd.to_timedelta(df_tempos_execucao['Tempo Execução']).dt.total_seconds() / 60
        tempo_total_str = df_tempos.loc[0, 'Total']
        tempo_total_timedelta = pd.to_timedelta(tempo_total_str + ':00' if len(tempo_total_str) <= 5 else tempo_total_str)
        JORNADA_TRABALHO_MIN = tempo_total_timedelta.total_seconds() / 60
        SERVICOS_EXTRAS_IMPRODUTIVIDADE = float(str(df_tempos.loc[0, 'Improdutividade_serviços_extras']).replace(',', '.'))

        df_fator_k['Centro Operativo'] = df_fator_k['Centro Operativo'].astype(str).str.strip().str.upper()
        df_fator_k['Fator_K_Estimado'] = pd.to_numeric(df_fator_k['Fator_K_Estimado'].astype(str).str.replace(',', '.'), errors='coerce')

        df_polos['Centro Operativo'] = df_polos['Centro Operativo'].astype(str).str.strip().str.upper()
        df_polos = pd.merge(df_polos, df_fator_k[['Centro Operativo', 'Fator_K_Estimado']], on='Centro Operativo', how='left')
        
        polos_sem_k = df_polos[df_polos['Fator_K_Estimado'].isnull()]
        if not polos_sem_k.empty:
            st.warning(f"AVISO: Os polos {polos_sem_k['Centro Operativo'].tolist()} não foram encontrados em 'fator_k.csv' e usarão um Fator K padrão de 1.4.")
            df_polos['Fator_K_Estimado'] = df_polos['Fator_K_Estimado'].fillna(1.4)

        colunas_necessarias = {'TDC': 'ID_Servico', 'Centro Operativo': 'Polo', 'LATITUD': 'Latitude', 'LONGITUD': 'Longitude', 'valor_factura_sum': 'Valor_Divida', 'tipo_servico': 'Tipo_Servico', 'MUNICIPIO': 'Municipio', 'Executor_Solicitado': 'Tipo_Equipe_Requerida', 'Trâmite_Solicitado': 'Mix_Solic'}
        df_servicos = df_servicos_raw[list(colunas_necessarias.keys())].rename(columns=colunas_necessarias)

        for col in ['Polo', 'Municipio', 'Tipo_Servico', 'Tipo_Equipe_Requerida', 'Mix_Solic']:
            df_servicos[col] = df_servicos[col].astype(str).str.strip().str.upper()
        for col_tempo in ['Equipe', 'Serviço', 'Mix_solic']:
            df_tempos_execucao[col_tempo] = df_tempos_execucao[col_tempo].astype(str).str.strip().str.upper()
        
        df_servicos = df_servicos[df_servicos['Polo'] != 'NAN'].copy()

        df_servicos = pd.merge(df_servicos, df_tempos_execucao[['Equipe', 'Serviço', 'Mix_solic', 'Tempo_Execucao_Min']], left_on=['Tipo_Equipe_Requerida', 'Tipo_Servico', 'Mix_Solic'], right_on=['Equipe', 'Serviço', 'Mix_solic'], how='left')

        servicos_sem_tempo = df_servicos[df_servicos['Tempo_Execucao_Min'].isnull()]
        if not servicos_sem_tempo.empty:
            st.warning(f"AVISO: {len(servicos_sem_tempo)} serviços não encontraram tempo de execução em Tempos.csv e foram removidos da roteirização.")
            df_servicos.dropna(subset=['Tempo_Execucao_Min'], inplace=True)
        
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
        
        df_polos_completo = pd.merge(df_polos, df_equipes, on="Centro Operativo")

        return df_servicos, df_polos_completo, df_feriados, JORNADA_TRABALHO_MIN, SERVICOS_EXTRAS_IMPRODUTIVIDADE
    except Exception as e:
        st.error(f"ERRO ao preparar os dados: {e}")
        return None, None, None, None, None

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

def obter_distancia_real_google(origem_coords, destino_coords, waypoints_coords, chave_api):
    if chave_api == "COLE_SUA_CHAVE_DE_API_AQUI" or not chave_api: return None
    base_url = "https://maps.googleapis.com/maps/api/directions/json"
    origin_str = f"{origem_coords[0]},{origem_coords[1]}"
    destination_str = f"{destino_coords[0]},{destino_coords[1]}"
    
    # A função agora assume que recebe uma lista de waypoints dentro do limite
    waypoints_str = "|".join([f"{lat},{lon}" for lat, lon in waypoints_coords])
    params = {"origin": origin_str, "destination": destination_str, "waypoints": f"optimize:true|{waypoints_str}", "key": chave_api}
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        if data['status'] == 'OK': return data['routes'][0]['legs']
        else: return None
    except requests.exceptions.RequestException:
        return None

def executar_roteirizacao(params):
    polos_para_processar = params["polos_para_processar"]
    df_servicos_filtrado = params["df_servicos_filtrado"]
    df_polos_completo = params["df_polos_completo"]
    escolha_estrategia = params["estrategia"]
    escolha_restricao = params["restricao"]
    consultar_google_api = params["usar_google_api"]
    JORNADA_TRABALHO_MIN = params["JORNADA_TRABALHO_MIN"]
    SERVICOS_EXTRAS_IMPRODUTIVIDADE = params["SERVICOS_EXTRAS_IMPRODUTIVIDADE"]
    
    todas_as_rotas_df = pd.DataFrame()
    servicos_nao_atendidos_df = pd.DataFrame()
    dados_relatorio = []

    progress_bar = st.progress(0)
    total_polos = len(polos_para_processar)

    for i, nome_polo_atual in enumerate(polos_para_processar):
        progress_bar.progress((i) / total_polos, text=f"Processando Polo: {nome_polo_atual}...")
        
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
            else:
                num_equipes = int(info_polo.get('Quantidades_equipes_Cesto', 0))
                capacidade_base = int(info_polo.get('Capacidade_maxima_Cesto', 0))

            grupo_servicos = df_servicos_filtrado[(df_servicos_filtrado['Polo'] == nome_polo_atual) & (df_servicos_filtrado['Tipo_Equipe_Requerida'] == tipo_equipe)].copy()

            if grupo_servicos.empty or num_equipes == 0:
                if not grupo_servicos.empty:
                     servicos_nao_atendidos_df = pd.concat([servicos_nao_atendidos_df, grupo_servicos])
                continue

            locations = [(info_polo['latitude'], info_polo['longitude'])] + list(zip(grupo_servicos['Latitude'], grupo_servicos['Longitude']))
            manager = pywrapcp.RoutingIndexManager(len(locations), num_equipes, 0)
            routing = pywrapcp.RoutingModel(manager)

            def distance_callback(from_index, to_index):
                from_node, to_node = manager.IndexToNode(from_index), manager.IndexToNode(to_index)
                distancia = int(haversine(locations[from_node], locations[to_node], unit=Unit.METERS) * fator_k_polo)
                if escolha_estrategia == '2': return distancia * FATOR_CUSTO_DISTANCIA
                return distancia

            transit_callback_index = routing.RegisterTransitCallback(distance_callback)
            routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

            if escolha_restricao == '1':
                capacidade_servicos_ajustada = int(capacidade_base + SERVICOS_EXTRAS_IMPRODUTIVIDADE)
                routing.AddDimensionWithVehicleCapacity(routing.RegisterUnaryTransitCallback(lambda from_index: 1), 0, [capacidade_servicos_ajustada] * num_equipes, True, 'Capacity')
            elif escolha_restricao == '2':
                def time_callback(from_index, to_index):
                    from_node, to_node = manager.IndexToNode(from_index), manager.IndexToNode(to_index)
                    dist_metros = haversine(locations[from_node], locations[to_node], unit=Unit.METERS) * fator_k_polo
                    tempo_deslocamento = (dist_metros / 1000) * MINUTOS_POR_KM
                    tempo_execucao = grupo_servicos.iloc[from_node - 1]['Tempo_Execucao_Min'] if from_node > 0 else 0
                    return int(tempo_deslocamento + tempo_execucao)
                time_callback_index = routing.RegisterTransitCallback(time_callback)
                routing.AddDimension(time_callback_index, 0, int(JORNADA_TRABALHO_MIN), True, 'Time')
            
            for node_idx in range(1, len(locations)):
                penalty = 0
                valor_divida_atual = grupo_servicos.iloc[node_idx - 1]['Valor_Divida']
                if escolha_estrategia == '1': penalty = 15000
                elif escolha_estrategia == '2': penalty = int(valor_divida_atual * 100)
                elif escolha_estrategia == '3':
                    dist_do_polo = int(haversine(locations[0], locations[node_idx], unit=Unit.METERS) * fator_k_polo)
                    penalty = int((valor_divida_atual * 10000) / (dist_do_polo if dist_do_polo > 0 else 1))
                routing.AddDisjunction([manager.NodeToIndex(node_idx)], penalty if penalty > 0 else 1)

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
                        gmaps_url, legs_info = "N/A", None
                        
                        # --- ATUALIZAÇÃO: LÓGICA DE QUEBRA DE ROTAS PARA A API ---
                        if consultar_google_api == '1':
                            pontos_coords = [locations[idx + 1] for idx in pontos_da_rota_indices]
                            depot_coords = locations[0]
                            full_path_points = [depot_coords] + pontos_coords + [depot_coords]
                            
                            chunk_size = 27 # Limite da API: origin + 25 waypoints + destination
                            all_legs_info = []

                            if len(full_path_points) > chunk_size:
                                st.info(f"Rota da Equipe {tipo_equipe.capitalize()} {vehicle_id + 1} com {len(pontos_coords)} pontos será dividida em chamadas menores para a API do Google.")
                                for i in range(0, len(full_path_points) - 1, chunk_size - 1):
                                    chunk = full_path_points[i : i + chunk_size]
                                    if len(chunk) < 2: continue
                                    
                                    chunk_origin, chunk_destination, chunk_waypoints = chunk[0], chunk[-1], chunk[1:-1]
                                    chunk_legs = obter_distancia_real_google(chunk_origin, chunk_destination, chunk_waypoints, CHAVE_API_GOOGLE)
                                    
                                    if chunk_legs:
                                        all_legs_info.extend(chunk_legs)
                                    else:
                                        all_legs_info = None; break
                                legs_info = all_legs_info
                            else:
                                legs_info = obter_distancia_real_google(depot_coords, depot_coords, pontos_coords, CHAVE_API_GOOGLE)

                            if legs_info:
                                origin_url, waypoints_url = f"{depot_coords[0]},{depot_coords[1]}", "/".join([f"{lat},{lon}" for lat,lon in pontos_coords])
                                gmaps_url = f"https://www.google.com/maps/dir/{origin_url}/{waypoints_url}/{origin_url}"
                            elif len(pontos_coords) > 0:
                                st.warning(f"AVISO: Falha na consulta à API do Google para a Equipe {tipo_equipe.capitalize()} {vehicle_id + 1}. Usando apenas estimativas locais.")
                        
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
                                km_trecho_google, tempo_trecho_google = round(leg['distance']['value'] / 1000, 2), round(leg['duration']['value'] / 60, 2)
                            
                            polo_rotas_list.append({'Polo': nome_polo_atual, 'Equipe': f"Equipe {tipo_equipe.capitalize()} {vehicle_id + 1}", 'Tipo_Equipe': tipo_equipe.capitalize(), 'Ordem_Visita': i + 1, 'ID_Servico': servico_atual['ID_Servico'], 'Valor_Divida': servico_atual['Valor_Divida'], 'Tempo_Execucao_Min': servico_atual['Tempo_Execucao_Min'], 'KM_Trecho_Estimado': round(km_trecho_estimado, 2), 'Tempo_Trecho_Estimado_Min': round(tempo_deslocamento_estimado, 2), 'KM_Trecho_Google': km_trecho_google, 'Tempo_Trecho_Google_Min': tempo_trecho_google, 'Link_Google_Maps': gmaps_url})
                            ponto_anterior_loc = ponto_atual_loc
                        
                        dist_retorno_metros = haversine(ponto_anterior_loc, locations[0], unit=Unit.METERS) * fator_k_polo
                        km_retorno_estimado, tempo_retorno_estimado = dist_retorno_metros / 1000, (dist_retorno_metros / 1000) * MINUTOS_POR_KM
                        km_retorno_google, tempo_retorno_google = "N/A", "N/A"
                        if legs_info and len(legs_info) == len(pontos_da_rota_indices) + 1:
                            leg = legs_info[-1]
                            km_retorno_google, tempo_retorno_google = round(leg['distance']['value'] / 1000, 2), round(leg['duration']['value'] / 60, 2)
                        polo_rotas_list.append({'Polo': nome_polo_atual, 'Equipe': f"Equipe {tipo_equipe.capitalize()} {vehicle_id + 1}", 'Tipo_Equipe': tipo_equipe.capitalize(), 'Ordem_Visita': len(pontos_da_rota_indices) + 1, 'ID_Servico': 'RETORNO_AO_DEPOSITO', 'Valor_Divida': 0, 'Tempo_Execucao_Min': 0, 'KM_Trecho_Estimado': round(km_retorno_estimado, 2), 'Tempo_Trecho_Estimado_Min': round(tempo_retorno_estimado, 2), 'KM_Trecho_Google': km_retorno_google, 'Tempo_Trecho_Google_Min': tempo_retorno_google, 'Link_Google_Maps': gmaps_url})
                
                if polo_rotas_list: todas_as_rotas_df = pd.concat([todas_as_rotas_df, pd.DataFrame(polo_rotas_list)])
                nao_atendidos_indices = set(range(len(grupo_servicos))) - servicos_atendidos_indices
                if nao_atendidos_indices: servicos_nao_atendidos_df = pd.concat([servicos_nao_atendidos_df, grupo_servicos.iloc[list(nao_atendidos_indices)]])
                dados_relatorio.append({'Polo': f"{nome_polo_atual} - {tipo_equipe}", 'Data': time.strftime("%Y-%m-%d"), 'Total_Servicos_Disponiveis': len(grupo_servicos), 'Servicos_Roteirizados': len(servicos_atendidos_indices), 'Servicos_Nao_Roteirizados': len(nao_atendidos_indices), 'Aproveitamento_%': f"{(len(servicos_atendidos_indices) / len(grupo_servicos) * 100):.2f}" if len(grupo_servicos) > 0 else "0.00", 'Valor_Total_Roteirizado_R$': grupo_servicos.iloc[list(servicos_atendidos_indices)]['Valor_Divida'].sum()})
            else:
                if not grupo_servicos.empty: servicos_nao_atendidos_df = pd.concat([servicos_nao_atendidos_df, grupo_servicos])

    progress_bar.progress(1.0, text="Processo concluído!")
    
    resumo_equipes_df = pd.DataFrame()
    if not todas_as_rotas_df.empty:
        df_resumo = todas_as_rotas_df.groupby('Equipe').agg(Quantidade_servicos_alocados=('ID_Servico', lambda x: (x != 'RETORNO_AO_DEPOSITO').sum()), KM_percorridos=('KM_Trecho_Estimado', 'sum'), Tempo_total_deslocamento=('Tempo_Trecho_Estimado_Min', 'sum'), Tempo_total_servicos=('Tempo_Execucao_Min', 'sum')).reset_index()
        df_resumo['Tempo_total_rota'] = df_resumo['Tempo_total_deslocamento'] + df_resumo['Tempo_total_servicos']
        resumo_equipes_df = df_resumo.round(2)

    return todas_as_rotas_df, servicos_nao_atendidos_df, resumo_equipes_df, pd.DataFrame(dados_relatorio)

def gerar_mapa_de_rotas(df_rotas, df_polos_info, polos_processados):
    if df_rotas.empty: return None
    polos_filtrados = df_polos_info[df_polos_info['Centro Operativo'].isin(polos_processados)]
    if polos_filtrados.empty: return None
    
    mapa = folium.Map(location=[polos_filtrados['latitude'].mean(), polos_filtrados['longitude'].mean()], zoom_start=10)
    cores = ['blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'red', 'lightblue', 'lightgreen', 'gray', 'pink', 'lightgray']
    mapa_cores_equipe = {equipe: cores[i % len(cores)] for i, equipe in enumerate(df_rotas['Equipe'].unique())}
    
    for _, polo in polos_filtrados.iterrows():
        folium.Marker(location=[polo['latitude'], polo['longitude']], popup=f"<strong>Polo: {polo['Centro Operativo']}</strong>", icon=folium.Icon(color='black', icon='industry', prefix='fa')).add_to(mapa)
    
    for equipe_unica in df_rotas['Equipe'].unique():
        rota_da_equipe = df_rotas[df_rotas['Equipe'] == equipe_unica].sort_values(by='Ordem_Visita')
        if rota_da_equipe.empty: continue
        
        polo_da_rota = rota_da_equipe['Polo'].iloc[0]
        info_polo = df_polos_info[df_polos_info['Centro Operativo'] == polo_da_rota].iloc[0]
        polo_coords = (info_polo['latitude'], info_polo['longitude'])
        
        pontos_da_rota = [polo_coords] + list(zip(rota_da_equipe['Latitude'], rota_da_equipe['Longitude'])) + [polo_coords]
        
        cor_da_rota = mapa_cores_equipe[equipe_unica]
        folium.PolyLine(pontos_da_rota, color=cor_da_rota, weight=3, opacity=0.8, tooltip=f"<strong>{equipe_unica}</strong>").add_to(mapa)
        
        for _, servico in rota_da_equipe.iterrows():
            popup_html = f"<strong>Equipe:</strong> {servico['Equipe']}<br><strong>Ordem:</strong> {servico['Ordem_Visita']}<br><strong>ID Serviço:</strong> {servico['ID_Servico']}"
            folium.Marker(location=[servico['Latitude'], servico['Longitude']], popup=folium.Popup(popup_html, max_width=300), icon=folium.Icon(color=cor_da_rota, icon='info-sign')).add_to(mapa)
            
    return mapa

# ==============================================================================
# INTERFACE DA APLICAÇÃO WEB (STREAMLIT)
# ==============================================================================

st.set_page_config(layout="wide", page_title="Roteirizador Inteligente")
st.title(" Roteirizador Inteligente ")

dados_config_carregados = carregar_dados_config()

if all(df is not None for df in dados_config_carregados):
    df_polos, df_equipes, df_feriados, df_tempos, df_fator_k = dados_config_carregados

    st.sidebar.header("Parâmetros da Roteirização")
    uploaded_file = st.sidebar.file_uploader("1. Carregue o arquivo 'servicos.csv' do dia", type=["csv"])

    if uploaded_file is not None:
        df_servicos_raw = carregar_dados_servicos(uploaded_file)
        
        if df_servicos_raw is not None:
            dados_preparados = preparar_dados(df_polos, df_equipes, df_servicos_raw, df_feriados, df_tempos, df_fator_k)

            if all(dp is not None for dp in dados_preparados):
                df_servicos, df_polos_completo, df_feriados, JORNADA_TRABALHO_MIN, SERVICOS_EXTRAS_IMPRODUTIVIDADE = dados_preparados

                hoje, dia_semana = date.today(), date.today().weekday()
                if dia_semana in [0, 1, 2, 3]: data_despacho = hoje + timedelta(days=1)
                elif dia_semana == 4: data_despacho = hoje + timedelta(days=3)
                elif dia_semana == 5: data_despacho = hoje + timedelta(days=2)
                else: data_despacho = hoje + timedelta(days=1)
                dias_pt = ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"]

                st.sidebar.info(f"""**Data de Hoje:** {hoje.strftime('%d/%m/%Y')} ({dias_pt[hoje.weekday()]})  
                **Roteirizando para:** {data_despacho.strftime('%d/%m/%Y')} ({dias_pt[data_despacho.weekday()]})""")

                polos_disponiveis = ["Processar TODOS"] + sorted(df_servicos['Polo'].unique())
                polo_selecionado_ui = st.sidebar.selectbox("2. Escolha o Polo", polos_disponiveis)
                
                tipo_servico_ui = st.sidebar.radio("3. Escolha o Tipo de Serviço", ('Cortes + Recortes (Todos)', 'Apenas Cortes', 'Apenas Recortes'), horizontal=True)
                estrategia_ui = st.sidebar.radio("4. Escolha a Estratégia", ('Rota mais CURTA', 'Rota mais VALIOSA', 'Rota mais EFICIENTE'))
                restricao_ui = st.sidebar.radio("5. Escolha a Restrição Principal", ('Por TEMPO de trabalho', 'Por CAPACIDADE de serviços'), horizontal=True)
                usar_google_api_ui = st.sidebar.radio("6. Enriqueçer com Google Maps?", ('NÃO (mais rápido)', 'SIM (custo por consulta)'), horizontal=True)

                if st.sidebar.button(" Gerar Rotas ", use_container_width=True, type="primary"):
                    
                    polos_para_processar = polos_disponiveis[1:] if polo_selecionado_ui == "Processar TODOS" else [polo_selecionado_ui]
                    municipios_a_processar = df_servicos[df_servicos['Polo'].isin(polos_para_processar)]['Municipio'].unique()
                    restrito, motivo = verificar_dia_restrito(data_despacho, municipios_a_processar, df_feriados)
                    
                    servicos_a_processar = df_servicos[df_servicos['Polo'].isin(polos_para_processar)]
                    df_servicos_filtrado = pd.DataFrame()

                    if tipo_servico_ui == 'Apenas Cortes': tipo_filtro = 'CORTE'
                    elif tipo_servico_ui == 'Apenas Recortes': tipo_filtro = 'RECORTE'
                    else: tipo_filtro = 'TODOS'

                    if restrito and tipo_filtro in ['CORTE', 'TODOS']:
                        st.error(f"ERRO: Roteirização de CORTES não é permitida para o dia {data_despacho.strftime('%d/%m/%Y')} ({motivo}).")
                    else:
                        if tipo_filtro == 'CORTE': df_servicos_filtrado = servicos_a_processar[servicos_a_processar['Tipo_Servico'] == 'CORTE'].copy()
                        elif tipo_filtro == 'RECORTE': df_servicos_filtrado = servicos_a_processar[servicos_a_processar['Tipo_Servico'] == 'RECORTE'].copy()
                        else: df_servicos_filtrado = servicos_a_processar.copy()

                        if df_servicos_filtrado.empty:
                            st.warning("Nenhum serviço encontrado para os filtros selecionados.")
                        else:
                            with st.spinner('Aguarde... Otimizando as rotas. Isso pode levar alguns minutos.'):
                                params = {
                                    "polos_para_processar": polos_para_processar,
                                    "df_servicos_filtrado": df_servicos_filtrado,
                                    "df_polos_completo": df_polos_completo,
                                    "estrategia": {'Rota mais CURTA': '1', 'Rota mais VALIOSA': '2', 'Rota mais EFICIENTE': '3'}[estrategia_ui],
                                    "restricao": {'Por CAPACIDADE de serviços': '1', 'Por TEMPO de trabalho': '2'}[restricao_ui],
                                    "usar_google_api": {'SIM (custo por consulta)': '1', 'NÃO (mais rápido)': '2'}[usar_google_api_ui],
                                    "JORNADA_TRABALHO_MIN": JORNADA_TRABALHO_MIN,
                                    "SERVICOS_EXTRAS_IMPRODUTIVIDADE": SERVICOS_EXTRAS_IMPRODUTIVIDADE
                                }
                                
                                todas_as_rotas_df, servicos_nao_atendidos_df, resumo_equipes_df, dados_relatorio_df = executar_roteirizacao(params)

                                st.success("Roteirização concluída com sucesso!")
                                
                                rotas_com_coords = pd.merge(todas_as_rotas_df[todas_as_rotas_df['ID_Servico'] != 'RETORNO_AO_DEPOSITO'], df_servicos[['ID_Servico', 'Latitude', 'Longitude']], on='ID_Servico', how='left')

                                tab1, tab2, tab3, tab4 = st.tabs(["📊 Resumo das Equipes", "🗺️ Mapa das Rotas", "📋 Rotas Detalhadas", "🚫 Serviços Não Roteirizados"])

                                with tab1:
                                    st.subheader("Resumo por Equipe")
                                    st.dataframe(resumo_equipes_df)
                                    st.download_button("Download Resumo (CSV)", resumo_equipes_df.to_csv(index=False, sep=';').encode('utf-8-sig'), "resumo_equipes.csv", "text/csv", key='download-resumo')

                                with tab2:
                                    st.subheader("Mapa Interativo das Rotas")
                                    mapa_folium = gerar_mapa_de_rotas(rotas_com_coords, df_polos_completo, polos_para_processar)
                                    if mapa_folium:
                                        st_folium(mapa_folium, width=1200, height=600, returned_objects=[])
                                    else:
                                        st.warning("Nenhuma rota gerada para exibir no mapa.")

                                with tab3:
                                    st.subheader("Detalhes das Rotas Otimizadas")
                                    df_para_mostrar = todas_as_rotas_df[todas_as_rotas_df['ID_Servico'] != 'RETORNO_AO_DEPOSITO']
                                    st.dataframe(df_para_mostrar)
                                    st.download_button("Download Rotas (CSV)", df_para_mostrar.to_csv(index=False, sep=';').encode('utf-8-sig'), "rotas_otimizadas.csv", "text/csv", key='download-rotas')

                                with tab4:
                                    st.subheader("Serviços Não Roteirizados")
                                    st.dataframe(servicos_nao_atendidos_df)
                                    st.download_button("Download Não Roteirizados (CSV)", servicos_nao_atendidos_df.to_csv(index=False, sep=';').encode('utf-8-sig'), "servicos_nao_roteirizados.csv", "text/csv", key='download-nao-roteirizados')
    else:
        st.info("Aguardando o carregamento do arquivo 'servicos.csv' na barra lateral para iniciar.")
