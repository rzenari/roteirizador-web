import pandas as pd

print("--- INICIANDO SCRIPT DE DIAGNÓSTICO DE DADOS (v2) ---")

try:
    # Tenta carregar os dois arquivos principais
    df_servicos = pd.read_csv("servicos.csv", encoding='utf-8', sep=';', low_memory=False)
    df_tempos = pd.read_csv("Tempos.csv", encoding='utf-8', sep=';')
    print("Arquivos 'servicos.csv' e 'Tempos.csv' carregados com sucesso.")
except FileNotFoundError as e:
    print(f"\nERRO CRÍTICO: Arquivo não encontrado. Verifique o nome e o local do arquivo: {e}")
    exit()
except Exception as e:
    print(f"\nERRO ao ler os arquivos: {e}")
    exit()

# --- Etapa 1: Limpeza dos dados (exatamente como no roteirizador) ---

# Padroniza as colunas de 'servicos.csv'
for col in ['Polo', 'Executor_Solicitado', 'tipo_servico', 'Trâmite_Solicitado']:
    if col in df_servicos.columns:
        df_servicos[col] = df_servicos[col].astype(str).str.strip().str.upper()
    else:
        print(f"\nAVISO: A coluna '{col}' não foi encontrada em 'servicos.csv'.")
        
# Padroniza as colunas de 'Tempos.csv'
for col in ['Equipe', 'Serviço', 'Mix_solic']:
    if col in df_tempos.columns:
        df_tempos[col] = df_tempos[col].astype(str).str.strip().str.upper()
    else:
         print(f"\nAVISO: A coluna '{col}' não foi encontrada em 'Tempos.csv'.")


# --- Etapa 2: Análise de Divergências ---

# Filtra apenas os serviços do polo de Niterói
df_servicos_niteroi = df_servicos[df_servicos['Polo'] == 'NITERÓI'].copy()

if df_servicos_niteroi.empty:
    print("\nANÁLISE: Nenhum serviço encontrado para o polo 'NITERÓI' em 'servicos.csv'.")
else:
    print(f"\nANÁLISE: Encontrados {len(df_servicos_niteroi)} serviços para 'NITERÓI'.")
    print("\nContagem por tipo de equipe em Niterói:")
    print(df_servicos_niteroi['Executor_Solicitado'].value_counts())

    # Pega as combinações únicas de chaves dos dois arquivos
    # CORREÇÃO: Converte os dados para um formato que o Python consegue comparar (set de tuplas)
    df_chaves_servicos = df_servicos_niteroi[['Executor_Solicitado', 'tipo_servico', 'Trâmite_Solicitado']].drop_duplicates()
    df_chaves_tempos = df_tempos[['Equipe', 'Serviço', 'Mix_solic']].drop_duplicates()

    set_chaves_servicos = set(map(tuple, df_chaves_servicos.to_numpy()))
    # Renomeia as colunas do df_tempos para serem iguais antes de converter, para garantir a ordem
    df_chaves_tempos.columns = ['Executor_Solicitado', 'tipo_servico', 'Trâmite_Solicitado']
    set_chaves_tempos = set(map(tuple, df_chaves_tempos.to_numpy()))

    # Encontra as combinações que estão em 'servicos.csv' mas não em 'Tempos.csv'
    chaves_faltando = set_chaves_servicos - set_chaves_tempos

    print("\n--- RESULTADO DO DIAGNÓSTICO ---")
    if not chaves_faltando:
        print("✅ Todas as combinações de serviços em Niterói encontraram uma correspondência em Tempos.csv.")
        print("   O problema pode ser outro. Verifique se há equipes 'Cesto' cadastradas para Niterói em equipes.csv.")
    else:
        print("❌ ERRO DE DADOS ENCONTRADO!")
        print("As seguintes combinações de (Equipe, Serviço, Trâmite) existem em 'servicos.csv' para Niterói,")
        print("mas NÃO foram encontradas em 'Tempos.csv'. Por isso, estes serviços estão sendo descartados:\n")
        
        for chave in sorted(list(chaves_faltando)):
            print(f"  - Equipe: '{chave[0]}', Serviço: '{chave[1]}', Trâmite: '{chave[2]}'")
        
        print("\nPara corrigir, adicione as linhas correspondentes a estas combinações no seu arquivo 'Tempos.csv'.")

print("\n--- DIAGNÓSTICO CONCLUÍDO ---")