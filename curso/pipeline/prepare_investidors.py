
import pandas as pd
import duckdb

def read_source(path):
    
    try:
        
        df_source = pd.read_csv(path, encoding='latin1', decimal=',')

        df_source.columns = df_source.columns.str.replace(' ', '_').str.lower()

        df_source['codigo_do_investidor'] = df_source['codigo_do_investidor'].astype(str)

    except Exception as e:

        raise ValueError(f"Error to read source: {e}")
    
    return df_source

def adjust_date_coluns(df_source):

    try:
        df_source['data_de_adesao'] = pd.to_datetime(df_source['data_de_adesao'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

    except Exception as e:

        raise ValueError(f"Error to read source: {e}")
    
    return df_source

# Função para salvar os dados no DuckDB
def save_to_duckdb(df, db_path):
    try:
        # Conectar ao banco DuckDB
        conn = duckdb.connect(db_path)

         # Gravar os dados na tabela 'staging.my_table' (ajuste o nome da tabela conforme necessário)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS staging.investidores AS SELECT * FROM df LIMIT 0;
        """)
        

        # Inserir os dados na tabela 'staging.my_table'
        conn.register('df', df)  # Registra o DataFrame como uma tabela temporária
        conn.execute("""
            INSERT INTO staging.investidores SELECT * FROM df;
        """)

        # Fechar a conexão
        conn.close()
        print("Dados gravados com sucesso no DuckDB!")
    
    except Exception as e:
        raise ValueError(f"Erro ao gravar dados no DuckDB: {e}")

def main():

    path_operations = 'curso/data/raw/investidores.csv'

    db_path = '/workspaces/analytics_engineering_curse/curso/datawarehouse/tesouro.duckdb'  # Caminho do seu banco DuckDB

    df = read_source(path_operations)

    df = adjust_date_coluns(df)

    save_to_duckdb(df, db_path)

if __name__ == "__main__":
    
    main()
