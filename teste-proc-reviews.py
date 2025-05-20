"""
DAG para transformação de dados de reviews do S3 sem dependências do Glue
Converte o script original do Glue para execução no Airflow
"""
from datetime import datetime, timedelta
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configurações do DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stamped_reviews_transformation',
    default_args=default_args,
    description='Transforma dados de reviews da Stamped da Bronze para Silver',
    schedule='0 6 * * *',  # Diariamente às 6h
    catchup=False,
)

# Função auxiliar para listar arquivos no S3
def list_s3_files(bucket: str, prefix: str) -> List[str]:
    """Lista todos os arquivos em um bucket/prefixo do S3"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    return s3_hook.list_keys(bucket_name=bucket, prefix=prefix)

# Função auxiliar para ler arquivos Parquet do S3
def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Lê um arquivo Parquet diretamente do S3 e retorna como DataFrame pandas"""
    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_client = s3_hook.get_conn()
        
        # Usar get_object em vez de read_key para obter os dados binários diretamente
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        
        buffer = BytesIO(data)
        return pd.read_parquet(buffer)
    except Exception as e:
        logging.error(f"Erro ao ler arquivo {key} do bucket {bucket}: {str(e)}")
        raise

# Função auxiliar para escrever DataFrame em Parquet no S3
def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str, partition_cols: Optional[List[str]] = None) -> None:
    """Escreve um DataFrame pandas como Parquet no S3, com suporte a particionamento"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    buffer = BytesIO()
    
    if partition_cols:
        # Para particionamento, usamos PyArrow diretamente
        table = pa.Table.from_pandas(df)
        
        # Escrever em memória primeiro
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # Upload para S3
        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=key,
            bucket_name=bucket,
            replace=True
        )
    else:
        # Caso simples sem particionamento
        df.to_parquet(buffer, compression='snappy')
        buffer.seek(0)
        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=key,
            bucket_name=bucket,
            replace=True
        )

def process_stamped_reviews(ds, **kwargs):
    """
    Função principal que executa a transformação dos dados
    Equivalente ao script do Glue, mas adaptado para o Airflow sem PySpark
    """
    # Configuração de logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Obter data de execução
    execution_date = kwargs.get('logical_date', datetime.now()).strftime('%Y-%m-%d')
    
    # Caminho da camada Bronze
    bronze_bucket = 'lakehouse-gocase'
    bronze_prefix = f'bronze/stamped/reviews/{execution_date}/'
    logger.info(f"Lendo dados de s3://{bronze_bucket}/{bronze_prefix}")
    
    # Verificar se existem arquivos na pasta bronze
    files = list_s3_files(bronze_bucket, bronze_prefix)
    
    if not files:
        logger.warning(f"Nenhum arquivo encontrado em s3://{bronze_bucket}/{bronze_prefix}")
        return "Nenhum arquivo para processar"
    
    # Filtrar apenas arquivos parquet
    parquet_files = [f for f in files if f.endswith('.parquet')]
    
    if not parquet_files:
        logger.warning(f"Nenhum arquivo Parquet encontrado em s3://{bronze_bucket}/{bronze_prefix}")
        return "Nenhum arquivo Parquet para processar"
    
    logger.info(f"Encontrados {len(parquet_files)} arquivos Parquet")
    
    # Ler todos os arquivos Parquet e combinar em um único DataFrame
    dataframes = []
    
    for parquet_file in parquet_files:
        try:
            logger.info(f"Iniciando leitura do arquivo: {parquet_file}")
            logger.info(f"Bucket: {bronze_bucket}, Key: {parquet_file}")
            
            df = read_parquet_from_s3(bronze_bucket, parquet_file)
            
            if not df.empty:
                logger.info(f"Arquivo lido com sucesso. Shape: {df.shape}")
                logger.info(f"Colunas encontradas: {df.columns.tolist()}")
                dataframes.append(df)
                logger.info(f"Arquivo {parquet_file} lido com {len(df)} registros")
            else:
                logger.warning(f"Arquivo {parquet_file} está vazio")
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {parquet_file}: {str(e)}")
            logger.error("Detalhes do erro:", exc_info=True)
            continue  # Continua para o próximo arquivo mesmo se houver erro
    
    if not dataframes:
        logger.warning("Nenhum dado válido encontrado nos arquivos Parquet")
        return "Nenhum dado válido para processar"
    
    # Combinar todos os DataFrames
    try:
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Total de {len(combined_df)} registros combinados")
    except Exception as e:
        logger.error(f"Erro ao combinar DataFrames: {str(e)}")
        return f"Erro na combinação de dados: {str(e)}"
    
    # Padronizar nomes das colunas (minúsculas)
    combined_df.columns = [col.lower() for col in combined_df.columns]
    logger.info(f"Colunas após padronização: {combined_df.columns.tolist()}")
    
    # Aplicando transformações semelhantes ao script Glue
    try:
        # Conversão de tipos
        if 'id' in combined_df.columns:
            combined_df['id'] = pd.to_numeric(combined_df['id'], errors='coerce').fillna(0).astype('int64')
        
        if 'reviewrating' in combined_df.columns:
            combined_df['reviewrating'] = pd.to_numeric(combined_df['reviewrating'], errors='coerce').fillna(0).astype('int64')
        
        if 'reviewvotesup' in combined_df.columns:
            combined_df['reviewvotesup'] = pd.to_numeric(combined_df['reviewvotesup'], errors='coerce').fillna(0).astype('int64')
        
        if 'reviewvotesdown' in combined_df.columns:
            combined_df['reviewvotesdown'] = pd.to_numeric(combined_df['reviewvotesdown'], errors='coerce').fillna(0).astype('int64')
        
        # Verificar e processar coluna de data
        date_column = None
        
        if 'datecreated' in combined_df.columns:
            date_column = 'datecreated'
            combined_df[date_column] = pd.to_datetime(combined_df[date_column], errors='coerce')
        elif 'reviewdate' in combined_df.columns:
            date_column = 'reviewdate'
            combined_df[date_column] = pd.to_datetime(combined_df[date_column], errors='coerce')
        else:
            logger.warning("Nenhuma coluna de data adequada encontrada, usando data atual")
            combined_df['process_date'] = datetime.now()
            date_column = 'process_date'
        
        # Criar colunas de particionamento
        combined_df['year'] = combined_df[date_column].dt.year
        combined_df['month'] = combined_df[date_column].dt.month
        combined_df['day'] = combined_df[date_column].dt.day
        
        # Remover registros com valores nulos nas colunas de partição
        before_null_filter = len(combined_df)
        combined_df = combined_df.dropna(subset=['year', 'month', 'day'])
        null_partitions_removed = before_null_filter - len(combined_df)
        
        if null_partitions_removed > 0:
            logger.info(f"Removidos {null_partitions_removed} registros com partições nulas")
        
        # Remover duplicatas com base no ID, se existir
        if 'id' in combined_df.columns:
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=['id'])
            duplicates_removed = before_dedup - len(combined_df)
            
            if duplicates_removed > 0:
                logger.info(f"Removidas {duplicates_removed} duplicatas")
        
        # Mostrar informações sobre os dados
        logger.info(f"Número total de registros após transformações: {len(combined_df)}")
        
        # Gravar dados convertidos no S3, por partição
        if not combined_df.empty:
            # Definir caminho da camada Silver
            silver_bucket = 'lakehouse-gocase'
            silver_prefix = 'silver/stamped/reviews'
            
            # Converter tipos das colunas de partição para inteiros
            combined_df['year'] = combined_df['year'].astype('int64')
            combined_df['month'] = combined_df['month'].astype('int64')
            combined_df['day'] = combined_df['day'].astype('int64')
            
            # Processar cada partição separadamente
            partition_groups = combined_df.groupby(['year', 'month', 'day'])
            
            for partition, group_df in partition_groups:
                year, month, day = partition
                
                # Formatar caminho da partição
                partition_path = f"{silver_prefix}/year={year}/month={month}/day={day}/"
                partition_file = f"{partition_path}data_{year}_{month}_{day}.parquet"
                
                logger.info(f"Gravando {len(group_df)} registros em s3://{silver_bucket}/{partition_file}")
                
                try:
                    # Escrever DataFrame como Parquet no S3
                    write_parquet_to_s3(group_df, silver_bucket, partition_file)
                except Exception as e:
                    logger.error(f"Erro ao gravar partição {partition}: {str(e)}")
            
            logger.info("Todas as partições foram processadas e gravadas com sucesso")
            return f"Processados {len(combined_df)} registros em {len(partition_groups)} partições"
        else:
            logger.warning("Não há dados para escrita após as transformações")
            return "Nenhum dado para escrita após transformações"
        
    except Exception as e:
        logger.error(f"Erro durante processamento: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

# Função para verificar se os dados fonte existem
def check_source_data(**context):
    """Verifica se os dados fonte existem no S3 antes de iniciar o processamento"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        execution_date = context['logical_date'].strftime('%Y-%m-%d')
        logger.info(f"Verificando dados para a data: {execution_date}")
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket = 'lakehouse-gocase'
        prefix = f'bronze/stamped/reviews/{execution_date}/'
        
        logger.info(f"Conectando ao bucket S3: {bucket}")
        logger.info(f"Procurando arquivos no prefixo: {prefix}")
        
        # Listar objetos para verificar se existem arquivos
        try:
            objects = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
            logger.info(f"Encontrados {len(objects) if objects else 0} objetos no total")
        except Exception as e:
            logger.error(f"Erro ao listar objetos do S3: {str(e)}")
            raise ValueError(f"Erro ao acessar S3: {str(e)}")
        
        if not objects:
            logger.error(f"Nenhum arquivo encontrado em {prefix}")
            raise ValueError(f"Não foram encontrados dados em {prefix}")
        
        # Filtrar apenas arquivos parquet
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')]
        logger.info(f"Encontrados {len(parquet_files)} arquivos Parquet")
        
        if not parquet_files:
            logger.error(f"Nenhum arquivo Parquet encontrado em {prefix}")
            raise ValueError(f"Não foram encontrados arquivos Parquet em {prefix}")
        
        return f"Dados encontrados em {prefix}: {len(parquet_files)} arquivos Parquet"
    
    except Exception as e:
        logger.error(f"Erro durante a verificação dos dados: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

# Task para verificar os dados fonte
check_data_task = PythonOperator(
    task_id='check_source_data',
    python_callable=check_source_data,
    dag=dag,
)

# Task para processar os dados
process_task = PythonOperator(
    task_id='process_stamped_reviews',
    python_callable=process_stamped_reviews,
    dag=dag,
)

# Definir a ordem das tasks
check_data_task >> process_task