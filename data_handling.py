import boto3
import csv
import io
from datetime import datetime
import unicodedata

# === Função auxiliar ===
def normalizar(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').strip().lower()

# === Credenciais temporárias AWS ===

s3 = session.client('s3')

bucket_origem = 'bucket-trusted-063'
arquivo_s3 = 'limpo/colaboradores_sem_duplicatas.csv'
bucket_destino = 'bucket-trusted-063'

# === Leitura do arquivo da S3 ===
obj = s3.get_object(Bucket=bucket_origem, Key=arquivo_s3)
conteudo = obj['Body'].read().decode('utf-8')
csvfile = io.StringIO(conteudo)

# === Processamento ===
leitor = csv.reader(csvfile, delimiter=';')
cabecalho = next(leitor)
cabecalho_normalizado = [normalizar(col) for col in cabecalho]

# Posição da data
try:
    idx_data_venc = cabecalho_normalizado.index(normalizar("Data de vencimento (se aplicável)"))
except ValueError:
    raise Exception("Coluna 'Data de vencimento (se aplicável)' não encontrada.")

# Organização por pastas
pastas = {}
sem_data = []

for linha in leitor:
    data_str = linha[idx_data_venc].strip()
    if data_str:
        try:
            data = datetime.strptime(data_str, "%Y-%m-%d")
            ano = data.strftime("%Y")
            mes = data.strftime("%b").lower()
            pasta = f"{ano}/{mes}/"
            nome_arquivo = f"colaboradores_{data.strftime('%Y%m')}.csv"
            caminho = pasta + nome_arquivo

            if caminho not in pastas:
                pastas[caminho] = [linha]
            else:
                pastas[caminho].append(linha)
        except Exception:
            sem_data.append(linha)
    else:
        sem_data.append(linha)

# === Envia os arquivos organizados para a S3 ===
for caminho, linhas in pastas.items():
    saida_csv = io.StringIO()
    escritor = csv.writer(saida_csv, delimiter=';')
    escritor.writerow(cabecalho)
    escritor.writerows(linhas)

    s3.put_object(
        Bucket=bucket_destino,
        Key=caminho,
        Body=saida_csv.getvalue().encode('utf-8')
    )

# Arquivo para registros sem data
if sem_data:
    saida_sem_data = io.StringIO()
    escritor = csv.writer(saida_sem_data, delimiter=';')
    escritor.writerow(cabecalho)
    escritor.writerows(sem_data)

    s3.put_object(
        Bucket=bucket_destino,
        Key='sem_data/colaboradores_sem_data.csv',
        Body=saida_sem_data.getvalue().encode('utf-8')
    )

print("✅ Arquivos organizados por ano/mês e enviados para a S3 com sucesso!")
