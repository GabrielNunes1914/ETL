import csv
import unicodedata
from datetime import datetime
import boto3
import io

def normalizar(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').strip().lower()

# === CONFIGURAÇÕES S3 ===
bucket_origem = 'bucket-raw-063'
bucket_destino = 'bucket-trusted-063'
arquivo_s3 = 'colaboradores_certificados_pv.csv'
arquivo_saida = 'limpo/colaboradores_sem_duplicatas.csv'

# === CREDENCIAIS TEMPORÁRIAS ===

# Cliente S3
s3 = session.client('s3')

# Faz download do arquivo da S3 (bucket de origem)
obj = s3.get_object(Bucket=bucket_origem, Key=arquivo_s3)
conteudo = obj['Body'].read().decode('utf-8')
csvfile = io.StringIO(conteudo)

linhas_validas = set()

# Campos obrigatórios
campos_obrigatorios = [
    "Nome completo do colaborador",
    "E-mail",
    "Nome do certificado",
    "Tipo do certificado",
    "Data de conclusão",
    "Departamento/time",
    "Área de conhecimento"
]

campos_obrigatorios_normalizados = [normalizar(campo) for campo in campos_obrigatorios]

leitor = csv.reader(csvfile, delimiter=';')
cabecalho = next(leitor)
cabecalho_normalizado = [normalizar(col) for col in cabecalho]

# Índices
indices_obrigatorios = [cabecalho_normalizado.index(campo) for campo in campos_obrigatorios_normalizados]
idx_data_conclusao = cabecalho_normalizado.index(normalizar("Data de conclusão"))
idx_nome_colaborador = cabecalho_normalizado.index(normalizar("Nome completo do colaborador"))

for linha in leitor:
    if any(not linha[i].strip() for i in indices_obrigatorios):
        continue

    try:
        data_str = linha[idx_data_conclusao].strip()
        data_conclusao = datetime.strptime(data_str, "%Y-%m-%d")
        if not (datetime(2000, 1, 1) <= data_conclusao <= datetime.today()):
            continue
    except Exception:
        continue

    linhas_validas.add(tuple(linha))

# Ordena alfabeticamente pelo nome do colaborador
linhas_ordenadas = sorted(linhas_validas, key=lambda linha: linha[idx_nome_colaborador].lower())

# Salva resultado em memória
saida_csv = io.StringIO()
escritor = csv.writer(saida_csv, delimiter=';')
escritor.writerow(cabecalho)
for linha in linhas_ordenadas:
    escritor.writerow(linha)

# Upload para S3 (bucket de destino)
s3.put_object(Bucket=bucket_destino, Key=arquivo_saida, Body=saida_csv.getvalue().encode('utf-8'))

print("Arquivo processado com sucesso e salvo no bucket trusted! ✅📤")
