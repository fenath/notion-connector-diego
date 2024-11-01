import time
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Inicializar a autenticação
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
SERVICE_ACCOUNT_FILE = 'credentials.json'

# Credenciais do serviço
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Função para iniciar o serviço do Google Drive
def init_drive_service():
    return build('drive', 'v3', credentials=creds)

# Função para listar arquivos do Google Drive (no caso, filtrando planilhas)
def list_files(service):
    try:
        results = service.files().list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            fields="files(id, name, modifiedTime, webViewLink)"
        ).execute()
        return results.get('files', [])
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

# Função para monitorar mudanças
def watch_files(service, interval=10):
    file_state = {}
    
    while True:
        files = list_files(service)
        if files is None:
            print("Error fetching files. Retrying...")
            time.sleep(interval)
            continue
        
        for file in files:
            file_id = file['id']
            modified_time = file['modifiedTime']
            
            # Se o arquivo é novo ou foi modificado, atualiza o estado e emite o sinal
            if file_id not in file_state or file_state[file_id] != modified_time:
                file_state[file_id] = modified_time
                signal = {
                    'link_arquivo': file['webViewLink'],
                    'modificado_em': modified_time
                }
                print("Atualização detectada:", signal)

        # Espera antes de verificar novamente
        time.sleep(interval)

# Executar monitoramento
if __name__ == '__main__':
    drive_service = init_drive_service()
    watch_files(drive_service, interval=60)  # Checa atualizações a cada 60 segundos
