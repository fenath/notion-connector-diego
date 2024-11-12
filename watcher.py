import time
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dateutil import tz
from datetime import datetime
from logger import FileLogger
from monolit import Monolit
import sys
import os

from_zone = tz.tzutc()
to_zone = tz.tzlocal()

def is_first_time():
    return '--first' in sys.argv[1:]

# Configurar credenciais e acessar o Google Sheets e o Google Drive API
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.metadata.readonly'
]
SERVICE_ACCOUNT_FILE = 'credentials.json'

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Inicializar cliente do Google Sheets com gspread e Google Drive API
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# Lista de IDs das planilhas específicas para monitoramento
spreadsheet_urls = []

with open('spreadsheet_urls.txt', 'r', encoding='utf-8') as f:
    spreadsheet_urls = f.read().splitlines()
    spreadsheet_urls = filter(lambda x: x.startswith('https://'), spreadsheet_urls)

def url_to_id(url):
    return url.split('/')[-2]

spreadsheet_ids = list(map(url_to_id, spreadsheet_urls))

# Função para obter informações de um arquivo específico
def get_file_info(file_id):
    return drive_service.files().get(
        fileId=file_id,
        fields="id, name, modifiedTime, webViewLink",
    ).execute()

class FileWatcher:
    def __init__(self, files={}, idle_time_mins=10):
        self._log = FileLogger()
        if type(files) is not dict:
            raise TypeError("files must be a dict")
        self._files = files
        self._files = dict(sorted(self._files.items(), key=lambda x: x[1]['modified_time']))
        self._idle_time_mins = idle_time_mins
        self._observers = []

    def add_update(self, file, modified_time, file_info):
        if type(modified_time) is not datetime:
            raise TypeError("modified_time must be a datetime object,"+
                            f" received {str(modified_time)} as {type(modified_time)}")

        if modified_time.date() != datetime.today().date():
            return

        notified_at = modified_time if is_first_time() else datetime.now()

        if not self._files.get(file, None):
            self._log.log('addeded: ', file_info['name'], ' - ', datetime_to_local(modified_time))
            self._files[file] = {
                "modified_time": modified_time,
                "file_info": file_info,
                "notified_at": notified_at
            }
            return

        self._files[file]['modified_time'] = modified_time
        self._files[file]['file_info'] = file_info

    def check_idles(self):
        for file, props in self._files.items():
            notified_at = props.get('notified_at', None)
            modified_time = props['modified_time']

            self._log.log(f"Verificando:   '{file}'")
            self._log.log(f"Modificado em: {datetime_to_local(modified_time)}")
            self._log.log(f"Notificado em: {datetime_to_local(notified_at)}")
            self._log.log(f'Delta - tempo: {(datetime.now() - modified_time).seconds} segundos')
            if (modified_time > notified_at) and (datetime.now() - modified_time).seconds > self._idle_time_mins * 60:
                self._files[file]['notified_at'] = datetime.now()
                self._log.log('Notificando...')
                self.notify(self._files[file])

    def attach(self, observer):
        self._observers.append(observer)

    def detach(self, observer):
        self._observers.remove(observer)

    def notify(self, file):
        for observer in self._observers:
            observer.update(file)


class Listener:
    def update(self, file):
        last_modified_in = datetime.now() - file['modified_time']
        last_modified_in = last_modified_in.seconds
        print(f"{file['file_info']['name']} deixou de ser modificado em "+
            f'{datetime.strftime(file["modified_time"], "%d/%m %H:%M")}'+
            f' ({last_modified_in} segundos atrás)')

class CmdListener:
    def __init__(self, cmd):
        self._cmd = cmd

    def update(self, file):
        if self._cmd:
            os.system(self._cmd)

def get_start_page_token():
    try:
        res = drive_service.changes().getStartPageToken().execute()
    except Exception as e:
        print(f"Erro ao obter token de página inicial: {e}")
        return None
    return res.get('startPageToken')

def get_changes(page_token):
    print(page_token)
    response = drive_service.changes().list(
        pageToken=page_token,
        spaces='drive',
    ).execute()

    for change in response.get('changes'):
        file_id = change.get('fileId')
        file_info = get_file_info(file_id)
        modified_time = datetime.strptime(file_info.get('modifiedTime'), "%Y-%m-%dT%H:%M:%S.%fZ")
        print(f"Arquivo {file_info.get('name')} modificado em {modified_time}")

    return response.get('nextPageToken')

# Função para monitorar mudanças nos arquivos especificados
def watch_specific_files(watcher, listeners, interval=10):

    global drive_service

    for listener in listeners:
        watcher.attach(listener)

    while True:
        for idx, id in enumerate(spreadsheet_ids):
            print('Obtendo arquivos...', end='')
            print(f'{idx+1}/{len(spreadsheet_ids)}' ,  flush=True, end='\r')
            if watcher._files.get(id, None) is None:
                file_info = get_file_info(id)
            else:
                file_info = watcher._files[id]['file_info']
            modified_time = getModifiedTime(drive_service, id)
            modified_time = datetime.strptime(modified_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            modified_time = datetime_to_local(modified_time)
            modified_time = modified_time.replace(tzinfo=None)
            watcher.add_update(id, modified_time, file_info)
        
        watcher.check_idles()
        time.sleep(interval)
        print('', end='\r', flush=True)

def datetime_to_local(dt):
    utc = dt.replace(tzinfo=from_zone)
    return utc.astimezone(to_zone)

def getModifiedTime(drive_service, fileId):
    revisions = []
    pageToken = ""
    while pageToken is not None:
        res = drive_service.revisions().list(
            fileId=fileId, 
            fields="nextPageToken,revisions(modifiedTime)",
            pageSize=1000,
            pageToken=pageToken if pageToken != "" else None
        ).execute()
        r = res.get("revisions", [])
        revisions += r
        pageToken = res.get("nextPageToken")
    return revisions[-1]["modifiedTime"]


# Executar monitoramento
# watcher.py --first
# watcher.py --interval 5
# watcher.py --idle-mins 0.1
# watcher.py --notify-cmd "echo 'Arquivo modificado'"
# watcher.py --connector
def main():
    notify_cmd = None
    if '--notify-cmd' in (sys.argv):
        notify_cmd = sys.argv[sys.argv.index('--notify-cmd')+1]

    watcher = FileWatcher({}, idle_time_mins=1/6)

    listeners = []
    if '--log' in sys.argv:
        listeners.append(Listener())

    if '--connector' in sys.argv or '-c' in sys.argv:
        print('Attaching connector...')
        connector = Monolit()
        listeners.append(connector)

    if notify_cmd:
        listeners.append(CmdListener(notify_cmd))

    print(f'Monitorando {len(spreadsheet_ids)} arquivos...')
    watch_specific_files(watcher, listeners, interval=5)

if __name__ == '__main__':
    main()
