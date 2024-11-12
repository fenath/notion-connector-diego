import datetime as dt

class Logger:
    def __init__(self):
        self._log = []

    def log(self, *args):
        msg = ' '.join([str(arg) for arg in args])
        msg_date = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        msg = '[LOG] ' + f'({msg_date})' + ' ' + msg
        self._log.append(msg)

    def get_log(self):
        return self._log

    def get_last_log(self):
        return self._log[-1]


class FileLogger:
    def __init__(self):
        self._log = Logger()
        self._filename = 'log.txt'

    def log(self, *args):
        self._log.log(*args)
        with open(self._filename, 'a', encoding='utf-8') as f:
            f.write(self._log.get_last_log()+'\n')
