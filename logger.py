
class Logger:
    def __init__(self):
        self._log = []

    def log(self, *args):
        self._log.append(' '.join([str(arg) for arg in args]))

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
