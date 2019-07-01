from sqlalchemy import create_engine
import yaml
from timeit import default_timer
import os
import logging.config
import yaml
import traceback
from pathlib import Path

log_path = Path(__file__).parent / 'logging.yaml'
logging.config.dictConfig(yaml.load(open(log_path)))
    
def connect_sqlalchemy():
    """
    Create SQLalchemy connection with PostgreSQL database
    :return: SQLalchemy connection
    """
    
    path = os.path.dirname(os.path.abspath(__file__))
    server = yaml.load(open(path + '/config.yaml', 'r'))['servers'][229]

    url = 'postgresql://{}:{}@{}/{}'.format(server['user'], 
                                            server['password'],
                                            server['host'], 
                                            server['database'])
    return create_engine(url)

def log_level():

    translate = {10: 'DEBUG', 20: 'INFO'}
    return translate[logging.getLogger().getEffectiveLevel()]

class Logger(object):
    def __init__(self, message='', level='DEBUG'):
        self.stack = traceback.extract_stack(None, 2)
        self.log = '{file}:{line} {func}(): '.format(file=self.stack[0][0],
                                                     line=self.stack[0][1],
                                                     func=self.stack[0][2])
        self.message = self.log + message
        self.timer = default_timer
        self.level = level

    def print_message(self):

        if self.level == 'DEBUG':
            logging.debug(self.message)
        elif self.level == 'INFO':
            logging.info(self.message)

    def __enter__(self):
        self.print_message()
        self.start = self.timer()
        return self
        
    def __exit__(self, *args):
        end = self.timer()
        self.elapsed_secs = end - self.start
        self.elapsed = self.elapsed_secs  # millisecs
        self.message = self.message + ' in {0:.2f} s'.format(self.elapsed)
        self.print_message()


