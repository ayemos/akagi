import sys
import os
import logging

logger = logging.getLogger('akagi')
logger.setLevel(logging.INFO)


sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)

log_file_path = os.path.join('/', 'tmp', 'akagi.log')

fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.DEBUG)  # log all

logger.addHandler(sh)
logger.addHandler(fh)
logger.debug("Logged to %(log_file_path)s" % locals())
