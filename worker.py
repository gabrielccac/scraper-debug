import time
import logging
import threading
import os
import sys
import signal
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

# Site-specific configuration
SITE_NAME = 'dfimoveis'
REDIS_HOST = '5.161.248.214'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redispass'

# --- Configuration ---

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(threadName)s] - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
for l in ["seleniumbase", "selenium", "pika"]: logging.getLogger(l).setLevel(logging.WARNING)





