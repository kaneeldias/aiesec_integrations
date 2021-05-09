import logging

logging.basicConfig(filename='log.log', format='%(asctime)s %(name)s - %(message)s', level=logging.INFO)


def get(name):
    return logging.getLogger(name)