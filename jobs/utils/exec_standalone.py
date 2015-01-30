def do(func, ntime=1, *args, **kwargs):
    import logging
    logger = logging.getLogger('main_console')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s'))
    logger.addHandler(handler)

    import time
    for n in range(0, ntime):
        logger.info('PROCESS %d', n)
        func(logger, *args, **kwargs)
        time.sleep(1)