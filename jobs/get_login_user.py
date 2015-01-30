def do_job(logger, from_serial=None, limit=1000):
    from utils.database import DataBase, CodeBook
    test_db = DataBase(DataBase.Configs.TEST_DB)
    try:
        logger.info('LAST_SERIAL\t%d', last_serial)
        # somethings
    finally:
        test_db.close()
    return


if __name__ == '__main__':
    from utils import exec_standalone
    exec_standalone.do(do_job, limit=1)