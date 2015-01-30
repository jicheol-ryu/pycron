import importlib
import re
from datetime import datetime


def main():
    current_dttm = datetime.now()
    exec_id = str(int((current_dttm - datetime(1970, 1, 1)).total_seconds()))

    manager_logger = JobLogger(exec_id, 'JOB_MANAGER', 'job.log')
    manager_logger.info('START')

    try:
        job_config_list = read_config()
        for job_config in job_config_list:
            try:
                job_data = create_job_data(job_config)
                if not check_exec_time(job_data, current_dttm):
                    continue
                job_func_name, job_func = find_function(job_data)
                job_args, job_kwargs = parsing_job_args(job_data)
                job_logger = JobLogger(exec_id, 'JOB_LOGGER_'+job_data['module'], job_data['module']+'.log')
            except ValueError as ve:
                manager_logger.error('INCORRECT_CONFIG\t%s\t%s', str(ve), job_config)
                continue

            module_info = '%s\t%s\t%s' % (job_func_name, str(job_args), str(job_kwargs))
            manager_logger.info('EXEC_JOB\t%s', module_info)
            job_logger.info('START\t%s', module_info)
            try:
                job_func(job_logger, *tuple(job_args), **job_kwargs)
            except:
                job_logger.error('', exc_info=True)
                manager_logger.error('ERROR\t%s', module_info)
            finally:
                job_logger.info('FINISH')
    except:
        manager_logger.error('', exc_info=True)
        raise
    finally:
        manager_logger.info('FINISH')
    return


def read_config():
    job_config_list = list()
    config_fo = open('crontab_config')
    try:
        # read all lines
        lines = config_fo.readlines()
        # remove empty and comment lines
        lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith('#')]
        for line in lines:
            job_config_list.append(line)
    finally:
        config_fo.close()
    return job_config_list


def create_job_data(job_config):
    splits = re.split('\\s+', job_config)
    if 6 > len(splits):
        raise ValueError("short line")
    return {'min': splits[0], 'hour': splits[1], 'weekday': splits[2], 'monthday': splits[3],
            'module': splits[4], 'method': splits[5], 'args': splits[6:]}


def check_exec_time(job_data, current_dttm):
    is_exec_time = True
    is_exec_time = is_exec_time and match_time_component(job_data['min'], current_dttm.minute)
    is_exec_time = is_exec_time and match_time_component(job_data['hour'], current_dttm.hour)
    is_exec_time = is_exec_time and match_time_component(job_data['weekday'], current_dttm.day)
    is_exec_time = is_exec_time and match_time_component(job_data['monthday'], current_dttm.isoweekday())
    return is_exec_time


def find_function(job_data):
    module_name = 'jobs.' + job_data['module']
    job_func_name = module_name + '.' + job_data['method']
    try:
        job_module = importlib.import_module(module_name)
        job_func = getattr(job_module, job_data['method'])
    except (ImportError, AttributeError):
        raise ValueError('"No Module: ' + job_func_name)
    return job_func_name, job_func


def parsing_job_args(job_data):
    ## parsing args
    args = list()
    kwargs = dict()
    src_args = job_data['args']
    for src_arg in src_args:
        arg_match = re.match('(.+)=(.*)', src_arg)
        if arg_match:
            kwargs[arg_match.group(1)] = arg_match.group(2)
        else:
            args.append(src_arg)
    return args, kwargs


def match_time_component(expression, time_value):
    # *           match all
    if '*' == expression:
        return True
    # x           if ${time_value} == x
    time_match = re.match('^(\\d+)$', expression)
    if time_match:
        return time_value == int(time_match.group(1))
    # x/y         if ${time_value} mod y == x
    time_match = re.match('^(\\d+)/(\\d+)$', expression)
    if time_match:
        return time_value % int(time_match.group(2)) == int(time_match.group(1))
    # x1,x2,...   if ${time_value} == x1 or ${time_value} == x2 or ...
    time_match = re.findall('(\\d+)', expression)
    if time_match:
        for number in time_match:
            if time_value == int(number):
                return True
        return False
    raise ValueError('unknown expression in config: ' + expression)


class JobLogger:
    __loggers = {}

    def __init__(self, exec_id, logger_name, file_name):
        import logging
        import logging.handlers

        if not exec_id in JobLogger.__loggers:
            JobLogger.__loggers[exec_id] = dict()

        if logger_name in JobLogger.__loggers[exec_id]:
            self.__logger = JobLogger.__loggers[exec_id][logger_name]
            self.__exec_id = exec_id
        else:
            new_logger = logging.getLogger(logger_name)
            new_logger.setLevel(logging.DEBUG)
            handler = logging.handlers.TimedRotatingFileHandler(filename='logs/'+file_name, when='midnight')
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
            handler.setFormatter(formatter)
            new_logger.addHandler(handler)
            JobLogger.__loggers[exec_id][logger_name] = new_logger
            self.__logger = new_logger
            self.__exec_id = exec_id

    def info(self, msg, *args, **kwargs):
        args = ('%s\t'+msg, self.__exec_id, ) + args
        self.__logger.info(*args, **kwargs)

    def error(self, msg, *args, **kwargs):
        args = ('%s\t'+msg, self.__exec_id, ) + args
        self.__logger.error(*args, **kwargs)


if __name__ == '__main__':
    main()