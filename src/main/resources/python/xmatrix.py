""" Xmatrix Python SDK """

import os
import io
import uuid
import pickle
import shutil
from multiprocessing import Process
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.serializers import CloudPickleSerializer, AutoBatchedSerializer, \
    PickleSerializer, NoOpSerializer
from sklearn.model_selection import train_test_split
from kafka import KafkaConsumer
from multiprocessing.managers import BaseManager
from multiprocessing import JoinableQueue

basestring = unicode = str


def pickle_dump_file(value, fp):
    """ pickle dump files to path """
    try:
        pickle.dump(value, fp, protocol=2)
    except pickle.PickleError:
        raise
    except Exception as error:
        raise Exception("Could not serialize broadcast" % error)
    fp.close()
    return fp.name


def pickle_load_file(file_path):
    """ pickle load file with gc"""
    try:
        with open(file_path, "rb", 1 << 20) as fp:
            # gc.disable()
            try:
                return pickle.load(fp)
            finally:
                pass
                # gc.enable()
    except Exception:
        return []


def get_ml_params(pickle_name="python_temp.pickle"):
    """ pickle load get params """
    pickle_path = os.path.join(os.getcwd(), pickle_name)
    if os.path.exists(pickle_path):
        if "ml_params" not in globals():
            _params = pickle_load_file(os.path.join(os.getcwd(), pickle_name))
            _param_list = [
                "kafkaParam", "fitParam", "internalSystemParam", "systemParam"
            ]
            globals()["ml_params"] = [
                _params.get(i, dict()) for i in _param_list
            ]
        return globals()["ml_params"]
    else:
        default_param = [{},
                         {
                             "batchSize": "1000",
                             "labelSize": "2",
                             "inputCol": "features",
                             "label": "label",
                             "consumeThreads": "2"
                         }, {
                             "tempModelLocalPath": "temp_model_dir"
                         }, {
                             "funcPath": "udf_func.pickle"
                         }]
        return default_param


class MsgQueue(BaseManager):
    pass


def queue_start(authkey, queues, queue_max_size=2048, mode="local"):
    """ create new multiprocessing.manager or return existing one """
    qdict, kdict = dict(), dict()
    for q in queues:
        qdict[q] = JoinableQueue(queue_max_size)
    MsgQueue.register('get_queue', callable=lambda qname: qdict[qname])
    MsgQueue.register('get', callable=lambda key: kdict[key])
    MsgQueue.register(
        'set', callable=lambda key, value: kdict.setdefault(key, value))
    if mode == 'remote':
        mgr = MsgQueue(address=('', 0), authkey=authkey)
    else:
        mgr = MsgQueue(authkey=authkey)
    mgr.start()
    return mgr


def read_kafka_data():
    """ read train data from kafka """
    kafka_param, _, internal_system_param, system_param = get_ml_params()

    mgr = queue_start(
        authkey=uuid.uuid4().bytes, queue_max_size=10, queues=['input'])

    def from_data(args, mgr):
        """ """
        consumer = KafkaConsumer(
            kafka_param["topic"],
            group_id=kafka_param["group_id"],
            bootstrap_servers=kafka_param["bootstrap.servers"],
            auto_offset_reset="earliest",
            enable_auto_commit=False)
        try:
            stop_count, fail_msg_count, no_message_count = 0, 0, 0
            while 1:
                messages = consumer.poll(
                    timeout_ms=1000, max_records=args["max_records"])
                queue = mgr.get_queue("input")
                group_msgs_count, group_msgs = 0, []
                for tp, records in messages.items():
                    for record in records:
                        try:
                            with io.BytesIO(record.value) as f:
                                msg_value = pickle.load(f)
                            if msg_value == "_stop_":
                                stop_count += 1
                            else:
                                group_msgs.append(msg_value)
                                group_msgs_count += 1
                        except Exception:
                            fail_msg_count += 1
                if len(group_msgs) > 0:
                    no_message_count = 0
                    queue.put(group_msgs, block=True)
                if len(group_msgs) == 0 and no_message_count < 10:
                    no_message_count += 1
                if (stop_count >= internal_system_param["stopFlagNum"] and
                    group_msgs_count == 0) or (no_message_count >= 10 and
                                               group_msgs_count == 0):
                    queue.put(["_stop_"], block=True)
                    break
        finally:
            consumer.close()

    def _read_data(max_records=64, consume_threads=1):
        """  """

        def process_produce(consume_threads=1):
            """ kafka produce with process """
            for i in range(consume_threads):
                Process(
                    target=from_data, args=({
                                                "max_records": max_records
                                            }, mgr)).start()

        def thread_produce(consume_threads=1):
            """ kafka produce with thread """
            from threading import Thread
            for i in range(consume_threads):
                Thread(
                    target=from_data, args=({
                                                "max_records": max_records
                                            }, mgr)).start()

        if "useThread" in system_param:
            thread_produce(consume_threads=consume_threads)
        else:
            process_produce(consume_threads=consume_threads)

        queue = mgr.get_queue("input")
        leave_msg_group, total_wait_count = [], 0

        while True:
            msg_group, count, wait_count, should_break = [], 0, 0, False

            while count < max_records:
                if queue.empty():
                    wait_count += 1
                    total_wait_count += 1
                items = queue.get(block=True)
                if items[-1] == "_stop_":
                    should_break = True
                    break
                items = items + leave_msg_group
                leave_msg_group = []
                items_size = len(items)

                if items_size == max_records:
                    msg_group = items
                    break
                if items_size > max_records:
                    msg_group = items[0:max_records]
                    leave_msg_group = items[max_records:items_size]
                    break
                if items_size < max_records:
                    leave_msg_group = leave_msg_group + items
                count += 1

            if len(leave_msg_group) > 0:
                msg_group = leave_msg_group

            yield msg_group
            if should_break:
                break

        queue.task_done()

    return _read_data


def ml_configure_params(clf):
    """ configure sklearn instance params """
    _, fit_params, *_ = get_ml_params()

    def t(v, convert_v):
        if type(v) == float:
            return float(convert_v)
        elif type(v) == int:
            return int(convert_v)
        elif type(v) == list and len(v) > 0:
            if type(v[0]) == int:
                return [int(i) for i in v]
            if type(v[0]) == float:
                return [float(i) for i in v]
        else:
            return convert_v

    for name in clf.get_params():
        if name in fit_params:
            dv = clf.get_params()[name]
            setattr(clf, name, t(dv, fit_params[name]))


def ml_validate_data(validate_name="validate_table.pickle"):
    """ get validate data """
    X, y = [], []
    _, fit_params, *_ = get_ml_params()
    raw_data = pickle_load_file(os.path.join(os.getcwd(), validate_name))
    validate_data = [pickle.load(io.BytesIO(i)) for i in raw_data]
    x_name = fit_params.get("inputCol", "features")
    y_name = fit_params.get("label", "label")
    for item in validate_data:
        X.append(item[x_name].toArray())
        y.append(item[y_name])
    return X, y


def generate_local_validate_data(X_test, y_test,
                                 validate_name="validate_table.pickle"):
    validate_list = []
    for k, v in enumerate(y_test):
        validate_list.append(pickle.dumps(dict_to_row({"label": v, "features": list_to_vect(X_test[k])})))
    with open(os.path.join(os.getcwd(), validate_name), "wb") as fp:
        pickle_dump_file(validate_list, fp)


def ml_batch_data(fn, data_func=None):
    """ """
    kafka_params, fit_params, *_ = get_ml_params()
    batch_size = int(fit_params.get("batchSize", 1000))
    label_size = int(fit_params.get("labelSize", -1))
    consume_threads = int(fit_params.get("consumeThreads", "1"))
    x_name = fit_params.get("inputCol", "features")
    y_name = fit_params.get("label", "label")
    if kafka_params:
        rd = read_kafka_data()
        for items in rd(
                max_records=batch_size, consume_threads=consume_threads):
            if len(items) == 0:
                continue
            X = [item[x_name].toArray() for item in items]
            y = [item[y_name] for item in items]
            fn(X, y, label_size)
    else:
        local_dataset = data_func()
        X = [item[x_name].toArray() for item in local_dataset]
        y = [item[y_name] for item in local_dataset]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.1, random_state=42)
        generate_local_validate_data(X_test, y_test)
        fn(X_train, y_train, label_size)


def ml_save_model(model, model_name="model.pickle"):
    """  """
    _, _, inertnal_system_param, _ = get_ml_params()
    temp_model_path = inertnal_system_param.get("tempModelLocalPath", "/tmp/")
    if os.path.exists(temp_model_path):
        shutil.rmtree(temp_model_path)
    os.makedirs(temp_model_path)
    with open(os.path.join(temp_model_path, model_name), "wb") as fp:
        pickle_dump_file(model, fp)


def wrap_function(func, profiler=None):
    """  """

    def pickle_command(command):
        return CloudPickleSerializer().dumps(command)

    ser = AutoBatchedSerializer(PickleSerializer())
    command = (func, profiler, NoOpSerializer(), ser)
    return bytearray(pickle_command(command))


def udf(func):
    """  """
    *_, system_param = get_ml_params()
    func_path = system_param["funcPath"]
    with open(func_path, "wb") as fp:
        fp.write(wrap_function(func))


def dict_to_row(rd):
    return Row(**rd)


def list_to_vect(l):
    return Vectors.dense(l)