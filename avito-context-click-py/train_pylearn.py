import argparse
import gc
import os

import numpy as np
import csv
import gzip

from pylearn2.datasets.dense_design_matrix import DenseDesignMatrix
from pylearn2.utils.string_utils import preprocess
from pylearn2.config import yaml_parse
from pylearn2.utils import serial


class CSVDataset(DenseDesignMatrix):
    def __init__(self, path, n_labels=2, start=None, stop=None, del_raw=True, x_only=False):
        self.del_raw = del_raw
        path = preprocess(path)

        x, y = CSVDataset._load_data(path, del_raw=del_raw)
        if np.isnan(np.min(y)):
            y = None
        else:
            y = y.astype(int).reshape(-1, 1)

        if start is not None:
            if stop is None:
                stop = x.shape[0]
            assert start >= 0
            assert start < stop
            if not (stop <= x.shape[0]):
                raise ValueError("stop must be less than the # of examples but " +
                                 "stop is " + str(stop) + " and there are " + str(x.shape[0]) +
                                 " examples.")
            x = x[start:stop, :]
            if y is not None:
                y = y[start:stop, :]

        if x_only:
            y = None
            n_labels = None

        super(CSVDataset, self).__init__(X=x, y=y, y_labels=n_labels)

    @staticmethod
    def _open_path(path):

        if not os.path.exists(path) and os.path.exists(path + '.gz'):
            path += '.gz'

        if path.endswith('.gz'):
            return gzip.open(path, mode='rt')
        else:
            return open(path, mode='rt')

    @staticmethod
    def _load_data(path, del_raw):

        npy_path = path + '.npz'

        if os.path.exists(npy_path):
            if not os.path.exists(path) or os.path.getmtime(npy_path) > os.path.getmtime(path):
                data = np.load(npy_path)
                return data['x'], data['y']

        # Convert the .csv file to numpy
        y_list = []
        x_list = []
        with CSVDataset._open_path(path) as csv_file:

            invalid_y = {'', 'na', "nan", 'NA', 'NaN'}

            is_header = True
            for row in csv.reader(csv_file):
                if is_header:
                    is_header = False
                else:
                    y_list.append(float(row[0]) if row[0] not in invalid_y else np.nan)
                    x_list.append(list(map(float, row[1:])))

            x = np.array(x_list, dtype=np.float32)
            y = np.array(y_list, dtype=np.float32)

        np.savez_compressed(npy_path, x=x, y=y)
        if del_raw:
            os.remove(path)
        return x, y


def train(config, config_args):

    # Load config replacing tags
    with open(config, 'r') as f:
        config = ''.join(f.readlines())
    for nam in config_args:
        config = config.replace('${' + nam + "}", config_args[nam])

    train_obj = yaml_parse.load(config)

    try:
        iter(train_obj)
        iterable = True
    except TypeError:
        iterable = False

    # # Undo our custom logging setup.
    # restore_defaults()
    # root_logger = logging.getLogger()
    # formatter = CustomFormatter(prefix='%(asctime)s ', only_from='pylearn2')
    # handler = CustomStreamHandler(formatter=formatter)
    # root_logger.addHandler(handler)
    # root_logger.setLevel(logging.INFO)

    if iterable:
        for number, subobj in enumerate(iter(train_obj)):
            # Execute this training phase.
            subobj.main_loop()
            del subobj
            gc.collect()
    else:
        train_obj.main_loop()


def add_config_args(cfg_args, cfg_dict=None):
    if cfg_dict is None:
        cfg_dict = {}
    cfg_args = vars(cfg_args)
    for nam in cfg_args:
        val = str(cfg_args[nam])
        cfg_dict[nam] = val
        cfg_dict[nam.lower()] = val
        cfg_dict[nam.upper()] = val
    return cfg_dict


def predict(model_file, test_data_file, test_pred_file):

    model = serial.load(model_file)
    dataset = CSVDataset(path=test_data_file, x_only=True)

    # use smallish batches to avoid running out of memory
    batch_size = 100
    model.set_batch_size(batch_size)
    # dataset must be multiple of batch size of some batches will have
    # different sizes. theano convolution requires a hard-coded batch size
    n_row = dataset.X.shape[0]
    extra = batch_size - n_row % batch_size
    assert (n_row + extra) % batch_size == 0
    if extra > 0:
        dataset.X = np.concatenate((dataset.X, np.zeros((extra, dataset.X.shape[1]),
                                                        dtype=dataset.X.dtype)), axis=0)
    assert dataset.X.shape[0] % batch_size == 0

    x_theano = model.get_input_space().make_batch_theano()
    y_theano = model.fprop(x_theano)

    # from theano import tensor as T
    from theano import function
    f = function([x_theano], y_theano)

    y = []
    for i in range(int(dataset.X.shape[0] / batch_size)):
        x_arg = dataset.X[i*batch_size:(i+1)*batch_size, :]
        if x_theano.ndim > 2:
            x_arg = dataset.get_topological_view(x_arg)
        y.append(f(x_arg.astype(x_theano.dtype)))

    y = np.concatenate(y)

    y = y[:n_row, :]

    # wirtes prediction to output
    n_col = y.shape[1]
    with open(test_pred_file, 'w') as out:
        for r in range(n_row):
            for c in range(n_col):
                if n_col == 2 and c == 0:
                    continue
                if c > 0 and n_col > 2:
                    out.write(',')
                out.write('%f' % (y[r, c]))

            out.write('\n')


if __name__ == "__main__":

    # -train_config /home/lucas/ml-r-tb/contest/avito-context-click/data/template/zens_nn.yaml
    # -model_file /home/lucas/ml-r-tb/contest/avito-context-click/data/output-py/zens_nn/data.1.model.pkl
    # -train_data_file /home/lucas/ml-r-tb/contest/avito-context-click/data/output-py/zens_nn/data.1.tr.csv
    # -val_data_file /home/lucas/ml-r-tb/contest/avito-context-click/data/output-py/zens_nn/data.1.val.csv
    # -test_data_file /home/lucas/ml-r-tb/contest/avito-context-click/data/output-py/zens_nn/data.1.test.csv
    # -test_pred_file /home/lucas/ml-r-tb/contest/avito-context-click/data/output-py/zens_nn/data.1.test.pred
    # -n_features 5

    parser = argparse.ArgumentParser(
        description="Launch an training from a YAML configuration file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-train_config',
                        type=str,
                        default=None,
                        help='A YAML configuration file specifying the training procedure')

    parser.add_argument('-model_file',
                        type=str,
                        default=None,
                        help='File with model path used to predict')

    parser.add_argument('-test_data_file',
                        type=str,
                        default=None,
                        help='File data to predict')

    parser.add_argument('-test_pred_file',
                        type=str,
                        default=None,
                        help='File to output predictions to')

    args, extra_args = parser.parse_known_args()
    for arg in extra_args:
        if arg.startswith("-"):
            parser.add_argument(arg, type=str)

    args = parser.parse_args()

    # if args.train_config is not None:
    #     config_dict = add_config_args(cfg_args=args)
    #     train(config=args.train_config, config_args=config_dict)

    if args.test_data_file is not None and args.model_file is not None and args.test_pred_file is not None:
        predict(model_file=args.model_file, test_data_file=args.test_data_file, test_pred_file=args.test_pred_file)
