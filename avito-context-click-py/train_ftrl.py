"""
           DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
                   Version 2, December 2004

Copyright (C) 2004 Sam Hocevar <sam@hocevar.net>

Everyone is permitted to copy and distribute verbatim or modified
copies of this license document, and changing it is allowed as long
as the name is changed.

           DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
  TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

 0. You just DO WHAT THE FUCK YOU WANT TO.
"""

from datetime import datetime
from csv import DictReader
from math import exp, log, sqrt, copysign
import os
import gzip
import argparse
import warnings
import random
import pickle as pkl
import inspect


class DataObject(object):

    def __props__(self):
        attribs = inspect.getmembers(self, lambda attr: not(inspect.isroutine(attr)))
        return [a for a in attribs if not(a[0].startswith('__') and a[0].endswith('__'))]

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.__props__()))


class FTRLProximal(object):
    """ Our main algorithm: Follow the regularized leader - proximal

        In short,
        this is an adaptive-learning-rate sparse logistic-regression with
        efficient L1-L2-regularization

        Reference:
        http://www.eecs.tufts.edu/~dsculley/papers/ad-click-prediction.pdf
    """

    class StatusData(DataObject):

        def __init__(self):
            self.start = datetime.now()
            self.loss = 0.
            self.count = 0
            self.loss_check = 0.
            self.count_check = 0
            self.total_count = 0

    class InstanceData(DataObject):

        def __init__(self, x, y, row):
            self.x = x
            self.y = y
            self.row = row

    def __init__(self, alpha, beta, l1, l2, bits, two_way, dropout, seed, col_out,
                 col_in_cat, test_pred_col, test_pred_extra_cols, no_pred_suffix):
        # parameters
        self.alpha = alpha
        self.beta = beta
        self.l1 = l1
        self.l2 = l2

        # feature related parameters
        self.two_way = two_way
        self.dropout = max(min(dropout, 1.), 0.)
        self.rnd = random.Random()
        self.rnd.seed(seed)
        self.seed = seed

        self.bits = bits
        self.key_cache = {}
        self.d = 2 ** bits
        self.n = [0.] * self.d
        self.z = [0.] * self.d
        
        self.epoch = 1
        self.col_out = col_out
        self.col_in_cat = col_in_cat
        self.test_pred_col = test_pred_col
        self.test_pred_extra_cols = test_pred_extra_cols
        self.no_pred_suffix = no_pred_suffix

    def _calc_col_index(self, val_id):
        index_offset = 1
        return (abs(hash(val_id)) % (self.d-index_offset)) + index_offset

    def _open_pred_path(self, outpath):
        return open(outpath + ('' if self.no_pred_suffix else "." + str(self.epoch)), 'w')

    @staticmethod
    def _get_prob(wtx):
        return 1. / (1. + exp(-max(min(wtx, 35.), -35.)))
      
    def _get_w(self, i):
        sign = copysign(1, self.z[i])
        if sign * self.z[i] <= self.l1:
            return 0.
        else:
            return (sign * self.l1 - self.z[i]) / ((self.beta + sqrt(self.n[i])) / self.alpha + self.l2)

    def _predict(self, x):
        """ Get probability estimation on x

            INPUT:
                      x: features
                dropped: if the weight was dropped
            OUTPUT:
                probability of p(y = 1 | x; w)
        """
        wtx = sum([self._get_w(i) for i in x])
        return FTRLProximal._get_prob(wtx)

    def _update(self, x, y):
        """ Update model using x, y

            INPUT:
                x: feature, a list of indices
                y: answer

            MODIFIES:
                self.n: increase by squared gradient
                self.z: weights
        """

        w = [0.]*(len(x)+1)
        wtx = 0.
        for j, i in enumerate(x):
            if self.dropout > 0. and self.rnd.random() < self.dropout:
                w[j] = None
            else:
                w[j] = self._get_w(i)
                wtx += w[j]
        # wtx /= (1.-self.dropout)

        p = FTRLProximal._get_prob(wtx)
        g = p - y

        # update z and n
        for j, i in enumerate(x):
            # implement dropout as overfitting prevention
            if w[j] is None:
                continue

            sigma = (sqrt(self.n[i] + g * g) - sqrt(self.n[i])) / self.alpha
            self.z[i] += g - sigma * w[j]
            self.n[i] += g * g

    @staticmethod
    def _open_path(path):
        
        if not os.path.exists(path) and os.path.exists(path + '.gz'):
            path += '.gz'
        print('\nOpening %s to read.' % path)

        # noinspection PyUnresolvedReferences
        if path.endswith('.gz'):
            return gzip.open(path, mode='rt')
        else:
            return open(path, mode='rt')

    def _data(self, path):

        for row in DictReader(FTRLProximal._open_path(path)):

            data = FTRLProximal.InstanceData(x=[0] * (len(self.col_in_cat) + len(self.two_way) + 1), y=None, row=row)
            if self.col_out in row:
                row_val = row[self.col_out]
                data.y = float(row_val) if row_val == '0' or row_val == '1' else None

            # bias is 0
            ix = 1

            # one-hot encode features
            for col_nam in self.col_in_cat:
                data.x[ix] = self._calc_col_index(str(ix+self.seed) + "_" + row[col_nam])
                ix += 1
            # one-hot encode two way features
            for cols_two_way in self.two_way:
                data.x[ix] = self._calc_col_index(str(ix+self.seed) + "_" +
                                                  row[cols_two_way[0]] + "_" + row[cols_two_way[1]])
                ix += 1
            yield data

    @staticmethod
    def log_loss(pred, actual):
        """ FUNCTION: Bounded logloss

            INPUT:
                p: our prediction
                y: real answer

            OUTPUT:
                logarithmic loss of p given y
        """

        pred = max(min(pred, 1. - 10e-15), 10e-15)
        return -log(pred) if actual == 1. else -log(1. - pred)

    def fit(self, path):

        self._on_start_fit(path=path)

        print('\n\n\nTraining started...')
        fit_status = FTRLProximal.StatusData()

        for data in self._data(path):
            self._fit_instance(fit_status=fit_status, data=data)

        self._on_end_fit(fit_status=fit_status)

    def _on_start_fit(self, path):
        pass

    def _fit_instance(self, fit_status, data):

        fit_status.total_count += 1
        if fit_status.total_count % 20 == 0:
            p = self._predict(data.x)
            cur_loss = FTRLProximal.log_loss(p, data.y)
            fit_status.loss += cur_loss
            fit_status.loss_check += cur_loss
            fit_status.count += 1
            fit_status.count_check += 1

        self._update(data.x, data.y)

        if fit_status.total_count % 10000000 == 0:
            print('Epoch %d (%d instances), train logloss: %f (since last %f - %d samples), elapsed time: %s' % (
                self.epoch, fit_status.total_count, fit_status.loss / fit_status.count,
                fit_status.loss_check / fit_status.count_check, fit_status.count_check,
                str(datetime.now() - fit_status.start)))
            fit_status.loss_check = 0.
            fit_status.count_check = 0

    def _on_end_fit(self, fit_status):
        print('Epoch %d finished (%d total instances), train logloss: %f (since last %f - %d samples),'
              ' elapsed time: %s' % (
                  self.epoch, fit_status.total_count, fit_status.loss / fit_status.count,
                  fit_status.loss_check / fit_status.count_check, fit_status.count_check,
                  str(datetime.now() - fit_status.start)))
        fit_status.loss_check = 0.
        fit_status.count_check = 0
        self.epoch += 1

    def pred_proba(self, path):
        print('\n\n\nPredicting started...')
        pred_status = FTRLProximal.StatusData()
        pred_lst = []

        for data in self._data(path):
            pred = self._predict(data.x)
            pred_lst.append(pred)
            self._update_pred_status(data=data, pred=pred, pred_status=pred_status)

        self._on_end_pred_proba(pred_status=pred_status)
        return pred_lst

    def save_pred_proba(self, inpath, outpath):

        print('\n\n\nSaving prediction started...')
        pred_status = FTRLProximal.StatusData()

        with self._open_pred_path(outpath) as out_file:
            self._write_pred_headers(out_file)
            for data in self._data(inpath):
                self._write_instance_prediction(pred_status=pred_status, out_file=out_file, data=data)

        self._on_end_pred_proba(pred_status=pred_status)

    def _write_pred_headers(self, out_file):
        out_file.write('%s\n' % ','.join(self.test_pred_extra_cols + [self.test_pred_col]))

    def _write_instance_prediction(self, pred_status, out_file, data):
        pred = self._predict(data.x)
        values = [data.row[col_nam] for col_nam in self.test_pred_extra_cols] + [str(pred)]
        out_file.write('%s\n' % ','.join(values))
        self._update_pred_status(data=data, pred=pred, pred_status=pred_status)

    # noinspection PyMethodMayBeStatic
    def _update_pred_status(self, data, pred, pred_status):
        if data.y is not None:
            pred_status.loss += FTRLProximal.log_loss(pred, data.y)
            pred_status.count += 1
        pred_status.total_count += 1

    # noinspection PyMethodMayBeStatic
    def _on_end_pred_proba(self, pred_status):
        if pred_status.count > 0:
            print('Prediction logloss: %f (%d instances)' %
                  (pred_status.loss / pred_status.count, pred_status.count))
        print('Prediction saving time (%d instances): %s' %
              (pred_status.total_count, str(datetime.now() - pred_status.start)))

    def fit_and_save_pred_proba(self, inpath, outpath, train_is_test_col):

        self._on_start_fit(path=inpath)

        print('\n\n\nTraining and predicting started...')
        fit_status = FTRLProximal.StatusData()
        pred_status = FTRLProximal.StatusData()

        with self._open_pred_path(outpath) as out_file:
            self._write_pred_headers(out_file)
            for data in self._data(inpath):
                if data.row[train_is_test_col] == '1':
                    self._write_instance_prediction(pred_status=pred_status, out_file=out_file, data=data)
                else:
                    self._fit_instance(fit_status=fit_status, data=data)

        self._on_end_fit(fit_status=fit_status)
        self._on_end_pred_proba(pred_status=pred_status)

    @staticmethod
    def load_model(model_file):
        start = datetime.now()
        print('\n\nLoading model from \'%s\'' % model_file)
        with gzip.open(model_file, 'rb') as model_stream:
            model = pkl.load(model_stream)
        print('Loading model time: %s' % (str(datetime.now() - start)))
        return model

    @staticmethod
    def save_model(model, model_file):
        start = datetime.now()
        print('\n\nSaving model to \'%s\'' % model_file)
        with gzip.open(model_file, 'wb') as model_stream:
            pkl.dump(model, model_stream, pkl.HIGHEST_PROTOCOL)
        print('Saving model time: %s' % (str(datetime.now() - start)))


def train_and_predict(train_file, train_is_test_col, test_file, test_pred_file, train_model_file, epochs, load_model,
                      alpha, beta, l1, l2, bits, two_way, dropout, seed,
                      col_out, col_in_cat, test_pred_col, test_pred_extra_cols,
                      no_pred_suffix):

    start = datetime.now()

    if load_model and os.path.exists(train_model_file):
        learner = FTRLProximal.load_model(train_model_file)
    else:
        learner = FTRLProximal(alpha=alpha, beta=beta, l1=l1, l2=l2, bits=bits, two_way=two_way,
                               dropout=dropout, seed=seed,
                               col_out=col_out, col_in_cat=col_in_cat,
                               test_pred_col=test_pred_col,
                               test_pred_extra_cols=test_pred_extra_cols, no_pred_suffix=no_pred_suffix)

    # training
    if train_file is not None:
        for e in range(epochs):
            if train_is_test_col is None:
                learner.fit(path=train_file)
            else:
                learner.fit_and_save_pred_proba(inpath=train_file, outpath=test_pred_file,
                                                train_is_test_col=train_is_test_col)

    # predicting
    if test_file is not None:
        learner.save_pred_proba(inpath=test_file, outpath=test_pred_file)

    if train_model_file is not None:
        FTRLProximal.save_model(learner, train_model_file)

    print('Total elapsed time: %s' % (str(datetime.now() - start)))


def arg_to_list(arg_map, name, sort=True):
    if not isinstance(arg_map[name], list):
        arg_map[name] = [arg_map[name]]
    if sort:
        arg_map[name] = sorted(arg_map[name])


def arg_build_2way(arg_map):

    two_way_arg = arg_map['two_way']
    input_cols = sorted(arg_map['col_in_cat'])

    two_way_lst = []
    two_way_added = set()

    l = len(input_cols)
    for two_way in two_way_arg:
        two_way = sorted(two_way.split(' '))
        for i in range(l):
            for j in range(i + 1, l):
                if input_cols[i].startswith(two_way[0]) and input_cols[j].startswith(two_way[1]):
                    input_key = input_cols[i] + '\t' + input_cols[j]
                    if input_key not in two_way_added:
                        two_way_added.add(input_key)
                        two_way_lst.append([input_cols[i], input_cols[j]])
    arg_map['two_way'] = two_way_lst


def arg_replace(arg_map, name, old, new):
    if arg_map[name] is not None:
        arg_map[name] = arg_map[name].replace(old, new)

if __name__ == "__main__":

    # -train_file /home/lucas/ml-r-tb/contest/avito-context-click/data/output-py/ftrl_05/data.val.all.csv
    # -train_is_test_col IsTestRow
    # -test_pred_file /home/lucas/ml-r-tb/contest/avito-context-click/data/output-py/ftrl_05/data.val.all.tmp.pred
    # -col_out IsClick
    # -col_in_cat AdID UserID Position
    # -col_query_id SearchID
    # -min_freq 1

    parser = argparse.ArgumentParser(description='Train and predict using FTRL proximal algorithm',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-train_file',
                        default=None,
                        type=str,
                        help='CSV with training data')

    parser.add_argument('-train_model_file',
                        default=None,
                        type=str,
                        help='Path to save the trained model to be used later to resume training')

    parser.add_argument('-train_is_test_col',
                        default=None,
                        type=str,
                        help='If given its assumed that test instances are written in TRAIN_FILE with TRAIN_TEST_COL'
                             ' set to 1 if its a test instance; those test instances will be predicted on the fly in'
                             ' such cases')

    parser.add_argument('-test_file',
                        default=None,
                        type=str,
                        help='CSV with testing data')

    parser.add_argument('-test_pred_file',
                        default=None,
                        type=str,
                        help='Path to save predictions')

    parser.add_argument('-test_pred_col',
                        default='Pred',
                        type=str,
                        help='Name of the prediction column that will be writen to TEST_PRED_FILE')

    parser.add_argument('-test_pred_extra_cols',
                        default=[],
                        type=str,
                        nargs='+',
                        help='Extra columns that will be copied from TEST_FILE to TEST_PRED_FILE')

    parser.add_argument('-no_pred_suffix',
                        default=False,
                        action='store_true',
                        help="Removes epoch suffix from prediction file")

    parser.add_argument('-load_model',
                        default=False,
                        action='store_true',
                        help="Loads saved model to resume training if exists")

    parser.add_argument('-col_out',
                        required=True,
                        type=str,
                        help='Name of the output column')

    parser.add_argument('-col_in_cat',
                        required=True,
                        type=str,
                        nargs='+',
                        help='List of the names of the categorical input columns')

    parser.add_argument('-two_way',
                        default=[],
                        type=str,
                        nargs='+',
                        help='Two way features list in format \'F1 F2\', so all fields starting with \'F1\' '
                             'will be combined with all fields starting with \'F2\'')

    parser.add_argument('-alpha',
                        default=.1,
                        type=float,
                        help='Learning rate')

    parser.add_argument('-beta',
                        default=1.,
                        type=float,
                        help='Smoothing parameter for adaptive learning rate')

    parser.add_argument('-l1',
                        default=1.,
                        type=float,
                        help='L1 regularization, larger value means more regularized')

    parser.add_argument('-l2',
                        default=1.,
                        type=float,
                        help='L2 regularization, larger value means more regularized')

    parser.add_argument('-bits',
                        default=24,
                        type=int,
                        help='Bits to use with the hashing trick to define weights'
                             ' (a -1 value will make it take longer but it will'
                             ' make sure there will be no collisions)')

    parser.add_argument('-dropout',
                        default=0.,
                        type=float,
                        help='Percentage of weights to dropout at each update')

    parser.add_argument('-seed',
                        default=1,
                        type=int,
                        help='Seed to use for random operations (like dropout) and hash offset'
                             ' so changing the seed will also change the hash colisions')

    parser.add_argument('-epochs',
                        default=1,
                        type=int,
                        help='Learn training data for N passes')

    args = vars(parser.parse_args())

    arg_to_list(arg_map=args, name='col_in_cat')
    arg_to_list(arg_map=args, name='two_way')
    arg_to_list(arg_map=args, name='test_pred_extra_cols')

    arg_build_2way(arg_map=args)

    arg_replace(arg_map=args, name='train_model_file', old='{TRAIN_FILE}', new=args['train_file'])

    print('\n\n' + (' '*100) + '\n\n')
    print(args)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=Warning)
        train_and_predict(**args)
