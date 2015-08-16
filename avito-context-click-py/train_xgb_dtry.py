import pandas as pd
import numpy as np
import sys
import getopt
from sklearn import feature_extraction
from sklearn.cross_validation import StratifiedKFold
from sklearn.preprocessing import LabelEncoder
#sys.path.append('/Users/ef/xgboost/wrapper')
import xgboost as xgb
import random
from sklearn.metrics import log_loss
import os

def load_train_data(path):
    df = pd.read_csv(path)
    y = df['IsClick'].values
    ids = df['ID'].values
    df.drop(['IsClick','ID'], axis=1, inplace=True)
    #df.drop(['AdTitleWordLikeli'], axis=1, inplace=True)
    #df.drop(['AdIDRareWordCount'], axis=1, inplace=True)
    #df.drop(['IPIDlikeli'], axis=1, inplace=True)
    #df.drop(['IPIDUserAgentOSIDlikeli'], axis=1, inplace=True)
    #df.drop(['UserAdViewTotalCount', 'UserAdViewUniqueCount','UserAdViewTotalCount2', 'UserAdViewUniqueCount2'], axis=1, inplace=True)
    #df.drop(['LocationUserUniqueCount', 'CategoryUserUniqueCount'], axis=1, inplace=True)
    #df.drop(['AdPosition1Count', 'AdPosition7Count'], axis=1, inplace=True)
    #df.drop(['UserAgentIDlikeli', 'UserAgentOSIDlikeli', 'UserDeviceIDlikeli', 'UserAgentFamilyIDlikeli'], axis=1, inplace=True)
    #df.drop(['AdCategoryPriceDeviation'], axis=1, inplace=True)
    X = df.values.copy().astype(np.float32)
    #np.random.seed(seed=2015)
    #np.random.shuffle(X)
    return X, y, ids
    
def load_test_data(path):
    df = pd.read_csv(path)
    if 'IsClick' in df.columns.values:
        df.drop(['IsClick'], axis=1, inplace=True)
    ids = df['ID'].values
    df.drop(['ID'], axis=1, inplace=True)
    #df.drop(['AdTitleWordLikeli'], axis=1, inplace=True)
    #df.drop(['AdIDRareWordCount'], axis=1, inplace=True)
    #df.drop(['IPIDlikeli'], axis=1, inplace=True)
    #df.drop(['IPIDUserAgentOSIDlikeli'], axis=1, inplace=True)
    #df.drop(['UserAdViewTotalCount', 'UserAdViewUniqueCount','UserAdViewTotalCount2', 'UserAdViewUniqueCount2'], axis=1, inplace=True)
    #df.drop(['LocationUserUniqueCount', 'CategoryUserUniqueCount'], axis=1, inplace=True)
    #df.drop(['AdPosition1Count', 'AdPosition7Count'], axis=1, inplace=True)
    #df.drop(['UserAgentIDlikeli', 'UserAgentOSIDlikeli', 'UserDeviceIDlikeli', 'UserAgentFamilyIDlikeli'], axis=1, inplace=True)
    #df.drop(['AdCategoryPriceDeviation'], axis=1, inplace=True)
    X = df.values.copy().astype(np.float32)
    return X, ids

opts, args = getopt.getopt(sys.argv[1:], "t:v:p:e:", ["train=", "test=", "pred=", "epoch="])
opts = {x[0]:x[1] for x in opts}
train_file = opts['--train']
test_file = opts['--test']
pred_file = opts['--pred']
epoch = int(opts['--epoch'])

X, y, ids_train = load_train_data(train_file)
X_test, ids_test = load_test_data(test_file)
num_features = X.shape[1]

param = {}
param['objective'] = 'binary:logistic'
param['eta'] = 0.2 #0.1
param['max_depth'] = 10
param['eval_metric'] = 'logloss'
param['silent'] = 1
param['nthread'] = 6
param['gamma'] = 0.8
param['min_child_weight'] = 4 #10
#param['subsample'] = 0.8
param['colsample_bytree'] = 0.7
param['colsample_bylevel'] = 0.8
num_round = 75 #85

for e in range(epoch):
    print "processing iteration", e
    param['seed'] = 3015 + (10*e)
    plst = list(param.items())

    index_shuffle = [i for i in range(X.shape[0])]
    random.shuffle(index_shuffle)
    xgmat_train = xgb.DMatrix( X[index_shuffle,:], label=y[index_shuffle], missing = -1.0)
    bst = xgb.train( plst, xgmat_train, num_round );
    #fscore = [ (v,k) for k,v in bst.get_fscore().iteritems() ]
    #fscore.sort(reverse=True)
    #print fscore
    xgmat_test = xgb.DMatrix( X_test, missing = -1.0 )
    pred_list = bst.predict( xgmat_test )
    pred_list = [round(x, 5) for x in pred_list]
    preds = pd.DataFrame(pred_list, columns=['IsClick'])
    preds['ID'] = ids_test
    preds.to_csv(os.path.splitext(pred_file)[0] + '.epoch' + str(e) + '.csv', index=False)
