import pandas as pd
import argparse
import xgboost as xgb
import os

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='train xgb model',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-train',
        default="/home/lucas/ml-r-tb/contest/avito-context-click/avito-context-click-r/"
                "../data/output-r/data.val.tr.full.libsvm.head#data.val.tr.full.cache",
        type=str,
        help='CSV with training data')
    parser.add_argument(
        '-test',
        default="/home/lucas/ml-r-tb/contest/avito-context-click/avito-context-click-r/"
                "../data/output-r/data.val.tr.full.libsvm.head#data.val.tr.full.cache",
        type=str,
        help='CSV with test data')
    parser.add_argument(
        '-pred',
        default="/home/lucas/ml-r-tb/contest/avito-context-click/avito-context-click-r/"
                "../data/output-r/data.val.tt.full.pred",
        type=str,
        help='Prediction path')
    parser.add_argument(
        '-epoch',
        default=4,
        type=int,
        help='Epochs')

    args = parser.parse_args()

    pred_file = args.pred

    xg_params = {
        'objective': 'binary:logistic',
        'eta': 0.2,
        'max_depth': 10,
        'eval_metric': 'logloss',
        'silent': 1,
        'nthread': 6,
        'gamma': 0.8,
        'min_child_weight': 4,
        'colsample_bytree': 0.7,
        'colsample_bylevel': 0.8
    }
    num_round = 75
    xg_data_tr = xgb.DMatrix(args.train)
    xg_data_tst = xgb.DMatrix(args.test)

    for e in range(args.epoch):
        print("processing iteration %d" % e)
        xg_params['seed'] = 3015 + (10*e)
        model = xgb.train(
            params=list(xg_params.items()),
            dtrain=xg_data_tr,
            num_boost_round=75,
            feval=[(xg_data_tst, 'val')]
        )
        pred_list = model.predict(xg_data_tst)
        pred_list = [round(x, 6) for x in pred_list]
        preds = pd.DataFrame(pred_list, columns=['IsClick'])
        preds.to_csv(os.path.splitext(pred_file)[0] + '.epoch' + str(e) + '.csv', index=False)
