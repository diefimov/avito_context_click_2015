from sklearn.calibration import CalibratedClassifierCV
from sklearn.cross_validation import KFold
import numpy as np


def calibrate_probs(y_val, prob_val, prob_test, n_folds=2, method='isotonic', random_state=5968):
    """ Calling from R:

        suppressMessages(library("rPython")) # Load RPython
        python.load("path/to/util_rpython.py")

        data.pred.calib <- python.call('calibrate_probs',
                                   y_val=y_val, # Actual values from validation
                                   prob_val=pred_val, # Predicted values from validation
                                   prob_test=pred_test) # Predicted values from test

        # data.pred.calib will be a list, so to get the calibrated predictions for each value we do:
        calib_pred_val = data.pred.calib$val
        calib_pred_test = data.pred.calib$test

    """

    y_val = np.asarray(y_val, dtype=float)
    prob_val = np.asarray(prob_val, dtype=float).reshape((-1, 1))
    prob_test = np.asarray(prob_test, dtype=float).reshape((-1, 1))

    prob_clb_val = np.zeros(len(y_val))
    prob_clb_test = np.zeros(len(prob_test))

    kf_val_full = KFold(len(y_val), n_folds=n_folds, random_state=random_state)

    for ix_train, ix_test in kf_val_full:
        kf_val_inner = KFold(len(ix_train), n_folds=n_folds, random_state=random_state)
        clf = CalibratedClassifierCV(method=method, cv=kf_val_inner)
        clf.fit(prob_val[ix_train], y_val[ix_train])
        prob_clb_val[ix_test] = clf.predict_proba(prob_val[ix_test])[:, 1]
        prob_clb_test += clf.predict_proba(prob_test)[:, 1]/n_folds

    return {'val': list(prob_clb_val), 'test': list(prob_clb_test)}
