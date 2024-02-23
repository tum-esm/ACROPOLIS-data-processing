import math
import numpy as np
from sklearn.metrics import r2_score

def rmse(y_true, y_meas):
    MSE = np.square(np.subtract(y_true, y_meas)).mean()
    RMSE = math.sqrt(MSE)

    return RMSE


def calc_r2(y_true, y_meas):
    return r2_score(y_true, y_meas)