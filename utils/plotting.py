import numpy as np
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Rectangle
import statsmodels.api as sm


def plot_acf(X, nlags=40, conf_interval=0.05, is_pacf=False):
    """ Plots the Autocorrelations for different lags

    Arguments:
        X: the time-series to plot ACF for
        nlags: number of lags to plot
        conf_interval: the confidence interval to plot for every autocorrelation
        is_pacf: whether or not to use PACF (ACF by default)

    """
    acf_f = sm.tsa.pacf if is_pacf else sm.tsa.acf
    acf_title = 'PACF' if is_pacf else 'ACF'

    # The confidence intervals are returned by the functions as (lower, upper)
    # The plotting function needs them in the form (x-lower, upper-x)
    X_acf, X_acf_confs = acf_f(X, nlags=nlags, alpha=0.05)

    errorbars = np.ndarray((2, len(X_acf)))
    errorbars[0, :] = X_acf - X_acf_confs[:, 0]
    errorbars[1, :] = X_acf_confs[:, 1] - X_acf

    plt.plot(X_acf, 'ro')
    plt.errorbar(
        range(len(X_acf)),
        X_acf,
        yerr=errorbars,
        fmt='none',
        ecolor='gray',
        capthick=2)
    plt.xlabel('Lag')
    plt.ylabel('Autocorrelation')
    plt.title(acf_title)

    # 5% box
    box = Rectangle((1, -0.05), nlags, 0.10)
    # Create patch collection with specified colour/alpha
    pc = PatchCollection([box], facecolor='r', alpha=0.5)
    plt.gca().add_collection(pc)

    # print stats, skipping first one
    print('Number of autocorrelated lags: {0}'.format(
        sum(X_acf > 0.05) + sum(X_acf < -0.05) - 1))
    print('Number of lags where 0 is not included in confidence interval: {0}'.
          format(sum(X_acf_confs[:, 0] > 0) + sum(X_acf_confs[:, 1] < 0) - 1))
