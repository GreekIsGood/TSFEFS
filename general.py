import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta as td

import json
import random
import string

from copy import deepcopy as dc
import os


int_types = (int,np.integer)
float_types = (float,np.float128,np.float64,np.float32,np.float16)
str_types = (str,np.str_)
bool_types = (bool,np.bool_)
arr_types = (list,np.ndarray,pd.Series)

_df_ = pd.DataFrame({"t":["2020-01-01 00:00:00.0000"]})
_df_["t"] = _df_["t"].apply(lambda x: dt.strptime(x,"%Y-%m-%d %H:%M:%S.%f"))
_s_ = list(_df_["t"])[0]
dt_types = (dt,type(_s_))
