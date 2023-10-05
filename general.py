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
