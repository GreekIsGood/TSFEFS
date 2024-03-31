


###############################################################################################
################## The following are copied from GreekCaptures/constants.py, ##################
##################### but what's the best arrangement other than copying? #####################

"""
NULL_ASSIGNMENT: table exists but a particular value missing.
NO_TABLE_ASSIGNMENT: the table for that shop doesn't exit.

Note:
1. The 2 "nothingness" are different in nature.
2. NO_TABLE_ASSIGNMENT after filled in, is the value to be served.
3. NULL_ASSIGNMENT after filled in, will be taken as other values by EMA/STLD/...
Both are extreme values,
but Because of 2 & 3, 
NULL_ASSIGNMENT has to be less extreme compared to NO_TABLE_ASSIGNMENT
"""
NULL_ASSIGNMENT = -9.99
NO_TABLE_ASSIGNMENT = -99.99



"""
Notes:
1. nan values for cat is always included.
    E.g.,
    type_1: [1,0,0,...]
    type_2: [0,1,0,...]
    ...
    type_nan: [0,0,...,1]
2. if there's a new value,
    it will be represented by all zeros,
    [0,0,0,...]
I.e., there's no NULL assignment for OHK.
"""
# MAX_CAT_DIM = 8
# NEWCAT_ASSIGNMENT = don't! For OHK, just reflect it by [0,0,...]
nan_strs = ["","nan","na","-"]


"""
Even the UNKNOWN key of cats is stored in some dict,
there have to be a way to identify which one is an UNKOWN key.
It is possible for the origin data to contain a str value == "unknown".
To avoid this possibility, weird pattern 
 "#@^*$unknown@^#*$" represents UNKNOWN,
Even if str value "unknown" exists, it won't crash with the actual UNKNOWN.
"""
UNKNOWN_OHK_SUFFIX = "#@^*$unknown@^#*$"


"""
For the transformation from human readable feat names 
to database storable column names.
"""
colon_code = (':',"_8_0_8_")
hyphen_code = ('-',"_1_0_1_")
tilde_code = ('~',"_3_0_3_")
sharp_code = ('#',"_5_0_5_")

#################### The above are copied from GreekCaptures/constants.py, ####################
##################### but what's the best arrangement other than copying? #####################
###############################################################################################






###############################################################################################
#################### The following are copied from GreekCaptures/feats.py, ####################
##################### but what's the best arrangement other than copying? #####################

def transform_featname_to_alias(featname):
    storename = featname.replace(*colon_code)
    storename = storename.replace(*hyphen_code)
    storename = storename.replace(*tilde_code)
    storename = storename.replace(*sharp_code)
    return storename

def recover_alias_to_featname(storename):
    featname = storename.replace(*(colon_code[::-1]))
    featname = featname.replace(*(hyphen_code[::-1]))
    featname = featname.replace(*(tilde_code[::-1]))
    featname = featname.replace(*(sharp_code[::-1]))
    return featname

###################### The above are copied from GreekCaptures/feats.py, ######################
##################### but what's the best arrangement other than copying? #####################
###############################################################################################



