from BaseFEFS import *




class TSFEFS(BaseFEFS):
    
    """
    The basics are implemented in BaseFEFS, 
    the variations are those that are needed to be taken care of:
    1. the df[seq_col] read by read_csv from BaseFEFS are not formated as str.
    2. df[seq_col] are not formated as datetime after reading in.
    3. Although sort df[seq_col] has occurred in BaseFEFS, it is not sorted in datetime order.
    """
    
    ##########################################################################################################
    ########################################## Class Variables: BEG ##########################################
    extension = "tsfefs"    
    ########################################## Class Variables: END ##########################################
    ##########################################################################################################



    def print_info(self):
        super().print_info()
        print("datetime_format:", self.datetime_format)
        print("seq_col dtype used in read_csv:", self.seq_read_dtype)
        print("seq_col date type (in tuple):", dt_types)
        return



    ###########################################################################################################
    ############################################ Init methods: BEG ############################################
    def __init__(self):
        super().__init__()
        self.datetime_format = None
        self.seq_trans_method = lambda x: dt.strptime(x, self.datetime_format)
        self.seq_inv_trans_method = lambda x: x.strftime(self.datetime_format)
        self.seq_read_dtype = str
        self.seq_type = dt_types


    
    """ Use BaseFEFS's """
    # @classmethod
    # def create(cls, dict_meta, name, path=None):
    #     pass
    
    
    """ Use BaseFEFS's """
    # def __check_cache_config(self):
    #     pass
    
    

    def load_meta(self,dict_meta):
        self.datetime_format = dict_meta["datetime_format"]
        # Hopefully   self.datetime_format   can be passed to   super().load_meta
        self = super().load_meta(dict_meta)
        return self
    
    
    def dump_meta(self):
        dict_meta = super().dump_meta()
        dict_meta["datetime_format"] = self.datetime_format
        return dict_meta


    """ Use BaseFEFS's """
    # def load_index_df(self, df_index, orders=None):
    #     pass

    
    """ Use BaseFEFS's """
    # def dump_index_df(self):
    #     pass
    


    def clone(self, path, name):
        
        # It is supposed to be followed by tsfefs += df
        tsfefs = super().clone(path, name)
        tsfefs.datetime_format = self.datetime_format
        return tsfefs

    ############################################ Init methods: END ############################################
    ###########################################################################################################
        

        
        
        
        
        
        
        
        
        
    """ Use BaseFEFS's """
    ##########################################################################################################
    ######################################### Path, Name, Piece: BEG #########################################
    # @staticmethod
    # def get_random_string(n):
    #     pass
    
    # def gen_valid_piece(self, prefix="", suffix=""):
    #     pass

    # def __decompose_fullpath(self, fullpath):
    #     pass
                
    # def __decompose_piece_fullname(self,piece_fullname):
    #     pass

    # def __compose_fullpath(self):
    #     pass

    # def __compose_piece_fullname(self,piece):
    #     pass
    ######################################### Path, Name, Piece: END #########################################
    ##########################################################################################################


    
    
    
    """ Use BaseFEFS's """
    # def vprint(self,*value):
    #     pass
        
        
        
    def has_valid_status(self):
        
        if not super().has_valid_status():
            return False
        
        if self.datetime_format is None:
            return False
        return True
        
            
            
    """ Use BaseFEFS's """
    # @classmethod
    # def get_index(cls,df):
    #     pass
            

    
    """ Use BaseFEFS's """
    # def __read_piece(self, idx):
    #     pass    
    
    
    """ Use BaseFEFS's """
    # def __write_piece(self, idx):
    #     pass



    
    

    """ Use BaseFEFS's """
    ##################################################################################################################
    ################################################# Cache Ops: BEG #################################################
    # """
    # Called by:
    #   - __getitem__using_idx
    #   - __getitem__using_indices
    #   - __setitem__using_idx
    #   - __setitem__using_indices
    #   - __delitem__using_idx
    #   - __delitem__using_indices
    # """
    # def renew_idx(self, idx):
    #     pass


    # """
    # Called by:
    #   - __action_delete
    # """
    # def remove_idx(self, idx): 
    #     pass


    # """
    # Called by:
    #   - __action_split
    # """
    # def split_idx(self, idx):
    #     pass


    # """
    # Called by:
    #   - __action_merge
    # """
    # def merge_idx(self, idx):
    #     pass

    
    # """
    # Called by:
    #   - __add_tsfefs
    #   - __add_df
    # """
    # def include_idx(self, idx):
    #     pass


    # def __set_piece_to_none(self,idx):
    #     pass


    # def __count_opened_rows(self,idx):
    #     pass


    # def __maintain_cache_by_rows(self):
    #     pass
    

    # def __maintain_cache_by_len(self):
    #     pass


    # def maintain_cache(self):
    #     pass

    ################################################# Cache Ops: END #################################################
    ##################################################################################################################
    
    
    
    
    
    


    """ Use BaseFEFS's """
    #########################################################################################################
    ############################################## Action: BEG ##############################################

    # def __action_nothing(self, idx):
    #     return

    # def __action_update(self, idx):
    #     pass

    # def __action_save(self, idx):
    #     pass

    # def __action_delete(self, idx):
    #     pass

    # def __action_split(self, idx):
    #     pass

    # def __action_merge(self, idx):
    #     pass

    # def __sanity_check(self):
    #     pass

    # def take_actions(self, max_level=0):
    #     pass

    # def has_pending_actions(self):
    #     pass

    ############################################## Action: END ##############################################
    #########################################################################################################

    
    
    
    
    
    
    
    
    
    
    
    """ Use BaseFEFS's """
    ################################################################################################################
    ############################################## Import,Export: BEG ##############################################
    # def __no_overlapping_time_range(self):
    #     pass
    

    # """
    # Export all as a single df
    # """
    # def export_dataframe(self):
    #     pass


    # def export_dstfile(self, dstfile):
    #     pass


    # def export_dstfolder(self, dstfolder):
    #     pass


    # def import_dataframe(self, df):
    #     pass


    # def import_srcfile(self, srcfile):
    #     pass


    # def import_srcfolder(self, srcfolder):
    #     pass


    # def all_pieces_as_csv(self):
    #     pass
    
    ############################################## Import,Export: END ##############################################
    ################################################################################################################

    

    

    
    """ Use BaseFEFS's """
    ###################################################################################################################
    ############################################# Overriding __len__: BEG #############################################
    # def __len__(self):
    #     pass
    ############################################# Overriding __len__: END #############################################
    ###################################################################################################################
    
    

    
    
    
    
    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - general: BEG #####################################
    # def __idx_check(self,idx):
    #     pass

    # def __indices_check(self,indices):
    #     pass

    # def __str_check(self,colname):
    #     pass

    # def __strs_check(self,colnames):
    #     pass
    
    # def __bools_check(self,B):
    #     pass

    # # Only for __setitems__'s strs and indices (and thus bools).
    # def __value_check(self, value, assignment_w, assignment_h):
    #     pass
    ##################################### Overriding Square Bracket - general: END #####################################
    ####################################################################################################################
   



    

    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - getitem: BEG #####################################
    # def __get_piece_idx(self, idx, orders):
    #     pass

    # def __getitem__using_idx(self,idx):
    #     pass

    # def __getitem__using_str(self,colname):
    #     pass

    # def __get_indices_in_same_piece(self, indices, orders):
    #     pass

    # def __getitem__using_indices(self,indices):
    #     pass

    # def __getitem__using_bools(self,B):
    #     pass

    # def __getitem__using_strs(self,colnames):
    #     pass

    # def __getitem__(self, key):
    #     pass
    ##################################### Overriding Square Bracket - getitem: END #####################################
    ####################################################################################################################



    
    
    
    
    
    
    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - setitem: BEG #####################################
    # def __setitem__using_idx(self, idx, value):
    #     pass

    # def __setitem__using_str(self, colname, value):
    #     pass

    # def __setitem__using_indices(self,indices,value):
    #     pass

    # def __setitem__using_bools(self,B,value):
    #     pass

    # def __setitem__using_strs(self,colnames,value):
    #     pass

    # def __setitem__(self, key, value):
    #     pass
    ##################################### Overriding Square Bracket - setitem: END #####################################
    ####################################################################################################################

    
    




    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - delitem: BEG #####################################
    # def __delitem__using_idx(self,idx):
    #     pass

    # def __delitem__using_str(self,colname):
    #     pass

    # def __delitem__using_indices(self,indices):
    #     pass

    # def __delitem__using_bools(self,B):
    #     pass

    # def __delitem__using_strs(self,colnames):
    #     pass

    # def __delitem__(self, key):
    #     pass
    ##################################### Overriding Square Bracket - delitem: END #####################################
    ####################################################################################################################



    
    



    

    """ Use BaseFEFS's """
    ######################################################################################################################
    ############################################# Overriding ==, >, <, >=, <=: BEG #######################################
    # """
    # The operators will be about the comparison of timestamps
    # Need to check:
    # 1. tsfefs1 <op> tsfefs2
    # 2. tsfefs <op> df
    # 3. df <op> tsfefs
    # 4. tsfefs <op> ts
    # 5. tsfefs <op> pd.Series(tss)

    # The codes are strictly prohibited from using tsfefs[time_col],
    # since such operation will affect the cache.
    # """
    # def __read_seq(self, idx):
    #     pass
    
    # def __get_time(self, idx):
    #     pass

    # def __get_times(self):
    #     pass

    # def __eq_series(self, S):
    #     pass

    # def __eq_tsfefs(self, tsfefs):
    #     pass

    # def __eq_df(self, df):
    #     pass

    # def __eq_dt(self, ts):
    #     pass

    # def __eq__(self, other):
    #     pass

    # def __ne_series(self, S):
    #     pass

    # def __ne_tsfefs(self, tsfefs):
    #     pass

    # def __ne_df(self, df):
    #     pass

    # def __ne_dt(self, ts):
    #     pass

    # def __ne__(self, other):
    #     pass

    # def __lt_series(self, S):
    #     pass

    # def __lt_tsfefs(self, tsfefs):
    #     pass

    # def __lt_df(self, df):
    #     pass

    # def __lt_dt(self, ts):
    #     pass

    # def __lt__(self, other):
    #     pass

    # def __gt_series(self, S):
    #     pass

    # def __gt_tsfefs(self, tsfefs):
    #     pass

    # def __gt_df(self, df):
    #     pass

    # def __gt_dt(self, ts):
    #     pass

    # def __gt__(self, other):
    #     pass

    # def __le_series(self, S):
    #     pass

    # def __le_tsfefs(self, tsfefs):
    #     pass

    # def __le_df(self, df):
    #     pass

    # def __le_dt(self, ts):
    #     pass

    # def __le__(self, other):
    #     pass

    # def __ge_series(self, S):
    #     pass

    # def __ge_tsfefs(self, tsfefs):
    #     pass

    # def __ge_df(self, df):
    #     pass

    # def __ge_dt(self, ts):
    #     pass

    # def __ge__(self, other):
    #     pass

    ############################################# Overriding ==, >, <, >=, <=: END #######################################
    ######################################################################################################################

 








    """ Use BaseFEFS's """
    #####################################################################################################################
    ################################################# Overriding +: BEG #################################################
    # def __add_tsfefs(self, tsfefs):
    #     pass

    # def __add_df(self, df):
    #     pass

    # def __add__(self, other):
    #     pass
    ################################################# Overriding +: END #################################################
    #####################################################################################################################


    
    
    
    
    
    
    

    """ Use BaseFEFS's """
    ####################################################################################################################
    #################################################### MERGE: BEG ####################################################
    # # # @classmethod
    # # # def merge(cls, left_obj, right_obj, path="", name="", on="", target=None):
    # # @staticmethod
    # # def merge(left_obj, right_obj, path="", name="", on="", target=None):
    # #     pass


    # def __get_tsfefs_reference(self,path,name):
    #     pass


    # def merge_with_df(self, df_another, on, target):
    #     pass


    # def merge_with_tsfefs(self, tsfefs_another, on, target):
    #     pass


    # def merge(self, right_obj, on, target=None, path=None, name=None):
    #     pass
    #################################################### MERGE: END ####################################################
    ####################################################################################################################
    
    
    
    

    
    
    
    
    
    
    
    """ Use BaseFEFS's """
    #####################################################################################################################
    ########################################## conflict resolve, optimize: BEG ##########################################

    # def conflict_exist(self):
    #     pass


    # def resolve_conflict(self):
    #     pass


    # def optimize_files(self):
    #     pass

    ########################################## conflict resolve, optimize: END ##########################################
    #####################################################################################################################
    
    

    
    
    
    
    

    """ Use BaseFEFS's """
    ####################################################################################################################
    ################################################# Read, Write: BEG #################################################
    # """
    # Equals to df.read_csv
    # """
    # def read(self, fullname):
    #     pass


    # def write(self, fullname=None):
    #     pass
    ################################################# Read, Write: END #################################################
    ####################################################################################################################



    """ Use BaseFEFS's """
    # def remove(self):
    #     pass




