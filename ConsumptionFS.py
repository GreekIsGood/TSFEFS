from BaseFEFS import *

sys.path.append("GreekCaptures/general")
sys.path.append("GreekCaptures/utils")

from feats import *




class ConsumptionFS(BaseFEFS):
    
    ##########################################################################################################
    ########################################## Class Variables: BEG ##########################################
    extension = "consumpfs"
    ########################################## Class Variables: END ##########################################
    ##########################################################################################################



    def print_info(self):
        super().print_info()
        print("Feat tables included:", self.ftables)
        print()
        for ftable in self.ftables:
            # print each ftable's features
            print("%s's features:"%ftable, [ fdecode(feat) for feat in self.ftable_feats[ftable] ])
            print("%s's info:"%ftable, self.ftable_info[ftable])
            print()
        return



    ###########################################################################################################
    ############################################ Init methods: BEG ############################################
    def __init__(self):
        super().__init__()
        self.ftables = None # list 
        self.ftable_feats = None # dict
        self.ftable_info = None # dict
        self.seq_type = int_types
        
        

    """ Use BaseFEFS's """
    """ Only need to modify load_meta """
    # @classmethod
    # def create(cls, dict_meta, name, path=None):
    #     pass
    


    """ Use BaseFEFS's """
    # def __check_cache_config(self):
    #     pass
    
    
    
    def load_meta(self,dict_meta):
        self = super().load_meta(dict_meta)
        self.ftables = dict_meta["ftables"]
        self.ftable_feats = self.parse_featnames() # take in self.ftables and self.colnames, not in dict_meta.
        self.ftable_info = dict_meta["ftable_info"]
        return self



    def dump_meta(self):
        dict_meta = super().dump_meta()
        dict_meta["ftables"] = self.ftables
        dict_meta["ftable_info"] = self.ftable_info
        return dict_meta


    
    """ Use BaseFEFS's """
    # def load_index_df(self, df_index, orders=None):
    #     pass

    
    """ Use BaseFEFS's """
    # def dump_index_df(self):
    #     pass
    

    
    def clone(self, path, name):
        
        # It is supposed to be followed by consumpfs += df
        consumpfs = super().clone(path, name)
        consumpfs.ftables = dc(self.ftables)
        consumpfs.ftable_feats = dc(self.ftable_feats)
        consumpfs.ftable_info = dc(self.ftable_info)
        return consumpfs

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
        
        if consumpfs.ftables is None:
            return False
        elif consumpfs.ftable_feats is None:
            return False
        elif consumpfs.ftable_info is None:
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
    
    
    
    
    ####################################################################################################################
    ############################################## Consume functions: BEG ##############################################

    
    def __consume_all(self):
    
        df_consump = self.export_dataframe()
        del self[:]
        if self.has_pending_actions():
            self.take_actions(max_level=4)              
        return df_consump
    
    
    
    def __consume_range_of_records(self, fr, to, replacement=False):

        assert isinstance(fr, int_types) and isinstance(to, int_types)
        
        assert to >= fr
        assert to <= len(self)
        
        df_consump = self[fr:(to+1)]
        df_consump = df_consump.reset_index(drop=True)
        
        if not replacement:
            del self[indices]
            if self.has_pending_actions():
                self.take_actions(max_level=4)
                          
        return df_consump        
        
        

    def __consume_by_rand_ids(self, req_ids, replacement=False):
        
        assert isinstance(req_ids, arr_types)
        
        """ all req_ids must be in df[seq_col] """
        S = self[seq_col]
        assert len(set(req_ids) - set(S)) == 0

        req_ids = pd.Series(req_ids)
        req_ids = req_ids.sort_values() # don't reset_index
        
        """ 
        There can be duplicated ids in req_ids,
        hence indices can also has duplications.
        """
        indices = [] # indices to be retrieved from S
        idx = 0 # current pointer
        s = S[0]
        
        req_id_iloc = 0 # current iloc pointer
        req_id = req_ids.iloc[req_id_iloc]
        
        while req_id_iloc < len(req_ids):
            if req_id == s:
                indices += [idx]
                req_id_iloc += 1
                req_id = req_ids.iloc[req_id_iloc]                
            elif req_id > s:
                idx += 1
                s = S[idx]
            else:
                assert False


        indices = pd.Series(indices)
        indices.index = req_ids.index
        indices = [ indices[i] for i in range(len(indices)) ]
        
        df_consump = self[indices]
        df_consump = df_consump.reset_index(drop=True)
        
        if not replacement:
            del self[indices]
            if self.has_pending_actions():
                self.take_actions(max_level=4)
                    
        return df_consump



    def __consume_rand_req_ids(self, randcnt, replacement=False):
        
        assert isinstance(randcnt, int_types)
        
        S = self[seq_col]
        
        if replacement:
            # random.sample won't pick the same number
            req_ids = random.sample(S, k=randcnt)
        else:
            # random.choices may pick the same number
            req_ids = random.choices(S, k=randcnt)
            
        return self.__consume_by_req_ids(req_ids, replacement=replacement)

    
    
    def consume(self, req_ids=None, fr=None, to=None, randcnt=None, replacement=False):
        
        if all([ param is None for param in [ req_ids, fr, to, randcnt ] ]):
            df_consump = self.__consume_all()
        elif req_ids is not None:
            assert (fr is None) and (to is None)
            assert randcnt is None
            df_consump = self.__consume_by_req_ids(req_ids, replacement=replacement)
        elif randcnt is not None:
            assert (fr is None) and (to is None)
            df_consump = self.__consume_by_rand_ids(randcnt, replacement=replacement)
        elif fr is not None:
            assert to is not None
            df_consump = self.__consume_range_of_records(fr, to, replacement=replacement)
        else:
            assert False
            
        return df_consump

    ############################################## Consume functions: END ##############################################
    ####################################################################################################################



    
    


