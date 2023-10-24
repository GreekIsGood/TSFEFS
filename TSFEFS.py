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
        return



    ###########################################################################################################
    ############################################ Init methods: BEG ############################################
    def __init__(self):
        super().__init__()
        self.datetime_format = None
        self.seq_trans_method = lambda x: dt.strptime(x, self.datetime_format)
        self.seq_inv_trans_method = lambda x: x.strftime(self.datetime_format)
        self.seq_dtype = str


    # @classmethod
    # def create(cls, dict_meta, name, path=None):
    #     """
    #     Usecase
    #     --------
    #     When you start with nothing but will add dataframes subsequently.
    #     """

    #     tsfefs.datetime_format = None
    #     tsfefs = super().create(dict_meta, name, path=path)
    #     tsfefs.datetime_format = dict_meta["datetime_format"]
    #     dtf = tsfefs.datetime_format
    #     try:
    #         tsfefs.fr = dt.strptime(tsfefs.fr, dtf)
    #         tsfefs.to = dt.strptime(tsfefs.to, dtf)
    #     except:
    #         pass   
    #     return tsfefs

    
    
    # def __check_cache_config(self):
    # use BaseFEFS's method
    
    

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

    #     dtf = self.datetime_format
    #     df_index["fr"] = df_index["fr"].apply(lambda x: dt.strptime(x,dtf))
    #     df_index["to"] = df_index["to"].apply(lambda x: dt.strptime(x,dtf))

    #     """
    #     the df_index being passed to super() is the dt formated one.
    #     """
    #     self = super().load_index_df(df_index, orders=orders)
    #     return self


    
    """ Use BaseFEFS's """
    # def dump_index_df(self):

    #     df_index, orders = super().dump_index_df()

    #     """
    #     df_index["fr"]  and  df_index["to"]  are not in str yet.
    #     """
    #     dtf = self.datetime_format
    #     df_index["fr"] = df_index["fr"].apply(lambda x: x.strftime(dtf))
    #     df_index["to"] = df_index["to"].apply(lambda x: x.strftime(dtf))
    #     return (df_index, orders)
    


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
    # use BaseFEFS's method
    
    # def gen_valid_piece(self, prefix="", suffix=""):
    # use BaseFEFS's method

    # def __decompose_fullpath(self, fullpath):
    # use BaseFEFS's method
                
    # def __decompose_piece_fullname(self,piece_fullname):
    # use BaseFEFS's method

    # def __compose_fullpath(self):
    # use BaseFEFS's method

    # def __compose_piece_fullname(self,piece):
    # use BaseFEFS's method
    ######################################### Path, Name, Piece: END #########################################
    ##########################################################################################################


    
    
    
    """ Use BaseFEFS's """
    # def vprint(self,*value):
    # use BaseFEFS's method
        
        
        
    def has_valid_status(self):
        
        if not super().has_valid_status():
            return False
        
        if self.datetime_format is None:
            return False
        return True
        
            
            
    """ Use BaseFEFS's """
    # @classmethod
    # def get_index(cls,df):
    # use BaseFEFS's method
            

    # @deprecated
    # def __to_csv(self,idx):
    #     pass
        

    # @deprecated
    # def __read_csv(self,idx):
    #     pass


    
    """ Use BaseFEFS's """
    # def __read_piece(self, idx):

    #     df = super().__read_piece(idx)
    #     type_ = self.types[idx]
    #     if type_ == "csv":

    #         self.__read_seq(idx)


    #         df = pd.read_csv(fullname, dtype={self.time_col:str})

    #         # dtf = self.datetime_format
    #         # df[self.time_col] = df[self.time_col].apply(lambda x: dt.strptime(x,dtf))
    #         # df = df.sort_values(by=self.time_col).reset_index(drop=True)
    #         df = df.sort_values(by=self.seq_col).reset_index(drop=True)

    #         self.dfs[idx] = df
    #         return self.dfs[idx]

    #     elif self.types[idx] == "tsfefs":    

    #         tsfefs = TSFEFS()
    #         fullname += '.' + TSFEFS.extension
    #         tsfefs.read(fullname)
    #         return tsfefs

    #     else:
    #         print("File type \"tsfefs\" not yet implemented")
    #         assert False
    #     return
    
    
    
    """ Use BaseFEFS's """
    # def __write_piece(self, idx):

    #     df = self.dfs[idx]
    #     assert df is not None

    #     tsfefs_fullname = self.__compose_fullpath()
    #     if not os.path.isdir(tsfefs_fullname):
    #         os.mkdir(tsfefs_fullname)            

    #     type_ = self.types[idx]
    #     piece = self.pieces[idx]
    #     fullname = self.__compose_piece_fullname(piece)
    #     if type_ == "csv":
    #         df = df.sort_values(by=self.time_col).reset_index(drop=True)
    #         self.dfs[idx] = df
    #         dtf = self.datetime_format
    #         df2 = dc(df)
    #         df2[self.time_col] = df2[self.time_col].apply(lambda x: x.strftime(dtf))
    #         df2.to_csv(fullname, index=False)
    #         del df2; df2 = None
    #     elif type_ == "tsfefs":
    #         tsfefs = df
    #         tsfefs.take_actions(max_level=4) #??? or max_level=3?
    #         fullname += '.' + TSFEFS.extension
    #         tsfefs.write(fullname) # by calling write, this tsfefs object will have its path and name reset.
    #     else:
    #         print("Type", type_, "not supported")
    #         assert False
    #     return







    
    

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
    #     if idx in self.cache:
    #         self.cache.remove(idx)
    #     self.cache.append(idx)

    # """
    # Called by:
    #   - __action_delete
    # """
    # def remove_idx(self, idx): 
    #     if idx in self.cache:
    #         self.cache.remove(idx)
    #     self.cache = [ idx_ - 1 if idx_ > idx else idx_ for idx_ in self.cache ]



    # """
    # Called by:
    #   - __action_split
    # """
    # def split_idx(self, idx):

    #     """
    #      - idx: the originally index, which will be retained, with new pieces created from it.
    #      - new_idx: split will lengthen the array, len(self.pieces)-1

    #     The new idx will be inserted in the cache behind the idx.
    #     """
    #     new_idx = len(self.pieces)-1


    #     """
    #     If idx not in cache, new_idx, which is split from idx, should not be in cache as well.
    #     """
    #     if idx in self.cache:
    #         loc = self.cache.index(idx)
    #         if new_idx in self.cache:
    #             self.cache.remove(new_idx)
    #         self.cache.insert(loc,new_idx)
    #     else:
    #         if new_idx in self.cache:
    #             self.cache.remove(new_idx)
    #     # if idx not in cache, no need to have new_idx in cache.



    # """
    # Called by:
    #   - __action_merge
    # """
    # def merge_idx(self, idx):

    #     tobemerged_idx = self.action_params[idx]
    #     assert isinstance(tobemerged_idx,int)

    #     if tobemerged_idx in self.cache:
    #         loc2 = self.cache.index(tobemerged_idx)
    #         if idx in self.cache:
    #             loc1 = self.cache.index(idx)
    #             if loc2 > loc1:
    #                 self.cache.remove(idx)
    #                 self.cache.insert(loc2,idx)
    #         else:
    #             self.cache.insert(loc2,idx)

    # """
    # Called by:
    #   - __add_tsfefs
    #   - __add_df
    # """
    # def include_idx(self, idx):
    #     """
    #     only include idx to the lower priority.
    #     Note: 0 is the lowest priority.
    #     """
    #     if idx not in self.cache:
    #         self.cache.insert(0,idx)



    # def __set_piece_to_none(self,idx):
    #     df = self.dfs[idx]
    #     if df is None:
    #         return
    #     type_ = self.types[idx]
    #     if type_ == "csv":
    #         pass
    #     elif type_ == "tsfefs":
    #         for idx2 in range(len(df.pieces)):
    #             df.__set_piece_to_none(idx2)
    #     else:
    #         assert False        

    #     del df
    #     df = None
    #     self.dfs[idx] = None
    #     return


    # def __count_opened_rows(self,idx):
    #     df = self.dfs[idx]
    #     type_ = self.types[idx]
    #     if df is None:
    #         return 0
    #     if type_ == "csv":
    #         return self.row_cnts[idx]
    #     elif type_ == "tsfefs":
    #         tsfefs = df
    #         row_cnt = 0
    #         for idx_ in range(len(tsfefs.pieces)):
    #             row_cnt += tsfefs.__count_opened_rows(idx_)
    #         return row_cnt
    #     else:
    #         assert False


    # def __maintain_cache_by_rows(self):
    #     cache_config = self.cache_config
    #     rows_in_cache = cache_config["rows_in_cache"]

    #     cache = []
    #     for i in reversed(range(len(self.cache))):
    #         idx = self.cache[i]
    #         row_cnt = self.__count_opened_rows(idx)
    #         if rows_in_cache >= row_cnt:
    #             rows_in_cache -= row_cnt
    #             cache.append(idx)
    #             continue
    #         break

    #     cache = list(reversed(cache))
    #     clear_indices = set(self.cache) - set(cache)
    #     for idx in clear_indices:
    #         self.__set_piece_to_none(idx)
    #     self.cache = cache
    #     return




    # def __maintain_cache_by_len(self):
    #     cache_config = self.cache_config
    #     len_of_cache = cache_config["len_of_cache"]
    #     if len(self.cache) <= len_of_cache:
    #         return

    #     self.cache = self.cache[-len_of_cache:]
    #     for idx in range(len(self.pieces)):
    #         if idx in self.cache:
    #             continue
    #         self.__set_piece_to_none(idx)





    # def maintain_cache(self):

    #     # action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
    #     # assert np.all(np.array(action_levels) == TSFEFS.action_domain.index(""))
    #     assert not self.has_pending_actions()


    #     self.__check_cache_config()
    #     cache_config = self.cache_config
    #     rows_in_cache = cache_config["rows_in_cache"]
    #     len_of_cache = cache_config["len_of_cache"]

    #     if rows_in_cache is not None:
    #         self.__maintain_cache_by_rows()
    #     elif len_of_cache is not None:
    #         self.__maintain_cache_by_len()
    #     else:
    #         assert False
    #     return

    ################################################# Cache Ops: END #################################################
    ##################################################################################################################
    
    
    
    
    
    


    """ Use BaseFEFS's """
    #########################################################################################################
    ############################################## Action: BEG ##############################################

    # def __action_nothing(self, idx):
    #     return


    # def __action_update(self, idx):

    #     df = self.dfs[idx]
    #     assert df is not None

    #     type_ = self.types[idx]
    #     len_ = None
    #     if type_ == "csv":
    #         len_ = len(df)
    #     elif type_ == "tsfefs":
    #         tsfefs = df
    #         len_ = tsfefs.row_cnt
    #     else:
    #         print("Type", type_, "not supported")
    #         assert False

    #     fr, to = None, None
    #     if len_ > 0:
    #         if type_ == "csv":
    #             S_ = df[self.seq_col]
    #         elif type_ == "tsfefs":
    #             S_ = df.__get_times()
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False
    #         fr, to = min(S_), max(S_)

    #     self.frs[idx], self.tos[idx] = fr, to
    #     self.row_cnts[idx] = len_
    #     frs = [ fr for fr in self.frs if fr is not None ]
    #     tos = [ to for to in self.tos if to is not None ]
    #     self.fr, self.to = min(frs), max(tos)
    #     row_cnts = [ rcnt for rcnt in self.row_cnts if rcnt is not None ]
    #     self.row_cnt = sum(row_cnts)
    #     self.actions[idx] = "save" # it will be saved later

    #     self.vprint(self.pieces[idx], "turned to status \"save\"")

    #     if len_ == 0:
    #         self.actions[idx] = "delete"

    #         self.vprint(self.pieces[idx], "turned to status \"delete\" because its len is 0")

    #     return


    # def __action_save(self, idx):
    #     self.__write_piece(idx)
    #     self.actions[idx] = ""
    #     return


    # def __action_delete(self, idx):

    #     df = self.dfs[idx]
    #     assert df is not None

    #     type_ = self.types[idx]
    #     piece = self.pieces[idx]
    #     fullname = self.__compose_piece_fullname(piece)
    #     if type_ == "csv":
    #         if os.path.isfile(fullname):
    #             os.remove(fullname)
    #     elif type_ == "tsfefs":
    #         tsfefs = df
    #         # fullname = tsfefs.__compose_fullpath()
    #         # if os.path.isdir(fullname):
    #         #     for f in os.listdir(fullname):
    #         #         os.remove("%s/%s"%(fullname,f))
    #         #     os.rmdir(fullname)
    #         tsfefs.remove()

    #     else:
    #         print("Type", type_, "not supported")
    #         assert False

    #     del self.pieces[idx]
    #     del self.types[idx]
    #     del self.frs[idx]; del self.tos[idx]
    #     del self.row_cnts[idx]
    #     del self.actions[idx]; del self.action_params[idx]
    #     del self.dfs[idx]
    #     del df; df = None

    #     self.remove_idx(idx)

    #     frs = [ fr for fr in self.frs if fr is not None ]
    #     tos = [ to for to in self.tos if to is not None ]
    #     if len(frs) > 0:
    #         assert len(tos) > 0
    #         self.fr, self.to = min(frs), max(tos)
    #     else:
    #         self.fr, self.to = None, None

    #     self.row_cnt = sum(self.row_cnts)
    #     return


    # def __action_split(self, idx):

    #     df = self.dfs[idx]
    #     assert df is not None

    #     type_ = self.types[idx]

    #     """
    #     The design is not advanced enough to be able to split 1 TSFEFS into 2 TSFEFSs,
    #     so let's have all the splits be csvs.
    #     """
    #     if type_ == "tsfefs":
    #         df = df.export_dataframe()
    #     df = df.sort_values(by=self.seq_col).reset_index(drop=True)

    #     n = len(df)
    #     num_rows_per_file = int(self.max_row_per_piece/2)
    #     num_of_files = int(np.ceil(n/num_rows_per_file))

    #     dfs = [ df.iloc[(i*num_rows_per_file):((i+1)*num_rows_per_file)] for i in range(num_of_files) ]

    #     if len(dfs) == 0:
    #         assert False
    #     elif len(dfs) == 1:
    #         """
    #         no need for split action
    #         """
    #         return
    #     else:
    #         self.types[idx] = "csv"
    #         type_ = "csv"
    #         self.dfs[idx] = df


    #     # print("__action_split, idx:", idx)
    #     # print("__action_split:", [ len(df_) for df_ in dfs ])
    #     df = dfs[0]
    #     self.dfs[idx] = df
    #     """ can leave fr and to updates when "update" """
    #     self.actions[idx] = "update"

    #     dfs = dfs[1:]

    #     if type_ == "csv":
    #         for df in dfs:
    #             """
    #             instead of using 
    #               self += df
    #             the following codes are preferred.

    #             Calling __add__ will let the new_idx to be included in cache,
    #             which will mess up the split_idx logic.
    #             """
    #             piece = self.gen_valid_piece(prefix="", suffix="")
    #             self.pieces += [piece]
    #             self.types += [type_]
    #             self.frs += [None] # leave to "update" action
    #             self.tos += [None] # leave to "update" action
    #             self.row_cnts += [None]
    #             self.actions += ["update"]
    #             self.action_params += [None]
    #             self.dfs += [df]

    #             self.split_idx(idx) # this will the new indices next to the location of idx in cache

    #     else: # elif self.types[idx] == "tsfefs"
    #         print("File type \"tsfefs\" not yet implemented")
    #         assert False
    #     return


    # def __action_merge(self, idx):

    #     df = self.dfs[idx]

    #     """
    #     I might have reasons for this line,
    #     but seems like it is now causing trouble.
    #     Let's recall the reason?
    #     """
    #     # assert df is not None
    #     """
    #     Temporarily replace the above line by this
    #     """
    #     if df is None:
    #         df = self.__read_piece(idx)
    #         self.dfs[idx] = df



    #     idx_tobemerged = self.action_params[idx]
    #     assert isinstance(idx_tobemerged,int)
    #     df_tobemerged = self.dfs[idx_tobemerged]



    #     """
    #     I might have reasons for this line,
    #     but seems like it is now causing trouble.
    #     Let's recall the reason?
    #     """
    #     # assert df_tobemerged is not None
    #     """
    #     Temporarily replace the above line by this
    #     """
    #     if df_tobemerged is None:
    #         df_tobemerged = self.__read_piece(idx_tobemerged)
    #         self.dfs[idx_tobemerged] = df_tobemerged





    #     type_ = self.types[idx]
    #     type_tobemerged = self.types[idx_tobemerged]
    #     if  type_ == "csv"  and  type_tobemerged == "csv":
    #         df = pd.concat([df,df_tobemerged]).reset_index(drop=True)
    #     elif  type_ == "csv"  and  type_tobemerged == "tsfefs":
    #         df_tobemerged = df_tobemerged.export_dataframe()
    #         df = pd.concat([df,df_tobemerged]).reset_index(drop=True)
    #     elif  type_ == "tsfefs"  and  type_tobemerged == "csv":
    #         df += df_tobemerged
    #     elif  type_ == "tsfefs"  and  type_tobemerged == "tsfefs":
    #         df += df_tobemerged            
    #     else:
    #         print("Type",  type_tobemerged, "not supported.")
    #         assert False


    #     self.merge_idx(idx) # has to be run before action_params[idx] is set to None


    #     self.dfs[idx] = df
    #     self.action_params[idx] = None
    #     self.actions[idx] = "update"

    #     self.frs[idx_tobemerged] = None
    #     self.tos[idx_tobemerged] = None
    #     self.row_cnts[idx_tobemerged] = None
    #     self.actions[idx_tobemerged] = "delete"
    #     self.action_params[idx_tobemerged] = None        
    #     return


    # def __sanity_check(self):
    #     n = len(self.pieces)
    #     listoflists = [ self.types, self.frs, self.tos, self.row_cnts, 
    #                    self.actions, self.action_params, self.dfs ]
    #     assert all([ n == len(l) for l in listoflists ])
    #     return


    # def __map_action_to_func(self,level):
    #     if level == 0:
    #         return self.__action_update
    #     elif level == 1:
    #         return self.__action_split
    #     elif level == 2:
    #         return self.__action_merge
    #     elif level == 3:
    #         return self.__action_save
    #     elif level == 4:
    #         return self.__action_delete
    #     elif level == 5:
    #         return self.__action_nothing
    #     else:
    #         print("No such action with level %i"%level)
    #         assert False            


    # def take_actions(self, max_level=0):
    #     """
    #     Never set max_level = 5, infinite loop.
    #     """
    #     # Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
    #     current_level = 0
    #     while current_level <= max_level:
    #         self.vprint("current_level:", current_level)
    #         action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
    #         action_indices = [ idx for idx in reversed(range(len(action_levels))) if action_levels[idx] == current_level ]
    #         # print("current_level:", current_level, "; max_level:", max_level)
    #         # print("self.verbose:", self.verbose)
    #         if len(action_indices) == 0:
    #             # print("take_actions", 1, "; current_level:", current_level)
    #             current_level += 1
    #         else: # len(action_indices) > 0:
    #             # print("take_actions", 2, "; current_level:", current_level, "; len(action_indices):", len(action_indices))
    #             for idx in action_indices:
    #                 # print("idx:", idx)
    #                 self.__map_action_to_func(current_level)(idx)
    #             current_level = 0
    #     return


    # def has_pending_actions(self):
    #         action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
    #         b = any(np.array(action_levels) != TSFEFS.action_domain.index(""))
    #         return b

    ############################################## Action: END ##############################################
    #########################################################################################################

    
    
    
    
    
    
    
    
    
    
    
    """ Use BaseFEFS's """
    ################################################################################################################
    ############################################## Import,Export: BEG ##############################################
    # def __no_overlapping_time_range(self):
    #     """
    #     ensure fr1 < to1 < fr2 < to2 < ...
    #     """
    #     df = pd.DataFrame({"fr":self.frs,"to":self.tos})
    #     df = df.sort_values(by="fr")
    #     orders = list(df.index)
    #     arr = np.array(df)
    #     arr = arr.reshape(-1).tolist() # becomes [fr1,to1,fr2,to2,...]
    #     assert all([ arr[i+1] >= arr[i] for i in range(len(arr)-1) ])
    #     return orders



    # """
    # Export all as a single df
    # """
    # def export_dataframe(self):

    #     orders = self.__no_overlapping_time_range()

    #     dfs = []

    #     # rearrange all the lists according to orders
    #     for idx in orders:

    #         df = self.dfs[idx]
    #         if df is None:
    #             df = self.__read_piece(idx)

    #         type_ = self.types[idx]
    #         if type_ == "csv":
    #             # dfs.append(self.dfs[idx])
    #             dfs.append(df)
    #         elif type_ == "tsfefs":
    #             # dfs.append(self.dfs[idx].export_dataframe())
    #             dfs.append(df.export_dataframe())
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False

    #     if len(dfs) == 0:
    #         return pd.DataFrame()

    #     df = pd.concat(dfs).reset_index(drop=True)
    #     return df



    # def export_dstfile(self, dstfile):
    #     df = self.export_dataframe()
    #     df.to_csv(dstfile,index=False)
    #     return



    # def export_dstfolder(self, dstfolder):
    #     """
    #     Don't allow if any of those frs and tos are None,
    #     otherwise can't name the dst files.
    #     """
    #     assert self.has_valid_status() # no None in the lists

    #     """
    #     Don't allow if there is pending actions, 
    #     otherwise can't copy from src to dst.
    #     """
    #     assert not self.has_pending_actions()


    #     if not os.path.isdir(dstfolder):
    #         os.mkdir(dstfolder)

    #     """
    #     The export format is e.g., "3. 2020-01-10 03:05:11 ~ 2020-01-10 03:05:11.csv"
    #     """
    #     fullpath = self.__compose_fullpath()
    #     for idx in range(len(self.pieces)):
    #         piece = self.pieces[idx]
    #         fr, to = self.frs[idx], self.tos[idx]

    #         src = "%s/%s"%(fullpath,piece)
    #         fr = fr.strftime(self.datetime_format)
    #         to = to.strftime(self.datetime_format)
    #         f = "%i. %s ~ %s.csv"%(idx,fr,to)
    #         dst = "%s/%s"%(dstfolder,f)
    #         os.system("cp \"%s\" \"%s\""%(src,dst))
    #     return



    # def import_dataframe(self, df):

    #     self.pieces = []
    #     self.types = []
    #     self.frs = []
    #     self.tos = []
    #     self.row_cnts = []

    #     self.actions = []
    #     self.action_params = []
    #     self.dfs = []

    #     self.cache = []

    #     df = df.sort_values(by=self.seq_col).reset_index(drop=True)
    #     self += df
    #     self.__action_split(0)
    #     return


    # def import_srcfile(self, srcfile):
    #     df = pd.read_csv(srcfile, dtype={self.seq_col:str})
    #     assert self.seq_col in df.columns
    #     df[self.seq_col] = df[self.seq_col].apply(lambda x: dt.strptime(x,self.datetime_format))        
    #     self.import_dataframe(df)
    #     return


    # def import_srcfolder(self, srcfolder):

    #     """
    #     List of files to be copied
    #     """
    #     files = os.listdir(srcfolder)
    #     if srcfolder[-1] == '/':
    #         srcfolder = srcfolder[:-1]
    #     files = [ "%s/%s"%(srcfolder,f) for f in files ]
    #     files = [ f for f in files if not os.path.isdir(f) ]


    #     """
    #     starts from nothing
    #     """
    #     self.pieces = []
    #     self.types = []
    #     self.frs = []
    #     self.tos = []
    #     self.row_cnts = []

    #     self.actions = []
    #     self.action_params = []
    #     self.dfs = []

    #     self.cache = []


    #     fullpath = self.__compose_fullpath()
    #     if not os.path.isdir(fullpath):
    #         os.mkdir(fullpath)


    #     """
    #     1. copy files to fullpath
    #     2. initialize the lists
    #     3. do everything as if done in __action_update but not calling __action_update
    #     """
    #     for idx,f in enumerate(files):
    #         src = f
    #         piece = self.gen_valid_piece(prefix="", suffix="")
    #         dst = "%s/%s"%(fullpath,piece)
    #         # print("Command:", "cp \"%s\" \"%s\""%(src,dst))
    #         os.system("cp \"%s\" \"%s\""%(src,dst))

    #         self.pieces += [piece]
    #         self.types += ["csv"]

    #         # dfs[idx] has to exist before calling __get_time(idx)
    #         self.dfs += [None]
    #         S = self.__get_time(idx)

    #         self.frs += [min(S)]
    #         self.tos += [max(S)]
    #         self.row_cnts += [len(S)]

    #         self.actions += [""] # the above info are filled, to avoid calling __action_update
    #         self.action_params += [None]

    #     self.fr = min(self.frs)
    #     self.to = max(self.tos)
    #     self.row_cnt = sum(self.row_cnts)
    #     return



    # def all_pieces_as_csv(self):

    #     dfs = []
    #     for idx in range(len(self.pieces)):
    #         type_ = self.types[idx]
    #         if type_ == "csv":
    #             continue
    #         elif type_ == "tsfefs":
    #             df = self.dfs[idx].export_dataframe()
    #             dfs += [df]
    #             self.actions[idx] = "delete"
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False
    #     self.take_actions(max_level=4)

    #     for df in dfs:
    #         self += df
    #         self.actions[len(self.actions)-1] = "split"
    #     self.take_actions(max_level=2)
    #     return
    ############################################## Import,Export: END ##############################################
    ################################################################################################################

    

    

    
    """ Use BaseFEFS's """
    ###################################################################################################################
    ############################################# Overriding __len__: BEG #############################################
    # def __len__(self):
    #     return self.row_cnt
    ############################################# Overriding __len__: END #############################################
    ###################################################################################################################
    
    

    
    
    
    
    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - general: BEG #####################################
    # def __idx_check(self,idx):
    #     assert isinstance(idx,int_types)

    #     while idx < 0:
    #         # idx += self.row_cnt
    #         idx += len(self)

    #     # if idx > self.row_cnt - 1:
    #     if idx > len(self) - 1:
    #         print("Index out of range")
    #         assert False

    #     return idx


    # def __indices_check(self,indices):

    #     assert isinstance(indices,list)
    #     assert all([ isinstance(idx,int_types) for idx in indices ])
    #     # assert len(indices) <= self.row_cnt
    #     assert len(indices) <= len(self)

    #     for i in range(len(indices)):
    #         idx = indices[i]
    #         while idx < 0:
    #             # idx += self.row_cnt
    #             idx += len(self)
    #         indices[i] = idx

    #     if len(indices) > len(set(indices)):
    #         print("Dupicated indices")
    #         assert False

    #     # assert all([ idx < self.row_cnt for idx in indices ])
    #     assert all([ idx < len(self) for idx in indices ])
    #     return indices


    # def __str_check(self,colname):
    #     assert isinstance(colname,str_types)

    #     if colname not in self.colnames:
    #         print("No such column name as %s, try update action"%colname)
    #         assert False


    # def __strs_check(self,colnames):

    #     assert isinstance(colnames,list)
    #     assert all([ isinstance(s,str_types) for s in colnames ])

    #     if len(set(colnames) - set(self.colnames)) > 0:
    #         print("Some requested column names don't exits.")
    #         print("You may consider the \"update\" action.")
    #         assert False

    # def __bools_check(self,B):
    #     assert isinstance(B,list)
    #     assert all([ isinstance(b,bool_types) for b in B ])
    #     # assert len(B) == self.row_cnt # has to be equal length
    #     assert len(B) == len(self) # has to be equal length


    # # Only for __setitems__'s strs and indices (and thus bools).
    # def __value_check(self, value, assignment_w, assignment_h):
    #     """
    #     Only for __setitems__'s strs and indices (and bools).
    #     Return 2D or 1D array, 
    #     in which the array by accessing arr[idx:(idx+n)] they will return the corresponding rows for assignments.
    #     """
    #     # check value
    #     if isinstance(value,(arr_types,pd.DataFrame)):
    #         value = np.array(value)
    #         if len(value.shape) == 1:
    #             if value.shape[0] == 1: # this is value = [x]
    #                 value = value[0] # will be turned to list 
    #             else:
    #                 assert value.shape[0] == assignment_w
    #         elif len(value.shape) == 2:
    #             assert (value.shape[1] == assignment_w) or (value.shape[1] == 1)
    #             assert (value.shape[0] == assignment_h) or (value.shape[0] == 1)                
    #         else: # at most 2 dimensional array, of course
    #             print("Dimension of the assignment:", value.shape)
    #             assert False
    #     elif isinstance(value,range):
    #         print(type(value), "type can only be assigned to pd.Series")
    #         assert False

    #     if not isinstance(value,(arr_types,pd.DataFrame)):
    #         value = [value]*assignment_h # turn singular to list 

    #     return value

    ##################################### Overriding Square Bracket - general: END #####################################
    ####################################################################################################################
   



    

    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - getitem: BEG #####################################

    # def __get_piece_idx(self, idx, orders):

    #     accum_row_cnt = 0
    #     _order = None
    #     for _ord in orders:
    #         row_cnt = self.row_cnts[_ord]
    #         if idx > row_cnt - 1:
    #             idx -= row_cnt
    #             accum_row_cnt += row_cnt
    #             continue
    #         _order = _ord
    #         break

    #     return (_order, accum_row_cnt)



    # def __getitem__using_idx(self,idx):

    #     idx = self.__idx_check(idx)        
    #     orders = self.__no_overlapping_time_range()
    #     _order, accum_row_cnt = self.__get_piece_idx(idx, orders)

    #     df = self.dfs[_order]
    #     if df is None:
    #         df = self.__read_piece(_order)
    #         self.dfs[_order] = df

    #     idx -= accum_row_cnt
    #     type_ = self.types[_order]
    #     if type_ == "csv":

    #         self.renew_idx(_order)

    #         return df.iloc[idx]
    #     elif type_ == "tsfefs":
    #         tsfefs = df

    #         self.renew_idx(_order)

    #         return tsfefs[idx]
    #     else:
    #         print("Type", type_, "not supported")
    #         assert False

    #     print("Error! Find it out!")
    #     assert False



    # def __getitem__using_str(self,colname):

    #     self.__str_check(colname)

    #     values = []
    #     orders = self.__no_overlapping_time_range()
    #     for _order in orders:
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)
    #             self.dfs[_order] = df
    #         values.extend(df[colname])
    #     S = pd.Series(values)
    #     S.name = colname
    #     return S


    # def __get_indices_in_same_piece(self, indices, orders):

    #     idx = indices[0]
    #     _order, accum_row_cnt = self.__get_piece_idx(idx, orders)            
    #     if _order is None:
    #         assert False

    #     idx_beg = accum_row_cnt
    #     idx_end = accum_row_cnt + self.row_cnts[_order]
    #     idx_range = range(idx_beg,idx_end)

    #     adjusted_indices = []
    #     for idx in indices:
    #         if idx in idx_range:
    #             adjusted_indices.append(idx-accum_row_cnt)
    #             continue
    #         break

    #     return (_order, adjusted_indices)



    # def __getitem__using_indices(self,indices):

    #     indices = self.__indices_check(indices)
    #     indices_copied = dc(indices)

    #     dfs = []
    #     orders = self.__no_overlapping_time_range()

    #     while len(indices) > 0:
    #         _order, adjusted_indices = self.__get_indices_in_same_piece(indices, orders)
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)

    #         type_ = self.types[_order]            
    #         if type_ == "csv":
    #             df_ = df.iloc[adjusted_indices]

    #             self.renew_idx(_order)

    #         elif type_ == "tsfefs":
    #             tsfefs = df
    #             df_ = tsfefs[adjusted_indices]

    #             self.renew_idx(_order)

    #         else:
    #             print("Type", type_, "not supported")
    #             assert False

    #         dfs.append(df_)
    #         indices = indices[len(adjusted_indices):]


    #     if len(dfs) == 0:
    #         return pd.DataFrame()

    #     df = pd.concat(dfs).reset_index(drop=True)
    #     df.index = indices_copied
    #     return df



    # def __getitem__using_bools(self,B):

    #     self.__bools_check(B)

    #     indices = np.array(range(len(B)))[B].tolist()
    #     return self.__getitem__using_indices(indices)


    # def __getitem__using_strs(self,colnames):

    #     self.__strs_check(colnames)

    #     dfs = []
    #     indices = []
    #     cum_row_cnt = 0
    #     orders = self.__no_overlapping_time_range()
    #     for _order in orders:
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)
    #             self.dfs[_order] = df
    #         dfs.append(df[colnames])
    #         # indices += list(np.array(df.index) + cum_row_cnt)
    #         indices += list(np.array(TSFEFS.get_index(df)) + cum_row_cnt)
    #         cum_row_cnt += self.row_cnts[_order]

    #     if len(dfs) == 0:
    #         return pd.DataFrame()

    #     df = pd.concat(dfs).reset_index(drop=True)
    #     df.index = indices
    #     return df


    # def __getitem__(self, key):
    #     """
    #     Accetpable queries:
    #     Single:
    #      - int: tsfefs[33]
    #      - str: tsfefs["age"]
    #     Multiple:
    #      - slice of ints: tsfefs[20:], tsfefs[20:50], tsfefs[::2], ...
    #      - list of ints: tsfefs[[1,2,5,10]]
    #      - list of strs: tsfefs[["time","purchased","item","price"]]
    #      - list of bools: tsfefs[[True,False,True,...]] # must have length == row_cnt
    #     """        
    #     if isinstance(key,str_types):
    #         return self.__getitem__using_str(key)
    #     elif isinstance(key,int_types):
    #         return self.__getitem__using_int(key)
    #     elif isinstance(key,(arr_types,slice)):
    #         if isinstance(key,np.ndarray):
    #             assert len(key.shape) == 1 # must be in (n,)
    #             key = list(key)
    #         elif isinstance(key,pd.Series):
    #             key = list(key)
    #         elif isinstance(key,slice):
    #             assert all([ (s is None) or isinstance(s,int) for s in [ key.start,key.stop,key.step ] ])
    #             key = list(range(self.row_cnt))[key]


    #         if len(key) == 0:
    #             return pd.DataFrame()


    #         type_ = type(key[0])
    #         assert isinstance(key[0],(int_types,str_types,bool_types))
    #         assert all([ isinstance(k,type_) for k in key ])

    #         if type_ in str_types:
    #             return self.__getitem__using_strs(key)
    #         elif type_ in bool_types:
    #             return self.__getitem__using_bools(key)
    #         elif type_ in int_types:
    #             return self.__getitem__using_indices(key)
    #         else:
    #             print("No such type" )
    #             assert False
    #     else:
    #         print("Something's wrong for the type(s)")
    #         assert False

    ##################################### Overriding Square Bracket - getitem: END #####################################
    ####################################################################################################################



    
    
    
    
    
    
    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - setitem: BEG #####################################

    # def __setitem__using_idx(self, idx, value):

    #     idx = self.__idx_check(idx)

    #     # if type(value) in [list, np.ndarray, pd.Series]:
    #     if isinstance(value, arr_types):
    #         arr = np.array(value)
    #         assert len(arr.shape) == 1 # like (4,)
    #         col_len = arr.shape[0]
    #         assert col_len == len(self.colnames)
    #     else:
    #         pass # you can put any type of single value to the idx row.

    #     orders = self.__no_overlapping_time_range()
    #     for _order in orders:
    #         row_cnt = self.row_cnts[_order]
    #         if idx > row_cnt - 1:
    #             idx -= row_cnt
    #             continue
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)
    #         self.actions[_order] = "update"
    #         type_ = self.types[_order]
    #         if type_ == "csv":
    #             df.iloc[idx] = value
    #             self.dfs[_order] = df
    #         elif type_ == "tsfefs":
    #             tsfefs = df
    #             tsfefs[idx] = value
    #             self.dfs[_order] = tsfefs
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False

    #         self.renew_idx(_order)
    #         return

    #     print("Error! Find it out!")
    #     assert False
    #     return


    # def __setitem__using_str(self, colname, value):
    #     # print("self.path, self.name")
    #     # print(self.path, self.name)
    #     # print(type(value))
    #     # print(value)

    #     # don't use the same checking routine here
    #     # self.__str_check(colname)

    #     assert isinstance(colname,str_types)

    #     # if type(value) in [list,np.ndarray,pd.Series]:
    #     if isinstance(value,(arr_types,range)):
    #         assert len(value) == self.row_cnt
    #         value = list(value)
    #     elif isinstance(value,pd.DataFrame):
    #         assert len(value) == self.row_cnt
    #         assert len(value.columns) == 1
    #         col = list(value.columns)[0]
    #         value = np.array(value[col])
    #     else:
    #         value = [value]*self.row_cnt

    #     cnt = 0
    #     orders = self.__no_overlapping_time_range()
    #     for _order in orders:
    #         idx = _order
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(idx)

    #         cnt_new = cnt + self.row_cnts[_order]
    #         df[colname] = value[ cnt : cnt_new ]
    #         # print("type(df)")
    #         # print(type(df))
    #         # print("df[colname]")
    #         # print(df[colname])
    #         cnt = cnt_new
    #         self.dfs[_order] = df
    #         self.actions[_order] = "update"

    #     # print()
    #     # print()
    #     if colname not in self.colnames:
    #         self.colnames += [colname]
    #     return



    # def __setitem__using_indices(self,indices,value):

    #     indices = self.__indices_check(indices)
    #     value = self.__value_check(value, len(self.colnames), len(indices))
    #     orders = self.__no_overlapping_time_range()

    #     cnt = 0
    #     for idx in indices:
    #         df_ = None
    #         for _order in orders:
    #             row_cnt = self.row_cnts[_order]
    #             if idx > row_cnt - 1:
    #                 idx -= row_cnt
    #                 continue
    #             df = self.dfs[_order]
    #             if df is None:
    #                 df = self.__read_piece(_order)

    #             type_ = self.types[_order]
    #             if type_ == "csv":
    #                 df_ = df.iloc[idx:(idx+1)]
    #             elif type_ == "tsfefs":
    #                 tsfefs = df
    #                 df_ = tsfefs[idx:(idx+1)]
    #             else:
    #                 print("Type", type_, "is not supported")
    #                 assert False
    #             break

    #         if len(df_) == 0:
    #             print("Indice could be out of range")
    #             assert False

    #         cnt_new = cnt + 1
    #         if type_ == "csv":
    #             df.iloc[[idx]] = value[ cnt : cnt_new ]
    #             self.dfs[_order] = df
    #         elif type_ == "tsfefs":
    #             tsfefs[[idx]] = value[ cnt : cnt_new ]
    #             self.dfs[_order] = tsfefs
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False
    #         cnt = cnt_new 
    #         self.actions[_order] = "update"

    #         self.renew_idx(_order)

    #     return


    # def __setitem__using_bools(self,B,value):
    #     self.__bools_check(B)
    #     indices = np.array(range(len(B)))[B].tolist()
    #     self.__setitem__using_indices(indices,value)
    #     return


    # def __setitem__using_strs(self,colnames,value):

    #     self.__strs_check(colnames)
    #     value = self.__value_check(value, len(colnames), self.row_cnt)
    #     orders = self.__no_overlapping_time_range()

    #     cum_row_cnt = 0
    #     for _order in orders:
    #         idx = _order
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(idx)
    #         new_cum_row_cnt = cum_row_cnt + self.row_cnts[_order]
    #         df[colnames] = value[cum_row_cnt:new_cum_row_cnt]
    #         self.dfs[_order] = df
    #         cum_row_cnt = new_cum_row_cnt
    #         self.actions[_order] = "update"
    #     return


    # def __setitem__(self, key, value):
    #     """
    #     Accetpable assignments:
    #     Single:
    #      - int: tsfefs[33] = value # row index 33 assignment
    #      - str: tsfefs["age"] = value
    #     Multiple:
    #      - slice of ints: tsfefs[20:] = value, tsfefs[20:50] = value, tsfefs[::2] = value, ...
    #      - list of ints: tsfefs[[1,2,5,10]] = value
    #      - list of strs: tsfefs[["time","purchased","item","price"]] = value
    #      - list of bools: tsfefs[[True,False,True,...]] = value # must have length == row_cnt
    #     """        
    #     if isinstance(key,str_types):
    #         self.__setitem__using_str(key, value)
    #     elif isinstance(key,int_types):
    #         self.__setitem__using_idx(key, value)
    #     elif isinstance(key,(arr_types,slice)):
    #         if isinstance(key,np.ndarray):
    #             assert len(key.shape) == 1 # must be in (n,)
    #             key = list(key)
    #         elif isinstance(key,pd.Series):
    #             key = list(key)
    #         elif isinstance(key,slice):                
    #             assert all([ (s is None) or isinstance(s,int) for s in [ key.start,key.stop,key.step ] ])
    #             key = list(range(self.row_cnt))[key]

    #         type_ = type(key[0])
    #         assert isinstance(key[0],(int_types,str_types,bool_types))
    #         assert all([ isinstance(k,type_) for k in key ])

    #         if type_ in str_types:
    #             self.__setitem__using_strs(key,value)
    #         elif type_ in bool_types:
    #             self.__setitem__using_bools(key,value)
    #         elif type_ in int_types:
    #             self.__setitem__using_indices(key,value)
    #         else:
    #             print("No such type" )
    #             assert False
    #     else:
    #         print("Something's wrong for the type(s)")
    #         assert False

    #     self.take_actions(max_level=0)
    #     return
    ##################################### Overriding Square Bracket - setitem: END #####################################
    ####################################################################################################################

    
    
    
    

  



    """ Use BaseFEFS's """
    ####################################################################################################################
    ##################################### Overriding Square Bracket - delitem: BEG #####################################

    # def __delitem__using_idx(self,idx):

    #     idx = self.__idx_check(idx)        
    #     orders = self.__no_overlapping_time_range()
    #     _order, accum_row_cnt = self.__get_piece_idx(idx, orders)

    #     df = self.dfs[_order]
    #     if df is None:
    #         df = self.__read_piece(_order)
    #         self.dfs[_order] = df

    #     idx -= accum_row_cnt
    #     type_ = self.types[_order]
    #     row_cnt = self.row_cnts[_order]
    #     len_ = None
    #     if type_ == "csv":
    #         """
    #         Since there's no del df[idx] for dataframe
    #         """
    #         B = [True]*row_cnt
    #         B[idx] = False
    #         df = df[B].reset_index(drop=True)
    #         len_ = len(df)
    #     elif type_ == "tsfefs":
    #         tsfefs = df
    #         del tsfefs[idx]
    #         tsfefs.take_actions(max_level=4)
    #         # len_ = tsfefs.row_cnt  # since action is taken, this tsfefs' row_cnt is updated.
    #         len_ = len(tsfefs)  # since action is taken, this tsfefs' row_cnt is updated.
    #         df = tsfefs
    #     else:
    #         print("Type", type_, "not supported")
    #         assert False

    #     self.dfs[_order] = df
    #     self.actions[_order] = "update"

    #     """
    #     Should it be renewed in cache?
    #     2023-10-07, decided not to renew in cache.
    #     """
    #     # self.renew_idx(_order) 

    #     if len_ == 0:
    #         self.actions[_order] = "delete"
    #     return 


    # def __delitem__using_str(self,colname):

    #     self.__str_check(colname)
    #     assert colname != self.time_col

    #     orders = self.__no_overlapping_time_range()
    #     for _order in orders:
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)
    #         del df[colname]
    #         self.dfs[_order] = df
    #         self.actions[_order] = "save"
    #     self.colnames.remove(colname)
    #     return    


    # def __delitem__using_indices(self,indices):

    #     indices = self.__indices_check(indices)
    #     indices_copied = dc(indices)

    #     orders = self.__no_overlapping_time_range()

    #     while len(indices) > 0:
    #         _order, adjusted_indices = self.__get_indices_in_same_piece(indices, orders)
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)

    #         type_ = self.types[_order]
    #         if type_ == "csv":
    #             row_cnt = self.row_cnts[_order]
    #             B = np.array([True]*row_cnt)
    #             B[adjusted_indices] = False
    #             df = df[B].reset_index(drop=True)
    #             len_ = len(df)
    #         elif type_ == "tsfefs":
    #             tsfefs = df
    #             del tsfefs[adjusted_indices]
    #             tsfefs.take_actions(max_level=4)
    #             len_ = tsfefs.row_cnt  # since action is taken, this tsfefs' row_cnt is updated.
    #             df = tsfefs
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False

    #         self.dfs[_order] = df
    #         self.actions[_order] = "update"

    #         """
    #         Should it be renewed in cache?
    #         2023-10-07, decided not to renew in cache.
    #         """
    #         # self.renew_idx(_order)

    #         if len_ == 0:
    #             self.actions[_order] = "delete"

    #         indices = indices[len(adjusted_indices):]
    #     return


    # def __delitem__using_bools(self,B):

    #     self.__bools_check(B)

    #     indices = np.array(range(len(B)))[B].tolist()
    #     self.__delitem__using_indices(indices)
    #     return    


    # def __delitem__using_strs(self,colnames):

    #     self.__strs_check(colnames)
    #     assert self.time_col not in colnames

    #     orders = self.__no_overlapping_time_range()
    #     for _order in orders:
    #         df = self.dfs[_order]
    #         type_ = self.types[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)

    #         if type_ == "csv":
    #             for col in colnames:
    #                 del df[col]
    #         elif type_ == "tsfefs":
    #             del df[colnames]
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False

    #         self.dfs[_order] = df
    #         self.actions[_order] = "save"

    #     for col in colnames:
    #         self.colnames.remove(col)
    #     return


    # def __delitem__(self, key):
    #     """
    #     Accetpable delete:
    #     Single:
    #      - int: del tsfefs[33]
    #      - str: del tsfefs["age"]
    #     Multiple:
    #      - slice of ints: del tsfefs[20:], del tsfefs[20:50], del tsfefs[::2], ...
    #      - list of ints: del tsfefs[[1,2,5,10]]
    #      - list of strs: del tsfefs[["time","purchased","item","price"]]
    #      - list of bools: del tsfefs[[True,False,True,...]] # must have length == row_cnt
    #     """        
    #     if isinstance(key,str_types):
    #         self.__delitem__using_str(key)
    #         return
    #     elif isinstance(key,int_types):
    #         self.__delitem__using_int(key)
    #         return
    #     elif isinstance(key,(arr_types,slice)):
    #         if isinstance(key,np.ndarray):
    #             assert len(key.shape) == 1 # must be in (n,)
    #             key = list(key)
    #         elif isinstance(key,pd.Series):
    #             key = list(key)
    #         elif isinstance(key,slice):                
    #             assert all([ (s is None) or isinstance(s,int) for s in [ key.start,key.stop,key.step ] ])
    #             key = list(range(self.row_cnt))[key]

    #         type_ = type(key[0])
    #         assert isinstance(key[0],(int_types,str_types,bool_types))
    #         assert all([ isinstance(k,type_) for k in key ])

    #         if type_ in str_types:
    #             self.__delitem__using_strs(key)
    #             return
    #         elif type_ in bool_types:
    #             self.__delitem__using_bools(key)
    #             return
    #         elif type_ in int_types:
    #             self.__delitem__using_indices(key)
    #             return
    #         else:
    #             print("No such type" )
    #             assert False
    #     else:
    #         print("Something's wrong for the type(s)")
    #         assert False

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

    #     piece = self.pieces[idx]
    #     fullname = self.__compose_piece_fullname(piece)

    #     if self.types[idx] == "csv":

    #         seq_col = self.seq_col
    #         dtf = self.datetime_format
    #         df = pd.read_csv(fullname, usecols=[seq_col], dtype={seq_col:str})
    #         df[seq_col] = df[seq_col].apply(lambda x: dt.strptime(x,dtf))
    #         df = df.sort_values(by=seq_col).reset_index(drop=True)
    #         S = df[seq_col]

    #     elif self.types[idx] == "tsfefs":

    #         tsfefs = TSFEFS()
    #         tsfefs.read(fullname)
    #         S = []
    #         # for order_ in tsfefs.__no_overlapping_time_range():
    #         #     S.extend(list(tsfefs.__read_time(order_)))
    #         # S = pd.Series(S)
    #         S = tsfefs.__get_times()

    #     else:
    #         print("File type \"tsfefs\" not yet implemented")
    #         assert False

    #     return S

    # def __get_time(self, idx):

    #     df = self.dfs[idx]
    #     type_ = self.types[idx]
    #     if df is None:
    #         S = self.__read_time(idx)
    #         return S

    #     if type_ == "csv":
    #         S = df[self.seq_col]
    #     elif type_ == "tsfefs":
    #         tsfefs = df
    #         S = tsfefs.__get_times()
    #     else:
    #         assert False

    #     return S


    # def __get_times(self):

    #     S = []
    #     for order_ in self.__no_overlapping_time_range():
    #         S_ = self.__get_time(order_)
    #         S_ = list(S_)
    #         S.extend(S_)
    #     S = pd.Series(S)
    #     return S



    # def __eq_series(self, S):
    #     assert self.row_cnt == len(S)
    #     assert [ isinstance(ts,dt) for ts in S ]        
    #     S_ = self.__get_times()
    #     B = S_ == S
    #     return B


    # def __eq_tsfefs(self, tsfefs):
    #     assert self.seq_col == tsfefs.seq_col
    #     assert self.row_cnt == tsfefs.row_cnt

    #     S = tsfefs.__get_times()
    #     return self.__eq_series(S)


    # def __eq_df(self, df):
    #     assert self.row_cnt == len(df)
    #     S = df[self.seq_col]
    #     return self.__eq_series(S)


    # def __eq_dt(self, ts):
    #     S = self.__get_times()
    #     return S == ts                


    # def __eq__(self, other):
    #     if isinstance(other,pd.Series):
    #         return self.__eq_series(other)
    #     elif isinstance(other,TSFEFS):
    #         return self.__eq_tsfefs(other)
    #     elif isinstance(other,pd.DataFrame):
    #         return self.__eq_df(other)
    #     elif isinstance(other,dt):
    #         return self.__eq_dt(other)
    #     else:
    #         print("Types", type(other), "is not supported.")
    #         assert False



    # def __ne_series(self, S):
    #     assert self.row_cnt == len(S)
    #     assert [ isinstance(ts,dt) for ts in S ]        
    #     S_ = self.__get_times()
    #     B = S_ != S
    #     return B


    # def __ne_tsfefs(self, tsfefs):
    #     assert self.seq_col == tsfefs.seq_col
    #     assert self.row_cnt == tsfefs.row_cnt

    #     S = tsfefs.__get_times()
    #     return self.__ne_series(S)


    # def __ne_df(self, df):
    #     assert self.row_cnt == len(df)
    #     S = df[self.seq_col]
    #     return self.__ne_series(S)


    # def __ne_dt(self, ts):
    #     S = self.__get_times()
    #     return S != ts                


    # def __ne__(self, other):
    #     if isinstance(other,pd.Series):
    #         return self.__ne_series(other)
    #     elif isinstance(other,TSFEFS):
    #         return self.__ne_tsfefs(other)
    #     elif isinstance(other,pd.DataFrame):
    #         return self.__ne_df(other)
    #     elif isinstance(other,dt):
    #         return self.__ne_dt(other)
    #     else:
    #         print("Types", type(other), "is not supported.")
    #         assert False




    # def __lt_series(self, S):
    #     assert self.row_cnt == len(S)
    #     assert [ isinstance(ts,dt) for ts in S ]
    #     S_ = self.__get_times()
    #     B = S_ < S
    #     return B

    # def __lt_tsfefs(self, tsfefs):
    #     assert self.seq_col == tsfefs.seq_col
    #     assert self.row_cnt == tsfefs.row_cnt        
    #     # return self[self.seq_col] < tsfefs[self.seq_col]
    #     S = tsfefs.__get_times()
    #     return self.__lt_series(S)

    # def __lt_df(self, df):
    #     assert self.row_cnt == len(df)
    #     # return self[self.seq_col] < df[self.seq_col]
    #     S = df[self.seq_col]
    #     return self.__lt_series(S)

    # def __lt_dt(self, ts):
    #     # return self[self.seq_col] < ts
    #     S = self.__get_times()
    #     B = S < ts
    #     return B

    # def __lt__(self, other):
    #     if isinstance(other,pd.Series):
    #         return self.__lt_series(other)
    #     elif isinstance(other,TSFEFS):
    #         return self.__lt_tsfefs(other)
    #     elif isinstance(other,pd.DataFrame):
    #         return self.__lt_df(other)
    #     elif isinstance(other,dt):
    #         return self.__lt_dt(other)
    #     else:
    #         print("Types", type(other), "is not supported.")
    #         assert False



    # def __gt_series(self, S):
    #     assert self.row_cnt == len(S)
    #     assert [ isinstance(ts,dt) for ts in S ]
    #     S_ = self.__get_times()
    #     B = S_ > S
    #     return B

    # def __gt_tsfefs(self, tsfefs):
    #     assert self.seq_col == tsfefs.seq_col
    #     assert self.row_cnt == tsfefs.row_cnt
    #     # return self[self.seq_col] > tsfefs[self.seq_col]
    #     S = tsfefs.__get_times()
    #     return self.__gt_series(S)        

    # def __gt_df(self, df):
    #     assert self.row_cnt == len(df)
    #     # return self[self.seq_col] > df[self.seq_col]
    #     S = df[self.seq_col]
    #     return self.__gt_series(S)        

    # def __gt_dt(self, ts):
    #     # return self[self.seq_col] > ts
    #     S = self.__get_times()
    #     return S > ts

    # def __gt__(self, other):
    #     if isinstance(other,pd.Series):
    #         return self.__gt_series(other)
    #     elif isinstance(other,TSFEFS):
    #         return self.__gt_tsfefs(other)
    #     elif isinstance(other,pd.DataFrame):
    #         return self.__gt_df(other)
    #     elif isinstance(other,dt):
    #         return self.__gt_dt(other)
    #     else:
    #         print("Types", type(other), "is not supported.")
    #         assert False


    # def __le_series(self, S):
    #     assert self.row_cnt == len(S)
    #     assert [ isinstance(ts,dt) for ts in S ]
    #     S_ = self.__get_times()
    #     B = S_ <= S
    #     return B

    # def __le_tsfefs(self, tsfefs):
    #     assert self.seq_col == tsfefs.seq_col
    #     assert self.row_cnt == tsfefs.row_cnt
    #     # return self[self.seq_col] <= tsfefs[self.seq_col]
    #     S = tsfefs.__get_times()
    #     return self.__le_series(S)

    # def __le_df(self, df):
    #     assert self.row_cnt == len(df)
    #     # return self[self.seq_col] <= df[self.seq_col]
    #     S = df[self.seq_col]
    #     return self.__le_series(S)

    # def __le_dt(self, ts):
    #     # return self[self.seq_col] <= ts
    #     S = self.__get_times()
    #     return S <= ts

    # def __le__(self, other):
    #     if isinstance(other,pd.Series):
    #         return self.__le_series(other)
    #     elif isinstance(other,TSFEFS):
    #         return self.__le_tsfefs(other)
    #     elif isinstance(other,pd.DataFrame):
    #         return self.__le_df(other)
    #     elif isinstance(other,dt):
    #         return self.__le_dt(other)
    #     else:
    #         print("Types", type(other), "is not supported.")
    #         assert False


    # def __ge_series(self, S):
    #     assert self.row_cnt == len(S)
    #     assert [ isinstance(ts,dt) for ts in S ]
    #     S_ = self.__get_times()
    #     B = S_ >= S
    #     return B

    # def __ge_tsfefs(self, tsfefs):
    #     assert self.seq_col == tsfefs.seq_col
    #     assert self.row_cnt == tsfefs.row_cnt        
    #     # return self[self.seq_col] >= tsfefs[self.seq_col]
    #     S = tsfefs.__get_times()
    #     return self.__ge_series(S)

    # def __ge_df(self, df):
    #     assert self.row_cnt == len(df)
    #     # return self[self.seq_col] >= df[self.seq_col]
    #     S = df[self.seq_col]
    #     return self.__ge_series(S)

    # def __ge_dt(self, ts):
    #     # return self[self.seq_col] >= ts
    #     S = self.__get_times()
    #     return S >= ts

    # def __ge__(self, other):
    #     if isinstance(other,pd.Series):
    #         return self.__ge_series(other)
    #     elif isinstance(other,TSFEFS):
    #         return self.__ge_tsfefs(other)
    #     elif isinstance(other,pd.DataFrame):
    #         return self.__ge_df(other)
    #     elif isinstance(other,dt):
    #         return self.__ge_dt(other)
    #     else:
    #         print("Types", type(other), "is not supported.")
    #         assert False

    ############################################# Overriding ==, >, <, >=, <=: END #######################################
    ######################################################################################################################

 








    """ Use BaseFEFS's """
    #####################################################################################################################
    ################################################# Overriding +: BEG #################################################
    # def __add_tsfefs(self, tsfefs):
    #     assert self.time_col == tsfefs.time_col
    #     assert len(set(self.colnames) - set(tsfefs.colnames)) == 0
    #     assert len(set(tsfefs.colnames) - set(self.colnames)) == 0
    #     assert self.datetime_format == tsfefs.datetime_format


    #     # assert not tsfefs.has_pending_actions()

    #     assert tsfefs.has_valid_status() # no None in the lists
    #     assert not tsfefs.conflict_exist()



    #     # the added tsfefs must be under the current path
    #     """
    #     tsfefs = dc(tsfefs)
    #     tsfefs.path = self.__compose_fullpath()
    #     """
    #     tsfefs = tsfefs.clone(self.__compose_fullpath(),tsfefs.name)
    #     if tsfefs.name in self.pieces:
    #         tsfefs.name = self.gen_valid_piece(prefix=tsfefs.name+"_", suffix="")
    #     self.pieces += [tsfefs.name]
    #     self.types += ["tsfefs"]
    #     # self.frs += [tsfefs.fr]
    #     # self.tos += [tsfefs.to]
    #     # self.row_cnts += [tsfefs.row_cnt]
    #     self.frs += [None]
    #     self.tos += [None]
    #     self.row_cnts += [None]
    #     self.actions += ["update"]
    #     self.action_params += [None]
    #     self.dfs += [tsfefs] # the elements can be df or TSFEFS        

    #     self.include_idx(len(self.pieces)-1) # len(self.pieces)-1: the new highest array idx

    #     return self


    # def __add_df(self, df):
    #     assert self.time_col in df.columns
    #     assert len(set(self.colnames) - set(df.columns)) == 0
    #     assert len(set(df.columns) - set(self.colnames)) == 0
    #     try:
    #         dtf = self.datetime_format
    #         # print("__add_df:", "dtf")
    #         # print(dtf)
    #         _ = df[self.time_col].apply(lambda x: x.strftime(dtf))
    #     except:
    #         print("Problem with df's datetime")
    #         assert False

    #     piece = self.gen_valid_piece(prefix="", suffix="")


    #     self.vprint("Newly added file:", piece)


    #     self.pieces += [piece]
    #     self.types += ["csv"]
    #     # self.frs += [tsfefs.fr]
    #     # self.tos += [tsfefs.to]
    #     # self.row_cnts += [tsfefs.row_cnt]
    #     self.frs += [None]
    #     self.tos += [None]
    #     self.row_cnts += [None]
    #     self.actions += ["update"]
    #     self.action_params += [None]
    #     self.dfs += [df] # the elements can be df or TSFEFS

    #     self.include_idx(len(self.pieces)-1) # len(self.pieces)-1: the new highest array idx

    #     return self


    # def __add__(self, other):
    #     # print(type(other))
    #     if isinstance(other,TSFEFS):
    #         return self.__add_tsfefs(other)
    #     elif isinstance(other,pd.DataFrame):
    #         return self.__add_df(other)
    #     else:
    #         print("Types", type(other), "not supported.")
    #         assert False
    ################################################# Overriding +: END #################################################
    #####################################################################################################################



    
    
    
    
    
    
    
    
    
    

    """ Use BaseFEFS's """
    ####################################################################################################################
    #################################################### MERGE: BEG ####################################################
    # # # @classmethod
    # # # def merge(cls, left_obj, right_obj, path="", name="", on="", target=None):
    # # @staticmethod
    # # def merge(left_obj, right_obj, path="", name="", on="", target=None):
    # #     """
    # #     1. For class merge, requiring a new path and a new name.
    # #     2. The left_obj has to be TSFEFS.
    # #     3. The right_obj can be (pd.DataFrame, TSFEFS)
    # #     4. Target is the targeted columns for the right_obj.
    # #     """
    # #     assert (path is not None) and (name is not None) and (on is not None)
    # #     assert "" not in [path, name, on]
    # #     assert isinstance(left_obj,TSFEFS)
    # #     assert isinstance(right_obj,(TSFEFS,pd.DataFrame))
    # #     tsfefs = left_obj.merge(right_obj, on, target=target, path=path, name=name)
    # #     return tsfefs


    # def __get_tsfefs_reference(self,path,name):
    #     if path is None:
    #         assert name is None
    #         tsfefs = self
    #     elif path is not None:
    #         assert name is not None            
    #         tsfefs = self.clone(path, name)
    #     else:
    #         assert False
    #     return tsfefs



    # def merge_with_df(self, df_another, on, target):

    #     """
    #     !!!!!!!!!!! Important !!!!!!!!!!!
    #     I am struggling with the following implementation.
    #     Ideally, the merge should be done by merging pieces, 
    #     i.e.,
    #         pd.merge(df_piece, df_another)
    #     instead of 
    #         pd.merge(self[on], df_another)

    #     This current implementation is for the sake of convenience,
    #     !!! CHANGE ANYTIME FEELING SLOW !!!
    #     """

    #     # orders = self.__no_overlapping_time_range()

    #     df_self_on = self[on]
    #     df_another_on = df_another[on+target].drop_duplicates().reset_index(drop=True)
    #     # print("1. df_another_on")
    #     # print(df_another_on)
    #     # print()

    #     on_col = "__on__"
    #     delim = "@$*^%"
    #     df_self_on[on_col] = df_self_on.apply(lambda x: delim.join([ str(xx) for xx in x ]), axis=1)
    #     df_self_on = df_self_on[[on_col]]
    #     df_another_on[on_col] = df_another_on[on].apply(lambda x: delim.join([ str(xx) for xx in x ]), axis=1)
    #     df_another_on = df_another_on[[on_col] + target]
    #     # print("2. df_another_on")
    #     # print(df_another_on)
    #     # print()

    #     """
    #     Check 1-to-1
    #     """
    #     df_uon = df_self_on[[on_col]]
    #     df_uon = df_uon.drop_duplicates().reset_index(drop=True) 
    #     n = len(df_uon)
    #     # print("len(df_uon):", n)
    #     # print()
    #     # print("df_uon")
    #     # print(df_uon)
    #     # print()
    #     df_one2one = pd.merge(df_uon, df_another_on, on=on_col, how="left").reset_index(drop=True)

    #     # no NA is allowed.
    #     df_one2one = df_one2one.dropna().reset_index(drop=True)
    #     # print("len(df_one2one):", len(df_one2one))
    #     assert len(df_one2one) == n 

    #     # no duplicate is allowed.
    #     df_one2one = df_one2one.drop_duplicates().reset_index(drop=True) 
    #     # print("len(df_one2one):", len(df_one2one))
    #     assert len(df_one2one) == n


    #     """
    #     real merge
    #     """
    #     df_target = pd.merge(df_self_on, df_another_on, on=on_col, how="left").reset_index(drop=True)
    #     # just to reassure
    #     assert len(df_self_on) == len(df_target)

    #     # the order of df_target must be the same
    #     for col in target:
    #         # setting new columns 
    #         assert col not in self.colnames
    #         self[col] = df_target[col]

    #     return self



    # def merge_with_tsfefs(self, tsfefs_another, on, target):
    #     return self.merge_with_df(tsfefs_another[on+target], on, target)



    # def merge(self, right_obj, on, target=None, path=None, name=None):

    #     """
    #     on: 
    #      (self[on[0]] == right_obj[on[0]]) and 
    #      (self[on[1]] == right_obj[on[1]]) and
    #      ...

    #      E.g., on=["cusID","branch"]

    #     target:
    #       - If None then all the remaining cols in right_obj will be merged
    #       - Else:

    #         E.g., on=["cusID","branch"], target=["gender","citizen"]
    #         Notice that for a unique comb of on, there cannot be more than 1 comb from the targets.
    #     """


    #     tsfefs = self.__get_tsfefs_reference(path,name)


    #     assert isinstance(on,(str,list))
    #     if isinstance(on,list):
    #         assert all([ isinstance(col,str) for col in on ])
    #     else:
    #         on = [on]


    #     if isinstance(right_obj, pd.DataFrame):
    #         right_cols = list(right_obj.columns)
    #     elif isinstance(right_obj, TSFEFS):
    #         """ if TSFEFS, its time_col should be a dummy"""
    #         right_cols = dc(right_obj.colnames)
    #         right_cols.remove(right_obj.time_col)
    #     else:
    #         assert False


    #     # validity of on
    #     assert len(set(on) - set(tsfefs.colnames)) == 0
    #     assert len(set(on) - set(right_cols)) == 0
    #     right_cols = list(set(right_cols) - set(on))


    #     # validity of target
    #     if target is not None:
    #         assert isinstance(target,(str,list))
    #         if isinstance(target,str):
    #             target = [target]
    #         else:
    #             assert all([ isinstance(col,str) for col in target ])
    #         assert len(set(target) - set(right_cols)) == 0
    #     else:
    #         target = right_cols



    #     # on and target should be mutually exclusive
    #     assert len(target) + len(on) == len(set(target+on))

    #     if isinstance(right_obj, pd.DataFrame):
    #         tsfefs = tsfefs.merge_with_df(right_obj, on, target)
    #     elif isinstance(right_obj, TSFEFS):
    #         tsfefs = tsfefs.merge_with_tsfefs(right_obj, on, target)
    #     else:
    #         assert False

    #     return tsfefs
    #################################################### MERGE: END ####################################################
    ####################################################################################################################
    
    
    
    
    
    
    
    
    
    
    
    
    """ Use BaseFEFS's """
    #####################################################################################################################
    ########################################## conflict resolve, optimize: BEG ##########################################

    # def conflict_exist(self):
    #     try:
    #         _ = self.__no_overlapping_time_range()
    #         return False
    #     except:
    #         return True


    # def resolve_conflict(self):

    #     while self.conflict_exist():

    #         df_index, orders = self.dump_index_df() # already sorted by "fr"
    #         self = self.load_index_df(df_index, orders)

    #         for idx in range(len(self.pieces)-1):


    #             if self.frs[idx+1] < self.tos[idx]:

    #                 # print("Delay no more")

    #                 df0 = self.dfs[idx]
    #                 if df0 is None:
    #                     df0 = self.__read_piece(idx)
    #                     self.dfs[idx] = df0

    #                 df1 = self.dfs[idx+1]
    #                 if df1 is None:
    #                     df1 = self.__read_piece(idx+1)
    #                     self.dfs[idx+1] = df1

    #                 B0 = df0[self.time_col] < self.frs[idx+1]                          
    #                 df0a = df0[B0].reset_index(drop=True)
    #                 df0b = df0[~B0].reset_index(drop=True)

    #                 B1 = df1[self.time_col] <= self.tos[idx]
    #                 df1a = df1[B1].reset_index(drop=True)
    #                 df1b = df1[~B1].reset_index(drop=True)

    #                 self.actions[idx] = "delete"
    #                 self.actions[idx+1] = "delete"

    #                 df_new = pd.concat([df0b,df1a]).reset_index(drop=True)
    #                 df_new = df_new.sort_values(by=self.time_col).reset_index(drop=True)

    #                 if len(df0a) > 0:
    #                     self += df0a
    #                 if len(df1b) > 0:
    #                     self += df1b
    #                 if len(df_new) > 0:
    #                     self += df_new
    #                 break

    #         self.take_actions(max_level=4)



    # def optimize_files(self):

    #     while True:


    #         df_index, orders = self.dump_index_df() # already sorted by "fr"
    #         self = self.load_index_df(df_index, orders)

    #         idx = 0
    #         pieces_len_adjustment = 0
    #         while idx < len(self.pieces):

    #             # print("2nd while loop, idx:", idx)
    #             # print("2nd while loop, len(self.pieces):", len(self.pieces))

    #             if self.row_cnts[idx] >= self.max_row_per_piece:
    #                 self.actions[idx] = "split"
    #                 pieces_len_adjustment += 1
    #                 # print("Loc 1:", "split")
    #                 # print("Loc 1, len(self.dfs[idx]):", len(self.dfs[idx]))
    #                 # self.print_tsfefs_info()
    #                 break
    #             elif self.row_cnts[idx] < self.max_row_per_piece/2:
    #                 idx_next = idx + 1
    #                 if idx_next < len(self.pieces):
    #                     if self.row_cnts[idx_next] + self.row_cnts[idx] < self.max_row_per_piece:
    #                         self.actions[idx] = "merge"
    #                         pieces_len_adjustment -= 1
    #                         self.action_params[idx] = idx_next
    #                         # print("Loc 2:", "merge")
    #                         # self.print_tsfefs_info()
    #                     elif self.row_cnts[idx_next] > self.max_row_per_piece/2:
    #                         self.actions[idx_next] = "split"
    #                         pieces_len_adjustment += 1
    #                         # print("Loc 3:", "split")
    #                         # self.print_tsfefs_info()
    #                     else:
    #                         # better not do anything, leave it alone.
    #                         idx += 1
    #                         continue
    #                     break

    #             idx += 1

    #         # "split" is 1, "merge" is 2.
    #         # but since after merge there will be "delete", which is 4, s
    #         # so max_level = 4.
    #         self.take_actions(max_level=4)
    #         # self.print_tsfefs_info()
    #         # print("idx:", idx)
    #         # print("len(self.pieces):", len(self.pieces))
    #         # print("pieces_len_adjustment:", pieces_len_adjustment)
    #         if idx == len(self.pieces) - pieces_len_adjustment:
    #             break

    ########################################## conflict resolve, optimize: END ##########################################
    #####################################################################################################################
    
    

    
    
    
    
    

    """ Use BaseFEFS's """
    ####################################################################################################################
    ################################################# Read, Write: BEG #################################################
    # """
    # Equals to df.read_csv
    # """
    # def read(self, fullname):

    #     self.path, self.name = self.__decompose_fullpath(fullname)

    #     full_meta_dict_name = '/'.join([fullname,TSFEFS.meta_json_name])
    #     dict_meta = json.load(open(full_meta_dict_name,'r'))        
    #     self = self.load_meta(dict_meta)

    #     full_index_df_name = '/'.join([fullname,TSFEFS.index_df_name])
    #     df_index = pd.read_csv(full_index_df_name, dtype={dict_meta["time_col"]:str})
    #     self = self.load_index_df(df_index)

    #     self.actions = [ "" for i in range(len(df_index)) ]
    #     self.action_params = [ None for i in range(len(df_index)) ]
    #     self.dfs = [ None for i in range(len(df_index)) ]
    #     self.cache = []
    #     return


    # def write(self, fullname=None):

    #     if fullname is None:
    #         fullname = self.__compose_fullpath()
    #     else:
    #         self.path, self.name = self.__decompose_fullpath(fullname) # just to ensure the .tsfefs extension exits

    #     if not os.path.isdir(fullname):
    #         os.mkdir(fullname)

    #     # Consistency of actions check
    #     # should have all actions cleared
    #     assert not self.has_pending_actions()
    #     # action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
    #     # assert all(np.array(action_levels) == TSFEFS.action_domain.index(""))


    #     dict_meta = self.dump_meta()
    #     full_meta_dict_name = '/'.join([fullname,TSFEFS.meta_json_name])
    #     json.dump(dict_meta, open(full_meta_dict_name,'w'))


    #     df_index, orders = self.dump_index_df()
    #     full_index_df_name = '/'.join([fullname,TSFEFS.index_df_name])
    #     df_index.to_csv(full_index_df_name, index=False)
    #     return
    ################################################# Read, Write: END #################################################
    ####################################################################################################################



    """ Use BaseFEFS's """
    # def remove(self):
    #     fullname = self.__compose_fullpath()

    #     def recursive_remove(fullpath):
    #         for f in os.listdir(fullpath):
    #             fullname = "%s/%s"%(fullpath,f)
    #             if os.path.isdir(fullname):
    #                 recursive_remove(fullname)
    #             else:
    #                 os.remove(fullname)
    #         os.rmdir(fullpath)

    #     if os.path.isdir(fullname):
    #         # for f in os.listdir(fullname):
    #         #     os.remove("%s/%s"%(fullname,f))
    #         # os.rmdir(fullname)
    #         recursive_remove(fullname)

    #     # del tsfefs_case2; tsfefs_case2 = None

    #     return




