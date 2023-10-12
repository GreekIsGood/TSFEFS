from general import *




class TSFEFS():
    
    ##########################################################################################################
    ########################################## Class Variables: BEG ##########################################
    extension = "tsfefs"
    index_df_name = ".index.csv"
    meta_json_name = ".meta.json"
    # Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
    action_domain = ["update", "split", "merge", "save", "delete", ""]
    ########################################## Class Variables: END ##########################################
    ##########################################################################################################



    def print_tsfefs_info(self):
        print("path:",self.path, ", name:", self.name)
        print("pieces:", self.pieces)
        print("fr:", self.fr, ", to:", self.to)
        print("frs:", [ fr.strftime(self.datetime_format) for fr in self.frs ])
        print("tos:", [ to.strftime(self.datetime_format) for to in self.tos ])
        print("types:", self.types)
        print("time_col:", self.time_col, ", datetime_format:", self.datetime_format)
        print("piece_name_len:", self.piece_name_len)
        print("colnames:", self.colnames, ", row_cnt:", self.row_cnt)
        print("row_cnts:", self.row_cnts)
        print("actions:", self.actions, ", action_params:", self.action_params)
        print("cache:", self.cache, ", cache_config:", self.cache_config)

        # print(tsfefs.dfs)
        return



    ###########################################################################################################
    ############################################ Init methods: BEG ############################################
    def __init__(self):
        
        self.verbose = False
        
        # It has to be followed by self.read()
        self.path = None
        self.name = None
        
        # Keys from dict_meta
        self.max_row_per_piece = None
        self.row_cnt = None
        self.time_col = None
        self.datetime_format = None
        self.piece_name_len = None
        self.fr = None
        self.to = None
        self.colnames = None
        self.cache_config = None
        
        # Columns from df_index
        self.pieces = None
        self.types = None
        self.frs = None
        self.tos = None
        self.row_cnts = None
            
        # Following are not columns in df_index
        """
        Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
        
         - ""        : Nothing to do
         - "update"  : Update df_index's fr and to, and update meta's fr and to.
                       After finishing the update, action will become "save".
         - "save"    : Save file to the same piece name. 
         - "delete"  : Delete file, remove from all lists, update meta's fr and to.
         - "split"   : Retain this file, create another one(s). Both/all leave with a "update" action.
         - "merge",i : Find i in params, retain this file, leave this with a "save" action,
                       the index i file with a "delete" action.               
        """
        self.actions = None
        self.action_params = None
        self.dfs = None # the elements can be df or TSFEFS
        
        # Cache section
        self.cache = None



    @classmethod
    def create(cls, dict_meta, name, path=None):
        """
        Usecase
        --------
        When you start with nothing but will add dataframes subsequently.
        """
        
        # It is supposed to be followed by tsfefs += df
        tsfefs = TSFEFS()
        
        if path is None:
            path = os.getcwd()
        tsfefs.path = path
        tsfefs.name = name
        
        
        tsfefs.row_cnt = None
        tsfefs.fr = None
        tsfefs.to = None        
        tsfefs.pieces = []
        tsfefs.types = []
        tsfefs.frs = []
        tsfefs.tos = []
        tsfefs.row_cnts = []

        tsfefs.actions = []
        tsfefs.action_params = []
        tsfefs.dfs = []

        # Cache section
        tsfefs.cache = []
        
        # Keys from dict_meta
        tsfefs = tsfefs.load_meta(dict_meta)
        
        
        return tsfefs

    
    def __check_cache_config(self):
        """
        cache_config = {"rows_in_cache":1000000, "len_of_cache":None}
        or 
        cache_config = {"rows_in_cache":None, "len_of_cache":10}
        """
        cache_config = self.cache_config
        assert isinstance(cache_config,dict)
        assert ("rows_in_cache" in cache_config.keys()) and ("len_of_cache" in cache_config.keys())
        assert (cache_config["rows_in_cache"] is None) or (cache_config["len_of_cache"] is None)
        assert (cache_config["rows_in_cache"] is not None) or (cache_config["len_of_cache"] is not None)
    
    
    def load_meta(self,dict_meta):
        # Keys from dict_meta
        self.max_row_per_piece = dict_meta["max_row_per_piece"]
        self.time_col = dict_meta["time_col"]
        self.datetime_format = dict_meta["datetime_format"]
        self.piece_name_len = dict_meta["piece_name_len"]
        self.colnames = dict_meta["colnames"]
        self.cache_config = dict_meta["cache_config"]
        self.__check_cache_config()
        
        try:
            self.row_cnt = dict_meta["row_cnt"]
        except:
            pass

        
        dtf = self.datetime_format
        try:
            self.fr = dt.strptime(dict_meta["fr"], dtf)
            self.to = dt.strptime(dict_meta["to"], dtf)
        except:
            pass
        return self

    
    def dump_meta(self):
        # Keys from dict_meta
        dict_meta = {}
        dict_meta["max_row_per_piece"] = self.max_row_per_piece
        dict_meta["time_col"] = self.time_col
        dict_meta["datetime_format"] = self.datetime_format
        dict_meta["piece_name_len"] = self.piece_name_len
        dict_meta["colnames"] = self.colnames
        dict_meta["cache_config"] = self.cache_config

        try:
            dict_meta["row_cnt"] = self.row_cnt
        except:
            pass

        dtf = self.datetime_format
        try:
            fr, to = self.fr, self.to
            dict_meta["fr"] = fr.strftime(dtf)
            dict_meta["to"] = to.strftime(dtf)
        except:
            pass
        return dict_meta


    def load_index_df(self, df_index, orders=None):
        
        if len(df_index) == 0:
            self.pieces = []
            self.types = []
            self.frs = []
            self.tos = []
            self.row_cnts = []
            return self

        dtf = self.datetime_format
        df_index["fr"] = df_index["fr"].apply(lambda x: dt.strptime(x,dtf))
        df_index["to"] = df_index["to"].apply(lambda x: dt.strptime(x,dtf))
        df_index = df_index.sort_values(by="fr").reset_index(drop=True)
        
        self.pieces = list(df_index["piece"])
        self.types = list(df_index["type"])
        self.frs = list(df_index["fr"])
        self.tos = list(df_index["to"])
        self.row_cnts = list(df_index["row_cnt"])
        
        try:
            self.fr, self.to = min(self.frs), max(self.tos)
            self.row_cnt = sum(self.row_cnts)
        except:
            pass
            
        
        if orders is not None:
            self.dfs = [ self.dfs[order_] for order_ in orders ]
            self.actions = [ self.actions[order_] for order_ in orders ]
            self.action_params = [ self.action_params[order_] for order_ in orders ]            
            """
            Let's say 
             orders = [3,5,4,0,2,1] => idx3 -> idx0, idx5 -> idx1, ...
            If cache = [4,3,5] 
            => [ orders.index(4), orders.index(3), orders.index(1) ]
            """
            self.cache = [ orders.index(cac) for cac in self.cache ]
        
        return self


    def dump_index_df(self):
        
        df_empty = pd.DataFrame({"piece":[],"type":[],"fr":[],"to":[],"row_cnt":[]})
        if self.pieces is None:
            assert all([ l is None for l in [self.types, self.frs, self.tos, self.row_cnts] ])
            return df_empty
        elif len(self.pieces) == 0:
            assert all([ len(l)==0 for l in [self.types, self.frs, self.tos, self.row_cnts] ])
            return df_empty
            
        df_index = pd.DataFrame()
        df_index["piece"] = self.pieces
        df_index["type"] = self.types
        df_index["fr"] = self.frs
        df_index["to"] = self.tos
        df_index["row_cnt"] = self.row_cnts
        df_index = df_index.sort_values(by="fr")
        orders = list(df_index.index)
        df_index = df_index.reset_index(drop=True)
        
        dtf = self.datetime_format
        df_index["fr"] = df_index["fr"].apply(lambda x: x.strftime(dtf))
        df_index["to"] = df_index["to"].apply(lambda x: x.strftime(dtf))
        return (df_index, orders)
    


    # def clone(self, path_name=True, meta=True, index=True, status=True):

    #     if path_name and meta and index and status:
    #         tsfefs = dc(self)
    #         return tsfefs


    #     # It is supposed to be followed by tsfefs += df
    #     tsfefs = TSFEFS()

    #     if path_name:
    #         tsfefs.path = self.path
    #         tsfefs.name = self.name

    #     if meta:
    #         tsfefs.max_row_per_piece = self.max_row_per_piece
    #         tsfefs.time_col = self.time_col
    #         tsfefs.datetime_format = self.datetime_format
    #         tsfefs.piece_name_len = self.piece_name_len
    #         tsfefs.colnames = self.colnames
    #         tsfefs.cache_config = self.cache_config           

    #     if index:
    #         tsfefs.row_cnt = self.row_cnt
    #         tsfefs.fr = self.fr
    #         tsfefs.to = self.to
    #         tsfefs.pieces = dc(self.pieces)
    #         tsfefs.types = dc(self.types)
    #         tsfefs.frs = dc(self.frs)
    #         tsfefs.tos = dc(self.tos)
    #         tsfefs.row_cnts = dc(self.row_cnts)

    #     if status:
    #         tsfefs.actions = dc(self.actions)
    #         tsfefs.action_params = dc(self.action_params)
    #         tsfefs.dfs = dc(self.dfs)
    #         # Cache section
    #         tsfefs.cache = dc(self.cache)

    #     return tsfefs
    
    def clone(self, path, name):
        
        # It is supposed to be followed by tsfefs += df
        tsfefs = TSFEFS()
        
        tsfefs.path = path
        tsfefs.name = name
            
        tsfefs.max_row_per_piece = self.max_row_per_piece
        tsfefs.time_col = self.time_col
        tsfefs.datetime_format = self.datetime_format
        tsfefs.piece_name_len = self.piece_name_len
        tsfefs.colnames = dc(self.colnames)
        tsfefs.cache_config = dc(self.cache_config)
        tsfefs.fr = self.fr
        tsfefs.to = self.to
        tsfefs.row_cnt = self.row_cnt
            
        tsfefs.pieces = dc(self.pieces)
        tsfefs.types = dc(self.types)
        tsfefs.frs = dc(self.frs)
        tsfefs.tos = dc(self.tos)
        tsfefs.row_cnts = dc(self.row_cnts)

        tsfefs.actions = dc(self.actions)
        tsfefs.action_params = dc(self.action_params)
        tsfefs.dfs = [ dc(df_) for df_ in self.dfs ]
        # Cache section
        tsfefs.cache = dc(self.cache)
        
        
        # compensating codes for self.dfs[idx] is None
        for idx in range(len(tsfefs.pieces)):
            
            if self.dfs[idx] is None:
                
                assert tsfefs.dfs[idx] is None
                assert self.pieces[idx] == tsfefs.pieces[idx] 
                
                piece = self.pieces[idx] 
                src = self.__compose_piece_fullname(piece)
                dst = tsfefs.__compose_piece_fullname(piece)
                tsfefs_fullname = tsfefs.__compose_fullpath()
                if not os.path.isdir(tsfefs_fullname):
                    os.mkdir(tsfefs_fullname)
                # print("Command:", "cp \"%s\" \"%s\""%(src,dst))
                os.system("cp \"%s\" \"%s\""%(src,dst))
                
            elif self.actions[idx] == "": # i.e., self.dfs[idx] is not None but no "save" command
                tsfefs.actions[idx] = "save"

        return tsfefs        

    ############################################ Init methods: END ############################################
    ###########################################################################################################
        

        
        
        
        
        
        
        
        
        
    ##########################################################################################################
    ######################################### Path, Name, Piece: BEG #########################################
    #@classmethod
    @staticmethod
    def get_random_string(n):
        # choose from all lowercase letter
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(n))
        return result_str   
    
    def gen_valid_piece(self, prefix="", suffix=""):
        while True:
            piece = TSFEFS.get_random_string(self.piece_name_len)
            piece = prefix + piece + suffix
            if piece not in self.pieces:
                break
        return piece

        
    def __decompose_fullpath(self, fullpath):
        path = fullpath.split('/')
        name = path[-1]
        path = '/'.join(path[:-1])
        
        names = name.split('.')
        assert names[-1] == TSFEFS.extension        
        name = '.'.join(names[:-1])
        return (path,name)
                
    def __decompose_piece_fullname(self,piece_fullname):
        fullpath = piece_fullname.split('/')
        piece = fullpath[-1]
        fullpath = '/'.join(fullpath[:-1])
        path, name = self.__decompose_fullpath(fullpath)
        return (path,name,piece)


    def __compose_fullpath(self):
        fullpath = '/'.join([self.path, self.name + '.' + TSFEFS.extension])
        return fullpath

    def __compose_piece_fullname(self,piece):
        fullname = '/'.join([self.__compose_fullpath(), piece])
        return fullname
    ######################################### Path, Name, Piece: END #########################################
    ##########################################################################################################


    
    
    
    def vprint(self,*value):
        try:
            if self.verbose: # may not have the explicit self.verbose attribute
                print(value)
        except:
            pass
        
        
        
    def has_valid_status(self):
        attributes = [
            self.path, self.name, self.max_row_per_piece, self.row_cnt, self.time_col, self.datetime_format,
            self.piece_name_len, self.fr, self.to, self.colnames, self.cache_config, self.pieces, self.types,
            self.frs, self.tos, self.row_cnts, self.dfs, self.actions, self.action_params, self.cache ]
        if any([ attr is None for attr in attributes ]):
            return False
        
        lists = [ self.pieces, self.types, self.frs, self.tos, self.row_cnts ]
        if any([ val is None for list_ in lists for val in list_ ]):
            return False
        return True
        
            
            
    @classmethod
    def get_index(cls,df):
        if isinstance(df,pd.DataFrame):
            return list(df.index)
        elif isinstance(df,TSFEFS):
            tsfefs = df
            indices = list(range(tsfefs.row_cnt))
            return indices
        else:
            print("Type", type(df), "not supported")
            assert False
            

    """
    Just save csv, not considering other TSFEFS / piece structure.
    """
    def __to_csv(self,idx):
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
        df = df.sort_values(by=self.time_col).reset_index(drop=True)
        self.dfs[idx] = df
        dtf = self.datetime_format
        df2 = dc(df)
        df2[self.time_col] = df2[self.time_col].apply(lambda x: x.strftime(dtf))
        df2.to_csv(fullname, index=False)
        del df2; df2 = None
        return

    """
    Just read csv, not considering other TSFEFS / piece structure.
    """
    def __read_csv(self,idx):
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
        df = pd.read_csv(fullname,dtype={self.time_col:str})
        dtf = self.datetime_format
        df[self.time_col] = df[self.time_col].apply(lambda x: dt.strptime(x,dtf))
        return df


    def __read_piece(self, idx):
        
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
        if self.types[idx] == "csv":
            df = pd.read_csv(fullname, dtype={self.time_col:str})
            dtf = self.datetime_format
            df[self.time_col] = df[self.time_col].apply(lambda x: dt.strptime(x,dtf))
            df = df.sort_values(by=self.time_col).reset_index(drop=True)
            self.dfs[idx] = df
            return self.dfs[idx]
        
        elif self.types[idx] == "tsfefs":
            tsfefs = TSFEFS()
            fullname += '.' + TSFEFS.extension
            tsfefs.read(fullname)
            return tsfefs
        else:
            print("File type \"tsfefs\" not yet implemented")
            assert False
        return
    
    
    
    def __write_piece(self, idx):
        
        df = self.dfs[idx]
        assert df is not None

        tsfefs_fullname = self.__compose_fullpath()
        if not os.path.isdir(tsfefs_fullname):
            os.mkdir(tsfefs_fullname)            
           
        type_ = self.types[idx]
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
        if type_ == "csv":
            df = df.sort_values(by=self.time_col).reset_index(drop=True)
            self.dfs[idx] = df
            dtf = self.datetime_format
            df2 = dc(df)
            df2[self.time_col] = df2[self.time_col].apply(lambda x: x.strftime(dtf))
            df2.to_csv(fullname, index=False)
            del df2; df2 = None
        elif type_ == "tsfefs":
            tsfefs = df
            tsfefs.take_actions(max_level=4) #??? or max_level=3?
            fullname += '.' + TSFEFS.extension
            tsfefs.write(fullname) # by calling write, this tsfefs object will have its path and name reset.
        else:
            print("Type", type_, "not supported")
            assert False
        return







    
    

    ##################################################################################################################
    ################################################# Cache Ops: BEG #################################################
    
    """
    Called by:
      - __getitem__using_idx
      - __getitem__using_indices
      - __setitem__using_idx
      - __setitem__using_indices
      - __delitem__using_idx
      - __delitem__using_indices
    """
    def renew_idx(self, idx):
        if idx in self.cache:
            self.cache.remove(idx)
        self.cache.append(idx)

    """
    Called by:
      - __action_delete
    """
    def remove_idx(self, idx): 
        if idx in self.cache:
            self.cache.remove(idx)
        self.cache = [ idx_ - 1 if idx_ > idx else idx_ for idx_ in self.cache ]

        

    """
    Called by:
      - __action_split
    """
    def split_idx(self, idx):
        
        """
         - idx: the originally index, which will be retained, with new pieces created from it.
         - new_idx: split will lengthen the array, len(self.pieces)-1
        
        The new idx will be inserted in the cache behind the idx.
        """
        new_idx = len(self.pieces)-1
        
        
        """
        If idx not in cache, new_idx, which is split from idx, should not be in cache as well.
        """
        if idx in self.cache:
            loc = self.cache.index(idx)
            if new_idx in self.cache:
                self.cache.remove(new_idx)
            self.cache.insert(loc,new_idx)
        else:
            if new_idx in self.cache:
                self.cache.remove(new_idx)
        # if idx not in cache, no need to have new_idx in cache.



    """
    Called by:
      - __action_merge
    """
    def merge_idx(self, idx):
        
        tobemerged_idx = self.action_params[idx]
        assert isinstance(tobemerged_idx,int)
        
        if tobemerged_idx in self.cache:
            loc2 = self.cache.index(tobemerged_idx)
            if idx in self.cache:
                loc1 = self.cache.index(idx)
                if loc2 > loc1:
                    self.cache.remove(idx)
                    self.cache.insert(loc2,idx)
            else:
                self.cache.insert(loc2,idx)
                
    """
    Called by:
      - __add_tsfefs
      - __add_df
    """
    def include_idx(self, idx):
        """
        only include idx to the lower priority.
        Note: 0 is the lowest priority.
        """
        if idx not in self.cache:
            self.cache.insert(0,idx)



    def __set_piece_to_none(self,idx):
        df = self.dfs[idx]
        if df is None:
            return
        type_ = self.types[idx]
        if type_ == "csv":
            pass
        elif type_ == "tsfefs":
            for idx2 in range(len(df.pieces)):
                df.__set_piece_to_none(idx2)
        else:
            assert False        
        
        del df
        df = None
        self.dfs[idx] = None
        return


    def __count_opened_rows(self,idx):
        df = self.dfs[idx]
        type_ = self.types[idx]
        if df is None:
            return 0
        if type_ == "csv":
            return self.row_cnts[idx]
        elif type_ == "tsfefs":
            tsfefs = df
            row_cnt = 0
            for idx_ in range(len(tsfefs.pieces)):
                row_cnt += tsfefs.__count_opened_rows(idx_)
            return row_cnt
        else:
            assert False
        

    def __maintain_cache_by_rows(self):
        cache_config = self.cache_config
        rows_in_cache = cache_config["rows_in_cache"]
        
        cache = []
        for i in reversed(range(len(self.cache))):
            idx = self.cache[i]
            row_cnt = self.__count_opened_rows(idx)
            if rows_in_cache >= row_cnt:
                rows_in_cache -= row_cnt
                cache.append(idx)
                continue
            break
        
        cache = list(reversed(cache))
        clear_indices = set(self.cache) - set(cache)
        for idx in clear_indices:
            self.__set_piece_to_none(idx)
        self.cache = cache
        return
            


        
    def __maintain_cache_by_len(self):
        cache_config = self.cache_config
        len_of_cache = cache_config["len_of_cache"]
        if len(self.cache) <= len_of_cache:
            return
        
        self.cache = self.cache[-len_of_cache:]
        for idx in range(len(self.pieces)):
            if idx in self.cache:
                continue
            self.__set_piece_to_none(idx)
                
            
        
    
    
    def maintain_cache(self):
        
        # action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
        # assert np.all(np.array(action_levels) == TSFEFS.action_domain.index(""))
        assert not self.has_pending_actions()
        

        self.__check_cache_config()
        cache_config = self.cache_config
        rows_in_cache = cache_config["rows_in_cache"]
        len_of_cache = cache_config["len_of_cache"]
        
        if rows_in_cache is not None:
            self.__maintain_cache_by_rows()
        elif len_of_cache is not None:
            self.__maintain_cache_by_len()
        else:
            assert False
        return

    ################################################# Cache Ops: END #################################################
    ##################################################################################################################
    
    
    
    
    
    


    #########################################################################################################
    ############################################## Action: BEG ##############################################
    
    def __action_nothing(self, idx):
        return

    
    def __action_update(self, idx):

        df = self.dfs[idx]
        assert df is not None
        
        type_ = self.types[idx]
        len_ = None
        if type_ == "csv":
            len_ = len(df)
        elif type_ == "tsfefs":
            tsfefs = df
            len_ = tsfefs.row_cnt
        else:
            print("Type", type_, "not supported")
            assert False

        fr, to = None, None
        if len_ > 0:
            if type_ == "csv":
                S_ = df[self.time_col]
            elif type_ == "tsfefs":
                S_ = df.__get_times()
            else:
                print("Type", type_, "not supported")
                assert False
            fr, to = min(S_), max(S_)

        self.frs[idx], self.tos[idx] = fr, to
        self.row_cnts[idx] = len_
        frs = [ fr for fr in self.frs if fr is not None ]
        tos = [ to for to in self.tos if to is not None ]
        self.fr, self.to = min(frs), max(tos)
        row_cnts = [ rcnt for rcnt in self.row_cnts if rcnt is not None ]
        self.row_cnt = sum(row_cnts)
        self.actions[idx] = "save" # it will be saved later
        
        self.vprint(self.pieces[idx], "turned to status \"save\"")
        
        if len_ == 0:
            self.actions[idx] = "delete"
            
            self.vprint(self.pieces[idx], "turned to status \"delete\" because its len is 0")
            
        return
    

    def __action_save(self, idx):
        self.__write_piece(idx)
        self.actions[idx] = ""
        return


    def __action_delete(self, idx):

        df = self.dfs[idx]
        assert df is not None

        type_ = self.types[idx]
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
        if type_ == "csv":
            if os.path.isfile(fullname):
                os.remove(fullname)
        elif type_ == "tsfefs":
            tsfefs = df
            # fullname = tsfefs.__compose_fullpath()
            # if os.path.isdir(fullname):
            #     for f in os.listdir(fullname):
            #         os.remove("%s/%s"%(fullname,f))
            #     os.rmdir(fullname)
            tsfefs.remove()
            
        else:
            print("Type", type_, "not supported")
            assert False

        del self.pieces[idx]
        del self.types[idx]
        del self.frs[idx]; del self.tos[idx]
        del self.row_cnts[idx]
        del self.actions[idx]; del self.action_params[idx]
        del self.dfs[idx]
        del df; df = None
        
        self.remove_idx(idx)

        frs = [ fr for fr in self.frs if fr is not None ]
        tos = [ to for to in self.tos if to is not None ]
        if len(frs) > 0:
            assert len(tos) > 0
            self.fr, self.to = min(frs), max(tos)
        else:
            self.fr, self.to = None, None
            
        self.row_cnt = sum(self.row_cnts)
        return


    def __action_split(self, idx):

        df = self.dfs[idx]
        assert df is not None
        
        type_ = self.types[idx]
        
        """
        The design is not advanced enough to be able to split 1 TSFEFS into 2 TSFEFSs,
        so let's have all the splits be csvs.
        """
        if type_ == "tsfefs":
            df = df.export_dataframe()
        df = df.sort_values(by=self.time_col).reset_index(drop=True)

        n = len(df)
        num_rows_per_file = int(self.max_row_per_piece/2)
        num_of_files = int(np.ceil(n/num_rows_per_file))

        dfs = [ df.iloc[(i*num_rows_per_file):((i+1)*num_rows_per_file)] for i in range(num_of_files) ]

        if len(dfs) == 0:
            assert False
        elif len(dfs) == 1:
            """
            no need for split action
            """
            return
        else:
            self.types[idx] = "csv"
            type_ = "csv"
            self.dfs[idx] = df

            
        # print("__action_split, idx:", idx)
        # print("__action_split:", [ len(df_) for df_ in dfs ])
        df = dfs[0]
        self.dfs[idx] = df
        """ can leave fr and to updates when "update" """
        self.actions[idx] = "update"

        dfs = dfs[1:]

        if type_ == "csv":
            for df in dfs:
                """
                instead of using 
                  self += df
                the following codes are preferred.
                
                Calling __add__ will let the new_idx to be included in cache,
                which will mess up the split_idx logic.
                """
                piece = self.gen_valid_piece(prefix="", suffix="")
                self.pieces += [piece]
                self.types += [type_]
                self.frs += [None] # leave to "update" action
                self.tos += [None] # leave to "update" action
                self.row_cnts += [None]
                self.actions += ["update"]
                self.action_params += [None]
                self.dfs += [df]
                
                self.split_idx(idx) # this will the new indices next to the location of idx in cache

        else: # elif self.types[idx] == "tsfefs"
            print("File type \"tsfefs\" not yet implemented")
            assert False
        return


    def __action_merge(self, idx):
        
        df = self.dfs[idx]
        
        """
        I might have reasons for this line,
        but seems like it is now causing trouble.
        Let's recall the reason?
        """
        # assert df is not None
        """
        Temporarily replace the above line by this
        """
        if df is None:
            df = self.__read_piece(idx)
            self.dfs[idx] = df
            
        
        
        idx_tobemerged = self.action_params[idx]
        assert isinstance(idx_tobemerged,int)
        df_tobemerged = self.dfs[idx_tobemerged]
        
        
        
        """
        I might have reasons for this line,
        but seems like it is now causing trouble.
        Let's recall the reason?
        """
        # assert df_tobemerged is not None
        """
        Temporarily replace the above line by this
        """
        if df_tobemerged is None:
            df_tobemerged = self.__read_piece(idx_tobemerged)
            self.dfs[idx_tobemerged] = df_tobemerged

            
            
            
        
        type_ = self.types[idx]
        type_tobemerged = self.types[idx_tobemerged]
        if  type_ == "csv"  and  type_tobemerged == "csv":
            df = pd.concat([df,df_tobemerged]).reset_index(drop=True)
        elif  type_ == "csv"  and  type_tobemerged == "tsfefs":
            df_tobemerged = df_tobemerged.export_dataframe()
            df = pd.concat([df,df_tobemerged]).reset_index(drop=True)
        elif  type_ == "tsfefs"  and  type_tobemerged == "csv":
            df += df_tobemerged
        elif  type_ == "tsfefs"  and  type_tobemerged == "tsfefs":
            df += df_tobemerged            
        else:
            print("Type",  type_tobemerged, "not supported.")
            assert False
            
            
        self.merge_idx(idx) # has to be run before action_params[idx] is set to None
        
            
        self.dfs[idx] = df
        self.action_params[idx] = None
        self.actions[idx] = "update"
                
        self.frs[idx_tobemerged] = None
        self.tos[idx_tobemerged] = None
        self.row_cnts[idx_tobemerged] = None
        self.actions[idx_tobemerged] = "delete"
        self.action_params[idx_tobemerged] = None        
        return


    def __sanity_check(self):
        n = len(self.pieces)
        listoflists = [ self.types, self.frs, self.tos, self.row_cnts, 
                       self.actions, self.action_params, self.dfs ]
        assert all([ n == len(l) for l in listoflists ])
        return
    
    
    def __map_action_to_func(self,level):
        if level == 0:
            return self.__action_update
        elif level == 1:
            return self.__action_split
        elif level == 2:
            return self.__action_merge
        elif level == 3:
            return self.__action_save
        elif level == 4:
            return self.__action_delete
        elif level == 5:
            return self.__action_nothing
        else:
            print("No such action with level %i"%level)
            assert False            


    def take_actions(self, max_level=0):
        """
        Never set max_level = 5, infinite loop.
        """
        # Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
        current_level = 0
        while current_level <= max_level:
            self.vprint("current_level:", current_level)
            action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
            action_indices = [ idx for idx in reversed(range(len(action_levels))) if action_levels[idx] == current_level ]
            # print("current_level:", current_level, "; max_level:", max_level)
            # print("self.verbose:", self.verbose)
            if len(action_indices) == 0:
                # print("take_actions", 1, "; current_level:", current_level)
                current_level += 1
            else: # len(action_indices) > 0:
                # print("take_actions", 2, "; current_level:", current_level, "; len(action_indices):", len(action_indices))
                for idx in action_indices:
                    # print("idx:", idx)
                    self.__map_action_to_func(current_level)(idx)
                current_level = 0
        return


    def has_pending_actions(self):
            action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
            b = any(np.array(action_levels) != TSFEFS.action_domain.index(""))
            return b

    ############################################## Action: END ##############################################
    #########################################################################################################

    
    
    
    
    
    
    
    
    
    
    
    ################################################################################################################
    ############################################## Import,Export: BEG ##############################################


    def __no_overlapping_time_range(self):
        """
        ensure fr1 < to1 < fr2 < to2 < ...
        """
        df = pd.DataFrame({"fr":self.frs,"to":self.tos})
        df = df.sort_values(by="fr")
        orders = list(df.index)
        arr = np.array(df)
        arr = arr.reshape(-1).tolist() # becomes [fr1,to1,fr2,to2,...]
        assert all([ arr[i+1] >= arr[i] for i in range(len(arr)-1) ])
        return orders
    

    
    """
    Export all as a single df
    """
    def export_dataframe(self):
        
        orders = self.__no_overlapping_time_range()
        
        dfs = []
        
        # rearrange all the lists according to orders
        for idx in orders:
            
            df = self.dfs[idx]
            if df is None:
                df = self.__read_piece(idx)

            type_ = self.types[idx]
            if type_ == "csv":
                # dfs.append(self.dfs[idx])
                dfs.append(df)
            elif type_ == "tsfefs":
                # dfs.append(self.dfs[idx].export_dataframe())
                dfs.append(df.export_dataframe())
            else:
                print("Type", type_, "not supported")
                assert False

        if len(dfs) == 0:
            return pd.DataFrame()
        
        df = pd.concat(dfs).reset_index(drop=True)
        return df

    
    
    def export_dstfile(self, dstfile):
        df = self.export_dataframe()
        df.to_csv(dstfile,index=False)
        return

    
    
    def export_dstfolder(self, dstfolder):
        """
        Don't allow if any of those frs and tos are None,
        otherwise can't name the dst files.
        """
        assert self.has_valid_status() # no None in the lists

        """
        Don't allow if there is pending actions, 
        otherwise can't copy from src to dst.
        """
        assert not self.has_pending_actions()
        

        if not os.path.isdir(dstfolder):
            os.mkdir(dstfolder)
            
        """
        The export format is e.g., "3. 2020-01-10 03:05:11 ~ 2020-01-10 03:05:11.csv"
        """
        fullpath = self.__compose_fullpath()
        for idx in range(len(self.pieces)):
            piece = self.pieces[idx]
            fr, to = self.frs[idx], self.tos[idx]
            
            src = "%s/%s"%(fullpath,piece)
            fr = fr.strftime(self.datetime_format)
            to = to.strftime(self.datetime_format)
            f = "%i. %s ~ %s.csv"%(idx,fr,to)
            dst = "%s/%s"%(dstfolder,f)
            os.system("cp \"%s\" \"%s\""%(src,dst))
        return

    

    def import_dataframe(self, df):
        
        self.pieces = []
        self.types = []
        self.frs = []
        self.tos = []
        self.row_cnts = []
            
        self.actions = []
        self.action_params = []
        self.dfs = []
        
        self.cache = []
        
        df = df.sort_values(by=self.time_col).reset_index(drop=True)
        self += df
        self.__action_split(0)
        return

    
    def import_srcfile(self, srcfile):
        df = pd.read_csv(srcfile, dtype={self.time_col:str})
        assert self.time_col in df.columns
        df[self.time_col] = df[self.time_col].apply(lambda x: dt.strptime(x,self.datetime_format))        
        self.import_dataframe(df)
        return

    
    def import_srcfolder(self, srcfolder):
        
        """
        List of files to be copied
        """
        files = os.listdir(srcfolder)
        if srcfolder[-1] == '/':
            srcfolder = srcfolder[:-1]
        files = [ "%s/%s"%(srcfolder,f) for f in files ]
        files = [ f for f in files if not os.path.isdir(f) ]
        
        
        """
        starts from nothing
        """
        self.pieces = []
        self.types = []
        self.frs = []
        self.tos = []
        self.row_cnts = []

        self.actions = []
        self.action_params = []
        self.dfs = []
        
        self.cache = []

        
        fullpath = self.__compose_fullpath()
        if not os.path.isdir(fullpath):
            os.mkdir(fullpath)

        
        """
        1. copy files to fullpath
        2. initialize the lists
        3. do everything as if done in __action_update but not calling __action_update
        """
        for idx,f in enumerate(files):
            src = f
            piece = self.gen_valid_piece(prefix="", suffix="")
            dst = "%s/%s"%(fullpath,piece)
            # print("Command:", "cp \"%s\" \"%s\""%(src,dst))
            os.system("cp \"%s\" \"%s\""%(src,dst))
            
            self.pieces += [piece]
            self.types += ["csv"]
            
            # dfs[idx] has to exist before calling __get_time(idx)
            self.dfs += [None]
            S = self.__get_time(idx)
            
            self.frs += [min(S)]
            self.tos += [max(S)]
            self.row_cnts += [len(S)]

            self.actions += [""] # the above info are filled, to avoid calling __action_update
            self.action_params += [None]
            
        self.fr = min(self.frs)
        self.to = max(self.tos)
        self.row_cnt = sum(self.row_cnts)
        return


    
    def all_pieces_as_csv(self):
        
        dfs = []
        for idx in range(len(self.pieces)):
            type_ = self.types[idx]
            if type_ == "csv":
                continue
            elif type_ == "tsfefs":
                df = self.dfs[idx].export_dataframe()
                dfs += [df]
                self.actions[idx] = "delete"
            else:
                print("Type", type_, "not supported")
                assert False
        self.take_actions(max_level=4)

        for df in dfs:
            self += df
            self.actions[len(self.actions)-1] = "split"
        self.take_actions(max_level=2)
        return
    ############################################## Import,Export: END ##############################################
    ################################################################################################################

    

    

    
    ###################################################################################################################
    ############################################# Overriding __len__: BEG #############################################
    def __len__(self):
        return self.row_cnt
    ############################################# Overriding __len__: END #############################################
    ###################################################################################################################
    
    

    
    
    
    
    ####################################################################################################################
    ##################################### Overriding Square Bracket - general: BEG #####################################
    def __idx_check(self,idx):
        assert isinstance(idx,int_types)
        
        while idx < 0:
            # idx += self.row_cnt
            idx += len(self)
            
        # if idx > self.row_cnt - 1:
        if idx > len(self) - 1:
            print("Index out of range")
            assert False
           
        return idx

    
    def __indices_check(self,indices):
        
        assert isinstance(indices,list)
        assert all([ isinstance(idx,int_types) for idx in indices ])
        # assert len(indices) <= self.row_cnt
        assert len(indices) <= len(self)
        
        for i in range(len(indices)):
            idx = indices[i]
            while idx < 0:
                # idx += self.row_cnt
                idx += len(self)
            indices[i] = idx
        
        if len(indices) > len(set(indices)):
            print("Dupicated indices")
            assert False
                
        # assert all([ idx < self.row_cnt for idx in indices ])
        assert all([ idx < len(self) for idx in indices ])
        return indices
    
    
    def __str_check(self,colname):
        assert isinstance(colname,str_types)
        
        if colname not in self.colnames:
            print("No such column name as %s, try update action"%colname)
            assert False
            

    def __strs_check(self,colnames):

        assert isinstance(colnames,list)
        assert all([ isinstance(s,str_types) for s in colnames ])
        
        if len(set(colnames) - set(self.colnames)) > 0:
            print("Some requested column names don't exits.")
            print("You may consider the \"update\" action.")
            assert False
            
    def __bools_check(self,B):
        assert isinstance(B,list)
        assert all([ isinstance(b,bool_types) for b in B ])
        # assert len(B) == self.row_cnt # has to be equal length
        assert len(B) == len(self) # has to be equal length

        
    # Only for __setitems__'s strs and indices (and thus bools).
    def __value_check(self, value, assignment_w, assignment_h):
        """
        Only for __setitems__'s strs and indices (and bools).
        Return 2D or 1D array, 
        in which the array by accessing arr[idx:(idx+n)] they will return the corresponding rows for assignments.
        """
        # check value
        if isinstance(value,(arr_types,pd.DataFrame)):
            value = np.array(value)
            if len(value.shape) == 1:
                if value.shape[0] == 1: # this is value = [x]
                    value = value[0] # will be turned to list 
                else:
                    assert value.shape[0] == assignment_w
            elif len(value.shape) == 2:
                assert (value.shape[1] == assignment_w) or (value.shape[1] == 1)
                assert (value.shape[0] == assignment_h) or (value.shape[0] == 1)                
            else: # at most 2 dimensional array, of course
                print("Dimension of the assignment:", value.shape)
                assert False
        elif isinstance(value,range):
            print(type(value), "type can only be assigned to pd.Series")
            assert False
            
        if not isinstance(value,(arr_types,pd.DataFrame)):
            value = [value]*assignment_h # turn singular to list 

        return value

    ##################################### Overriding Square Bracket - general: END #####################################
    ####################################################################################################################
   



    

    ####################################################################################################################
    ##################################### Overriding Square Bracket - getitem: BEG #####################################
    
    def __get_piece_idx(self, idx, orders):
        
        accum_row_cnt = 0
        _order = None
        for _ord in orders:
            row_cnt = self.row_cnts[_ord]
            if idx > row_cnt - 1:
                idx -= row_cnt
                accum_row_cnt += row_cnt
                continue
            _order = _ord
            break
            
        return (_order, accum_row_cnt)

    

    def __getitem__using_idx(self,idx):
        
        idx = self.__idx_check(idx)        
        orders = self.__no_overlapping_time_range()
        _order, accum_row_cnt = self.__get_piece_idx(idx, orders)
        
        df = self.dfs[_order]
        if df is None:
            df = self.__read_piece(_order)
            self.dfs[_order] = df

        idx -= accum_row_cnt
        type_ = self.types[_order]
        if type_ == "csv":
            
            self.renew_idx(_order)
            
            return df.iloc[idx]
        elif type_ == "tsfefs":
            tsfefs = df
            
            self.renew_idx(_order)
            
            return tsfefs[idx]
        else:
            print("Type", type_, "not supported")
            assert False
        
        print("Error! Find it out!")
        assert False
    
        
        
    def __getitem__using_str(self,colname):
        
        self.__str_check(colname)
        
        values = []
        orders = self.__no_overlapping_time_range()
        for _order in orders:
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(_order)
                self.dfs[_order] = df
            values.extend(df[colname])
        S = pd.Series(values)
        S.name = colname
        return S


    def __get_indices_in_same_piece(self, indices, orders):
        
        idx = indices[0]
        _order, accum_row_cnt = self.__get_piece_idx(idx, orders)            
        if _order is None:
            assert False
            
        idx_beg = accum_row_cnt
        idx_end = accum_row_cnt + self.row_cnts[_order]
        idx_range = range(idx_beg,idx_end)
        
        adjusted_indices = []
        for idx in indices:
            if idx in idx_range:
                adjusted_indices.append(idx-accum_row_cnt)
                continue
            break
            
        return (_order, adjusted_indices)
            
        
        
    def __getitem__using_indices(self,indices):
        
        indices = self.__indices_check(indices)
        indices_copied = dc(indices)

        dfs = []
        orders = self.__no_overlapping_time_range()
        
        while len(indices) > 0:
            _order, adjusted_indices = self.__get_indices_in_same_piece(indices, orders)
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(_order)

            type_ = self.types[_order]            
            if type_ == "csv":
                df_ = df.iloc[adjusted_indices]
                
                self.renew_idx(_order)
                
            elif type_ == "tsfefs":
                tsfefs = df
                df_ = tsfefs[adjusted_indices]
                
                self.renew_idx(_order)
                
            else:
                print("Type", type_, "not supported")
                assert False
                    
            dfs.append(df_)
            indices = indices[len(adjusted_indices):]
        

        if len(dfs) == 0:
            return pd.DataFrame()
        
        df = pd.concat(dfs).reset_index(drop=True)
        df.index = indices_copied
        return df
                


    def __getitem__using_bools(self,B):
        
        self.__bools_check(B)
        
        indices = np.array(range(len(B)))[B].tolist()
        return self.__getitem__using_indices(indices)

        
    def __getitem__using_strs(self,colnames):
        
        self.__strs_check(colnames)
        
        dfs = []
        indices = []
        cum_row_cnt = 0
        orders = self.__no_overlapping_time_range()
        for _order in orders:
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(_order)
                self.dfs[_order] = df
            dfs.append(df[colnames])
            # indices += list(np.array(df.index) + cum_row_cnt)
            indices += list(np.array(TSFEFS.get_index(df)) + cum_row_cnt)
            cum_row_cnt += self.row_cnts[_order]
        
        if len(dfs) == 0:
            return pd.DataFrame()
        
        df = pd.concat(dfs).reset_index(drop=True)
        df.index = indices
        return df
        
    
    def __getitem__(self, key):
        """
        Accetpable queries:
        Single:
         - int: tsfefs[33]
         - str: tsfefs["age"]
        Multiple:
         - slice of ints: tsfefs[20:], tsfefs[20:50], tsfefs[::2], ...
         - list of ints: tsfefs[[1,2,5,10]]
         - list of strs: tsfefs[["time","purchased","item","price"]]
         - list of bools: tsfefs[[True,False,True,...]] # must have length == row_cnt
        """        
        if isinstance(key,str_types):
            return self.__getitem__using_str(key)
        elif isinstance(key,int_types):
            return self.__getitem__using_int(key)
        elif isinstance(key,(arr_types,slice)):
            if isinstance(key,np.ndarray):
                assert len(key.shape) == 1 # must be in (n,)
                key = list(key)
            elif isinstance(key,pd.Series):
                key = list(key)
            elif isinstance(key,slice):                
                assert all([ (s is None) or isinstance(s,int) for s in [ key.start,key.stop,key.step ] ])
                key = list(range(self.row_cnt))[key]
                
            type_ = type(key[0])
            assert isinstance(key[0],(int_types,str_types,bool_types))
            assert all([ isinstance(k,type_) for k in key ])
            
            if type_ in str_types:
                return self.__getitem__using_strs(key)
            elif type_ in bool_types:
                return self.__getitem__using_bools(key)
            elif type_ in int_types:
                return self.__getitem__using_indices(key)
            else:
                print("No such type" )
                assert False
        else:
            print("Something's wrong for the type(s)")
            assert False
            
    ##################################### Overriding Square Bracket - getitem: END #####################################
    ####################################################################################################################



    
    
    
    
    
    
    ####################################################################################################################
    ##################################### Overriding Square Bracket - setitem: BEG #####################################
    
    def __setitem__using_idx(self, idx, value):
        
        idx = self.__idx_check(idx)

        # if type(value) in [list, np.ndarray, pd.Series]:
        if isinstance(value, arr_types):
            arr = np.array(value)
            assert len(arr.shape) == 1 # like (4,)
            col_len = arr.shape[0]
            assert col_len == len(self.colnames)
        else:
            pass # you can put any type of single value to the idx row.
            
        orders = self.__no_overlapping_time_range()
        for _order in orders:
            row_cnt = self.row_cnts[_order]
            if idx > row_cnt - 1:
                idx -= row_cnt
                continue
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(_order)
            self.actions[_order] = "update"
            type_ = self.types[_order]
            if type_ == "csv":
                df.iloc[idx] = value
                self.dfs[_order] = df
            elif type_ == "tsfefs":
                tsfefs = df
                tsfefs[idx] = value
                self.dfs[_order] = tsfefs
            else:
                print("Type", type_, "not supported")
                assert False
                
            self.renew_idx(_order)
            return
        
        print("Error! Find it out!")
        assert False
        return


    def __setitem__using_str(self, colname, value):
        # print("self.path, self.name")
        # print(self.path, self.name)
        # print(type(value))
        # print(value)
        
        # don't use the same checking routine here
        # self.__str_check(colname)
        
        assert isinstance(colname,str_types)
            
        # if type(value) in [list,np.ndarray,pd.Series]:
        if isinstance(value,(arr_types,range)):
            assert len(value) == self.row_cnt
            value = list(value)
        elif isinstance(value,pd.DataFrame):
            assert len(value) == self.row_cnt
            assert len(value.columns) == 1
            col = list(value.columns)[0]
            value = np.array(value[col])
        else:
            value = [value]*self.row_cnt

        cnt = 0
        orders = self.__no_overlapping_time_range()
        for _order in orders:
            idx = _order
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(idx)

            cnt_new = cnt + self.row_cnts[_order]
            df[colname] = value[ cnt : cnt_new ]
            # print("type(df)")
            # print(type(df))
            # print("df[colname]")
            # print(df[colname])
            cnt = cnt_new
            self.dfs[_order] = df
            self.actions[_order] = "update"
        
        # print()
        # print()
        if colname not in self.colnames:
            self.colnames += [colname]
        return


                    
    def __setitem__using_indices(self,indices,value):
        
        indices = self.__indices_check(indices)
        value = self.__value_check(value, len(self.colnames), len(indices))
        orders = self.__no_overlapping_time_range()
        
        cnt = 0
        for idx in indices:
            df_ = None
            for _order in orders:
                row_cnt = self.row_cnts[_order]
                if idx > row_cnt - 1:
                    idx -= row_cnt
                    continue
                df = self.dfs[_order]
                if df is None:
                    df = self.__read_piece(_order)
                
                type_ = self.types[_order]
                if type_ == "csv":
                    df_ = df.iloc[idx:(idx+1)]
                elif type_ == "tsfefs":
                    tsfefs = df
                    df_ = tsfefs[idx:(idx+1)]
                else:
                    print("Type", type_, "is not supported")
                    assert False
                break
                
            if len(df_) == 0:
                print("Indice could be out of range")
                assert False
                
            cnt_new = cnt + 1
            if type_ == "csv":
                df.iloc[[idx]] = value[ cnt : cnt_new ]
                self.dfs[_order] = df
            elif type_ == "tsfefs":
                tsfefs[[idx]] = value[ cnt : cnt_new ]
                self.dfs[_order] = tsfefs
            else:
                print("Type", type_, "not supported")
                assert False
            cnt = cnt_new 
            self.actions[_order] = "update"
                    
            self.renew_idx(_order)

        return
            

    def __setitem__using_bools(self,B,value):
        self.__bools_check(B)
        indices = np.array(range(len(B)))[B].tolist()
        self.__setitem__using_indices(indices,value)
        return


    def __setitem__using_strs(self,colnames,value):
        
        self.__strs_check(colnames)
        value = self.__value_check(value, len(colnames), self.row_cnt)
        orders = self.__no_overlapping_time_range()
        
        cum_row_cnt = 0
        for _order in orders:
            idx = _order
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(idx)
            new_cum_row_cnt = cum_row_cnt + self.row_cnts[_order]
            df[colnames] = value[cum_row_cnt:new_cum_row_cnt]
            self.dfs[_order] = df
            cum_row_cnt = new_cum_row_cnt
            self.actions[_order] = "update"
        return

        
    def __setitem__(self, key, value):
        """
        Accetpable assignments:
        Single:
         - int: tsfefs[33] = value # row index 33 assignment
         - str: tsfefs["age"] = value
        Multiple:
         - slice of ints: tsfefs[20:] = value, tsfefs[20:50] = value, tsfefs[::2] = value, ...
         - list of ints: tsfefs[[1,2,5,10]] = value
         - list of strs: tsfefs[["time","purchased","item","price"]] = value
         - list of bools: tsfefs[[True,False,True,...]] = value # must have length == row_cnt
        """        
        if isinstance(key,str_types):
            self.__setitem__using_str(key, value)
        elif isinstance(key,int_types):
            self.__setitem__using_idx(key, value)
        elif isinstance(key,(arr_types,slice)):
            if isinstance(key,np.ndarray):
                assert len(key.shape) == 1 # must be in (n,)
                key = list(key)
            elif isinstance(key,pd.Series):
                key = list(key)
            elif isinstance(key,slice):                
                assert all([ (s is None) or isinstance(s,int) for s in [ key.start,key.stop,key.step ] ])
                key = list(range(self.row_cnt))[key]
                
            type_ = type(key[0])
            assert isinstance(key[0],(int_types,str_types,bool_types))
            assert all([ isinstance(k,type_) for k in key ])
            
            if type_ in str_types:
                self.__setitem__using_strs(key,value)
            elif type_ in bool_types:
                self.__setitem__using_bools(key,value)
            elif type_ in int_types:
                self.__setitem__using_indices(key,value)
            else:
                print("No such type" )
                assert False
        else:
            print("Something's wrong for the type(s)")
            assert False
            
        self.take_actions(max_level=0)
        return
    ##################################### Overriding Square Bracket - setitem: END #####################################
    ####################################################################################################################

    
    
    
    

  



    ####################################################################################################################
    ##################################### Overriding Square Bracket - delitem: BEG #####################################
    
    def __delitem__using_idx(self,idx):

        idx = self.__idx_check(idx)        
        orders = self.__no_overlapping_time_range()
        _order, accum_row_cnt = self.__get_piece_idx(idx, orders)

        df = self.dfs[_order]
        if df is None:
            df = self.__read_piece(_order)
            self.dfs[_order] = df

        idx -= accum_row_cnt
        type_ = self.types[_order]
        row_cnt = self.row_cnts[_order]
        len_ = None
        if type_ == "csv":
            """
            Since there's no del df[idx] for dataframe
            """
            B = [True]*row_cnt
            B[idx] = False
            df = df[B].reset_index(drop=True)
            len_ = len(df)
        elif type_ == "tsfefs":
            tsfefs = df
            del tsfefs[idx]
            tsfefs.take_actions(max_level=4)
            # len_ = tsfefs.row_cnt  # since action is taken, this tsfefs' row_cnt is updated.
            len_ = len(tsfefs)  # since action is taken, this tsfefs' row_cnt is updated.
            df = tsfefs
        else:
            print("Type", type_, "not supported")
            assert False

        self.dfs[_order] = df
        self.actions[_order] = "update"

        """
        Should it be renewed in cache?
        2023-10-07, decided not to renew in cache.
        """
        # self.renew_idx(_order) 
            
        if len_ == 0:
            self.actions[_order] = "delete"
        return 


    def __delitem__using_str(self,colname):

        self.__str_check(colname)
        assert colname != self.time_col

        orders = self.__no_overlapping_time_range()
        for _order in orders:
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(_order)
            del df[colname]
            self.dfs[_order] = df
            self.actions[_order] = "save"
        self.colnames.remove(colname)
        return    


    def __delitem__using_indices(self,indices):

        indices = self.__indices_check(indices)
        indices_copied = dc(indices)
        
        orders = self.__no_overlapping_time_range()

        while len(indices) > 0:
            _order, adjusted_indices = self.__get_indices_in_same_piece(indices, orders)
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(_order)

            type_ = self.types[_order]
            if type_ == "csv":
                row_cnt = self.row_cnts[_order]
                B = np.array([True]*row_cnt)
                B[adjusted_indices] = False
                df = df[B].reset_index(drop=True)
                len_ = len(df)
            elif type_ == "tsfefs":
                tsfefs = df
                del tsfefs[adjusted_indices]
                tsfefs.take_actions(max_level=4)
                len_ = tsfefs.row_cnt  # since action is taken, this tsfefs' row_cnt is updated.
                df = tsfefs
            else:
                print("Type", type_, "not supported")
                assert False

            self.dfs[_order] = df
            self.actions[_order] = "update"

            """
            Should it be renewed in cache?
            2023-10-07, decided not to renew in cache.
            """
            # self.renew_idx(_order)

            if len_ == 0:
                self.actions[_order] = "delete"
    
            indices = indices[len(adjusted_indices):]
        return


    def __delitem__using_bools(self,B):

        self.__bools_check(B)

        indices = np.array(range(len(B)))[B].tolist()
        self.__delitem__using_indices(indices)
        return    


    def __delitem__using_strs(self,colnames):

        self.__strs_check(colnames)
        assert self.time_col not in colnames
        
        orders = self.__no_overlapping_time_range()
        for _order in orders:
            df = self.dfs[_order]
            type_ = self.types[_order]
            if df is None:
                df = self.__read_piece(_order)
                
            if type_ == "csv":
                for col in colnames:
                    del df[col]
            elif type_ == "tsfefs":
                del df[colnames]
            else:
                print("Type", type_, "not supported")
                assert False
                
            self.dfs[_order] = df
            self.actions[_order] = "save"
            
        for col in colnames:
            self.colnames.remove(col)
        return


    def __delitem__(self, key):
        """
        Accetpable delete:
        Single:
         - int: del tsfefs[33]
         - str: del tsfefs["age"]
        Multiple:
         - slice of ints: del tsfefs[20:], del tsfefs[20:50], del tsfefs[::2], ...
         - list of ints: del tsfefs[[1,2,5,10]]
         - list of strs: del tsfefs[["time","purchased","item","price"]]
         - list of bools: del tsfefs[[True,False,True,...]] # must have length == row_cnt
        """        
        if isinstance(key,str_types):
            self.__delitem__using_str(key)
            return
        elif isinstance(key,int_types):
            self.__delitem__using_int(key)
            return
        elif isinstance(key,(arr_types,slice)):
            if isinstance(key,np.ndarray):
                assert len(key.shape) == 1 # must be in (n,)
                key = list(key)
            elif isinstance(key,pd.Series):
                key = list(key)
            elif isinstance(key,slice):                
                assert all([ (s is None) or isinstance(s,int) for s in [ key.start,key.stop,key.step ] ])
                key = list(range(self.row_cnt))[key]

            type_ = type(key[0])
            assert isinstance(key[0],(int_types,str_types,bool_types))
            assert all([ isinstance(k,type_) for k in key ])

            if type_ in str_types:
                self.__delitem__using_strs(key)
                return
            elif type_ in bool_types:
                self.__delitem__using_bools(key)
                return
            elif type_ in int_types:
                self.__delitem__using_indices(key)
                return
            else:
                print("No such type" )
                assert False
        else:
            print("Something's wrong for the type(s)")
            assert False
            
    ##################################### Overriding Square Bracket - delitem: END #####################################
    ####################################################################################################################



    
    
    
    
    



    

    ######################################################################################################################
    ############################################# Overriding ==, >, <, >=, <=: BEG #######################################
    """
    The operators will be about the comparison of timestamps
    Need to check:
    1. tsfefs1 <op> tsfefs2
    2. tsfefs <op> df
    3. df <op> tsfefs
    4. tsfefs <op> ts
    5. tsfefs <op> pd.Series(tss)
    
    The codes are strictly prohibited from using tsfefs[time_col],
    since such operation will affect the cache.
    """
    def __read_time(self, idx):
                
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
                
        if self.types[idx] == "csv":

            time_col = self.time_col
            dtf = self.datetime_format
            df = pd.read_csv(fullname, usecols=[time_col], dtype={time_col:str})
            df[time_col] = df[time_col].apply(lambda x: dt.strptime(x,dtf))
            df = df.sort_values(by=time_col).reset_index(drop=True)
            S = df[time_col]
        
        elif self.types[idx] == "tsfefs":

            tsfefs = TSFEFS()
            tsfefs.read(fullname)
            S = []
            # for order_ in tsfefs.__no_overlapping_time_range():
            #     S.extend(list(tsfefs.__read_time(order_)))
            # S = pd.Series(S)
            S = tsfefs.__get_times()
        
        else:
            print("File type \"tsfefs\" not yet implemented")
            assert False
            
        return S

    def __get_time(self, idx):

        df = self.dfs[idx]
        type_ = self.types[idx]
        if df is None:
            S = self.__read_time(idx)
            return S

        if type_ == "csv":
            S = df[self.time_col]
        elif type_ == "tsfefs":
            tsfefs = df
            S = tsfefs.__get_times()
        else:
            assert False
            
        return S
            

    def __get_times(self):
        
        S = []
        for order_ in self.__no_overlapping_time_range():
            S_ = self.__get_time(order_)
            S_ = list(S_)
            S.extend(S_)
        S = pd.Series(S)
        return S
    
    

    def __eq_series(self, S):
        assert self.row_cnt == len(S)
        assert [ isinstance(ts,dt) for ts in S ]        
        S_ = self.__get_times()
        B = S_ == S
        return B
        
    
    def __eq_tsfefs(self, tsfefs):
        assert self.time_col == tsfefs.time_col
        assert self.row_cnt == tsfefs.row_cnt
        
        S = tsfefs.__get_times()
        return self.__eq_series(S)


    def __eq_df(self, df):
        assert self.row_cnt == len(df)
        S = df[self.time_col]
        return self.__eq_series(S)

    
    def __eq_dt(self, ts):
        S = self.__get_times()
        return S == ts                


    def __eq__(self, other):
        if isinstance(other,pd.Series):
            return self.__eq_series(other)
        elif isinstance(other,TSFEFS):
            return self.__eq_tsfefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__eq_df(other)
        elif isinstance(other,dt):
            return self.__eq_dt(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False

            

    def __ne_series(self, S):
        assert self.row_cnt == len(S)
        assert [ isinstance(ts,dt) for ts in S ]        
        S_ = self.__get_times()
        B = S_ != S
        return B
    
    
    def __ne_tsfefs(self, tsfefs):
        assert self.time_col == tsfefs.time_col
        assert self.row_cnt == tsfefs.row_cnt
        
        S = tsfefs.__get_times()
        return self.__ne_series(S)

    
    def __ne_df(self, df):
        assert self.row_cnt == len(df)
        S = df[self.time_col]
        return self.__ne_series(S)

    
    def __ne_dt(self, ts):
        S = self.__get_times()
        return S != ts                


    def __ne__(self, other):
        if isinstance(other,pd.Series):
            return self.__ne_series(other)
        elif isinstance(other,TSFEFS):
            return self.__ne_tsfefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__ne_df(other)
        elif isinstance(other,dt):
            return self.__ne_dt(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False
            
            
            
            
    def __lt_series(self, S):
        assert self.row_cnt == len(S)
        assert [ isinstance(ts,dt) for ts in S ]
        S_ = self.__get_times()
        B = S_ < S
        return B

    def __lt_tsfefs(self, tsfefs):
        assert self.time_col == tsfefs.time_col
        assert self.row_cnt == tsfefs.row_cnt        
        # return self[self.time_col] < tsfefs[self.time_col]
        S = tsfefs.__get_times()
        return self.__lt_series(S)
    
    def __lt_df(self, df):
        assert self.row_cnt == len(df)
        # return self[self.time_col] < df[self.time_col]
        S = df[self.time_col]
        return self.__lt_series(S)

    def __lt_dt(self, ts):
        # return self[self.time_col] < ts
        S = self.__get_times()
        B = S < ts
        return B

    def __lt__(self, other):
        if isinstance(other,pd.Series):
            return self.__lt_series(other)
        elif isinstance(other,TSFEFS):
            return self.__lt_tsfefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__lt_df(other)
        elif isinstance(other,dt):
            return self.__lt_dt(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False



    def __gt_series(self, S):
        assert self.row_cnt == len(S)
        assert [ isinstance(ts,dt) for ts in S ]
        S_ = self.__get_times()
        B = S_ > S
        return B

    def __gt_tsfefs(self, tsfefs):
        assert self.time_col == tsfefs.time_col
        assert self.row_cnt == tsfefs.row_cnt
        # return self[self.time_col] > tsfefs[self.time_col]
        S = tsfefs.__get_times()
        return self.__gt_series(S)        
        
    def __gt_df(self, df):
        assert self.row_cnt == len(df)
        # return self[self.time_col] > df[self.time_col]
        S = df[self.time_col]
        return self.__gt_series(S)        

    def __gt_dt(self, ts):
        # return self[self.time_col] > ts
        S = self.__get_times()
        return S > ts
    
    def __gt__(self, other):
        if isinstance(other,pd.Series):
            return self.__gt_series(other)
        elif isinstance(other,TSFEFS):
            return self.__gt_tsfefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__gt_df(other)
        elif isinstance(other,dt):
            return self.__gt_dt(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False


    def __le_series(self, S):
        assert self.row_cnt == len(S)
        assert [ isinstance(ts,dt) for ts in S ]
        S_ = self.__get_times()
        B = S_ <= S
        return B

    def __le_tsfefs(self, tsfefs):
        assert self.time_col == tsfefs.time_col
        assert self.row_cnt == tsfefs.row_cnt
        # return self[self.time_col] <= tsfefs[self.time_col]
        S = tsfefs.__get_times()
        return self.__le_series(S)
    
    def __le_df(self, df):
        assert self.row_cnt == len(df)
        # return self[self.time_col] <= df[self.time_col]
        S = df[self.time_col]
        return self.__le_series(S)
    
    def __le_dt(self, ts):
        # return self[self.time_col] <= ts
        S = self.__get_times()
        return S <= ts

    def __le__(self, other):
        if isinstance(other,pd.Series):
            return self.__le_series(other)
        elif isinstance(other,TSFEFS):
            return self.__le_tsfefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__le_df(other)
        elif isinstance(other,dt):
            return self.__le_dt(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False


    def __ge_series(self, S):
        assert self.row_cnt == len(S)
        assert [ isinstance(ts,dt) for ts in S ]
        S_ = self.__get_times()
        B = S_ >= S
        return B

    def __ge_tsfefs(self, tsfefs):
        assert self.time_col == tsfefs.time_col
        assert self.row_cnt == tsfefs.row_cnt        
        # return self[self.time_col] >= tsfefs[self.time_col]
        S = tsfefs.__get_times()
        return self.__ge_series(S)
    
    def __ge_df(self, df):
        assert self.row_cnt == len(df)
        # return self[self.time_col] >= df[self.time_col]
        S = df[self.time_col]
        return self.__ge_series(S)

    def __ge_dt(self, ts):
        # return self[self.time_col] >= ts
        S = self.__get_times()
        return S >= ts

    def __ge__(self, other):
        if isinstance(other,pd.Series):
            return self.__ge_series(other)
        elif isinstance(other,TSFEFS):
            return self.__ge_tsfefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__ge_df(other)
        elif isinstance(other,dt):
            return self.__ge_dt(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False
            
    ############################################# Overriding ==, >, <, >=, <=: END #######################################
    ######################################################################################################################

 


    #####################################################################################################################
    ################################################# Overriding +: BEG #################################################
    def __add_tsfefs(self, tsfefs):
        assert self.time_col == tsfefs.time_col
        assert len(set(self.colnames) - set(tsfefs.colnames)) == 0
        assert len(set(tsfefs.colnames) - set(self.colnames)) == 0
        assert self.datetime_format == tsfefs.datetime_format
        
        
        # assert not tsfefs.has_pending_actions()
        
        assert tsfefs.has_valid_status() # no None in the lists
        assert not tsfefs.conflict_exist()
      
        
        
        # the added tsfefs must be under the current path
        """
        tsfefs = dc(tsfefs)
        tsfefs.path = self.__compose_fullpath()
        """
        tsfefs = tsfefs.clone(self.__compose_fullpath(),tsfefs.name)
        if tsfefs.name in self.pieces:
            tsfefs.name = self.gen_valid_piece(prefix=tsfefs.name+"_", suffix="")
        self.pieces += [tsfefs.name]
        self.types += ["tsfefs"]
        # self.frs += [tsfefs.fr]
        # self.tos += [tsfefs.to]
        # self.row_cnts += [tsfefs.row_cnt]
        self.frs += [None]
        self.tos += [None]
        self.row_cnts += [None]
        self.actions += ["update"]
        self.action_params += [None]
        self.dfs += [tsfefs] # the elements can be df or TSFEFS        
        
        self.include_idx(len(self.pieces)-1) # len(self.pieces)-1: the new highest array idx

        return self


    def __add_df(self, df):
        assert self.time_col in df.columns
        assert len(set(self.colnames) - set(df.columns)) == 0
        assert len(set(df.columns) - set(self.colnames)) == 0
        try:
            dtf = self.datetime_format
            # print("__add_df:", "dtf")
            # print(dtf)
            _ = df[self.time_col].apply(lambda x: x.strftime(dtf))
        except:
            print("Problem with df's datetime")
            assert False

        piece = self.gen_valid_piece(prefix="", suffix="")
        
        
        self.vprint("Newly added file:", piece)
        
        
        self.pieces += [piece]
        self.types += ["csv"]
        # self.frs += [tsfefs.fr]
        # self.tos += [tsfefs.to]
        # self.row_cnts += [tsfefs.row_cnt]
        self.frs += [None]
        self.tos += [None]
        self.row_cnts += [None]
        self.actions += ["update"]
        self.action_params += [None]
        self.dfs += [df] # the elements can be df or TSFEFS
        
        self.include_idx(len(self.pieces)-1) # len(self.pieces)-1: the new highest array idx
            
        return self

        
    def __add__(self, other):
        # print(type(other))
        if isinstance(other,TSFEFS):
            return self.__add_tsfefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__add_df(other)
        else:
            print("Types", type(other), "not supported.")
            assert False
    ################################################# Overriding +: END #################################################
    #####################################################################################################################


    
    
    
    
    
    #####################################################################################################################
    ########################################## conflict resolve, optimize: BEG ##########################################
    
    def conflict_exist(self):
        try:
            _ = self.__no_overlapping_time_range()
            return False
        except:
            return True
        
        
    def resolve_conflict(self):
        
        while self.conflict_exist():
        
            df_index, orders = self.dump_index_df() # already sorted by "fr"
            self = self.load_index_df(df_index, orders)
            
            for idx in range(len(self.pieces)-1):
                
                
                if self.frs[idx+1] < self.tos[idx]:

                    # print("Delay no more")
                    
                    df0 = self.dfs[idx]
                    if df0 is None:
                        df0 = self.__read_piece(idx)
                        self.dfs[idx] = df0

                    df1 = self.dfs[idx+1]
                    if df1 is None:
                        df1 = self.__read_piece(idx+1)
                        self.dfs[idx+1] = df1

                    B0 = df0[self.time_col] < self.frs[idx+1]                          
                    df0a = df0[B0].reset_index(drop=True)
                    df0b = df0[~B0].reset_index(drop=True)

                    B1 = df1[self.time_col] <= self.tos[idx]
                    df1a = df1[B1].reset_index(drop=True)
                    df1b = df1[~B1].reset_index(drop=True)

                    self.actions[idx] = "delete"
                    self.actions[idx+1] = "delete"

                    df_new = pd.concat([df0b,df1a]).reset_index(drop=True)
                    df_new = df_new.sort_values(by=self.time_col).reset_index(drop=True)
                    
                    if len(df0a) > 0:
                        self += df0a
                    if len(df1b) > 0:
                        self += df1b
                    if len(df_new) > 0:
                        self += df_new
                    break
                    
            self.take_actions(max_level=4)

            
            
    def optimize_files(self):
                
        while True:
            
            
            df_index, orders = self.dump_index_df() # already sorted by "fr"
            self = self.load_index_df(df_index, orders)
            
            idx = 0
            pieces_len_adjustment = 0
            while idx < len(self.pieces):
                
                # print("2nd while loop, idx:", idx)
                # print("2nd while loop, len(self.pieces):", len(self.pieces))

                if self.row_cnts[idx] >= self.max_row_per_piece:
                    self.actions[idx] = "split"
                    pieces_len_adjustment += 1
                    # print("Loc 1:", "split")
                    # print("Loc 1, len(self.dfs[idx]):", len(self.dfs[idx]))
                    # self.print_tsfefs_info()
                    break
                elif self.row_cnts[idx] < self.max_row_per_piece/2:
                    idx_next = idx + 1
                    if idx_next < len(self.pieces):
                        if self.row_cnts[idx_next] + self.row_cnts[idx] < self.max_row_per_piece:
                            self.actions[idx] = "merge"
                            pieces_len_adjustment -= 1
                            self.action_params[idx] = idx_next
                            # print("Loc 2:", "merge")
                            # self.print_tsfefs_info()
                        elif self.row_cnts[idx_next] > self.max_row_per_piece/2:
                            self.actions[idx_next] = "split"
                            pieces_len_adjustment += 1
                            # print("Loc 3:", "split")
                            # self.print_tsfefs_info()
                        else:
                            # better not do anything, leave it alone.
                            idx += 1
                            continue
                        break
                    
                idx += 1
            
            # "split" is 1, "merge" is 2.
            # but since after merge there will be "delete", which is 4, s
            # so max_level = 4.
            self.take_actions(max_level=4)
            # self.print_tsfefs_info()
            # print("idx:", idx)
            # print("len(self.pieces):", len(self.pieces))
            # print("pieces_len_adjustment:", pieces_len_adjustment)
            if idx == len(self.pieces) - pieces_len_adjustment:
                break

    ########################################## conflict resolve, optimize: END ##########################################
    #####################################################################################################################
    
    

    
    
    
    
    
    
    ####################################################################################################################
    ################################################# Read, Write: BEG #################################################
    """
    Equals to df.read_csv
    """
    def read(self, fullname):
        
        self.path, self.name = self.__decompose_fullpath(fullname)

        full_meta_dict_name = '/'.join([fullname,TSFEFS.meta_json_name])
        dict_meta = json.load(open(full_meta_dict_name,'r'))        
        self = self.load_meta(dict_meta)
        
        full_index_df_name = '/'.join([fullname,TSFEFS.index_df_name])
        df_index = pd.read_csv(full_index_df_name, dtype={dict_meta["time_col"]:str})
        self = self.load_index_df(df_index)
        
        self.actions = [ "" for i in range(len(df_index)) ]
        self.action_params = [ None for i in range(len(df_index)) ]
        self.dfs = [ None for i in range(len(df_index)) ]
        self.cache = []
        return

    
    def write(self, fullname=None):

        if fullname is None:
            fullname = self.__compose_fullpath()
        else:
            self.path, self.name = self.__decompose_fullpath(fullname) # just to ensure the .tsfefs extension exits
            
        if not os.path.isdir(fullname):
            os.mkdir(fullname)
        
        # Consistency of actions check
        # should have all actions cleared
        assert not self.has_pending_actions()
        # action_levels = [ TSFEFS.action_domain.index(act) for act in self.actions ]
        # assert all(np.array(action_levels) == TSFEFS.action_domain.index(""))

        
        dict_meta = self.dump_meta()
        full_meta_dict_name = '/'.join([fullname,TSFEFS.meta_json_name])
        json.dump(dict_meta, open(full_meta_dict_name,'w'))
        

        df_index, orders = self.dump_index_df()
        full_index_df_name = '/'.join([fullname,TSFEFS.index_df_name])
        df_index.to_csv(full_index_df_name, index=False)
        return
    ################################################# Read, Write: END #################################################
    ####################################################################################################################

        
    def remove(self):
        fullname = self.__compose_fullpath()
        
        def recursive_remove(fullpath):
            for f in os.listdir(fullpath):
                fullname = "%s/%s"%(fullpath,f)
                if os.path.isdir(fullname):
                    recursive_remove(fullname)
                else:
                    os.remove(fullname)
            os.rmdir(fullpath)
            
        if os.path.isdir(fullname):
            # for f in os.listdir(fullname):
            #     os.remove("%s/%s"%(fullname,f))
            # os.rmdir(fullname)
            recursive_remove(fullname)
            
        # del tsfefs_case2; tsfefs_case2 = None
        
        return




