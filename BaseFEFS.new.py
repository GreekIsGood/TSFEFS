from general import *


#########################################################################################################################
######################################################### TO DO #########################################################
"""
Tasks
1. optimize/review maintain_cache <-- currently requiring all actions cleared before maintain_cache 
3. destructor (should be considered together with GreekEssentiates)
4. Internal clone (with a temp path also specified in meta)
5. optimize merge (not the __action_merge)
6. optimize/review operators: >, <, >=, <=, ==, … 

Done
 - Delete piece after save (done already?) <-- should be done, recheck.
 - Optimize the __setitem__using_indices() function: very inefficient, one idx at a time.
 - Optimize the resolve conflict function.
 - Optimize the optimize_files function.


"""
#########################################################################################################################
#########################################################################################################################


class BaseFEFS():
    
    ##########################################################################################################
    ########################################## Class Variables: BEG ##########################################
    extension = "fefs"
    index_df_name = ".index.csv"
    meta_json_name = ".meta.json"
    # Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
    action_domain = ["update", "split", "merge", "save", "delete", ""]
    ########################################## Class Variables: END ##########################################
    ##########################################################################################################

    
    def print_info(self):
        print("path:",self.path, ", name:", self.name)
        print("pieces:", self.pieces)
        print("types:", self.types)
        print("fr:", self.fr, ", to:", self.to)
        
        print_frs = self.frs
        print_tos = self.tos
        if self.seq_inv_trans_method is not None:
            assert self.seq_trans_method is not None
            try:
                print_frs = [ self.seq_inv_trans_method(fr) for fr in self.frs ]
                print_tos = [ self.seq_inv_trans_method(to) for to in self.tos ]
            except:            
                pass
        else:
            assert self.seq_trans_method is None
            
        print("frs:", print_frs)
        print("tos:", print_tos)
            
            
        print("seq_col:", self.seq_col)
        
        print("piece_name_len:", self.piece_name_len)
        print("colnames:", self.colnames, ", row_cnt:", self.row_cnt)
        print("row_cnts:", self.row_cnts)
        print("actions:", self.actions, ", action_params:", self.action_params)
        print("cache:", self.cache, ", cache_config:", self.cache_config)
        if self.dfs is None:
            print("dfs:", self.dfs)
        else:
            print("dfs:", [ "Non-emtpy" if df is not None else df for df in self.dfs ])
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
        # self.datetime_format = None
        self.seq_col = None
        
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
        self.dfs = None # the elements can be df or FEFS
        
        # Cache section
        self.cache = None
        
        self.seq_trans_method = None
        self.seq_inv_trans_method = None
        self.seq_read_dtype = None
        self.seq_type = None
        
        


    @classmethod
    def create(cls, dict_meta, name, path=None):
        """
        Usecase
        --------
        When you start with nothing but will add dataframes subsequently.
        """
        
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert cls != BaseFEFS
        
        
        # It is supposed to be followed by fefs += df
        fefs = cls()
        
        if path is None:
            path = os.getcwd()
        fefs.path = path
        fefs.name = name
        
        
        fefs.row_cnt = None
        fefs.fr = None
        fefs.to = None        
        fefs.pieces = []
        fefs.types = []
        
        fefs.frs = []
        fefs.tos = []
        fefs.row_cnts = []

        fefs.actions = []
        fefs.action_params = []
        fefs.dfs = []

        # Cache section
        fefs.cache = []
        
        
        # Keys from dict_meta
        fefs = fefs.load_meta(dict_meta)
        
        
        return fefs
    


    def __check_cache_config(self):
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
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
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        # Keys from dict_meta
        self.max_row_per_piece = dict_meta["max_row_per_piece"]
        # self.datetime_format = dict_meta["datetime_format"]
        self.seq_col = dict_meta["seq_col"]
        
        self.piece_name_len = dict_meta["piece_name_len"]
        self.colnames = dict_meta["colnames"]
        self.cache_config = dict_meta["cache_config"]
        self.__check_cache_config()
        
        try:
            self.row_cnt = dict_meta["row_cnt"]
        except:
            pass



        # dtf = self.datetime_format
        try:
            self.fr = dict_meta["fr"]
            self.to = dict_meta["to"]
            if self.seq_trans_method is not None:
                assert self.seq_inv_trans_method is not None
                self.fr = self.seq_trans_method(self.fr)
                self.to = self.seq_trans_method(self.to)
        except:
            pass

        return self



    def dump_meta(self):
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        # Keys from dict_meta
        dict_meta = {}
        dict_meta["max_row_per_piece"] = self.max_row_per_piece
        # dict_meta["datetime_format"] = self.datetime_format
        dict_meta["seq_col"] = self.seq_col
        
        dict_meta["piece_name_len"] = self.piece_name_len
        dict_meta["colnames"] = self.colnames
        dict_meta["cache_config"] = self.cache_config

        try:
            dict_meta["row_cnt"] = self.row_cnt
        except:
            pass

        # dtf = self.datetime_format
        try:
            dict_meta["fr"] = self.fr
            dict_meta["to"] = self.to
            if self.seq_inv_trans_method is not None:
                assert self.seq_trans_method is not None
                dict_meta["fr"] = self.seq_inv_trans_method(self.fr)
                dict_meta["to"] = self.seq_inv_trans_method(self.to)
        except:
            pass
        
        return dict_meta



    def load_index_df(self, df_index, orders=None):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        if len(df_index) == 0:
            self.pieces = []
            self.types = []
            self.frs = []
            self.tos = []
            self.row_cnts = []
            return self

        # dtf = self.datetime_format
        if self.seq_trans_method is not None:
            assert self.seq_inv_trans_method is not None
            df_index["fr"] = df_index["fr"].apply(lambda x: self.seq_trans_method(x))
            df_index["to"] = df_index["to"].apply(lambda x: self.seq_trans_method(x))
            
        """ modified @ 01-03-2024 """
        # df_index = df_index.sort_values(by="fr").reset_index(drop=True)
        df_index = df_index.sort_values(by=["fr","to"]).reset_index(drop=True)
        
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
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        df_empty = pd.DataFrame({"piece":[],"type":[],"fr":[],"to":[],"row_cnt":[]})
        empty_orders = list(df_empty.index)
        if self.pieces is None:
            assert all([ l is None for l in [self.types, self.frs, self.tos, self.row_cnts] ])
            return (df_empty, empty_orders)
        elif len(self.pieces) == 0:
            assert all([ len(l)==0 for l in [self.types, self.frs, self.tos, self.row_cnts] ])
            return (df_empty, empty_orders)
            
        df_index = pd.DataFrame()
        df_index["piece"] = self.pieces
        df_index["type"] = self.types
        df_index["fr"] = self.frs
        df_index["to"] = self.tos
        df_index["row_cnt"] = self.row_cnts
        
        # df_index = df_index.sort_values(by="fr")
        df_index = df_index.sort_values(by=["fr","to"])
        
        orders = list(df_index.index)
        df_index = df_index.reset_index(drop=True)
                
        if self.seq_inv_trans_method is not None:
            assert self.seq_trans_method is not None
            df_index["fr"] = df_index["fr"].apply(lambda x: self.seq_inv_trans_method(x))
            df_index["to"] = df_index["to"].apply(lambda x: self.seq_inv_trans_method(x))
        
        return (df_index, orders)
    

    
    def clone(self, path, name):
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        # It is supposed to be followed by fefs += df
        fefs = self.__class__()
        
        fefs.path = path
        fefs.name = name
            
        fefs.max_row_per_piece = self.max_row_per_piece
        # fefs.datetime_format = self.datetime_format
        fefs.seq_col = self.seq_col
        
        fefs.piece_name_len = self.piece_name_len
        fefs.colnames = dc(self.colnames)
        fefs.cache_config = dc(self.cache_config)
        fefs.fr = self.fr
        fefs.to = self.to
        fefs.row_cnt = self.row_cnt
            
        fefs.pieces = dc(self.pieces)
        fefs.types = dc(self.types)
        fefs.frs = dc(self.frs)
        fefs.tos = dc(self.tos)
        fefs.row_cnts = dc(self.row_cnts)

        fefs.actions = dc(self.actions)
        fefs.action_params = dc(self.action_params)
        fefs.dfs = [ dc(df_) for df_ in self.dfs ]
        
        # # Cache section
        fefs.cache = dc(self.cache)
        
        
        # compensating codes for self.dfs[idx] is None
        for idx in range(len(fefs.pieces)):
            
            if self.dfs[idx] is None:
                
                assert fefs.dfs[idx] is None
                assert self.pieces[idx] == fefs.pieces[idx] 
                
                piece = self.pieces[idx] 
                src = self.__compose_piece_fullname(piece)
                dst = fefs.__compose_piece_fullname(piece)
                fefs_fullname = fefs.__compose_fullpath()
                if not os.path.isdir(fefs_fullname):
                    os.mkdir(fefs_fullname)
                # print("Command:", "cp \"%s\" \"%s\""%(src,dst))
                os.system("cp \"%s\" \"%s\""%(src,dst))
                
            elif self.actions[idx] == "": # i.e., self.dfs[idx] is not None but no "save" command
                fefs.actions[idx] = "save"

        return fefs

    ############################################ Init methods: END ############################################
    ###########################################################################################################
        

        
        
        
        
        
        
        
        
        
    ##########################################################################################################
    ######################################### Path, Name, Piece: BEG #########################################
    @staticmethod
    def get_random_string(n):
        # choose from all lowercase letter
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(n))
        return result_str
    
    def gen_valid_piece(self, prefix="", suffix=""):
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        while True:
            piece = self.__class__.get_random_string(self.piece_name_len)
            piece = prefix + piece + suffix
            if piece not in self.pieces:
                break
        return piece

        
    def __decompose_fullpath(self, fullpath):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        path = fullpath.split('/')
        name = path[-1]
        path = '/'.join(path[:-1])
        
        names = name.split('.')
        assert names[-1] == self.__class__.extension
        name = '.'.join(names[:-1])
        return (path,name)
    

    def __decompose_piece_fullname(self,piece_fullname):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        

        fullpath = piece_fullname.split('/')
        piece = fullpath[-1]
        fullpath = '/'.join(fullpath[:-1])
        path, name = self.__decompose_fullpath(fullpath)
        return (path,name,piece)


    def __compose_fullpath(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        fullpath = '/'.join([self.path, self.name + '.' + self.__class__.extension])
        return fullpath
    

    def __compose_piece_fullname(self,piece):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        fullname = '/'.join([self.__compose_fullpath(), piece])
        return fullname

    
    def file_exists(self):
        fullpath = self.__compose_fullpath()
        return os.path.isdir(fullpath)
    
    ######################################### Path, Name, Piece: END #########################################
    ##########################################################################################################


    
    
    
    def vprint(self,*value):
        try:
            if self.verbose: # may not have the explicit self.verbose attribute
                print(value)
        except:
            pass
        
        
        
    def has_valid_status(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        attributes = [
            self.path, self.name, self.max_row_per_piece, self.row_cnt, 
            self.seq_col,
            self.piece_name_len, self.fr, self.to, self.colnames, 
            self.pieces, self.types,
            self.frs, self.tos, self.row_cnts, self.dfs, self.actions, self.action_params
        ]
        if any([ attr is None for attr in attributes ]):
            return False
        
        lists = [ self.pieces, self.types, self.frs, self.tos, self.row_cnts ]
        if any([ val is None for list_ in lists for val in list_ ]):
            return False
        return True
        
            
            
    @classmethod
    def get_index(cls,df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert cls != BaseFEFS
        
        
        if isinstance(df,pd.DataFrame):
            return list(df.index)
        elif isinstance(df,cls):
            fefs = df
            indices = list(range(fefs.row_cnt))
            return indices
        else:
            print("Type", type(df), "not supported")
            assert False
            

    

    def __read_piece(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        """
        Should these be added?
        Question @ 03-03-2024
        """
        # df = self.dfs[idx]
        # assert df is None
        
        
        piece = self.pieces[idx]
        type_ = self.types[idx]
        fullname = self.__compose_piece_fullname(piece)
        if type_ == "csv":

            if self.seq_read_dtype is None:
                df = pd.read_csv(fullname)
            else:
                df = pd.read_csv(fullname, dtype={self.seq_col:self.seq_read_dtype})

            # dtf = self.datetime_format
            if self.seq_trans_method is not None:
                assert self.seq_inv_trans_method is not None
                df[self.seq_col] = df[self.seq_col].apply(lambda x: self.seq_trans_method(x))                

            df = df.sort_values(by=self.seq_col).reset_index(drop=True)
            
            self.dfs[idx] = df
            """
            Comment @ 05-03-2024
            No, don't make any cache adjustment here.
            Do it outside.
            """
            # self.include_idx(idx)
            return self.dfs[idx]
        
        elif type_ == self.__class__.__name__:
                        
            fefs = self.__class__()
            fullname += '.' + self.__class__.extension
            fefs.read(fullname)
            self.dfs[idx] = fefs
            
            """
            Comment @ 05-03-2024
            No, don't make any cache adjustment here.
            Do it outside.
            """
            # self.include_idx(idx)

            return self.dfs[idx]
        
        else:
            print("File type \"%s\" not yet implemented"%str(type_))
            assert False
    
    
    
    
    def __write_piece(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        df = self.dfs[idx]
        assert df is not None

        fefs_fullname = self.__compose_fullpath()
        if not os.path.isdir(fefs_fullname):
            os.mkdir(fefs_fullname)
           
        type_ = self.types[idx]
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
        
        if type_ == "csv":
            
            df = df.sort_values(by=self.seq_col).reset_index(drop=True)
            
            self.dfs[idx] = df
            # dtf = self.datetime_format
            df2 = dc(df)
            if self.seq_inv_trans_method is not None:
                assert self.seq_trans_method is not None
                df2[self.seq_col] = df2[self.seq_col].apply(lambda x: self.seq_inv_trans_method(x))                
            
            df2.to_csv(fullname, index=False)
            del df2; df2 = None
            
        elif type_ == self.__class__.__name__:
            
            fefs = df
            fefs.take_actions(max_level=4) #??? or max_level=3?
            fullname += '.' + self.__class__.extension
            fefs.write(fullname) # by calling write, this tsfefs object will have its path and name reset.
            
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        if idx in self.cache:
            self.cache.remove(idx)
        self.cache.append(idx)
        

    """
    Called by:
      - __action_delete
      
    Question @ 03-03-2024
    Should it also be called when maintain_cache() is called?
    
    Answer @ 05-03-2024
    No, maintain_cache() will directly modify the cache as a list
    instead of telling which idx should be removed.
    """
    def remove_idx(self, idx): 

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        if idx in self.cache:
            self.cache.remove(idx)
        self.cache = [ idx_ - 1 if idx_ > idx else idx_ for idx_ in self.cache ]

        

    """
    Called by:
      - __action_split
    """
    def split_idx(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        """
         - idx: the originally index, which will be retained, with new pieces created from it.
         - new_idx: split will lengthen the array, len(self.pieces)-1
        
        The new idx will be inserted in the cache behind the idx.
        """
        new_idx = len(self.pieces)-1
        
        
        """
        If idx not in cache, new_idx, which is split from idx, should not be in cache as well.
        
        Question @ 03-03-2024
        Will the above situation exist?
        """
        if idx in self.cache:
            loc = self.cache.index(idx)
            if new_idx in self.cache:
                
                """
                Question @ 03-03-2024
                How is it possible to have new_idx in cache already?

                Question @ 05-03-2024
                This  split_idx(self, idx)  function, is it called before 
                or after the split df added?
                """
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        tobemerged_idx = self.action_params[idx]
        assert isinstance(tobemerged_idx,int)
        
        """
        Question 1 @ 05-03-2024
        This  merge_idx(self, idx)  function, is it called before 
        or after df merged?

        Answer 1 @ 06-03-2024
        After merge.
        
        ----------------------------------------------------------

        Question 2 @ 05-03-2024
        Should the  tobemerged_idx  be removed from the cache?
        Or is it removed somewhere else?
        
        Answer 2 @ 06-03-2024
        This  merge_idx()  function is only serving  __action_merge(),
        so let's only concern what's happening here and in  __action_merge().
        
        tobemerged_idx is not removed from here nor from  __action_merge(),
        however, its  .actions[tobemerged_idx]  will be set as "delete", 
        which will subsequently have  tobemerged_idx  removed from cache.
        """
        if tobemerged_idx in self.cache:
            loc2 = self.cache.index(tobemerged_idx)
            if idx in self.cache:
                loc1 = self.cache.index(idx)
                if loc2 > loc1:
                    self.cache.remove(idx)
                    self.cache.insert(loc2,idx)
            else:
                self.cache.insert(loc2,idx)

        return
        
                
                
    """
    Called by:
      - __add_fefs
      - __add_df
      
    Added @ 05-03-2024
      - __read_piece    
    """
    def include_idx(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        """
        only include idx to the lower priority.
        Note: 0 is the lowest priority.
        """
        if idx not in self.cache:
            self.cache.insert(0,idx)



    """
    Called by:
     - __maintain_cache_by_rows
     
    """
    def __set_piece_to_none(self,idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        df = self.dfs[idx]
        if df is None:
            return
        type_ = self.types[idx]
        if type_ == "csv":
            pass
        elif type_ == self.__class__.__name__:
            # elif type_ == "tsfefs":
            for idx2 in range(len(df.pieces)):
                df.__set_piece_to_none(idx2)
        else:
            assert False        
        
        del df
        df = None
        self.dfs[idx] = None
        return


    def __count_opened_rows(self,idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        df = self.dfs[idx]
        type_ = self.types[idx]
        if df is None:
            return 0
        if type_ == "csv":
            return self.row_cnts[idx]
        elif type_ == self.__class__.__name__:
            # elif type_ == "tsfefs":
            fefs = df
            row_cnt = 0
            for idx_ in range(len(fefs.pieces)):
                row_cnt += fefs.__count_opened_rows(idx_)
            return row_cnt
        else:
            assert False
        

    def __maintain_cache_by_rows(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        cache_config = self.cache_config
        len_of_cache = cache_config["len_of_cache"]
        if len(self.cache) <= len_of_cache:
            return
        
        
        """
        Modified @ 03-03-2024
        
        Old codes:
        
        self.cache = self.cache[-len_of_cache:]
        for idx in range(len(self.pieces)):
            if idx in self.cache:
                continue
            self.__set_piece_to_none(idx)
        """        
        clear_indices = self.cache[:-len_of_cache]
        for idx in clear_indices:
            self.__set_piece_to_none(idx)
        self.cache = self.cache[-len_of_cache:]
        return
                
            
        
    
    
    def maintain_cache(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        # action_levels = [ BaseFEFS.action_domain.index(act) for act in self.actions ]
        # assert np.all(np.array(action_levels) == BaseFEFS.action_domain.index(""))
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        return

    
    
    def __action_update(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        

        df = self.dfs[idx]
        assert df is not None
        
        type_ = self.types[idx]
        len_ = None
        if type_ == "csv":
            len_ = len(df)
        elif type_ == self.__class__.__name__:
            fefs = df
            len_ = fefs.row_cnt
        else:
            print("Type", type_, "not supported")
            assert False

        fr, to = None, None
        if len_ > 0:
            if type_ == "csv":
                S_ = df[self.seq_col]
            elif type_ == self.__class__.__name__:
                S_ = df.__get_seqs()
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        

        self.__write_piece(idx)
        self.actions[idx] = ""
        return

    

    """
    Question @ 06-03-2024
    Is  __action_merge  the only path to reach  __action_delete?
    
    Answer @ 06-03-2024
     - __action_merge
     - resolve_conflict 
     - optimize_files (bcoz will call __action_merge)
     - __delitem__using_idx
    will also set "delete" actions.
    """
    def __action_delete(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        df = self.dfs[idx]
        assert df is not None


        type_ = self.types[idx]
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
        if type_ == "csv":
            if os.path.isfile(fullname):
                os.remove(fullname)
        elif type_ == self.__class__.__name__:
            fefs = df
            fefs.remove()
            
        else:
            print("Type", type_, "not supported")
            assert False

            
        """
        Newline added @ 03-03-2024
        
        It has to be here instead of at the beginning.
        If it is a fefs and set to None at the beginning, 
        the fefs.remove() above will cause an error.
        """
        self.__set_piece_to_none(idx)
        
            
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
            
        row_cnts = [ rcnt for rcnt in self.row_cnts if rcnt is not None ]
        self.row_cnt = sum(row_cnts)
        return


    
    def __action_split(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        

        df = self.dfs[idx]
        assert df is not None

            
        type_ = self.types[idx]
        
        """
        The design is not advanced enough to be able to split 1 TSFEFS into 2 TSFEFSs,
        so let's have all the splits be csvs.
        """
        if type_ == self.__class__.__name__:
            df = df.export_dataframe()
        df = df.sort_values(by=self.seq_col).reset_index(drop=True)

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
            
            # """ Newly added lines @ 01-03-2024 """
            # self.dfs[idx] = None
            # !!! HERE !!! """ How to tell the cache? """
            self.actions[idx] = ""
            
            return

        else:
            self.types[idx] = "csv"
            type_ = "csv"
            self.dfs[idx] = df


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

        else: # elif self.types[idx] == "fefs"
            print("File type", type_, "not yet implemented")
            assert False
        return


    def __action_merge(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        df = self.dfs[idx]
        
        # """
        # I might have reasons for this line,
        # but seems like it is now causing trouble.
        # Let's recall the reason?
        # """
        assert df is not None
        # """
        # Temporarily replace the above line by this
        # """
        # if df is None:
        #     df = self.__read_piece(idx)
        #     self.dfs[idx] = df
            
        
        
        idx_tobemerged = self.action_params[idx]
        assert isinstance(idx_tobemerged,int)
        df_tobemerged = self.dfs[idx_tobemerged]
        
        
        
        # """
        # I might have reasons for this line,
        # but seems like it is now causing trouble.
        # Let's recall the reason?
        # """
        assert df_tobemerged is not None
        # """
        # Temporarily replace the above line by this
        # """
        # if df_tobemerged is None:
        #     df_tobemerged = self.__read_piece(idx_tobemerged)
        #     self.dfs[idx_tobemerged] = df_tobemerged

            
            
            
        
        type_ = self.types[idx]
        type_tobemerged = self.types[idx_tobemerged]
        if  type_ == "csv"  and  type_tobemerged == "csv":
            df = pd.concat([df,df_tobemerged]).reset_index(drop=True)
        elif  type_ == "csv"  and  type_tobemerged == self.__class__.__name__:
            df_tobemerged = df_tobemerged.export_dataframe()
            df = pd.concat([df,df_tobemerged]).reset_index(drop=True)
        elif  type_ == self.__class__.__name__  and  type_tobemerged == "csv":
            df += df_tobemerged
        elif  type_ == "tsfefs"  and  type_tobemerged == self.__class__.__name__:
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        n = len(self.pieces)
        listoflists = [ self.types, self.frs, self.tos, self.row_cnts, 
                       self.actions, self.action_params, self.dfs ]
        assert all([ n == len(l) for l in listoflists ])
        return
    
    
    def __map_action_to_func(self,level):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
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

            


    # def take_actions(self, max_level=0):

    #     """ 
    #     Base class should never be actually called,
    #     have to be an implemented class.
    #     """
    #     assert self.__class__ != BaseFEFS


    #     """
    #     Never set max_level = 5, infinite loop.
    #     """
    #     # Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
    #     current_level = 0
    #     while current_level <= max_level:
    #         self.vprint("current_level:", current_level)

    #         action_levels = [ self.__class__.action_domain.index(act) for act in self.actions ]

    #         action_indices = [ idx for idx in reversed(range(len(action_levels))) if action_levels[idx] == current_level ]
    #         if len(action_indices) == 0:
    #             current_level += 1
    #         else: # len(action_indices) > 0:
    #             for idx in action_indices:
    #                 self.__map_action_to_func(current_level)(idx)
    #             current_level = 0
    #     return

    
    def take_actions(self, max_level=0):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        """
        Never set max_level = 5, infinite loop.
        """
        # Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
        current_level = 0
        while current_level <= max_level:
            self.vprint("current_level:", current_level)
            
            uactions = set(self.actions)
            uaction_levels = [ self.__class__.action_domain.index(act) for act in uactions ]
            min_level = min(uaction_levels)
            
            if min_level == current_level:
                
                df_act = pd.DataFrame({"act":self.actions, "idx":range(len(self.actions))})
                current_level_action_name = self.__class__.action_domain[current_level]
                df_act = df_act[ df_act["act"] == current_level_action_name ].reset_index(drop=True)
                action_indices = list(reversed(list(df_act["idx"])))
                for idx in action_indices:
                    self.__map_action_to_func(current_level)(idx)
                current_level = 0
            
            elif min_level > current_level:
                
                current_level = min_level
                
            else: # min_level < current_level
                
                assert False
                
        return
    

    
    """
    Added @ 13-03-2024
    
    This is for the immediate need of taking action instead of waiting to accumulate 
    the whole to take action.
    
    The 1st known use is used by __setitem_xxx().
    """
    def __focused_take_actions(self, idx, max_level=0):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        """
        Never set max_level = 5, infinite loop.
        """
        # Levels: 0) "update", 1) "split", 2) "merge", 3) "save", 4) "delete", 5) "".
        current_level = 0
        while current_level <= max_level:
            self.vprint("current_level:", current_level)
            
            action = self.actions[ idx ]
            action_level = self.__class__.action_domain.index(action)
            
            if action_level == current_level:
                self.__map_action_to_func(current_level)(idx)
                """
                added @ 29-03-2024
                
                Without the following checking line(s), 
                if it is "delete" and action taken,
                the things at idx will be removed and will continue on the next one,
                which is originally at idx+1.
                """
                if self.__class__.action_domain[action_level] != "delete":
                    current_level = 0
                else:
                    break
            
            elif action_level > current_level:
                
                current_level = action_level
                
            else: # action_level < current_level
                
                assert False
                
        return
    
    
    

    def has_pending_actions(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
                
        # b = any(np.array(action_levels) != BaseFEFS.action_domain.index(""))
        action_levels = [ self.__class__.action_domain.index(act) for act in self.actions ]
        b = any(np.array(action_levels) != self.__class__.action_domain.index(""))
        
        return b

    ############################################## Action: END ##############################################
    #########################################################################################################

    
    
    
    
    
    
    
    
    
    
    
    ################################################################################################################
    ############################################## Import,Export: BEG ##############################################


    def __no_overlapping_seq_range(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
                
        """
        ensure fr1 < to1 < fr2 < to2 < ...
        """
        df = pd.DataFrame({"fr":self.frs,"to":self.tos})
        df = df.sort_values(by=["fr","to"])
        
        # print("##################")
        # print("1.")
        # print("df")
        # print(df)
        # print("##################")
        # print()
        
        orders = list(df.index)
        arr = np.array(df)
        
        # print("##################")
        # print("2.")
        # print("arr")
        # print(arr)
        # print("##################")
        # print()
        
        arr = arr.reshape(-1).tolist() # becomes [fr1,to1,fr2,to2,...]
        
        # print("##################")
        # print("3.")
        # print("arr")
        # print(arr)
        # print("##################")
        # print()
        
        # print("##################")
        # print("4.")
        # df2 = dc(df)
        # df2["fr"] = df2["fr"].apply(lambda x: str(x))
        # df2["to"] = df2["to"].apply(lambda x: str(x))
        # arr2 = np.array(df2)
        # arr2 = arr2.reshape(-1).tolist()
        # print("arr2")
        # print(arr2)
        # print("##################")
        # print()

        assert all([ arr[i+1] >= arr[i] for i in range(len(arr)-1) ])
        return orders
    

    
    """
    Export all as a single df
    """
    def export_dataframe(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
                        
        orders = self.__no_overlapping_seq_range()
        
        dfs = []
        
        # rearrange all the lists according to orders
        for idx in orders:
            
            df = self.dfs[idx]
            if df is None:
                df = self.__read_piece(idx)

            type_ = self.types[idx]
            if type_ == "csv":
                dfs.append(df)
            elif type_ == self.__class__.__name__:
                dfs.append(df.export_dataframe())
            else:
                print("Type", type_, "not supported")
                assert False

        if len(dfs) == 0:
            return pd.DataFrame()
        
        df = pd.concat(dfs).reset_index(drop=True)
        
        """
        New line of code @ 06-03-2024
        """
        del dfs[:]; del dfs; dfs = None
        
        return df

    
    
    def export_dstfile(self, dstfile):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
                        
        df = self.export_dataframe()
        if self.seq_inv_trans_method is not None:
            assert self.seq_trans_method is not None
            df[self.seq_col] = df[self.seq_col].apply(lambda x: self.seq_inv_trans_method(x))
        df.to_csv(dstfile,index=False)
        
        """
        New line of code @ 06-03-2024
        """
        del df; df = None
        
        return

    
    
    def export_dstfolder(self, dstfolder):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
                        
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
                        
            if self.seq_inv_trans_method is not None:
                assert self.seq_trans_method is not None
                fr = self.seq_inv_trans_method(fr)
                to = self.seq_inv_trans_method(to)
            
            src = "%s/%s"%(fullpath,piece)            
            
            f = "%i. %s ~ %s.csv"%(idx,fr,to)
            dst = "%s/%s"%(dstfolder,f)
            os.system("cp \"%s\" \"%s\""%(src,dst))
            
            # print("From %s to %s"%(src,dst))
            
        return

    

    def import_dataframe(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
                                
        self.pieces = []
        self.types = []
        self.frs = []
        self.tos = []
        self.row_cnts = []
            
        self.actions = []
        self.action_params = []
        self.dfs = []
        
        self.cache = []
        
        df = df.sort_values(by=self.seq_col).reset_index(drop=True)
        self += df
        self.__action_split(0)
        return



    def import_srcfile(self, srcfile):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
                        
        if self.seq_read_dtype is None:
            df = pd.read_csv(srcfile)
        else:
            df = pd.read_csv(srcfile, dtype={self.seq_col:self.seq_read_dtype})

        assert self.seq_col in df.columns
        if self.seq_trans_method is not None:
            assert self.seq_inv_trans_method is not None
            df[self.seq_col] = df[self.seq_col].apply(lambda x: self.seq_trans_method(x))
        self.import_dataframe(df)
        return



    def import_srcfolder(self, srcfolder):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
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
            S = self.__get_seq(idx)
            
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        dfs = []
        for idx in range(len(self.pieces)):
            type_ = self.types[idx]
            if type_ == "csv":
                continue
            elif type_ == self.__class__.__name__:
                # elif type_ == "tsfefs":
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        return self.row_cnt
    ############################################# Overriding __len__: END #############################################
    ###################################################################################################################
    
    

    
    
    
    
    ####################################################################################################################
    ##################################### Overriding Square Bracket - general: BEG #####################################
    def __idx_check(self,idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert isinstance(idx,int_types)
        
        while idx < 0:
            idx += len(self)
            
        if idx > len(self) - 1:
            print("Index out of range")
            assert False
           
        return idx



    def __indices_check(self,indices):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        
        assert isinstance(indices,list)
        assert all([ isinstance(idx,int_types) for idx in indices ])
        assert len(indices) <= len(self)
        
        for i in range(len(indices)):
            idx = indices[i]
            while idx < 0:
                idx += len(self)
            indices[i] = idx
        
        if len(indices) > len(set(indices)):
            print("Dupicated indices")
            assert False
                
        assert all([ idx < len(self) for idx in indices ])
        return indices
    
    
    def __str_check(self,colname):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert isinstance(colname,str_types)
        
        if colname not in self.colnames:
            print("No such column name as %s, try update action"%colname)
            assert False
        return
            

    def __strs_check(self,colnames):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert isinstance(colnames,list)
        assert all([ isinstance(s,str_types) for s in colnames ])
        
        if len(set(colnames) - set(self.colnames)) > 0:
            print("Some requested column names don't exits.")
            print("You may consider the \"update\" action.")
            assert False
        return


    def __bools_check(self,B):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert isinstance(B,list)
        assert all([ isinstance(b,bool_types) for b in B ])
        assert len(B) == len(self) # has to be equal length
        return

        
    # Only for __setitems__'s strs and indices (and thus bools).
    def __value_check(self, value, assignment_w, assignment_h):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        idx = self.__idx_check(idx)        
        orders = self.__no_overlapping_seq_range()
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
        
        elif type_ == self.__class__.__name__:
            # elif type_ == "tsfefs":
            fefs = df
            self.renew_idx(_order)
            return fefs[idx]
        
        else:
            print("Type", type_, "not supported")
            assert False
        
        print("Error! Find it out!")
        assert False
    
        
        
    # def __getitem__using_str(self,colname):

    #     """ 
    #     Base class should never be actually called,
    #     have to be an implemented class.
    #     """
    #     assert self.__class__ != BaseFEFS


    #     self.__str_check(colname)

    #     values = []
    #     orders = self.__no_overlapping_seq_range()
    #     for _order in orders:

    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)                
    #             self.dfs[_order] = df
    #             """
    #             Fixes on the way:
    #             1. the cache (if still reading the whole piece)
    #             2. don't read the whole piece
    #             """

    #         values.extend(df[colname])
    #     S = pd.Series(values)
    #     S.name = colname
    #     return S
        
    def __getitem__using_str(self,colname):
        assert self.__class__ != BaseFEFS
        self.__str_check(colname)
        if colname == self.seq_col:
            return self.__get_seqs()
        return self.__get_col_all_pieces(colname)
            

            


    def __get_indices_in_same_piece(self, indices, orders):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        indices = self.__indices_check(indices)
        indices_copied = dc(indices)

        dfs = []
        orders = self.__no_overlapping_seq_range()
        
        while len(indices) > 0:
            _order, adjusted_indices = self.__get_indices_in_same_piece(indices, orders)
            df = self.dfs[_order]
            if df is None:
                df = self.__read_piece(_order)

            type_ = self.types[_order]            
            if type_ == "csv":
                df_ = df.iloc[adjusted_indices]
                
                self.renew_idx(_order)

            elif type_ == self.__class__.__name__:
                fefs = df
                df_ = fefs[adjusted_indices]
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        self.__bools_check(B)
        
        indices = np.array(range(len(B)))[B].tolist()
        return self.__getitem__using_indices(indices)

        
    # def __getitem__using_strs(self,colnames):

    #     """ 
    #     Base class should never be actually called,
    #     have to be an implemented class.
    #     """
    #     assert self.__class__ != BaseFEFS


    #     self.__strs_check(colnames)

    #     dfs = []
    #     indices = []
    #     cum_row_cnt = 0
    #     orders = self.__no_overlapping_seq_range()
    #     for _order in orders:
    #         df = self.dfs[_order]
    #         if df is None:
    #             df = self.__read_piece(_order)
    #             self.dfs[_order] = df
    #         dfs.append(df[colnames])
    #         indices += list(np.array(self.__class__.get_index(df)) + cum_row_cnt)
    #         cum_row_cnt += self.row_cnts[_order]

    #     if len(dfs) == 0:
    #         return pd.DataFrame()

    #     df = pd.concat(dfs).reset_index(drop=True)
    #     df.index = indices
    #     return df

    """
    !!! DO NOT DELETE THE ABOVE COMMENTED CODES !!!
    The Q&A below is about the above codes.    
    
    Question @ 10-03-2024
    
    For the old version of  __getitem__using_strs(self,colnames),
    why should the indices be handled as another list?
    ???
    I've forgotten the reason...
    
    Answer @ 14-03-2024
    That comes from the worry of wrong order timestamps.
    E.g., during optimization of the sizes of the files,
    some files may be combined with a piece of another file.
    If the combined file is not sorted according to timestamps, 
    (or even worse the indices are not reset)
    than the indices will be in wrong order.
    
    But this should be rectified by having the codes ensuring the files,
    regardless in memory or in storage, should be sorted immediately 
    after any modification.
    """    
    def __getitem__using_strs(self,colnames):
        assert self.__class__ != BaseFEFS
        self.__strs_check(colnames)
        return self.__get_cols_all_pieces(colnames)

        
        
    def __getitem__(self, key):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        """
        Accetpable queries:
        Single:
         - int: fefs[33]
         - str: fefs["age"]
        Multiple:
         - slice of ints: fefs[20:], fefs[20:50], fefs[::2], ...
         - list of ints: fefs[[1,2,5,10]]
         - list of strs: fefs[["time","purchased","item","price"]]
         - list of bools: fefs[[True,False,True,...]] # must have length == row_cnt
        """        
        if isinstance(key,str_types):
            return self.__getitem__using_str(key)
        elif isinstance(key,int_types):
            return self.__getitem__using_idx(key)
        elif isinstance(key,(arr_types,slice)):
            if isinstance(key,np.ndarray):
                assert len(key.shape) == 1 # must be in (n,)
                key = list(key)
            elif isinstance(key,pd.Series):
                key = list(key)
            elif isinstance(key,slice):
                assert all([ (s is None) or isinstance(s,int) for s in [ key.start,key.stop,key.step ] ])
                key = list(range(self.row_cnt))[key]
                
            
            if len(key) == 0:
                return pd.DataFrame()
            
            
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
    
    """
    Revised @ 14-03-2024 from above.
    
    Haven't retained the previous codes.
    """
    def __setitem__using_idx(self, idx, value):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        idx = self.__idx_check(idx)

        if isinstance(value, arr_types):
            arr = np.array(value)
            assert len(arr.shape) == 1 # like (4,)
            col_len = arr.shape[0]
            assert col_len == len(self.colnames)
        else:
            pass # you can put any type of single value to the idx row.
            
        orders = self.__no_overlapping_seq_range()
        _order, accum_row_cnt = self.__get_piece_idx(idx, orders)
        
        df = self.dfs[_order]
        in_cache = True
        if df is None:
            in_cache = False # to indicate if it should be removed from self.dfs            
            df = self.__read_piece(_order)
            
        idx -= accum_row_cnt
        type_ = self.types[_order]
        if type_ == "csv":
            df.iloc[idx] = value
            self.dfs[_order] = df
        elif type_ == self.__class__.__name__:
            # elif type_ == "tsfefs":
            fefs = df
            """            
            Question @ 15-03-2024
            -------------------------------
            We have this line of code
            
                fefs[idx] = value
            
            . Is there a need to take 
            action update immediately, 
            just like how we do 
            
                self.__focused_take_actions(_order, max_level=4)
                
            at the end of this function?
            
            
            
            Answer @ 15-03-2024
            -------------------------------            
            Look, this function is 
            
                def __setitem__using_idx(self, ...):
            
            , and 
            
                fefs[idx] = value
                
            is actually triggering another 
            
                fefs.__setitem__using_idx(...)
                
            inside this function.
            So, as this function is having an immediate action update call 
            
                self.__focused_take_actions(_order, max_level=4)
                
            at the end of the function, 
            
                fefs[idx] = value
                
            will also have its own immedidate action update call within 
            
                fefs.__setitem__using_idx(...)
                
            , even before this function's update call.
            In conclusion, there's no need to add another immediate update action call.
            """
            fefs[idx] = value
            self.dfs[_order] = fefs
        else:
            print("Type", type_, "not supported")
            assert False

        
        """
        modified @ 13-03-2024

        See more comments in  __setitem__using_str()
        """
        """
        modify on "modified @ 13-03-2024" @ 18-03-2024

        This modification is to optimize the immediate aciton.
        If it is in cache, immediate action needed as "update", 
        "save" can be left to collective action.
        If it is not in cache, however, "update" followed by "save" is needed.
        """
        self.actions[_order] = "update" # will become "save" after taking "update" action.
        # self.__focused_take_actions(_order, max_level=3) # "save" is at 3
        if not in_cache: # remove from memory if previously not in cache
            self.__focused_take_actions(_order, max_level=3) # "save" is at 3
            self.dfs[_order] = None
            del df; df = None
        else:            
            self.__focused_take_actions(_order, max_level=0) # "update" is at 0
            """ No need to renew, let it be at the same position if already in cache. """
            # self.renew_idx(_order) 
            pass

        return
    
    


    def __setitem__using_str(self, colname, value):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        # don't use the same checking routine here
        # self.__str_check(colname)
        
        assert isinstance(colname,str_types)
            
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
        orders = self.__no_overlapping_seq_range()
        for _order in orders:
            idx = _order
            df = self.dfs[_order]
            in_cache = True
            if df is None:
                in_cache = False # to indicate if it should be removed from self.dfs
                df = self.__read_piece(idx)

            cnt_new = cnt + self.row_cnts[_order]
            df[colname] = value[ cnt : cnt_new ]
            cnt = cnt_new
            
            # self.dfs[_order] = df
            # self.actions[_order] = "update"
            
            """
            modified @ 13-03-2024
            
            Whenever a column is updated, the old codes would make it have all the dfs 
            stored before update.
            
            Everything in memory at the same time violates the idea of FEFS, 
            the revised method should be to update immediately,
            and then remove from memory if it is not originally in the cache.
            --------------------------------------------------------------------------------
            
            modification-on-modification @ 18-03-2024
            
            This is  __setitem__using_str(), which is either changing a column or 
            creating a new column.
            For those not in cache, saving as an immediate action is of course needed.
            However, for those in cache, yes they need to be saved, but not immediately.
            
            There's one possibility for those in cache to be needed to take action 
            immediately.
            For this  __setitem__using_str(), it is not prohibiting the  seq_col  
            to be modified, that means the time/seq could be changed completely.
            If time/seq is changed, "update" is needed, better immediately.
            
            So make the if-then-else as 
            1. If  in_cache  and  colname != seq_col, "save" without immediate action.
            2. If  in_cache  and  colname == seq_col, "update" immediately,
               which will become "save" after being updated.
            3. If not  in_cache  and  colname != seq_col, "save" immediately, 
               and remove from cache.
            4. If not  in_cache  and  colname == seq_col, "update" immediately,
               which will become "save" after being updated, 
               and then remove from cache.
               
            Below commented the @ 13-03-2024 version, 
            and the codes below the commented codes are the rewritten codes @ 18-03-2024.
            """
            # self.dfs[_order] = df
            # self.actions[_order] = "update"
            # self.__focused_take_actions(_order, max_level=4)
            # if not in_cache: # remove from memory if previously not in cache
            #     self.dfs[_order] = None
            #     del df; df = None
            # else:            
            #     """ No need to renew, let it be at the same position if already in cache. """
            #     # self.renew_idx(_order) 
            #     pass
            self.dfs[_order] = df
            
            if colname == self.seq_col:  
                
                self.actions[_order] = "update"
                
                if not in_cache: # remove from memory if previously not in cache
                    self.__focused_take_actions(_order, max_level=3) # "save" is at 3
                    self.dfs[_order] = None
                    del df; df = None
                else:            
                    self.__focused_take_actions(_order, max_level=0) # update is at 0
                    """ No need to renew, let it be at the same position if already in cache. """
                    # self.renew_idx(_order) 
                    pass
                
            else:
                
                self.actions[_order] = "save"
                
                if not in_cache: # remove from memory if previously not in cache
                    self.__focused_take_actions(_order, max_level=3) # "save" is at 3
                    self.dfs[_order] = None
                    del df; df = None
                else:            
                    """ No need to renew, let it be at the same position if already in cache. """
                    # self.renew_idx(_order) 
                    pass
                
            
        if colname not in self.colnames:
            self.colnames += [colname]
        return

    

                    
    # def __setitem__using_indices(self,indices,value):

    #     """ 
    #     Base class should never be actually called,
    #     have to be an implemented class.
    #     """
    #     assert self.__class__ != BaseFEFS


    #     indices = self.__indices_check(indices)
    #     value = self.__value_check(value, len(self.colnames), len(indices))
    #     orders = self.__no_overlapping_seq_range()

    #     cnt = 0
    #     for idx in indices:
    #         df_ = None
    #         for _order in orders:
    #             row_cnt = self.row_cnts[_order]
    #             if idx > row_cnt - 1:
    #                 idx -= row_cnt
    #                 continue
    #             df = self.dfs[_order]

    #             in_cache = True
    #             if df is None:
    #                 in_cache = False # to indicate if it should be removed from self.dfs
    #                 df = self.__read_piece(_order)

    #             type_ = self.types[_order]
    #             if type_ == "csv":
    #                 df_ = df.iloc[idx:(idx+1)]
    #             elif type_ == self.__class__.__name__:
    #                 fefs = df
    #                 df_ = fefs[idx:(idx+1)]
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
    #         elif type_ == self.__class__.__name__:
    #             fefs[[idx]] = value[ cnt : cnt_new ]
    #             self.dfs[_order] = fefs
    #         else:
    #             print("Type", type_, "not supported")
    #             assert False
    #         cnt = cnt_new 


    #         """
    #         modified @ 13-03-2024

    #         See more comments in  __setitem__using_str()
    #         """
    #         self.actions[_order] = "update"
    #         self.__focused_take_actions(_order, max_level=4)
    #         if not in_cache: # remove from memory if previously not in cache
    #             self.dfs[_order] = None
    #             del df; df = None
    #         else:            
    #             """ No need to renew, let it be at the same position if already in cache. """
    #             # self.renew_idx(_order) 
    #             pass       

    #     return
    
    """
    Revised @ 14-03-2024 from above.
    So, don't remove the above commented codes, yet.
    """
    def __setitem__using_indices(self,indices,value):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                        
        indices = self.__indices_check(indices)
        value = self.__value_check(value, len(self.colnames), len(indices))
        orders = self.__no_overlapping_seq_range()        
        
        cnt = 0
        while len(indices) > 0:
            
            _order, adjusted_indices = self.__get_indices_in_same_piece(indices, orders)

            df = self.dfs[_order]
            in_cache = True
            if df is None:
                in_cache = False # to indicate if it should be removed from self.dfs
                df = self.__read_piece(_order)

            type_ = self.types[_order]            
            if type_ == "csv":
                df_ = df.iloc[adjusted_indices]
            elif type_ == self.__class__.__name__:
                fefs = df
                df_ = fefs[adjusted_indices]
            else:
                print("Type", type_, "not supported")
                assert False

                
            if len(df_) < len(adjusted_indices):
                print("Indice could be out of range")
                assert False
                
            indices = indices[len(adjusted_indices):]

        
            cnt_new = cnt + len(adjusted_indices)
            if type_ == "csv":
                df.iloc[ adjusted_indices ] = value[ cnt : cnt_new ]
                self.dfs[_order] = df 
            elif type_ == self.__class__.__name__:
                fefs[ adjusted_indices ] = value[ cnt : cnt_new ]
                self.dfs[_order] = fefs
            else:
                print("Type", type_, "not supported")
                assert False
            cnt = cnt_new 
            
            
            """
            modified @ 13-03-2024
            
            See more comments in  __setitem__using_str()
            """
            """
            modify on "modified @ 13-03-2024" @ 18-03-2024
            
            This modification is to optimize the immediate aciton.
            If it is in cache, immediate action needed as "update", 
            "save" can be left to collective action.
            If it is not in cache, however, "update" followed by "save" is needed.
            """
            self.actions[_order] = "update"
            # self.__focused_take_actions(_order, max_level=4)
            if not in_cache: # remove from memory if previously not in cache
                self.__focused_take_actions(_order, max_level=3) # "save" is at 3
                self.dfs[_order] = None
                del df; df = None
            else:            
                self.__focused_take_actions(_order, max_level=0) # "update" is at 0
                """ No need to renew, let it be at the same position if already in cache. """
                # self.renew_idx(_order) 
                pass
            
        return
    
    
    
    
    
    

    def __setitem__using_bools(self,B,value):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                        
        self.__bools_check(B)
        indices = np.array(range(len(B)))[B].tolist()
        self.__setitem__using_indices(indices,value)
        return


    def __setitem__using_strs(self,colnames,value):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                                
        self.__strs_check(colnames)
        value = self.__value_check(value, len(colnames), self.row_cnt)
        orders = self.__no_overlapping_seq_range()
        
        cum_row_cnt = 0
        for _order in orders:
            idx = _order
            df = self.dfs[_order]
            
            in_cache = True
            if df is None:
                in_cache = False # to indicate if it should be removed from self.dfs
                df = self.__read_piece(idx)
            new_cum_row_cnt = cum_row_cnt + self.row_cnts[_order]
            df[colnames] = value[cum_row_cnt:new_cum_row_cnt]
            self.dfs[_order] = df
            cum_row_cnt = new_cum_row_cnt
            
            """
            modified @ 13-03-2024
            
            See more comments in  __setitem__using_str()
            -----------------------------------------------------------------------------
            modification-on-modification @ 18-03-2024
            
            See the same section in  __setitem__using_strs() @ 18-03-2024,
            same logic.
            """
            if self.seq_col in colnames:
                
                self.actions[_order] = "update"
                
                if not in_cache: # remove from memory if previously not in cache
                    self.__focused_take_actions(_order, max_level=3) # "save" is at 3
                    self.dfs[_order] = None
                    del df; df = None
                else:            
                    self.__focused_take_actions(_order, max_level=0) # update is at 0
                    """ No need to renew, let it be at the same position if already in cache. """
                    # self.renew_idx(_order) 
                    pass
                
            else:
                
                self.actions[_order] = "save"
                
                if not in_cache: # remove from memory if previously not in cache
                    self.__focused_take_actions(_order, max_level=3) # "save" is at 3
                    self.dfs[_order] = None
                    del df; df = None
                else:            
                    """ No need to renew, let it be at the same position if already in cache. """
                    # self.renew_idx(_order) 
                    pass
                
        return

        
    def __setitem__(self, key, value):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                        
        """
        Accetpable assignments:
        Single:
         - int: fefs[33] = value # row index 33 assignment
         - str: fefs["age"] = value
        Multiple:
         - slice of ints: fefs[20:] = value, fefs[20:50] = value, fefs[::2] = value, ...
         - list of ints: fefs[[1,2,5,10]] = value
         - list of strs: fefs[["time","purchased","item","price"]] = value
         - list of bools: fefs[[True,False,True,...]] = value # must have length == row_cnt
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

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        idx = self.__idx_check(idx)        
        orders = self.__no_overlapping_seq_range()
        _order, accum_row_cnt = self.__get_piece_idx(idx, orders)

        df = self.dfs[_order]
        in_cache = True
        if df is None:
            in_cache = False
            df = self.__read_piece(_order)
            self.dfs[_order] = df

        idx -= accum_row_cnt
        type_ = self.types[_order]
        # row_cnt = self.row_cnts[_order]
        len_ = None
        if type_ == "csv":
            """
            Since there's no del df[idx] for dataframe
            """
            B = [True]*row_cnt
            B[idx] = False
            df = df[B].reset_index(drop=True)
            len_ = len(df)
        elif type_ == self.__class__.__name__:
            fefs = df
            del fefs[idx]
            
            """
            Question @ 15-03-2024
            Should we take action here?
            
            Answer @ 15-03-2024
            No, thus the line
            
                fefs.take_actions(max_level=4)
                
            is commented.
                
            .
            This whole function is modified on 15-03-2024, 
            having the below line of code or not before the modification is not a concern anymore, 
            under the current modified structure we shouldn't take action.
            
            For this function  __delitem__using_idx(),
            it will be ended by either immediately executing "update" or "delete" action,
            
                self.actions[_order] = "delete"
            or 
                self.actions[_order] = "update"
            
            , followed by 
            
                self.__focused_take_actions(_order, max_level=4)
            .
            Now, let's realize that the above line of code
            
                del fefs[idx]
                
            is triggering  fefs.__delitem__using_idx() recursively.
            That means before reaching the end of this function to call 
            
                self.__focused_take_actions(_order, max_level=4)
                
            of "update" or "delete",
            
                fefs.__focused_take_actions(_order, max_level=4)
                
            is already called by fefs for its "upate" or "delete".
            This is exactly what we want to achieve by calling
            
                fefs.take_actions(max_level=4)
            
            , and hence we don't need to have this duplication.
            """
            # fefs.take_actions(max_level=4)
            
            
            len_ = len(fefs)  # since action is taken (recursively by "del fefs[idx]"), this fefs' row_cnt is updated.
            df = fefs
        else:
            print("Type", type_, "not supported")
            assert False

        self.dfs[_order] = df
        
                
        """
        Comments & Question @ 15-03-2024
        
        This is for the following codes commented between 
        ***********************
        # [ codes ] 
        ***********************
        , so don't delete the below commented codes.
        
        
        At the end of
        
            __action_update()  
            
        there will set 
        
            if len_ == 0:
                self.actions[idx] = "delete"
            
        . If now we have 

            self.actions[_order] = "update"
            self.__focused_take_actions(_order, max_level=4)
            
        , and if its length become 0, the piece and everything at _order will be deleted,
        since our action max_level is 4 (delete).
        Hence, after the line 
        
            self.__focused_take_actions(_order, max_level=4)
            
        , if the things at  _order  are deleted, all the items like 
        
            self.dfs[ _order ]
            self.actions[ _order ]
            ...
            
        will become invalid references.
        Here suggest 2 approaches.
        
            1. 
            Reduce max_level to 3 (save)
            Thus, although  __action_update()  will still run  

                if len_ == 0:
                    self.actions[idx] = "delete"

            , the  __action_delete()  won't be executed immediately, 
            but in the collective  take_actions(), 
            and thus within the current function  _order  is still a valid reference.

            Also there's no need to have the lines 

                if len_ == 0:
                    self.actions[_order] = "delete"

            at the end of this function.

            2. 
            Perform immediate action delete in a higher checking order than of immedidate action update.
            Create an if-then-else, 
            if length == 0, then immediate action delete,
            else immediate action update.
            
            If delete is taken action, inside  __action_delete()  
            
                self.__set_piece_to_none(idx)
                
            will be called, then we don't have to worry about things like 
            
                self.dfs[_order] = None
                del df; df = None
                
            , and the fefs' attributes like  fr, to, row_cnt, ..., and the cache as well, 
            will also be updated inside  __action_delete().
            After the if case,  _order  won't be accessed after because the function has ended.
            
            In contrast, if immedidate update is called, that means the object at  _order 
            is not comletely empty, and we can do everything of the immediate update 
            and those subsequent routines.
            
        The 2. approach is chosen.
        """
        # *****************************************************
        # """
        # modified @ 13-03-2024

        # See more comments in  __setitem__using_str()
        # """
        # self.actions[_order] = "update"
        # self.__focused_take_actions(_order, max_level=4)
        # if not in_cache: # remove from memory if previously not in cache
        #     self.dfs[_order] = None
        #     del df; df = None
        # else:            
        #     """ No need to renew, let it be at the same position if already in cache. """
        #     # self.renew_idx(_order) 
        #     pass

        # """
        # Should it be renewed in cache?
        # 2023-10-07, decided not to renew in cache.
        # """
        # # self.renew_idx(_order) 

        # if len_ == 0:
        #     self.actions[_order] = "delete"
        # *****************************************************
        
        """
        modified @ 15-03-2024

        See the "Comments & Question @ 15-03-2024" above, 
        and more comments in  __setitem__using_str().
        """
        
        if len_ == 0:
            self.actions[_order] = "delete"
            self.__focused_take_actions(_order, max_level=4) # "delete" is at 4.
        else:
            self.actions[_order] = "update"
            if not in_cache: # remove from memory if previously not in cache
                self.__focused_take_actions(_order, max_level=3) # "save" is at 3.
                self.dfs[_order] = None
                del df; df = None
            else:
                """
                comment @ 18-03-2024
                
                We still need to have both "update" and "save", 
                where "save" will be automatically be the status after "update" action is taken,
                but we only need immediate action for "update", 
                for "save" we can wait until the collective actions.
                """
                self.__focused_take_actions(_order, max_level=0) # "update" is at 0.
                """ No need to renew, let it be at the same position if already in cache. """
                # self.renew_idx(_order) 
                pass
            
        return 
    

    
    def __delitem__using_str(self,colname):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        self.__str_check(colname)
        assert colname != self.seq_col

        orders = self.__no_overlapping_seq_range()
        for _order in orders:
            df = self.dfs[_order]
            
            in_cache = True
            if df is None:
                in_cache = False
                df = self.__read_piece(_order)
                
            del df[colname]
            self.dfs[_order] = df
            self.actions[_order] = "save"
            
            """
            Modified @ 18-03-2024
            This handling method is different from the other immedidate actions.
            
            The others are having the immediate actions outside/above the 
            
                if not in_cache: 
                
            , taking immediate actions unconditionally, but one is conditional.
            
            If the df will be retained in cache, there is no rush to save it, 
            can leave it until the collective take_actions().
            Otherwise, if it has to be removed from cache, 
            the change has to be saved before removal.
            ------------------------------------------------------------------------
            
            Comment @ 18-03-2024
            
            After another round of modification, 
            "The others are having the immediate actions outside/above the 
            
                if not in_cache:
            "
            is not true anymore.
            They are now mostly inside the if-then-else cases.
            """
            if not in_cache: # remove from memory if previously not in cache
                self.__focused_take_actions(_order, max_level=3) # "save" is at 3.
                self.dfs[_order] = None
                del df; df = None
            
        self.colnames.remove(colname)
        return    

    

    def __delitem__using_indices(self,indices):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        indices = self.__indices_check(indices)
        indices_copied = dc(indices)
        
        orders = self.__no_overlapping_seq_range()

        while len(indices) > 0:
            _order, adjusted_indices = self.__get_indices_in_same_piece(indices, orders)
            df = self.dfs[_order]
            
            in_cache = True
            if df is None:
                in_cache = False
                df = self.__read_piece(_order)

            type_ = self.types[_order]
            if type_ == "csv":
                row_cnt = self.row_cnts[_order]
                B = np.array([True]*row_cnt)
                B[adjusted_indices] = False
                df = df[B].reset_index(drop=True)
                len_ = len(df)
            elif type_ == self.__class__.__name__:
                fefs = df
                del fefs[adjusted_indices]
                
                """
                Question @ 15-03-2024
                Should we take action here?

                Answer @ 15-03-2024
                Refer to the long answer in  __delitem__using_idx().
                """                
                # fefs.take_actions(max_level=4)

                len_ = len(fefs)  # since action is taken (recursively by "del fefs[idx]"), this fefs' row_cnt is updated.
                df = fefs
            else:
                print("Type", type_, "not supported")
                assert False
                
            self.dfs[_order] = df
            

            
            """
            modified @ 18-03-2024

            See the "Comments & Question @ 15-03-2024" in  __delitem__using_idx, 
            and more comments in  __setitem__using_str().
            """
            if len_ == 0:
                self.actions[_order] = "delete"
                self.__focused_take_actions(_order, max_level=4)
            else:
                self.actions[_order] = "update"
                if not in_cache: # remove from memory if previously not in cache
                    self.__focused_take_actions(_order, max_level=3) # "save" is at 3.
                    self.dfs[_order] = None
                    del df; df = None
                else:
                    """
                    comment @ 18-03-2024

                    We still need to have both "update" and "save", 
                    where "save" will be automatically be the status after "update" action is taken,
                    but we only need immediate action for "update", 
                    for "save" we can wait until the collective actions.
                    """
                    self.__focused_take_actions(_order, max_level=0) # "update" is at 0.
                    """ No need to renew, let it be at the same position if already in cache. """
                    # self.renew_idx(_order) 
                    pass
                
                
            # self.actions[_order] = "update"

            # """
            # Should it be renewed in cache?
            # 2023-10-07, decided not to renew in cache.
            # """
            # # self.renew_idx(_order)

            # if len_ == 0:
            #     self.actions[_order] = "delete"
    
            indices = indices[len(adjusted_indices):]
        
        return

    

    def __delitem__using_bools(self,B):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        self.__bools_check(B)

        indices = np.array(range(len(B)))[B].tolist()
        self.__delitem__using_indices(indices)
        return    


    def __delitem__using_strs(self,colnames):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        self.__strs_check(colnames)
        assert self.seq_col not in colnames
        
        orders = self.__no_overlapping_seq_range()
        for _order in orders:
            df = self.dfs[_order]
            type_ = self.types[_order]
            
            in_cache = True
            if df is None:
                in_cache = False
                df = self.__read_piece(_order)

            if type_ == "csv":
                for col in colnames:
                    del df[col]
            elif type_ == self.__class__.__name__:
                del df[colnames]
            else:
                print("Type", type_, "not supported")
                assert False
                
            self.dfs[_order] = df
            self.actions[_order] = "save"
            
            """
            modified @ 18-03-2024
            
            More details refer to  __delitem__using_str().
            """
            if not in_cache: # remove from memory if previously not in cache
                self.__focused_take_actions(_order, max_level=3) # "save" is at 3.
                self.dfs[_order] = None
                del df; df = None
            
        for col in colnames:
            self.colnames.remove(col)
        return

    

    def __delitem__(self, key):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        """
        Accetpable delete:
        Single:
         - int: del fefs[33]
         - str: del fefs["age"]
        Multiple:
         - slice of ints: del fefs[20:], del fefs[20:50], del fefs[::2], ...
         - list of ints: del fefs[[1,2,5,10]]
         - list of strs: del fefs[["time","purchased","item","price"]]
         - list of bools: del fefs[[True,False,True,...]] # must have length == row_cnt
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



    
    
    ####################################################################################################################
    ############################ The EQV of get_seq related functions, but not seq_col: BEG ############################

    """
    The equivalent of   __read_seq(self, idx),
    but for columns other than   seq_col.
    """
    def __read_col_single_piece(self, col, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        
        assert col != self.seq_col

        seq_col = self.seq_col
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
           
        type_ = self.types[idx]
        if type_ == "csv":
            
            df = pd.read_csv(fullname, usecols=[col, seq_col])

            if self.seq_trans_method is not None:
                assert self.seq_inv_trans_method is not None
                df[seq_col] = df[seq_col].apply(lambda x: self.seq_trans_method(x))

            df = df.sort_values(by=seq_col).reset_index(drop=True)
            df = df.drop(columns=[seq_col])
            S = df[col]
        
        elif type_ == self.__class__.__name__:

            fefs = self.__class__()
            fefs.read(fullname)
            S = fefs.__get_col_all_pieces(col)
        
        else:
            print("File type", self.__class__, "not supported")
            assert False
            
        return S

    
    

    """
    The equivalent of   __get_seq(self, idx),
    but for columns other than   seq_col.
    """
    def __get_col_single_piece(self, col, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        assert col != self.seq_col
                
        df = self.dfs[idx]
        type_ = self.types[idx]
        if df is None:
            S = self.__read_col_single_piece(col, idx)
            return S

        if type_ == "csv":
            S = df[col]
        elif type_ == self.__class__.__name__:
            fefs = df
            S = fefs.__get_col_all_pieces(col)
        else:
            assert False
            
        return S
    

    
    """
    The equivalent of   __get_seqs(self),
    but for columns other than   seq_col.
    """
    def __get_col_all_pieces(self, col):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        S = []
        for order_ in self.__no_overlapping_seq_range():
            S_ = self.__get_col_single_piece(col, order_)
            S_ = list(S_)
            S.extend(S_)
        S = pd.Series(S)
        return S

    
    
    
    
    """
    The multi-column version of  __read_col_single_piece(self, col, idx),
    and does not resist having  seq_col  in the requested columns.
    """
    def __read_cols_single_piece(self, cols, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        

        assert isinstance(cols, list)
        assert all( [ isinstance(col, str) for col in cols ] )
        
        seq_col = self.seq_col
        _cols = dc(cols)
        if seq_col not in cols:
            _cols += [ seq_col ]

        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
           
        type_ = self.types[idx]
        if type_ == "csv":
            
            df = pd.read_csv(fullname, usecols=_cols)

            if self.seq_trans_method is not None:
                assert self.seq_inv_trans_method is not None
                df[seq_col] = df[seq_col].apply(lambda x: self.seq_trans_method(x))

            df = df.sort_values(by=seq_col).reset_index(drop=True)
            df = df[cols]
        
        elif type_ == self.__class__.__name__:

            fefs = self.__class__()
            fefs.read(fullname)
            df = fefs.__get_cols_all_pieces(cols)
        
        else:
            print("File type", self.__class__, "not supported")
            assert False
            
        return df

    
    
    """
    The multi-column version of  __get_col_single_piece(self, col, idx),
    and does not resist having  seq_col  in the requested columns.
    """
    def __get_cols_single_piece(self, cols, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        assert isinstance(cols, list)
        assert all( [ isinstance(col, str) for col in cols ] )
        
                
        df = self.dfs[idx]
        type_ = self.types[idx]
        if df is None:
            df_ = self.__read_cols_single_piece(cols, idx)
            return df_

        if type_ == "csv":
            df_ = df[cols]
        elif type_ == self.__class__.__name__:
            fefs = df
            df_ = fefs.__get_cols_all_pieces(cols)
        else:
            assert False
            
        return df_
    

    
    """
    The multi-column version of  __get_col_all_pieces(self, col),
    and does not resist having  seq_col  in the requested columns.
    """
    def __get_cols_all_pieces(self, cols):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        dfs = []
        for order_ in self.__no_overlapping_seq_range():
            df_ = self.__get_cols_single_piece(cols, order_)
            dfs.extend(df_)
        df = pd.concat(dfs).reset_index(drop=True)
        return df
    
    ############################ The EQV of get_seq related functions, but not seq_col: END ############################
    ####################################################################################################################
    

    
    
    
    
    

    ######################################################################################################################
    ############################################# Overriding ==, >, <, >=, <=: BEG #######################################

    def __get_seq_type(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
        
        if self.seq_type is not None:
            return self.seq_type

                
        for df in self.dfs:
            if df is not None:
                break
                
        if df is None:
            S = self.__get_seq(0)
        else:
            S = df[self.seq_col]
            
        s = list(S)[0]
        # Elements in df[seq_col] have to be in the defined types
        type_tups = [ int_types, float_types, str_types, bool_types, arr_types, dt_types ]
        for type_tup in type_tups:
            if isinstance(s, type_tup):
                self.seq_type = type_tup
                return type_tup
        assert False
    



    """
    The operators will be about the comparison of timestamps
    Need to check:
    1. fefs1 <op> fefs2
    2. fefs <op> df
    3. df <op> fefs
    4. fefs <op> ts
    5. fefs <op> pd.Series(tss)
    
    The codes are strictly prohibited from using fefs[seq_col],
    since such operation will affect the cache.
    """
    def __read_seq(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        piece = self.pieces[idx]
        fullname = self.__compose_piece_fullname(piece)
           
        type_ = self.types[idx]
        if type_ == "csv":

            
            # dtf = self.datetime_format
            seq_col = self.seq_col

            if self.seq_read_dtype is None:
                df = pd.read_csv(fullname, usecols=[seq_col])
            else:
                df = pd.read_csv(fullname, usecols=[seq_col], dtype={seq_col:self.seq_read_dtype})

            if self.seq_trans_method is not None:
                assert self.seq_inv_trans_method is not None
                df[seq_col] = df[seq_col].apply(lambda x: self.seq_trans_method(x))

            df = df.sort_values(by=seq_col).reset_index(drop=True)
            S = df[seq_col]
        
        elif type_ == self.__class__.__name__:

            fefs = self.__class__()
            fullname += '.' + self.__class__.extension
            fefs.read(fullname)
            S = fefs.__get_seqs()
                    
        else:
            print("File type", self.__class__, "not supported")
            assert False
            
        return S
    
    

    def __get_seq(self, idx):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        df = self.dfs[idx]
        type_ = self.types[idx]
        if df is None:
            S = self.__read_seq(idx)
            return S

        if type_ == "csv":
            S = df[self.seq_col]
        elif type_ == self.__class__.__name__:
            fefs = df
            S = fefs.__get_seqs()
        else:
            assert False
            
        return S




    def __get_seqs(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        S = []
        for order_ in self.__no_overlapping_seq_range():
            S_ = self.__get_seq(order_)
            S_ = list(S_)
            S.extend(S_)
        S = pd.Series(S)
        return S

    
    def get_seqs(self):
        return self.__get_seqs()
    
    

    def __eq_series(self, S):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        seq_type_ = self.__get_seq_type()
        assert self.row_cnt == len(S)
        assert [ isinstance(s,seq_type_) for s in S ]        
        S_ = self.__get_seqs()
        B = S_ == S
        return B
        
    
    def __eq_fefs(self, fefs):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        assert self.seq_col == fefs.seq_col
        assert self.row_cnt == fefs.row_cnt
        
        S = fefs.__get_seqs()
        return self.__eq_series(S)


    def __eq_df(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        assert self.row_cnt == len(df)
        S = df[self.seq_col]
        return self.__eq_series(S)

    
    def __eq_s(self, s):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        S = self.__get_seqs()
        return S == s                


    def __eq__(self, other):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        if isinstance(other, pd.Series):
            return self.__eq_series(other)
        elif isinstance(other, self.__class__):
            return self.__eq_fefs(other)
        elif isinstance(other, pd.DataFrame):
            return self.__eq_df(other)
        elif isinstance(other, self.__get_seq_type()):
            return self.__eq_s(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False

        
            

    def __ne_series(self, S):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        seq_type_ = self.__get_seq_type()
        assert self.row_cnt == len(S)
        assert [ isinstance(s,seq_type_) for s in S ]        
        S_ = self.__get_seqs()
        B = S_ != S
        return B
    
    
    def __ne_fefs(self, fefs):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.seq_col == fefs.seq_col
        assert self.row_cnt == fefs.row_cnt
        
        S = fefs.__get_seqs()
        return self.__ne_series(S)

    
    def __ne_df(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.row_cnt == len(df)
        S = df[self.seq_col]
        return self.__ne_series(S)

    
    def __ne_s(self, s):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        S = self.__get_seqs()
        return S != s                


    def __ne__(self, other):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        if isinstance(other, pd.Series):
            return self.__ne_series(other)
        elif isinstance(other, self.__class__):
            return self.__ne_fefs(other)
        elif isinstance(other, pd.DataFrame):
            return self.__ne_df(other)
        elif isinstance(other, self.__get_seq_type()):
            return self.__ne_s(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False

            
            
            
    def __lt_series(self, S):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        seq_type_ = self.__get_seq_type()
        assert self.row_cnt == len(S)
        assert [ isinstance(s,seq_type_) for s in S ]
        S_ = self.__get_seqs()
        B = S_ < S
        return B
    

    def __lt_fefs(self, fefs):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.seq_col == fefs.seq_col
        assert self.row_cnt == fefs.row_cnt
        S = fefs.__get_seqs()
        return self.__lt_series(S)


    def __lt_df(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.row_cnt == len(df)
        S = df[self.seq_col]
        return self.__lt_series(S)
    

    def __lt_s(self, s):
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        S = self.__get_seqs()
        B = S < s
        return B
    

    def __lt__(self, other):
        
        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        if isinstance(other, pd.Series):
            return self.__lt_series(other)
        elif isinstance(other, self.__class__):
            return self.__lt_fefs(other)
        elif isinstance(other, pd.DataFrame):
            return self.__lt_df(other)
        elif isinstance(other, self.__get_seq_type()):
            return self.__lt_s(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False




    def __gt_series(self, S):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        seq_type_ = self.__get_seq_type()
        assert self.row_cnt == len(S)
        assert [ isinstance(s,seq_type_) for s in S ]
        S_ = self.__get_seqs()
        B = S_ > S
        return B
    

    def __gt_fefs(self, fefs):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.seq_col == fefs.seq_col
        assert self.row_cnt == fefs.row_cnt
        S = fefs.__get_seqs()
        return self.__gt_series(S)


    def __gt_df(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.row_cnt == len(df)
        S = df[self.seq_col]
        return self.__gt_series(S)

    
    def __gt_s(self, s):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        S = self.__get_seqs()
        B = S > s
        return B
    
    
    def __gt__(self, other):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        if isinstance(other, pd.Series):
            return self.__gt_series(other)
        elif isinstance(other, self.__class__):
            return self.__gt_fefs(other)
        elif isinstance(other, pd.DataFrame):
            return self.__gt_df(other)
        elif isinstance(other, self.__get_seq_type()):
            return self.__gt_s(other)        
        else:
            print("Types", type(other), "is not supported.")
            assert False

            

    def __le_series(self, S):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        seq_type_ = self.__get_seq_type()
        assert self.row_cnt == len(S)
        assert [ isinstance(s,seq_type_) for s in S ]
        S_ = self.__get_seqs()
        B = S_ <= S
        return B

    
    def __le_fefs(self, fefs):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.seq_col == fefs.seq_col
        assert self.row_cnt == fefs.row_cnt
        S = fefs.__get_seqs()
        return self.__le_series(S)
    
    
    def __le_df(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.row_cnt == len(df)
        S = df[self.seq_col]
        return self.__le_series(S)


    def __le_s(self, s):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        S = self.__get_seqs()
        B = S <= s
        return B
    

    def __le__(self, other):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        if isinstance(other, pd.Series):
            return self.__le_series(other)
        elif isinstance(other, self.__class__):
            return self.__le_fefs(other)
        elif isinstance(other,pd.DataFrame):
            return self.__le_df(other)
        elif isinstance(other, self.__get_seq_type()):
            return self.__le_s(other)
        else:
            print("Types", type(other), "is not supported.")
            assert False



    def __ge_series(self, S):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        seq_type_ = self.__get_seq_type()
        assert self.row_cnt == len(S)
        assert [ isinstance(s,seq_type_) for s in S ]
        S_ = self.__get_seqs()
        B = S_ >= S
        return B
    

    def __ge_fefs(self, fefs):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.seq_col == fefs.seq_col
        assert self.row_cnt == fefs.row_cnt
        S = fefs.__get_seqs()
        return self.__ge_series(S)
    
    
    def __ge_df(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.row_cnt == len(df)
        S = df[self.seq_col]
        return self.__ge_series(S)
    

    # def __ge_dt(self, ts):
    def __ge_s(self, s):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        S = self.__get_seqs()
        B = S >= s
        return B

    
    def __ge__(self, other):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        if isinstance(other, pd.Series):
            return self.__ge_series(other)
        elif isinstance(other, self.__class__):
            return self.__ge_fefs(other)
        elif isinstance(other, pd.DataFrame):
            return self.__ge_df(other)
        elif isinstance(other, self.__get_seq_type()):
            return self.__ge_s(other)        
        else:
            print("Types", type(other), "is not supported.")
            assert False
            
    ############################################# Overriding ==, >, <, >=, <=: END #######################################
    ######################################################################################################################



    
    
    
    


    #####################################################################################################################
    ################################################# Overriding +: BEG #################################################
    def __add_fefs(self, fefs):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.seq_col == fefs.seq_col
        assert len(set(self.colnames) - set(fefs.colnames)) == 0
        assert len(set(fefs.colnames) - set(self.colnames)) == 0
        # assert self.datetime_format == tsfefs.datetime_format
        
        
        # assert not fefs.has_pending_actions()
        
        assert fefs.has_valid_status() # no None in the lists
        assert not fefs.conflict_exist()
      
        
        # the added fefs must be under the current path
        fefs = fefs.clone(self.__compose_fullpath(),fefs.name)
        if fefs.name in self.pieces:
            fefs.name = self.gen_valid_piece(prefix=fefs.name+"_", suffix="")
        self.pieces += [fefs.name]
        self.types += [ self.__class__.__name__ ]
        
        self.frs += [None]
        self.tos += [None]
        self.row_cnts += [None]
        self.actions += ["update"]
        self.action_params += [None]
        self.dfs += [fefs]
        
        self.include_idx(len(self.pieces)-1) # len(self.pieces)-1: the new highest array idx
        
        """
        modified @ 19-03-2024
        
        Make the "update" immediately take action, save leave to the collective actions.
        """
        self.__focused_take_actions(len(self.pieces)-1, max_level=0) # "update" is at 0

        return self
    


    def __add_df(self, df):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        assert self.seq_col in df.columns
        assert len(set(self.colnames) - set(df.columns)) == 0
        assert len(set(df.columns) - set(self.colnames)) == 0
        try:
            if self.seq_inv_trans_method is not None:
                assert self.seq_trans_method is not None
                _ = df[self.seq_col].apply(lambda x: self.seq_inv_trans_method(x))
        except:
            print("Problem with df's %s"%self.seq_col)
            assert False
        
            
        
        piece = self.gen_valid_piece(prefix="", suffix="")
        
        
        self.vprint("Newly added file:", piece)
        
        
        self.pieces += [piece]
        self.types += ["csv"]
        self.frs += [None]
        self.tos += [None]
        self.row_cnts += [None]
        self.actions += ["update"]
        self.action_params += [None]
        self.dfs += [df] # the elements can be df or classes of BaseFEFS
        
        self.include_idx(len(self.pieces)-1) # len(self.pieces)-1: the new highest array idx

        """
        modified @ 19-03-2024
        
        Make the "update" immediately take action, save leave to the collective actions.
        """
        self.__focused_take_actions(len(self.pieces)-1, max_level=0) # "update" is at 0
        
        return self

    
        
    def __add__(self, other):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        # print(type(other))
        if isinstance(other, self.__class__):
            return self.__add_fefs(other)
        elif isinstance(other, pd.DataFrame):
            return self.__add_df(other)
        else:
            print("Types", type(other), "not supported.")
            assert False
    ################################################# Overriding +: END #################################################
    #####################################################################################################################



    
    
    
    
    
    
    
    
    
    
    
    ####################################################################################################################
    #################################################### MERGE: BEG ####################################################
    # # @classmethod
    # # def merge(cls, left_obj, right_obj, path="", name="", on="", target=None):
    # @staticmethod
    # def merge(left_obj, right_obj, path="", name="", on="", target=None):
    #     """
    #     1. For class merge, requiring a new path and a new name.
    #     2. The left_obj has to be classes of BaseFEFS.
    #     3. The right_obj can be (pd.DataFrame, classes of BaseFEFS)
    #     4. Target is the targeted columns for the right_obj.
    #     """
    #     assert (path is not None) and (name is not None) and (on is not None)
    #     assert "" not in [path, name, on]
    #     assert isinstance(left_obj,classes of BaseFEFS)
    #     assert isinstance(right_obj,(classes of BaseFEFS,pd.DataFrame))
    #     fefs = left_obj.merge(right_obj, on, target=target, path=path, name=name)
    #     return fefs

    
    def __get_fefs_reference(self,path,name):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS


        if path is None:
            assert name is None
            fefs = self
        elif path is not None:
            assert name is not None            
            fefs = self.clone(path, name)
        else:
            assert False
        return fefs
    

    
    def merge_with_df(self, df_another, on, target):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        """
        !!!!!!!!!!! Important !!!!!!!!!!!
        I am struggling with the following implementation.
        Ideally, the merge should be done by merging pieces, 
        i.e.,
            pd.merge(df_piece, df_another)
        instead of 
            pd.merge(self[on], df_another)
            
        This current implementation is for the sake of convenience,
        !!! CHANGE ANYTIME FEELING SLOW !!!
        """
        
        # orders = self.__no_overlapping_time_range()
        
        df_self_on = self[on]
        df_another_on = df_another[on+target].drop_duplicates().reset_index(drop=True)
        
        on_col = "__on__"
        delim = "@$*^%"
        df_self_on[on_col] = df_self_on.apply(lambda x: delim.join([ str(xx) for xx in x ]), axis=1)
        df_self_on = df_self_on[[on_col]]
        df_another_on[on_col] = df_another_on[on].apply(lambda x: delim.join([ str(xx) for xx in x ]), axis=1)
        df_another_on = df_another_on[[on_col] + target]
        
        """
        Check 1-to-1
        """
        df_uon = df_self_on[[on_col]]
        df_uon = df_uon.drop_duplicates().reset_index(drop=True) 
        n = len(df_uon)
        df_one2one = pd.merge(df_uon, df_another_on, on=on_col, how="left").reset_index(drop=True)

        # no NA is allowed.
        df_one2one = df_one2one.dropna().reset_index(drop=True)
        assert len(df_one2one) == n 
        
        # no duplicate is allowed.
        df_one2one = df_one2one.drop_duplicates().reset_index(drop=True) 
        assert len(df_one2one) == n

        
        """
        real merge
        """
        df_target = pd.merge(df_self_on, df_another_on, on=on_col, how="left").reset_index(drop=True)
        # just to reassure
        assert len(df_self_on) == len(df_target)
        
        # the order of df_target must be the same
        for col in target:
            # setting new columns 
            assert col not in self.colnames
            self[col] = df_target[col]
        
        return self
    
    
    
    def merge_with_fefs(self, fefs_another, on, target):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

        
        return self.merge_with_df(fefs_another[on+target], on, target)

        

    def merge(self, right_obj, on, target=None, path=None, name=None):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        """
        on: 
         (self[on[0]] == right_obj[on[0]]) and 
         (self[on[1]] == right_obj[on[1]]) and
         ...
         
         E.g., on=["cusID","branch"]
         
        target:
          - If None then all the remaining cols in right_obj will be merged
          - Else:
          
            E.g., on=["cusID","branch"], target=["gender","citizen"]
            Notice that for a unique comb of on, there cannot be more than 1 comb from the targets.
        """
        
        
        fefs = self.__get_fefs_reference(path,name)

        
        assert isinstance(on,(str,list))
        if isinstance(on,list):
            assert all([ isinstance(col,str) for col in on ])
        else:
            on = [on]

            
        if isinstance(right_obj, pd.DataFrame):
            right_cols = list(right_obj.columns)
        elif isinstance(right_obj, self.__class__):
            """ if classes of BaseFEFS, its seq_col should be a dummy"""
            right_cols = dc(right_obj.colnames)
            right_cols.remove(right_obj.seq_col)
        else:
            assert False

            
        # validity of on
        assert len(set(on) - set(fefs.colnames)) == 0
        assert len(set(on) - set(right_cols)) == 0
        right_cols = list(set(right_cols) - set(on))
            

        # validity of target
        if target is not None:
            assert isinstance(target,(str,list))
            if isinstance(target,str):
                target = [target]
            else:
                assert all([ isinstance(col,str) for col in target ])
            assert len(set(target) - set(right_cols)) == 0
        else:
            target = right_cols
                          

                          
        # on and target should be mutually exclusive
        assert len(target) + len(on) == len(set(target+on))

        if isinstance(right_obj, pd.DataFrame):
            fefs = fefs.merge_with_df(right_obj, on, target)
        elif isinstance(right_obj, self.__class__):
            fefs = fefs.merge_with_fefs(right_obj, on, target)
        else:
            assert False
            
        return fefs
    #################################################### MERGE: END ####################################################
    ####################################################################################################################
    
    
    
    
    
    
    
    
    
    
    
    
    #####################################################################################################################
    ########################################## conflict resolve, optimize: BEG ##########################################
    
    def conflict_exist(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                
        try:
            _ = self.__no_overlapping_seq_range()
            return False
        except:
            return True
        
        
        

    # def resolve_conflict(self):

    #     """ 
    #     Base class should never be actually called,
    #     have to be an implemented class.
    #     """
    #     assert self.__class__ != BaseFEFS


    #     while self.conflict_exist():

    #         df_index, orders = self.dump_index_df() # already sorted by "fr"
    #         self = self.load_index_df(df_index, orders)

    #         for idx in range(len(self.pieces)-1):


    #             if self.frs[idx+1] < self.tos[idx]:

    #                 df0 = self.dfs[idx]
    #                 if df0 is None:
    #                     df0 = self.__read_piece(idx)
    #                     self.dfs[idx] = df0

    #                 df1 = self.dfs[idx+1]
    #                 if df1 is None:
    #                     df1 = self.__read_piece(idx+1)
    #                     self.dfs[idx+1] = df1

    #                 B0 = df0[self.seq_col] < self.frs[idx+1]
    #                 df0a = df0[B0].reset_index(drop=True)
    #                 df0b = df0[~B0].reset_index(drop=True)

    #                 B1 = df1[self.seq_col] <= self.tos[idx]
    #                 df1a = df1[B1].reset_index(drop=True)
    #                 df1b = df1[~B1].reset_index(drop=True)

    #                 self.actions[idx] = "delete"
    #                 self.actions[idx+1] = "delete"

    #                 df_new = pd.concat([df0b,df1a]).reset_index(drop=True)
    #                 df_new = df_new.sort_values(by=self.seq_col).reset_index(drop=True)

    #                 if len(df0a) > 0:
    #                     self += df0a
    #                 if len(df1b) > 0:
    #                     self += df1b
    #                 if len(df_new) > 0:
    #                     self += df_new
    #                 break

    #         self.take_actions(max_level=4)

    #     return 
    """
    modified @ 20-03-2024
    
    Don't remove the above commented old version, yet.
    """
    def resolve_conflict(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                        
        while self.conflict_exist():
        
            df_index, orders = self.dump_index_df() # already sorted by "fr"
            self = self.load_index_df(df_index, orders)
            
            for idx in range(len(self.pieces)-1):
                
                
                if self.frs[idx+1] < self.tos[idx]:

                    df0 = self.dfs[idx]
                    in_cache0 = True
                    if df0 is None:
                        in_cache0 = False
                        df0 = self.__read_piece(idx)
                        self.dfs[idx] = df0

                    df1 = self.dfs[idx+1]
                    in_cache1 = True
                    if df1 is None:
                        in_cache1 = False
                        df1 = self.__read_piece(idx+1)
                        self.dfs[idx+1] = df1

                    B0 = df0[self.seq_col] < self.frs[idx+1]
                    df0a = df0[B0].reset_index(drop=True)
                    df0b = df0[~B0].reset_index(drop=True)

                    B1 = df1[self.seq_col] <= self.tos[idx]
                    df1a = df1[B1].reset_index(drop=True)
                    df1b = df1[~B1].reset_index(drop=True)

                    df_new = pd.concat([df0b,df1a]).reset_index(drop=True)
                    df_new = df_new.sort_values(by=self.seq_col).reset_index(drop=True)
                    """
                    modified @ 29-03-2024
                    
                    if any one of df0 or df1 is in cache, df_new is allowed to be in cache.
                    """
                    in_cache_new = in_cache0 or in_cache1
                    
                    """
                    added @ 29-03-2024
                    
                    if any one of idx or idx+1 is TSFEFS (w.l.o.g),
                    after the above df reorganize operations that generates 
                    df0a, df1b, and df_new, which are all dataframes (csv),
                    the ????.tsfefs folder(s) should be removed.
                    """
                    if self.types[idx] != "csv":
                        df0.remove()
                    if self.types[idx+1] != "csv":
                        df1.remove()
                    
                    

                    
                    """
                    Comment and new codes @ 20-03-2024
                    
                    Rationale for the code below.
                    The question that can explain the rationale is, 
                    why can't it be 
                    
                        self.actions[idx+1] = "update"
                        self.__focused_take_actions(idx+1, max_level=4)
                    ?
                    Here's what happens with the 2 hypothetical lines of codes.
                    No doubt the  __focused_take_actions()  will first take "update" action.
                    However, in  __action_update()  if the len(df) is 0, 
                    it will transit to the status "delete".
                    It seems convenient if we allow the  __focused_take_actions()  to have  max_level=4,
                    which will also take the transitted "delete" action (if it does).
                    
                    The above flow becomes a loophole, if df at idx+1 is not len == 0,
                    and not in cache, i.e., needs to be removed from dfs[idx+1].
                    Have these lines after the hypothetical codes,

                        if not in_cache1: # remove from memory if previously not in cache
                            self.dfs[idx+1] = None
                            del df; df = None
                            
                    if "delete" has been taken action, the items in  dfs[idx+1]  is something else,
                    then the cache becomes inconsistent.
                    
                    Hence, we need to separate the cases of "delete" and "update",
                    applicable to both  idx+1  and  idx.
                    
                    Additional note, it is preferred to complete  idx+1  prior to  idx,
                    as a good practice.
                    Similar reason as above, if something happens at  idx  to make it being deleted,
                    the whole RHS of  idx  will be shifted, and  idx+1  becomes something else.  
                    
                    ---------------------------------------------------------------------------------
                    
                    added @ 29-03-2024
                    
                    After turning the df at idx and df at idx+1 
                    into df0a, df1b, and df_new, 
                    all of these are no longer FEFS, and they must be in type "csv".
                    So all of them has to be explicitly specified as 
                    
                        self.dfs[...] = "csv"
                        
                    """
                    self.dfs[idx+1] = df1b
                    self.types[idx+1] = "csv"
                    if len(df1b) == 0:
                        self.actions[idx+1] = "delete"
                        self.__focused_take_actions(idx+1, max_level=4) # "delete" is at 4
                    else:
                        self.actions[idx+1] = "update"
                        self.__focused_take_actions(idx+1, max_level=3) # "save" is at 3
                        if not in_cache1: # remove from memory if previously not in cache
                            self.dfs[idx+1] = None
                            del df1b; df1b = None
                        else:            
                            """ No need to renew, let it be at the same position if already in cache. """
                            # self.renew_idx(_order) 
                            pass

                    self.dfs[idx] = df0a
                    self.types[idx] = "csv"
                    if len(df0a) == 0:
                        self.actions[idx] = "delete"
                        self.__focused_take_actions(idx, max_level=4) # "delete" is at 4
                    else:
                        self.actions[idx] = "update" # in "update" if find len(df) is 0, will be assigned with "delete"
                        self.__focused_take_actions(idx, max_level=3) # "save" is at 3
                        if not in_cache0: # remove from memory if previously not in cache
                            self.dfs[idx] = None
                            del df0a; df0a = None
                        else:            
                            """ No need to renew, let it be at the same position if already in cache. """
                            # self.renew_idx(_order) 
                            pass

                    if len(df_new) > 0:
                        self += df_new 
                        
                        """
                        comment @ 29-03-2024
                        
                        By default the added df_new will be in cache.
                        According to the above, its status in cache should be decided by  in_cache_new,
                        """
                        if not in_cache_new:
                            new_idx = len(self.pieces)-1
                            df_new = self.dfs[new_idx]
                            assert df_new is not None # they are newly created so must not be. None
                            self.__focused_take_actions(new_idx, max_level=3) # "save" is at 3                        
                            self.remove_idx(new_idx)
                            self.dfs[new_idx] = None
                            del df_new; df_new = None                
                        
                    break
                    
            # self.take_actions(max_level=4)
            
        return 

            

    # def optimize_files(self):

    #     """
    #     Base class should never be actually called,
    #     have to be an implemented class.
    #     """
    #     assert self.__class__ != BaseFEFS


    #     while True:

    #         df_index, orders = self.dump_index_df() # already sorted by "fr"
    #         self = self.load_index_df(df_index, orders)

    #         idx = 0
    #         pieces_len_adjustment = 0
    #         while idx < len(self.pieces):

    #             if self.row_cnts[idx] >= self.max_row_per_piece:                    
    #                 self.actions[idx] = "split"
    #                 pieces_len_adjustment += 1
    #                 break
    #             elif self.row_cnts[idx] < self.max_row_per_piece/2:
    #                 idx_next = idx + 1
    #                 if idx_next < len(self.pieces):
    #                     if self.row_cnts[idx_next] + self.row_cnts[idx] < self.max_row_per_piece:
    #                         self.actions[idx] = "merge"
    #                         pieces_len_adjustment -= 1
    #                         self.action_params[idx] = idx_next
    #                     elif self.row_cnts[idx_next] > self.max_row_per_piece/2:
    #                         self.actions[idx_next] = "split"
    #                         pieces_len_adjustment += 1
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
    #         if idx == len(self.pieces) - pieces_len_adjustment:
    #             break

    #     return
    
    """
    modified @ 21-03-2024
    """
    def optimize_files(self, verbose_for_optimize_files=False):

        """
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS
                
        df_index, orders = self.dump_index_df() # already sorted by "fr"
        self = self.load_index_df(df_index, orders)
            
        idx = 0
        while idx < len(self.pieces):
            
            if verbose_for_optimize_files:
                print("idx:", idx)
                self.print_info()
                print()
                print()
            

            reorganize = False
            
            if self.row_cnts[idx] >= self.max_row_per_piece:

                self.actions[idx] = "split"

                df = self.dfs[idx]
                in_cache = True
                if df is None:
                    in_cache = False # to indicate if it should be removed from self.dfs
                    df = self.__read_piece(idx)
                    self.dfs[idx] = df

                assert len(df) == self.row_cnts[idx]


                """
                Comment @ 21-03-2024

                The original df at idx will become smaller,
                then there should be 1 or more new df(s) being appended.

                The original one needs "update" action, 
                if it is not in cache, then also needs "save" action immediately,
                otherwise can leave the "save" until collective actions.

                The new df(s) will be assigned with "update" status inside  __action_split,
                and the design of  __action_split  includes inserting the new indices
                into the cache.
                For this  optimize_files()  function, let's don't let them be in cache, 
                so "save" should also be executed and then remove them from memory.
                """
                max_idx_before_split = len(self.pieces) - 1
                if in_cache:
                    self.__focused_take_actions(idx, max_level=1) # "split" is at 1
                    if verbose_for_optimize_files:
                        print("loc: 1, action: split")
                        print("__focused_take_actions(%i, max_level=1)"%idx)
                        self.print_info()
                        print()
                        print()
                else:
                    self.__focused_take_actions(idx, max_level=3) # "save" is at 3
                    self.dfs[idx] = None
                    del df; df = None
                    if verbose_for_optimize_files:
                        print("loc: 2, action: split")
                        print("__focused_take_actions(%i, max_level=3)"%idx)
                        self.print_info()
                        print()
                        print()

                max_idx_after_split = len(self.pieces) - 1
                new_df_cnt = max_idx_after_split - max_idx_before_split
                assert new_df_cnt >= 1

                for new_idx in reversed(range(len(self.pieces))[-new_df_cnt:]): # good practice in descending order
                    df_new = self.dfs[new_idx]
                    assert df_new is not None # they are newly created so must not be. None
                    self.__focused_take_actions(new_idx, max_level=3) # "save" is at 3                        
                    self.remove_idx(new_idx)
                    self.dfs[new_idx] = None
                    del df_new; df_new = None
                    if verbose_for_optimize_files:
                        print("loc: 3, action: add")
                        print("__focused_take_actions(%i, max_level=3)"%new_idx)
                        self.print_info()
                        print()
                        print()

                    
                """
                modified @ 21-03-2024

                Rationale of the following codes.

                Imagine right before reaching this idx, i.e., at idx - 1,
                the  dfs[idx-1]  deserves to be merged, 
                but due to the large size of  dfs[idx], the merge was skipped.

                Now, after splitting df[idx], each df is at most  max_row_per_piece/2,
                which means it is possible to have a 2nd chance to have 
                dfs[idx-1]  merge with  dfs[idx],
                that's why it is                     
                    idx -= 1
                """
                if idx - 1 >= 0:
                    idx -= 1
                else:
                    idx += 1

                # "split" messes up the seq/time order, need to reorganize at the end of the loop
                reorganize = True

            elif self.row_cnts[idx] < self.max_row_per_piece/2:

                idx_next = idx + 1
                """
                modification @ 27-03-2024

                With this checking case, the rest of the codes (if-then-else) below
                can be shifted 1 tab to the left, become less messy.                
                """
                if idx_next >= len(self.pieces):
                    # idx += 1
                    # continue
                    break # simply break the loop
                    

                """
                comment @ 27-03-2024
                
                The following already fulfilled 
                
                    idx_next < len(self.pieces):
                """
                if self.row_cnts[idx_next] + self.row_cnts[idx] < self.max_row_per_piece:

                    self.actions[idx] = "merge"
                    self.action_params[idx] = idx_next

                    """
                    modified @ 21-03-2024

                    After some thoughts, further clarity was gained for the functionality of 
                    __action_xxx.

                    In principle, keep the  __action_xxx  functions pure, 
                     - assume all the materials it needs are ready (e.g., the df is in memory),
                     - and hence, assume all the cache removal, if applicable, handled outside of it.
                    """
                    df = self.dfs[idx]
                    in_cache = True
                    if df is None:
                        in_cache = False # to indicate if it should be removed from self.dfs
                        df = self.__read_piece(idx)
                        self.dfs[idx] = df

                    df_next = self.dfs[idx_next]
                    in_cache_next = True
                    if df_next is None:
                        in_cache_next = False
                        df_next = self.__read_piece(idx_next)
                        self.dfs[idx_next] = df_next

                    """
                    modified @ 29-03-2024
                    
                    if any one of df or df_next is in cache, the merged one should be in cache.
                    """
                    in_cache |= in_cache_next
                    if in_cache:
                        self.__focused_take_actions(idx, max_level=2) # "merge" is at 2
                        if verbose_for_optimize_files:
                            print("loc: 4, action: merge")
                            print("__focused_take_actions(%i, max_level=2)"%idx)
                            self.print_info()
                            print()
                            print()
                    else:
                        self.__focused_take_actions(idx, max_level=3) # "save" is at 3
                        self.dfs[idx] = None
                        del df; df = None
                        if verbose_for_optimize_files:
                            print("loc: 5, action: merge")
                            print("__focused_take_actions(%i, max_level=3)"%idx)
                            self.print_info()
                            print()
                            print()

                    self.__focused_take_actions(idx_next, max_level=4) # "delete" is at 4
                    if verbose_for_optimize_files:
                        print("loc: 6, action: delete")
                        print("__focused_take_actions(%i, max_level=4)"%idx_next)
                        self.print_info()
                        print()
                        print()

                    """
                    comment @ 27-03-2024

                    after merge and removal of the merged df, the len will be 1 less,
                    having 
                        idx += 1
                    may make it exceed the len, 
                    if that's the case it's okay to let loop  
                        while idx < len(self.pieces) 
                    break.
                    """ 
                    idx += 1
                    # continue

                elif self.row_cnts[idx_next] > self.max_row_per_piece/2:

                    self.actions[idx_next] = "split"

                    """
                    comment @ 27-03-2024

                    The "split" handling would be the same as the case at the beginning.
                    Just that this one is on  idx_next ,
                    and there will be no modification on idx because the split on idx_next 
                    is aiming to let it merge with idx.                            
                    """
                    df_next = self.dfs[idx_next]
                    in_cache_next = True
                    if df_next is None:
                        in_cache_next = False # to indicate if it should be removed from self.dfs
                        df_next = self.__read_piece(idx_next)
                        self.dfs[idx_next] = df_next

                    assert len(df_next) == self.row_cnts[idx_next]


                    max_idx_before_split = len(self.pieces) - 1
                    if in_cache_next:
                        self.__focused_take_actions(idx_next, max_level=1) # "split" is at 1
                        if verbose_for_optimize_files:
                            print("loc: 7, action: split")
                            print("__focused_take_actions(%i, max_level=1)"%idx_next)
                            self.print_info()
                            print()
                            print()
                        
                    else:
                        self.__focused_take_actions(idx_next, max_level=3) # "save" is at 3
                        self.dfs[idx_next] = None
                        del df_next; df_next = None
                        if verbose_for_optimize_files:
                            print("loc: 8, action: split")
                            print("__focused_take_actions(%i, max_level=3)"%idx_next)
                            self.print_info()
                            print()
                            print()

                    max_idx_after_split = len(self.pieces) - 1
                    new_df_cnt = max_idx_after_split - max_idx_before_split
                    assert new_df_cnt >= 1

                    for new_idx in reversed(range(len(self.pieces))[-new_df_cnt:]): # good practice in descending order
                        df_new = self.dfs[new_idx]
                        assert df_new is not None # they are newly created so must not be. None
                        self.__focused_take_actions(new_idx, max_level=3) # "save" is at 3                        
                        self.remove_idx(new_idx)
                        self.dfs[new_idx] = None
                        del df_new; df_new = None
                        if verbose_for_optimize_files:
                            print("loc: 9, action: add")
                            print("__focused_take_actions(%i, max_level=3)"%new_idx)
                            self.print_info()
                            print()
                            print()

                    
                    # do nothing on idx
                    idx += 0
                    # break
                    # "split" messes up the seq/time order, need to reorganize at the end of the loop
                    reorganize = True

                else:
                    # better not do anything, leave it alone.
                    idx += 1
                    # continue
                    
            else:
                idx += 1


                
            if reorganize:
                """
                new lines @ 27-03-2024

                The 2 lines are needed bcoz "split" messes up the seq / time order
                """
                df_index, orders = self.dump_index_df() # already sorted by "fr"
                self = self.load_index_df(df_index, orders)


        return
    
    ########################################## conflict resolve, optimize: END ##########################################
    #####################################################################################################################
    
    

    
    
    
    
    
    
    ####################################################################################################################
    ################################################# Read, Write: BEG #################################################
    """
    Equals to df.read_csv
    """
    def read(self, fullname):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                        
        self.path, self.name = self.__decompose_fullpath(fullname)

        full_meta_dict_name = '/'.join([fullname, self.__class__.meta_json_name])
        dict_meta = json.load(open(full_meta_dict_name,'r'))        
        self = self.load_meta(dict_meta)
        
        full_index_df_name = '/'.join([fullname, self.__class__.index_df_name])
        if self.seq_read_dtype is None:
            df_index = pd.read_csv(full_index_df_name)
        else:
            df_index = pd.read_csv(full_index_df_name, dtype={"fr":self.seq_read_dtype, "to":self.seq_read_dtype})  
        self = self.load_index_df(df_index)
        
        self.actions = [ "" for i in range(len(df_index)) ]
        self.action_params = [ None for i in range(len(df_index)) ]
        self.dfs = [ None for i in range(len(df_index)) ]
        self.cache = []
        return

    
    def write(self, fullname=None):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                        
        if fullname is None:
            fullname = self.__compose_fullpath()
        else:
            self.path, self.name = self.__decompose_fullpath(fullname) # just to ensure the .fefs extension exits
            
        if not os.path.isdir(fullname):
            os.mkdir(fullname)
        
        # Consistency of actions check
        # should have all actions cleared
        assert not self.has_pending_actions()

        
        dict_meta = self.dump_meta()
        full_meta_dict_name = '/'.join([fullname, self.__class__.meta_json_name])
        json.dump(dict_meta, open(full_meta_dict_name,'w'))
        

        df_index, orders = self.dump_index_df()
        full_index_df_name = '/'.join([fullname, self.__class__.index_df_name])
        df_index.to_csv(full_index_df_name, index=False)
        return
    ################################################# Read, Write: END #################################################
    ####################################################################################################################

    
        
    def remove(self):

        """ 
        Base class should never be actually called,
        have to be an implemented class.
        """
        assert self.__class__ != BaseFEFS

                        
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
            recursive_remove(fullname)
            
        return


    
    
    
    
#     #####################################################################################################################
#     #################################################### Switch: BEG ####################################################
#     """ A completely new function """
    
#     def switch_seq_col(self, switch_col, path, name):

#         """ 
#         Base class should never be actually called,
#         have to be an implemented class.
#         """
#         assert self.__class__ != BaseFEFS
        
        
#         assert self.seq_col != switch_col
#         assert switch_col in self.colnames
        
#         # Create a new FEFS in the following
#         dict_meta = self.dump_meta()
#         dict_meta["seq_col"] = switch_col
        
#         assert isinstance(name, str)
#         fefs = TSFEFS.create(dict_meta, name)
#         fefs.path = path
#         fefs_fullpath = fefs.__compose_fullpath()
#         if not os.path.isdir(fefs_fullpath):
#             os.mkdir(fefs_fullpath)
            
#         """
#         Explanation:
#         There should be no need to read in the file and changing the seq_col type,
#         since the files should have the seq_col saved as strings.

#         Thus a direct copy of the files from src to dst should do.
#         """            
#         for idx in range(len(self.pieces)):
            
#             piece = self.pieces[idx]
#             type_ = self.types[idx]
#             piece_fullname = self.__compose_piece_fullname(piece)
            
#             if type_ == "csv":
                
#                 src = piece_fullname
#                 dst = '/'.join([fefs_fullpath, piece])
#                 os.system("cp \"%s\" \"%s\""%(src,dst))
                
#             elif type_ == self.__class__.__name__:
                
#                 sub_self = self.__class__()
#                 piece_fullname += '.' + self.__class__.extension
#                 sub_self.read(piece_fullname)
                
#                 sub_self.switch_seq_col(switch_col, fefs_fullpath, piece)
                
#             else:
                
#                 print("File type \"%s\" not yet implemented"%str(type_))
#                 assert False
