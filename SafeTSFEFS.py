from TSFEFS import *


class SafeTSFEFS():
    
    """
     - "time_col", "datetime_format" need to be specified.
     - "colnames" will be determined automatically.
    """
    default_incomplete_dict_meta = {
        "piece_name_len": 8, 
        "max_row_per_piece": 400000,
        "cache_config":{"rows_in_cache":None,"len_of_cache":3}
    }
    
    
    def print_tsfefs_info(self):
        self.tsfefs.print_tsfefs_info()


        
        




    ###########################################################################################################
    ############################################ Init methods: BEG ############################################
    
    def __init_tsfefs(self, path, name, tsfefs):
        self.tsfefs = None
        self.import_tsfefs(tsfefs, path=path, name=name)
        return
    
    def __init_dataframe(self, path, name, time_col, datetime_format, df):
        self.tsfefs = None
        self.import_dataframe(df, path=path, name=name, time_col=time_col, datetime_format=datetime_format)
        return
    
    def __init_srcfile(self, path, name, time_col, datetime_format, srcfile):
        self.tsfefs = None
        self.import_srcfile(srcfile, path=path, name=name, time_col=time_col, datetime_format=datetime_format)
        return
    
    def __init_srcfolder(self, path, name, time_col, datetime_format, srcfolder):
        self.tsfefs = None
        self.import_srcfolder(srcfolder, path=path, name=name, time_col=time_col, datetime_format=datetime_format)
        return
    

    def __init__(self, path, name, tsfefs=None, time_col=None, datetime_format=None, df=None, srcpath=None):
        
        """
        1. There are 3 initialization types: i) tsfefs, ii) df, iii) srcpath.
        2. It can have all the values be None, then nothing will be initialized, and path and name will become meaningless.
        3. Otherwise it has to be either 1 of the init types.
        4. If init is by df, (time_col, datetime_format, df) are bundled, and srcpath has to be None.
        5. If init is by srcpath, (time_col, datetime_format, srcpath) are bundled, and df has to be None.
        6. If init is either by df or by srcpath, tsfefs has to be None.
        7. If init is by tsfefs, all (time_col, datetime_format, df, srcpath) have to be None.
        """
        if (tsfefs is None) and (time_col is None) and (datetime_format is None) and (df is None) and (srcpath is None):
            self.tsfefs = TSFEFS()
            return
        
        
        by_tsfefs, by_df, by_srcpath = False, False, False
        
        if tsfefs is not None:
            assert isinstance(tsfefs, TSFEFS)
            assert time_col is None
            assert datetime_format is None
            assert df is None
            assert srcpath is None
            by_tsfefs = True
            self.__init_tsfefs(path, name, tsfefs)
            return
            
        assert tsfefs is None
        assert time_col is not None
        assert isinstance(time_col, str)
        assert datetime_format is not None
        assert isinstance(datetime_format, str)
        
        if df is not None:
            assert srcpath is None
            assert isinstance(df, pd.DataFrame)
            assert time_col in df.columns
            self.__init_dataframe(path, name, time_col, datetime_format, df)
            return
        
        assert srcpath is not None
        if os.path.isfile(srcpath):
            self.__init_srcfile(path, name, time_col, datetime_format, srcpath)
            return
        
        assert os.path.isdir(srcpath)
        self.__init_srcfolder(path, name, time_col, datetime_format, srcpath)
        return
        
    
    
    def clone(self, path, name):
        stsfefs = SafeTSFEFS(path, name, tsfefs=self.tsfefs)
        return stsfefs
    ############################################ Init methods: END ############################################
    ###########################################################################################################
    

    
    
    
    ###################################################################################################################
    ############################################# Overriding __len__: BEG #############################################
    def __len__(self):
        try:
            return len(self.tsfefs)
        except:
            return None
    ############################################# Overriding __len__: END #############################################
    ###################################################################################################################
    
    

    
    

    
    ######################################################################################################################
    ############################################# Overriding ==, >, <, >=, <=: BEG #######################################
    def __eq__(self, other):
        if isinstance(other,SafeTSFEFS):
            other = other.tsfefs
        return self.tsfefs == other
    
    def __ne__(self, other):
        if isinstance(other,SafeTSFEFS):
            other = other.tsfefs
        return self.tsfefs != other

    def __lt__(self, other):
        if isinstance(other,SafeTSFEFS):
            other = other.tsfefs
        return self.tsfefs < other

    def __gt__(self, other):
        if isinstance(other,SafeTSFEFS):
            other = other.tsfefs
        return self.tsfefs > other

    def __le__(self, other):
        if isinstance(other,SafeTSFEFS):
            other = other.tsfefs
        return self.tsfefs <= other

    def __ge__(self, other):
        if isinstance(other,SafeTSFEFS):
            other = other.tsfefs
        return self.tsfefs >= other
    ############################################# Overriding ==, >, <, >=, <=: END #######################################
    ######################################################################################################################


    
    
    
    
    

    #####################################################################################################################
    ################################################# Overriding +: BEG #################################################        
    def __add__(self, other):
        
        """
        This should have overcome the consecutive adds problem in TSFEFS.
        """
        remove_fake_tsfefs = False
        if isinstance(other,SafeTSFEFS):
            other = other.tsfefs.clone(os.getcwd(),"fake_name") # to avoid the instance being modified.
            other.take_actions(max_level=4)
            remove_fake_tsfefs = True
        elif isinstance(other,TSFEFS):
            other = other.clone(os.getcwd(),"fake_name") # to avoid the instance being modified.
            other.take_actions(max_level=4)
            remove_fake_tsfefs = True
            
        self.tsfefs += other

        if remove_fake_tsfefs:
            other.remove()
            del other; other=None

        self.tsfefs.take_actions(max_level=0)
        if self.tsfefs.conflict_exist():
            self.tsfefs.resolve_conflict()
            
        self.tsfefs.optimize_files() # will have all actions done to the highest level inside
        self.tsfefs.maintain_cache()
        
        return self
    ################################################# Overriding +: END #################################################        
    #####################################################################################################################

        
        
        
        
        
        
    ####################################################################################################################
    ################################################# Read, Write: BEG #################################################
    def read(self, fullname):
        # self.tsfefs = TSFEFS()
        self.tsfefs.read(fullname)
        if self.tsfefs.conflict_exist():
            self.tsfefs.resolve_conflict()
        self.tsfefs.take_actions(max_level=0)
        self.tsfefs.maintain_cache()
        return
    
    def write(self, fullname=None):
        if self.tsfefs.has_pending_actions():
            self.tsfefs.take_actions(max_level=4)
        self.tsfefs.write(fullname=fullname)
        return
    ################################################# Read, Write: END #################################################
    ####################################################################################################################

        

        

        
        
        
    ####################################################################################################################
    ##################################### Overriding Square Bracket - getitem: BEG #####################################
    def __getitem__(self, key):
        ret = self.tsfefs[key]
        self.tsfefs.maintain_cache()
        return ret
    
    def __setitem__(self, key, value):
        self.tsfefs[key] = value
        if self.tsfefs.has_pending_actions():
            self.tsfefs.take_actions(max_level=3)
        self.tsfefs.maintain_cache()
        return
    ##################################### Overriding Square Bracket - getitem: ENG #####################################
    ####################################################################################################################

    
    
    
    
    
    ####################################################################################################################
    ##################################### Overriding Square Bracket - delitem: BEG #####################################
    def __delitem__(self, key):
        del self.tsfefs[key]
        if self.tsfefs.has_pending_actions():
            self.tsfefs.take_actions(max_level=4)
        return
    ##################################### Overriding Square Bracket - delitem: END #####################################
    ####################################################################################################################

    

    
    
    
    ################################################################################################################
    ############################################## Import,Export: BEG ##############################################
    def __import_organize_kwargs(self, **kwargs):
        
        if len(kwargs.keys()) > 0:
            assert "path" in kwargs.keys()
            assert "name" in kwargs.keys()
            path = kwargs["path"]
            name = kwargs["name"]
            if len(kwargs.keys()) == 2:
                return (path, name, None, None)
            elif len(kwargs.keys()) == 4:
                assert "time_col" in kwargs.keys()
                assert "datetime_format" in kwargs.keys()
                time_col = kwargs["time_col"]
                datetime_format = kwargs["datetime_format"]
                return (path, name, time_col, datetime_format)
            else:
                assert False
                
        path = self.tsfefs.path
        name = self.tsfefs.name
        time_col = self.tsfefs.time_col
        datetime_format = self.tsfefs.datetime_format
        return (path, name, time_col, datetime_format)
    
    
    
    @classmethod
    def __fill_dict_meta(cls, time_col, datetime_format, cols):
        dict_meta = dc(SafeTSFEFS.default_incomplete_dict_meta)
        dict_meta["time_col"] = time_col
        dict_meta["datetime_format"] = datetime_format
        dict_meta["colnames"] = cols
        return dict_meta


    
        
    # def __init_tsfefs(self, path, name, tsfefs):
    def import_tsfefs(self, tsfefs, **kwargs):
        
        if self.tsfefs is not None:
            self.tsfefs.remove()
        
        
        path, name, _, _ = self.__import_organize_kwargs(**kwargs)
        
        
        tsfefs = tsfefs.clone(path, name)
        tsfefs.take_actions(max_level=0)
        if tsfefs.conflict_exist():
            tsfefs.resolve_conflict()
        tsfefs.take_actions(max_level=3)
        tsfefs.maintain_cache()
        self.tsfefs = tsfefs 
        return

    


    def import_dataframe(self, df, **kwargs):
        
        """
        Reference __init_df for the comments here.
        """
        if self.tsfefs is not None:
            self.tsfefs.remove()

        path, name, time_col, datetime_format = self.__import_organize_kwargs(**kwargs)
        
        
        df = dc(df)
        if df[time_col].dtype == object:
            
            try:
                df[time_col] = df[time_col].apply(lambda x: dt.strptime(x, datetime_format))
            except:
                print("The datetime_format %s doesn't fit the time_col")
                
        elif df[time_col].dtype in [ np.dtype('datetime64[ns]'), np.dtype('<M8[ns]') ]:
            
            T1 = df[time_col].apply(lambda x: x.strftime(datetime_format))
            T1 = T1.apply(lambda x: dt.strptime(x, datetime_format))
            T2 = df[time_col]
            
            tsgap2secs = lambda x: x.days*24*60*60 + x.seconds + x.microseconds/float(1000000)
            ts_gap = T2 - T1
            ts_gap = sum(ts_gap.apply(lambda x: abs(tsgap2secs(x))))
            
            assert ts_gap == 0
            
        else:
            print("Probably the type in time_col is not supported")
            assert False
            
            
        dict_meta = SafeTSFEFS.__fill_dict_meta(time_col, datetime_format, list(df.columns))

        
        tsfefs = TSFEFS.create(dict_meta, name)
        tsfefs.path = path
        tsfefs.import_dataframe(df)
        tsfefs.take_actions(max_level=3) # level:update
        tsfefs.maintain_cache()
        self.tsfefs = tsfefs
        return
    
    
    def import_srcfile(self, srcfile, **kwargs):

        if self.tsfefs is not None:
            self.tsfefs.remove()

        path, name, time_col, datetime_format = self.__import_organize_kwargs(**kwargs)
        
        
        df = pd.read_csv(srcfile, dtype={time_col:str}, nrows=0)
        assert time_col in df.columns
        
        dict_meta = SafeTSFEFS.__fill_dict_meta(time_col, datetime_format, list(df.columns))
        
        tsfefs = TSFEFS.create(dict_meta, name)
        tsfefs.path = path
        tsfefs.import_srcfile(srcfile)
        tsfefs.take_actions(max_level=3) # level:update
        tsfefs.maintain_cache()
        self.tsfefs = tsfefs
        return
        
        

    def import_srcfolder(self, srcfolder, **kwargs):

        if self.tsfefs is not None:
            self.tsfefs.remove()

        path, name, time_col, datetime_format = self.__import_organize_kwargs(**kwargs)
                

        """
        List of files to be copied
        """
        files = os.listdir(srcfolder)
        if srcfolder[-1] == '/':
            srcfolder = srcfolder[:-1]
        files = [ "%s/%s"%(srcfolder,f) for f in files ]
        files = [ f for f in files if not os.path.isdir(f) ]
        assert len(files) > 0        
        for f in files:
            df = pd.read_csv(f, dtype={time_col:str}, nrows=0)
            break
            
        dict_meta = SafeTSFEFS.__fill_dict_meta(time_col, datetime_format, list(df.columns))
        
        tsfefs = TSFEFS.create(dict_meta, name)
        tsfefs.path = path
        tsfefs.import_srcfolder(srcfolder)
        """
        Don't need to run 
            tsfefs.take_actions(max_level=3) # level:update
            tsfefs.maintain_cache()
        because if import srcfolder, all files are directly copied and saved,
        nothing in cache as well.        
        """
        tsfefs.resolve_conflict()   # since there could have multiple files in srcfolder, conflict may exists.
        tsfefs.optimize_files() # will have all actions done to the highest level inside
        tsfefs.maintain_cache()
        self.tsfefs = tsfefs
        return
    
    
    
    
    
    def export_tsfefs(self, path, name):
        return self.tsfefs.clone(path, name)


    def export_dataframe(self):
        return self.tsfefs.export_dataframe()    
    

    def export_dstfile(self, dstfile):
        self.tsfefs.export_dstfile(dstfile)
        return
        

    def export_dstfolder(self, dstfolder):
        if self.tsfefs.conflict_exist():
            self.tsfefs.resolve_conflict()
        self.tsfefs.optimize_files() # will have all actions done to the highest level inside
        self.tsfefs.export_dstfolder(dstfolder)
        return
    ############################################## Import,Export: END ##############################################
    ################################################################################################################

    
    
    
    def remove(self):
        self.tsfefs.remove()
        return

        
        

