import dill
import json
import hashlib
import pandas as pd
import types
import struct
from multiprocessing import shared_memory
from fb_dataframe import to_flatbuffer, fb_dataframe_head, fb_dataframe_group_by_sum, fb_dataframe_map_numeric_column
class FbSharedMemory:
    """
        Class for managing the shared memory for holding flatbuffer dataframes.
    """
    def __init__(self):
        try:
            self.df_shared_memory = shared_memory.SharedMemory(name = "CS598")
        except FileNotFoundError:
            # Shared memory is not created yet, create it with size 200M.
            self.df_shared_memory = shared_memory.SharedMemory(name = "CS598", create=True, size=200000000)

            # Add more initialization steps if needed here...
        self.start = 0
        self.startdict = dict()
        try:
            with open('startdict.json', 'r') as f:
                self.startdict = json.load(f)
        except FileNotFoundError:
            self.startdict = {}

    def add_dataframe(self, name: str, df: pd.DataFrame) -> None:
        """
            Adds a dataframe into the shared memory. Does nothing if a dataframe with 'name' already exists.

            @param name: name of the dataframe.
            @param df: the dataframe to add to shared memory.
        """
        # YOUR CODE HERE...
        if name in self.startdict:
            print(f"Dataframe with name {name} already exists.")
            return
        
        fb_data = to_flatbuffer(df)
        fb_size = len(fb_data)
        total_size = 4 + fb_size  # Include size for length prefix

        if self.start + total_size > self.df_shared_memory.size:
            raise MemoryError("Not enough shared memory available")

        struct.pack_into('I', self.df_shared_memory.buf, self.start, fb_size)
        self.df_shared_memory.buf[self.start+4:self.start+4+fb_size] = fb_data

        self.startdict[name] = self.start
        self.start += total_size
        with open('startdict.json', 'w') as f:
            json.dump(self.startdict, f)


    def _get_fb_buf(self, df_name: str) -> memoryview:
        """
            Returns the section of the buffer corresponding to the dataframe with df_name.
            Hint: get buffer section (fb_buf) holding the flatbuffer from shared memory.

            @param df_name: name of the Dataframe.
        """
        if df_name not in self.startdict:
            raise KeyError(f"Dataframe {df_name} not found in shared memory")
        
        start = self.startdict[df_name]
        size = struct.unpack_from('I', self.df_shared_memory.buf, start)[0]
        return memoryview(self.df_shared_memory.buf[start+4:start+4+size])


    def dataframe_head(self, df_name: str, rows: int = 5) -> pd.DataFrame:
        """
            Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
            similar to df.head(). If there are less than n rows, returns the entire Dataframe.

            @param df_name: name of the Dataframe.
            @param rows: number of rows to return.
        """
        fb_bytes = bytes(self._get_fb_buf(df_name))
        return fb_dataframe_head(fb_bytes, rows)

    def dataframe_group_by_sum(self, df_name: str, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
        """
            Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
            and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.
    
            @param df_name: name of the Dataframe.
            @param grouping_col_name: column to group by.
            @param sum_col_name: column to sum.
        """
        fb_bytes = bytes(self._get_fb_buf(df_name))
        return fb_dataframe_group_by_sum(fb_bytes, grouping_col_name, sum_col_name)

    def dataframe_map_numeric_column(self, df_name: str, col_name: str, map_func: types.FunctionType) -> None:
        """
            Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.

            @param df_name: name of the Dataframe.
            @param col_name: name of the numeric column to apply map_func to.
            @param map_func: function to apply to elements in the numeric column.
        """
        fb_dataframe_map_numeric_column(self._get_fb_buf(df_name), col_name, map_func)


    def close(self) -> None:
        """
            Closes the managed shared memory.
        """
        try:
            self.df_shared_memory.close()
            self.df_shared_memory.unlink()
        except:
            pass