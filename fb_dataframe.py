import flatbuffers
import pandas as pd
import struct
import time
import types
from flatbuffers import Builder
from DataFrame import Column, DataFrame, Metadata, ValueType

# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    """
        Converts a DataFrame to a flatbuffer. Returns the bytearray of the flatbuffer.

        The flatbuffer should follow a columnar format as follows:
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        | DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        You are free to put any bookkeeping items in the metadata. however, for autograding purposes:
        1. Make sure that the values in the columns are laid out in the flatbuffer as specified above
        2. Serialize int and float values using flatbuffer's 'PrependInt64' and 'PrependFloat64'
            functions, respectively (i.e., don't convert them to strings yourself - you will lose
            precision for floats).

        @param df: the dataframe.
    """
    builder = Builder(1024)
    metadata_string = builder.CreateString("DataFrame Metadata")
    column_metadata_list = []
    value_vectors = []
    value_vectors_dtype = []
    for column_name, dtype in df.dtypes.items():
        if dtype == 'int64':
            value_type = ValueType.ValueType().Int
        elif dtype == 'float64':
            value_type = ValueType.ValueType().Float
        elif dtype == 'object':
            value_type = ValueType.ValueType().String
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

        column_metadata_list.append((column_name, value_type))

        # Convert column values to FlatBuffer values
        column_values = df[column_name]
        value_vectors.append(column_values.tolist())
        value_vectors_dtype.append(dtype)
    columns = []
    for dtype, metadata, value_vector in reversed(list(zip(value_vectors_dtype ,column_metadata_list, value_vectors))):
        if dtype == 'int64':
            Column.StartIntValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependInt64(value)
            values = builder.EndVector(len(value_vector))

            col_name = builder.CreateString(metadata[0])
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddIntValues(builder, values)
            columns.append(Column.End(builder))
        elif dtype == 'float64':
            Column.StartFloatValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependFloat64(value)
            values = builder.EndVector(len(value_vector))
            
            col_name = builder.CreateString(metadata[0])
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddFloatValues(builder, values)
            columns.append(Column.End(builder))
        elif dtype == 'object':
            str_offsets = [builder.CreateString(str(value)) for value in value_vector]
            Column.StartStringValuesVector(builder, len(value_vector))
            for offset in reversed(str_offsets):
                builder.PrependUOffsetTRelative(offset)
            values = builder.EndVector(len(value_vector))
            
            col_name = builder.CreateString(metadata[0])
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddStringValues(builder, values)
            columns.append(Column.End(builder))

    # Create a vector of Column objects
    DataFrame.StartColumnsVector(builder, len(columns))
    for column in columns:
        builder.PrependUOffsetTRelative(column)
    columns_vector = builder.EndVector(len(columns))
    

    # Create the DataFrame object
    DataFrame.Start(builder)
    DataFrame.AddMetadata(builder, metadata_string)
    DataFrame.AddColumns(builder, columns_vector)
    df_data = DataFrame.End(builder)

    # Finish building the FlatBuffer
    builder.Finish(df_data)
    # Get the bytes from the builder
    return builder.Output()


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
        Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
        similar to df.head(). If there are less than n rows, return the entire Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param rows: number of rows to return.
    """
    buf = flatbuffers.Builder(0)
    buf.Bytes = fb_bytes

    # Get the DataFrame from bytes
    df = DataFrame.DataFrame.GetRootAsDataFrame(buf.Bytes, 0)

    # Prepare to collect columns data
    columns = {}

    # Iterate over columns
    for i in range(df.ColumnsLength()):
        col = df.Columns(i)
        meta = col.Metadata()
        col_name = meta.Name().decode()

        if meta.Dtype() == ValueType.ValueType().Int:
            # Extract integer values
            values = [col.IntValues(j) for j in range(min(col.IntValuesLength(), rows))]
        elif meta.Dtype() == ValueType.ValueType().Float:
            # Extract float values
            values = [col.FloatValues(j) for j in range(min(col.FloatValuesLength(), rows))]
        elif meta.Dtype() == ValueType.ValueType().String:
            # Extract string values
            values = [col.StringValues(j).decode() for j in range(min(col.StringValuesLength(), rows))]

        # Store column in dictionary
        columns[col_name] = values

    # Create a DataFrame from the dictionary
    result_df = pd.DataFrame(columns)

    return result_df


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
        Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.
        This function shouldn't do anything if col_name doesn't exist or the specified
        column is a string column.

        @param fb_buf: buffer containing bytes of the Flatbuffer Dataframe.
        @param col_name: name of the numeric column to apply map_func to.
        @param map_func: function to apply to elements in the numeric column.
    """
    # Access the buffer using the FlatBuffers builder
    buf = bytearray(fb_buf)  # Convert memoryview to mutable bytearray for in-place updates
    df = DataFrame.GetRootAsDataFrame(buf, 0)  # Use the correct method to get root from your schema
    
    # Iterate through columns to find the one to modify
    for i in range(df.ColumnsLength()):
        column = df.Columns(i)
        metadata = column.Metadata()
        if metadata.Name().decode() == col_name and metadata.Dtype() in {ValueType.Int, ValueType.Float}:
            if metadata.Dtype() == ValueType.Int:
                value_size = 8  # size of int64
                unpack_format = '<q'  # Little-endian int64
                start_offset = column.IntValues(0)  # Get the start offset of integer values
                num_elements = column.IntValuesLength()  # Get the length of integer values
            elif metadata.Dtype() == ValueType.Float:
                value_size = 8  # size of float64
                unpack_format = '<d'  # Little-endian float64
                start_offset = column.FloatValues(0)  # Get the start offset of float values
                num_elements = column.FloatValuesLength()  # Get the length of float values

            # Modify each element in the vector
            for j in range(num_elements):
                element_offset = start_offset + j * value_size
                # Unpack the value, apply the function, and repack it
                value, = struct.unpack_from(unpack_format, buf, element_offset)
                new_value = map_func(value)
                struct.pack_into(unpack_format, buf, element_offset, new_value)
                
            # No need to return anything as the buffer is modified in place
            fb_buf[:] = buf  # Ensure the original memoryview is updated
            break
    
