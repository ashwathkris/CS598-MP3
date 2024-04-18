from io import BytesIO
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
    buf = memoryview(fb_bytes)
    fb_df = DataFrame.DataFrame.GetRootAsDataFrame(buf, 0)

    # Initialize variables for column indices and the dictionary for sums
    group_sums = {}
    group_col_index = sum_col_index = -1

    # Determine the column indices based on names
    for i in range(fb_df.ColumnsLength()):
        col = fb_df.Columns(i)
        col_name = col.Metadata().Name().decode()
        if col_name == grouping_col_name:
            group_col_index = i
        elif col_name == sum_col_name:
            sum_col_index = i
            if group_col_index != -1:
                break  # Exit early if both columns are found

    if group_col_index == -1 or sum_col_index == -1:
        raise ValueError("Specified columns not found in the DataFrame.")

    group_col = fb_df.Columns(group_col_index)
    sum_col = fb_df.Columns(sum_col_index)

    # Process the columns directly for grouping and summing
    for j in range(group_col.IntValuesLength()):
        group_value = group_col.IntValues(j)
        sum_value = sum_col.IntValues(j)
        group_sums[group_value] = group_sums.get(group_value, 0) + sum_value

    # Sort and convert to DataFrame
    sorted_groups = sorted(group_sums.items())
    result_df = pd.DataFrame(sorted_groups, columns=[grouping_col_name, sum_col_name])
    result_df.set_index(grouping_col_name, inplace=True)
    return result_df


def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
        Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.
        This function shouldn't do anything if col_name doesn't exist or the specified
        column is a string column.

        @param fb_buf: buffer containing bytes of the Flatbuffer Dataframe.
        @param col_name: name of the numeric column to apply map_func to.
        @param map_func: function to apply to elements in the numeric column.
    """
    buf_reader = BytesIO(fb_buf)
    
    # Read metadata to find the column
    while True:
        # Read metadata length
        name_len = int.from_bytes(buf_reader.read(4), byteorder='little')
        if name_len == 0:
            break  # End of columns
        # Read metadata name
        name = buf_reader.read(name_len).decode('utf-8')
        # Check if the column name matches
        if name == col_name:
            # Get the column index
            col_index = int.from_bytes(buf_reader.read(4), byteorder='little')
            break
        else:
            # Skip dtype
            buf_reader.seek(1, 1)  # Skip 1 byte for ValueType enum
    
    else:
        # Column not found
        return
    
    # Move to the start of the column values
    buf_reader.seek(col_index)
    
    # Apply map function to each value in the column
    while True:
        # Read the next value
        try:
            value = int.from_bytes(buf_reader.read(8), byteorder='little')  # Assuming int column
        except struct.error:
            break  # End of column values
        # Apply map function
        new_value = map_func(value)
        # Move back to overwrite the current value
        buf_reader.seek(-8, 1)
        # Write the new value back to the buffer
        buf_reader.write(new_value.to_bytes(8, byteorder='little'))
