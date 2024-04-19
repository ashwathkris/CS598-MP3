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
    metalist=list()
    vecs=list()
    vecs_dtype=list()
    for c,d in df.dtypes.items():
        if(d == 'int64'):
            value_type = ValueType.ValueType().Int
        elif(d == 'float64'):
            value_type = ValueType.ValueType().Float
        elif(d == 'object'):
            value_type = ValueType.ValueType().String
        else:
            return
        metalist.append((c, value_type))
        v=df[c]
        vecs.append(v.tolist())
        vecs_dtype.append(d)
    columns = list()
    for dtype, metadata, vvec in reversed(list(zip(vecs_dtype ,metalist, vecs))):
        if(dtype == 'int64'):
            Column.StartIntValuesVector(builder, len(vvec))
            for value in reversed(vvec):
                builder.PrependInt64(value)
            values = builder.EndVector(len(vvec))
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
        elif(dtype == 'float64'):
            Column.StartFloatValuesVector(builder, len(vvec))
            for value in reversed(vvec):
                builder.PrependFloat64(value)
            values = builder.EndVector(len(vvec))
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
        elif(dtype == 'object'):
            str_offsets = [builder.CreateString(str(value)) for value in vvec]
            Column.StartStringValuesVector(builder, len(vvec))
            for offset in reversed(str_offsets):
                builder.PrependUOffsetTRelative(offset)
            values = builder.EndVector(len(vvec))
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
    DataFrame.StartColumnsVector(builder, len(columns))
    for c in columns:
        builder.PrependUOffsetTRelative(c)
    columns_vector = builder.EndVector(len(columns))
    DataFrame.Start(builder)
    DataFrame.AddMetadata(builder, metadata_string)
    DataFrame.AddColumns(builder, columns_vector)
    df_data = DataFrame.End(builder)
    builder.Finish(df_data)
    return builder.Output()

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
        Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
        similar to df.head(). If there are less than n rows, return the entire Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param rows: number of rows to return.
    """
    buf=flatbuffers.Builder(0)
    buf.Bytes=fb_bytes
    df=DataFrame.DataFrame.GetRootAsDataFrame(buf.Bytes,0)
    columns=dict()
    for i in range(df.ColumnsLength()):
        col=df.Columns(i)
        m=col.Metadata()
        c=m.Name().decode()
        if(m.Dtype() == ValueType.ValueType().Int):
            values = []
            max_index = min(col.IntValuesLength(), rows)
            for j in range(max_index):
                value = col.IntValues(j)
                values.append(value)
        elif(m.Dtype() == ValueType.ValueType().Float):
            values = []
            max_index = min(col.FloatValuesLength(), rows)
            for j in range(max_index):
                value = col.FloatValues(j)
                values.append(value)
        elif(m.Dtype() == ValueType.ValueType().String):
            values = []
            max_index = min(col.StringValuesLength(), rows)
            for j in range(max_index):
                value = col.StringValues(j).decode()
                values.append(value)
        columns[c] = values
    res=pd.DataFrame(columns)
    return res


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
    group_sums = dict()
    group_col_index = sum_col_index = -1
    for i in range(fb_df.ColumnsLength()):
        col = fb_df.Columns(i)
        col_name = col.Metadata().Name().decode()
        if col_name == grouping_col_name:
            group_col_index = i
        elif col_name == sum_col_name:
            sum_col_index = i
            if group_col_index != -1:
                break
    if group_col_index == -1 or sum_col_index == -1:
        return
    group_col = fb_df.Columns(group_col_index)
    sum_col = fb_df.Columns(sum_col_index)
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
    # Access the buffer using the FlatBuffers builder
    fb_bytes = bytearray(fb_buf)  # Convert memoryview to bytearray for in-place modifications
    fb_df = DataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)

    column = None
    for i in range(fb_df.ColumnsLength()):
        col = fb_df.Columns(i)
        if col.Metadata().Name().decode() == col_name:
            if col.Metadata().Dtype() in [ValueType.ValueType().Int, ValueType.ValueType().Float]:
                column = col
                break

    if not column:
        print("Column not found or not a numeric type.")
        return  # Early exit if column is not found or is of an incorrect type

    if column.Metadata().Dtype() == ValueType.ValueType().Int:
        offset_base = column._tab.Vector(column._tab.Offset(6))
        num_elements = column.IntValuesLength()
        for j in range(num_elements):
            offset = offset_base + j * 8
            original_value = struct.unpack('<q', fb_bytes[offset:offset + 8])[0]
            new_value = map_func(original_value)
            fb_bytes[offset:offset + 8] = struct.pack('<q', new_value)

    elif column.Metadata().Dtype() == ValueType.ValueType().Float:
        offset_base = column._tab.Vector(column._tab.Offset(8))
        num_elements = column.FloatValuesLength()
        for j in range(num_elements):
            offset = offset_base + j * 8
            original_value = struct.unpack('<d', fb_bytes[offset:offset + 8])[0]
            new_value = map_func(original_value)
            fb_bytes[offset:offset + 8] = struct.pack('<d', new_value)

    # Replace the original memoryview with the modified bytearray
    fb_buf[:] = fb_bytes 
    
