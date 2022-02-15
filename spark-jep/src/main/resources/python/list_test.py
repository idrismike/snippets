import numpy
import pandas as pd
import logging, sys

from java.util import ArrayList

#
# Applied https://gist.github.com/SemanticBeeng/0e9f9ed40866a5f844be13ab6ef3469e
#


log = logging.getLogger(f"list_test")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler(sys.stdout))


#
#
#
def count_records(datawindow) -> int:

    log.info(f">>>[Python]<<< calling count_records.")

    log.info(f">>>[Python]<<< datawindow type: {type(datawindow)}, data type: {type(datawindow.data())}")

    df = read_pa(datawindow)

    log.info(f"data types {df.shape}")

    return df.shape[0]


def read_pa(datawindow) -> pd.DataFrame:

    import pyarrow as pa

    reader = pa.RecordBatchFileReader(pa.BufferReader(datawindow.data().tobytes()))

    table = reader.read_all()
    log.info(f">>>[Python]<<< arrow table rows count {table.num_rows}")

    df = table.to_pandas()

    log.info(f"data schema (arrow) = {reader.schema}")
    log.info(f"data schema (pandas) = {table.schema}")

    log.info(f"data type (pandas) = {type(df)}")
    log.info(f"data (pandas) = {df.head(100)}")

    return df