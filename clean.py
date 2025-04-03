import pandas as pd
import yaml

with open('schema.yaml', 'r') as fin:
    schema = yaml.safe_load(fin)


def read_file(path, bpos_spec):
    """Read the fixed-width text file into a dataframe.
    `bpos_spec` is a dictionary with keys as column names and values as 2-tuples,
    containing the starting and ending byte positions of every field.
    These are included in the schema.yaml file.

    Example
    -------
    >>> import yaml
    >>> with open('schema.yaml', 'r') as fin:
    ...     schema = yaml.safe_load(fin)
    >>> household_df = read_file('TUS106HH.TXT', schema['HH_FIELD_SPANS'])
    >>> person_df = read_file('TUS106PER.TXT', schema['PERSON_FIELD_SPANS'])
    """
    df = pd.read_fwf(path, colspecs=list(bpos_spec.values()), header=None)
    df.columns = list(bpos_spec.keys())
    return df


def read_household(path):
    df = read_file(path, schema['HH_FIELD_SPANS'])
    df.set_index(['fsu_serial_no', 'sample_hh_no'], verify_integrity=True, inplace=True)
    # Map states
    state_codes = df['nss_region'].astype(str).str.zfill(3).str[:2]
    df['state_name'] = state_codes.map(schema['STATES'])
    for col, mapping in schema.items():
        if col.lower() in df:
            df[col.lower()] = df[col.lower()].map(mapping)
    return df


def read_person(path):
    df = read_file(path, schema['PERSON_FIELD_SPANS'])
    df.set_index(['fsu_serial_no', 'sample_hh_no', 'person_serial_no'], inplace=True)
    for col, mapping in schema.items():
        if col.lower() in df:
            df[col.lower()] = df[col.lower()].map(mapping)
    return df
