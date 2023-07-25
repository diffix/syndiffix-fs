import pandas as pd
import os
import subprocess
import shlex
import fire
from features import select_features_ml


def get_syndiffix_dir():
    dir = os.environ.get('SYNDIFFIX_DIR')
    if dir:
        return dir
    else:
        script_path = os.path.abspath(__file__)
        script_directory = os.path.dirname(script_path)
        return os.path.dirname(script_directory)


def run_syndiffix(input_path, output_path, columns, extra_args=[], user_args=''):
    syndiffix_dir = os.path.join(get_syndiffix_dir(), 'src', 'SynDiffix')

    type_symbols = {'text': 's', 'real': 'r', 'datetime': 't', 'int': 'i', 'boolean': 'b'}
    columns_args = [c['name'] + ':' + type_symbols[c['type']] for c in columns]

    user_args = shlex.split(user_args)

    abSharp = subprocess.run(
        ['dotnet', 'run', '--configuration', 'Release', input_path,
         '--output-path', output_path, '--columns', *columns_args, *extra_args, *user_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=syndiffix_dir
    )

    print(abSharp.stderr.decode("utf-8"))


def load_csv(path):
    from pandas.errors import ParserError

    df = pd.read_csv(path, keep_default_na=False, na_values=[''])

    # Try to infer datetime columns.
    for col in df.columns[df.dtypes == 'object']:
        try:
            df[col] = pd.to_datetime(df[col], format='ISO8601')
        except (ParserError, ValueError):
            pass

    return df


def columns_metadata(df):
    columns = []

    for col_name, col_type in zip(df.columns, df.dtypes):
        if pd.api.types.is_bool_dtype(col_type):
            t = 'boolean'
        elif pd.api.types.is_integer_dtype(col_type):
            t = 'int'
        elif pd.api.types.is_float_dtype(col_type) or pd.api.types.is_numeric_dtype(col_type):
            t = 'real'
        elif pd.api.types.is_datetime64_any_dtype(col_type):
            t = 'datetime'
        elif pd.api.types.is_string_dtype(col_type) or pd.api.types.is_object_dtype(col_type):
            t = 'text'
        else:
            raise Exception(f"Unknown type for column '{col_name}'.")

        col_data = {'name': col_name, 'type': t}
        columns.append(col_data)

    return columns


def main(
        input_path: str,
        output_path: str,
        ml_target: str = None,
        ml_patches: bool = True,
        syndiffix_args: str = ''):
    """
    Runs syndiffix on CSV file and writes synthetic data to output path.

    Parameters:
        input_path: Path of input CSV file.
        output_path: Path of output CSV file.
        ml_target: If specified, focuses on this column for better ML prediction.
        ml_patches: If set to false, limits columns to only ML features of ml_target.
        syndiffix_args: Extra arguments to pass to syndiffix.
    """

    input_path = os.path.abspath(input_path)
    output_path = os.path.abspath(output_path)

    df = load_csv(input_path)
    columns = columns_metadata(df)

    extra_args = []

    if ml_target:
        print('ML Target: ' + ml_target)

        print('Selecting ML features...')
        features = select_features_ml(df, ml_target)['kFeatures']
        print('ML Features: ' + (', '.join(features)))

        extra_args = [
            '--clustering-maincolumn', ml_target,
            '--clustering-mlfeatures', *features
        ]

        if not ml_patches:
            print('Using only target column and its features.')
            columns = [column for column in columns if column.name == ml_target or column.name in features]

    print('Running syndiffix...')
    run_syndiffix(input_path, output_path, columns, user_args=syndiffix_args, extra_args=extra_args)
    print(f"CSV saved to '{output_path}'.")


if __name__ == '__main__':
    fire.Fire(main)
