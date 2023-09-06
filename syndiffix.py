import os
import platform
import subprocess
import shlex
import fire
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.feature_selection import RFECV, VarianceThreshold
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, OrdinalEncoder, RobustScaler
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor

# ----------------------------------------------------------------
# Feature Selection
# ----------------------------------------------------------------


CATEGORY_THRESHOLD = 15


def is_categorical(column):
    return column.dtype == 'object' or column.nunique() <= CATEGORY_THRESHOLD


def get_feature_types(df):
    text_features = []
    continuous_features = []
    categorical_features = []

    for colname in df.columns:
        column = df[colname]
        nunique = column.nunique()
        if nunique <= CATEGORY_THRESHOLD:
            categorical_features.append(colname)
        elif column.dtype == 'object':
            text_features.append(colname)
        else:
            continuous_features.append(colname)

    return text_features, continuous_features, categorical_features


def one_hot_feature_names(encoder, categorical_features):
    cats = [
        encoder._compute_transformed_categories(i)
        for i, _ in enumerate(encoder.categories_)
    ]

    feature_names = []
    inverse_names = {}

    for i in range(len(cats)):
        category = categorical_features[i]
        names = [category + '$$' + str(label) for label in cats[i]]
        feature_names.extend(names)

        for name in names:
            inverse_names[name] = category

    return feature_names, inverse_names


def random_encoder(X):
    X = X.copy()

    for column in X.columns:
        column_text = X[column].apply(str)
        unique_values = column_text.unique()
        np.random.shuffle(unique_values)
        mapping = {value: index for index, value in enumerate(unique_values)}
        X[column] = column_text.map(mapping).astype(int)

    return X


def preprocess(df, one_hot_encode=True, variance_threshold=True):
    text_features, continuous_features, categorical_features = get_feature_types(df)
    ordinal_features = []

    if not one_hot_encode:
        ordinal_features = categorical_features
        categorical_features = []

    preprocessor = ColumnTransformer(
        transformers=[
            ('random_encode', FunctionTransformer(random_encoder), text_features),
            ('ord', OrdinalEncoder(), ordinal_features),
            ('num', RobustScaler(), continuous_features),
            ('cat', OneHotEncoder(sparse_output=False), categorical_features),
        ],
        sparse_threshold=0)

    result = preprocessor.fit_transform(df)

    inverse_lookup = {
        name: name for name in text_features + ordinal_features + continuous_features
    }

    categorical_names = []

    if len(categorical_features) > 0:
        categorical_names, categorical_inverse = one_hot_feature_names(
            preprocessor.named_transformers_['cat'],
            categorical_features
        )
        inverse_lookup.update(categorical_inverse)

    df_preprocessed = pd.DataFrame(
        result,
        columns=text_features + ordinal_features + continuous_features + categorical_names
    )

    if not variance_threshold:
        return df_preprocessed, inverse_lookup

    threshold = VarianceThreshold(0.00001)
    threshold.set_output(transform='pandas')

    df_filtered = threshold.fit_transform(df_preprocessed)

    return df_filtered, inverse_lookup


def convert_datetime_to_numeric(df):
    REFERENCE_DATE = pd.to_datetime('1800-01-01')

    for col_name, col_type in zip(df.columns, df.dtypes):
        if pd.api.types.is_datetime64_any_dtype(col_type):
            df[col_name] = pd.to_numeric(df[col_name] - REFERENCE_DATE).div(1e9).astype(float)

    return df


def split(df, column, one_hot_X):
    df.dropna(axis=0, inplace=True)
    convert_datetime_to_numeric(df)
    X, X_inv = preprocess(df.drop(column, axis=1), one_hot_encode=one_hot_X)
    y, _ = preprocess(df[[column]], one_hot_encode=False)
    return X, X_inv, y


def select_features_ml(df, column, one_hot_X=False):
    X, X_inv, y = split(df, column, one_hot_X)

    if y.shape[0] == 0 or y.shape[1] == 0:
        return {
            'valid': False,
            'features': [],
            'k': 0,
            'kFeatures': [],
            'cumulativeScore': [],
            'cumulativeScoreStd': [],
            'encoded': {
                'features': [],
                'k': 0,
                'kFeatures': [],
                'cumulativeScore': [],
                'cumulativeScoreStd': []
            }
        }

    if X.shape[1] == 1:
        return {
            'valid': False,
            'features': [X.columns[0]],
            'k': 1,
            'kFeatures': [X.columns[0]],
            'cumulativeScore': [0.0],
            'cumulativeScoreStd': [0.0],
            'encoded': {
                'features': [X.columns[0]],
                'k': 1,
                'kFeatures': [X.columns[0]],
                'cumulativeScore': [0.0],
                'cumulativeScoreStd': [0.0]
            }
        }

    if is_categorical(y[column]):
        estimator = DecisionTreeClassifier()
    else:
        estimator = DecisionTreeRegressor()

    rfecv = RFECV(estimator=estimator)
    rfecv.fit(X, y)

    feature_ranks = rfecv.ranking_
    feature_names = X.columns.tolist()

    sorted_features = sorted(zip(feature_names, feature_ranks), key=lambda x: x[1])

    encoded_k = int(rfecv.n_features_)
    encoded_features = [name for name, _ in sorted_features]
    encoded_scores = rfecv.cv_results_['mean_test_score'].tolist()
    encoded_scores_std = rfecv.cv_results_['std_test_score'].tolist()

    decoded_k = 0
    decoded_features = []
    decoded_scores = []
    decoded_scores_std = []

    for i, feature in enumerate(encoded_features):
        decoded_feature = X_inv[feature]
        if decoded_feature in decoded_features:
            if decoded_features[-1] == decoded_feature:
                decoded_scores[-1] = encoded_scores[i]
                decoded_scores_std[-1] = encoded_scores_std[i]
        else:
            decoded_features.append(decoded_feature)
            decoded_scores.append(encoded_scores[i])
            decoded_scores_std.append(encoded_scores_std[i])

        if i == encoded_k - 1:
            decoded_k = len(decoded_features)

    return {
        'valid': True,
        'features': decoded_features,
        'k': decoded_k,
        'kFeatures': decoded_features[:decoded_k],
        'cumulativeScore': decoded_scores,
        'cumulativeScoreStd': decoded_scores_std,
        'encoded': {
            'features': encoded_features,
            'k': encoded_k,
            'kFeatures': encoded_features[:encoded_k],
            'cumulativeScore': encoded_scores,
            'cumulativeScoreStd': encoded_scores_std
        }
    }


# ----------------------------------------------------------------
# SynDiffix Wrapper
# ----------------------------------------------------------------


def get_this_dir():
    script_path = os.path.abspath(__file__)
    return os.path.dirname(script_path)


def get_syndiffix_exe():
    bin_dir = os.environ.get('SYNDIFFIX_BIN')

    if not bin_dir:
        bin_dir = get_this_dir()

    exe_name = 'syndiffix'

    if platform.system() == 'Windows':
        exe_name += '.exe'

    full_path = os.path.join(bin_dir, exe_name)
    if not os.path.exists(full_path):
        print('Could not find executable for SynDiffix. If you want to run from source, add the --dev flag.')
        exit(1)

    return full_path


def run_syndiffix(input_path, output_path, columns, dev=False, extra_args=[], user_args=''):
    if dev:
        run_args = ['dotnet', 'run', '--configuration', 'Release']
        cwd = os.path.join(get_this_dir(), 'src', 'SynDiffix')
    else:
        run_args = [get_syndiffix_exe()]
        cwd = None

    print('Running SynDiffix...')

    columns_args = [c['name'] + ':' + c['type'] for c in columns]

    user_args = shlex.split(user_args)

    syndiffix_process = subprocess.run(
        [*run_args, input_path, '--output', output_path, *extra_args, *user_args, '--columns', *columns_args],
        cwd=cwd
    )

    return syndiffix_process.returncode


def load_csv(path):
    from pandas.errors import ParserError

    df = pd.read_csv(path, keep_default_na=False, na_values=[''], low_memory=False)

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
            t = 'b'
        elif pd.api.types.is_integer_dtype(col_type):
            t = 'i'
        elif pd.api.types.is_float_dtype(col_type) or pd.api.types.is_numeric_dtype(col_type):
            t = 'r'
        elif pd.api.types.is_datetime64_any_dtype(col_type):
            t = 't'
        elif pd.api.types.is_string_dtype(col_type) or pd.api.types.is_object_dtype(col_type):
            t = 's'
        else:
            raise Exception(f"Unknown type for column '{col_name}'.")

        col_data = {'name': col_name, 'type': t}
        columns.append(col_data)

    return columns


def process_aid_columns(arg):
    if isinstance(arg, list):
        return arg
    elif isinstance(arg, tuple):
        return list(arg)
    elif isinstance(arg, str):
        return [arg]
    else:
        return []


def main(
        input_path: str,
        output_path: str,
        aid_columns: list[str] = [],
        ml_target: str = None,
        ml_features_only: bool = False,
        syndiffix_args: str = '',
        dev: bool = False):
    """
    Runs SynDiffix on CSV file and writes synthetic data to output path.

    Parameters:
        input_path: Path of input CSV file.
        output_path: Path of output CSV file.
        aid_columns: Entity identifier columns. If not specified, assumes one row per entity.
        ml_target: If specified, focuses on this column for better ML prediction.
        ml_features_only: If set, limits columns to only ML features of ml_target.
        syndiffix_args: Extra arguments to pass to syndiffix.
        dev: Compile and run on the fly via 'dotnet run'.
    """

    input_path = os.path.abspath(input_path)
    output_path = os.path.abspath(output_path)

    df = load_csv(input_path)
    columns = columns_metadata(df)

    extra_args = []

    aid_columns = process_aid_columns(aid_columns)
    if len(aid_columns) > 0:
        print(f'AID Columns: {aid_columns}')
        extra_args += ['--aidcolumns', *aid_columns]

    if ml_target:
        print('ML Target: ' + ml_target)

        print('Selecting ML features...')
        features = select_features_ml(df, ml_target)['kFeatures']
        print('ML Features: ' + (', '.join(features)))

        extra_args += [
            '--clustering-maincolumn', ml_target,
            '--clustering-mainfeatures', *features
        ]

        if ml_features_only:
            print('Using only target column and its features.')
            columns = [column for column in columns if column['name'] == ml_target or column['name'] in features]

    return_code = run_syndiffix(input_path, output_path, columns, dev=dev,
                                user_args=syndiffix_args, extra_args=extra_args)

    if return_code == 0:
        print(f"CSV saved to '{output_path}'.")
    else:
        print('Failed to run SynDiffix.')

    exit(return_code)


if __name__ == '__main__':
    fire.Fire(main)
