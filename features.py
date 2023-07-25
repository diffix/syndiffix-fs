import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.feature_selection import RFECV, VarianceThreshold
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, OrdinalEncoder, RobustScaler
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor


def is_classification(column):
    return column.dtype == 'object' or column.nunique() <= 10


def get_feature_types(df):
    text_features = []
    continuous_features = []
    categorical_features = []

    for colname in df.columns:
        column = df[colname]
        nunique = column.nunique()
        if nunique <= 10:
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
        names = [category + "$$" + str(label) for label in cats[i]]
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
        return df_preprocessed

    threshold = VarianceThreshold(0.00001)
    threshold.set_output(transform="pandas")

    df_filtered = threshold.fit_transform(df_preprocessed)

    return df_filtered, inverse_lookup


def convert_datetime_to_numeric(df):
    from pandas.errors import ParserError
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

    if is_classification(y[column]):
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
    encoded_scores = rfecv.cv_results_["mean_test_score"].tolist()
    encoded_scores_std = rfecv.cv_results_["std_test_score"].tolist()

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
