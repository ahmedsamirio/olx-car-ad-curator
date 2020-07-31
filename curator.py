# Drop ads with na in specs columns(Mileage, Transmission, CC, etc..)
def load_used_specs():
    file = open("specification.pkl", "rb")
    return pickle.load(file)

def drop_na_rows(df, specs):
    df = df.dropna(how="any", subset=specs).reset_index(drop=True)
    return df

# Load all_features
# TODO: define a function that loads features conversion dict
def load_features_index():
    file = open("features.pkl", "rb")
    return pickle.load(file)


# One-hot encode features
# TODO: define a function that cleans the features list and one hot encodes them
def check_feature(x, feature):
    if x == ['']:
        return 0
    else:
        return int(feature in x)

def one_hot_encode_features(df, features_index):
    for feature_en, feature_ar in features_index.items():
        df[feature_en] = df.Features.apply(lambda x: check_feature(x, features))
    return df.iloc[:, 17:]

# One-hot encode other car specifications (Mileage, Transmission, CC, etc..)
# TODO: load a saved one-hot encoder and process the specs
def load_one_hot_encoder():
    file = open("encoder.pkl", "rb")
    return pickle.load(file)