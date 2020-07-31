import pickle


def load_used_specs():
    file = open("specification.pkl", "rb")
    return pickle.load(file)


# Drop ads with na in specs columns(Mileage, Transmission, CC, etc..)
def drop_na_rows(df, specs):
    df = df.dropna(how="any", subset=specs).reset_index(drop=True)
    return df


# Load all_features
def load_features_index():
    file = open("features.pkl", "rb")
    return pickle.load(file)


# One-hot encode features
def check_feature(x, feature):
    if x == ['']:
        return 0
    else:
        return int(feature in x)


# One-hot encode other car specifications (Mileage, Transmission, CC, etc..)
def one_hot_encode_features(df, features_index):
    for feature_en, feature_ar in features_index.items():
        df[feature_en] = df.Features.apply(lambda x: check_feature(x, features))
    return df.iloc[:, 17:]


def load_one_hot_encoder():
    file = open("encoder.pkl", "rb")
    return pickle.load(file)


def df_preprocessing(df):
    df = df.drop_duplicates()
    df = df.dropna(axis=0, subset=["Year"])
    df = df[df["Ad_type"] != "مطلوب للشراء"]  # drop ads looking to buy
    df = df[df["Pay_type"] == "كاش"]   # keep only cash ads
    df = df.reset_index(drop=True)  # reset index
    df["Year"] = df["Year"].astype(np.int)  # convert year to int
    return df


def reality_check(car_df):
    mean_price = car_df.Price.mean()
    for idx, ad in car_df.iterrows():
        condition = ad.Price - mean_price > (mean_price // 2)  # made into a variable so it could tuned as a hyperparameter
        if condition:
            car_df = car_df.drop(idx)
    car_df = car_df.reset_index(drop=True)
    return car_df


def make_ad_vector(df, features_index, features_encoder, specs):
    one_hot_features = one_hot_encode_features(df, features_index).values
    one_hot_specs = encoder.transform(df[specs])
    one_hot_array = np.concatenate([one_hot_specs, one_hot_features], axis=1)
    return one_hot_array



if __name__ == "__main__":
    model = ""
    year = ""

    raw_df = pd.read_csv('olx_raw.csv')
    df = df_preprocessing(raw_df)

    mask = (df.Model == model) & (df.Year.isin(year))
    car_df = df[mask]
    car_df = reality_check(car_df)

    specs = load_used_specs()
    features_index = load_features_index()
    encoder = load_one_hot_encoder()

    car_df = drop_na_rows(car_df, specs)

    max_price = car_df.Price.max()
    max_price_ad = car_df[car_df.Price == max_price].sample(1)
    max_price_ad_vector = make_ad_vector(max_price_ad, features_index, encoder, specs)

    one_hot_features = one_hot_encode_features(car_df, features_index).values
    one_hot_specs = encoder.transform(car_df[specs])
    one_hot_array = np.concatenate([one_hot_specs, one_hot_features], axis=1)

    for idx, ad in car_df.sort_values("Price").iterrows():
        ad = pd.DataFrame(ad).T
        ad_vector = make_ad_vector(ad, features_index, encoder, specs)
        dot_product = np.dot(ad_vector, max_price_ad_vector.T)
        if dot_product > np.dot(max_price_ad_vector, max_price_ad_vector.T) // 2:
            print(dot_product.item(), ad.Price, ad.URL)

    
