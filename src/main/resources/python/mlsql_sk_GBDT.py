import xmatrix
from xmatrix import list_to_vect, dict_to_row
from sklearn.ensemble import GradientBoostingClassifier

clf = GradientBoostingClassifier()

xmatrix.ml_configure_params(clf)


def data_func():
    row_data_list = []
    with open("data.csv", 'r') as fp:
        for line in fp:
            if not line:
                break
            t = dict(zip(["label", "id", "features"], line.split(",")))
            t["label"] = int(t["label"])
            t["features"] = list_to_vect(
                [int(i) for i in t["features"].split(":")])
            row_data_list.append(dict_to_row(t))
    return row_data_list


def train_func(X, y, label_size):
    clf.fit(X, y)


xmatrix.ml_batch_data(train_func, data_func)

X_test, y_test = xmatrix.ml_validate_data()
if len(X_test) > 0:
    testset_score = clf.score(X_test, y_test)
    print("validation_score:%f" % testset_score)

xmatrix.ml_save_model(clf)