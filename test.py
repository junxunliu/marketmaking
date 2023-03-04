# import ML libraries
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix

# Due to above analysis, I selected my first trail
trail_1 = ['loan_amnt', 'bc_open_to_buy', 'grade', 'pub_rec', 'open_acc', 'hardship_flag', 'num_rev_accts',
           'revol_bal', 'last_fico_range_high', 'last_fico_range_low', 'fico_range_low']
from itertools import permutations

perms = permutations(trail_1)
max_sens, max_spec = 0, 0
best_perm = []
for perm in perms:
    perm = list(perm)
    perm.append('loan_status')
    df_trail_1 = df_improved[perm]
    l = len(df_trail_1.columns.values)
    x, y = df_trail_1.iloc[:, 0:l - 1], df_trail_1.iloc[:, l - 1]
    # We will do a 70/30 split
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=42, stratify=y, shuffle=True)
    # Create Decision Tree classifer object
    clf = DecisionTreeClassifier(criterion='entropy', random_state=42)
    # Train Decision Tree Classifer
    clf = clf.fit(x_train, y_train)
    # Predict the response for test dataset
    y_pred = clf.predict(x_test)
    cm = confusion_matrix(y_test, y_pred)

    tp = float(cm[0][0])
    tn = float(cm[1][1])
    fp = float(cm[1][0])
    fn = float(cm[0][1])

    # Sensitivity: the ability of a test to correctly identify loans that will be bad.
    sensitivity = tp / (tp + fn)
    # Specificity: the ability of a test to correctly identify loans that will be good(Without bad).
    specificity = tn / (tn + fp)

    if sensitivity > max_sens:
        best_perm = perm
        max_sens = sensitivity
    max_spec = max(max_spec, specificity)
print(max_sens)
print(max_spec)