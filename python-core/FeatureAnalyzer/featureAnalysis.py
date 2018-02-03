
# author: Michele Samorani
import pandas as pd
import seaborn as sns
from sklearn import linear_model as lm
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import AdaBoostClassifier
import os


if __name__ == "__main__":

    df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../../output/features/Feature Matrix.csv'),index_col=0)
    dict = pd.read_csv(os.path.join(os.path.dirname(__file__), '../../output/features/Feature Dictionary.csv'),index_col=0,sep='\t'    )
    depVarName = df.columns[-1]
    depVarName = df.columns[-1]
    df2 = df.fillna(df.mean())
    df2 = 100 * (df2 - df2.min()) / (df2.max() - df2.min())
    df2[depVarName] = df[depVarName]

    X = df2.drop([depVarName],axis=1)
    Y = df2[depVarName]
    
    lr = lm.Lasso()
    lr.fit(X,Y)
    lr.coef_[0]
    d={X.columns[i] : lr.coef_[i] for i in range(0,len(X.columns)) }
    s=pd.Series(d)
    attr = s.abs().sort_values().head(4).index.tolist()
    dfViz = df.copy()
    
    i = 1
    for attName in attr:
        if df[attName].nunique() > 15:
            dfViz[attName] = pd.cut(x=df[attName],bins=5)
        chart = sns.factorplot(x=attName,y=depVarName,data=dfViz,kind = 'bar', aspect=3)
        chart.set(xlabel=attName, ylabel='Probability of ' + depVarName)
        chart.savefig(os.path.join(os.path.dirname(__file__), "../../output/Report/report-source/IMG"+ str(i)  + ".png"),dpi=500)
        f = open(os.path.join(os.path.dirname(__file__),'../../output/Report/report-source/DESCR'+str(i)+'.txt'),'w')
        f.write(dict.loc[attName,'Attribute_Descr'])
        f.close()

        i+=1
    
    if df2[depVarName].nunique() > 2:
        regression = True
    else:
        regression = False

    if not regression:
        nfolds = 10
        kf = KFold(n_splits=nfolds,random_state=2,shuffle=True)
        cl = AdaBoostClassifier()
        auc = cross_val_score(cl,X,y=Y,cv=kf,scoring='roc_auc').mean()
        f = open(os.path.join(os.path.dirname(__file__), '../../output/Report/report-source/text_at_bottom.txt'),'w')
        f.write('Area under the curve obtained by AdaBoost in a 10-fold cross validation: ' +str(round(auc,4)))
        f.close()

    if regression:
        from sklearn.model_selection import train_test_split
        import sklearn.metrics
        X_train, X_test, y_train, y_test = train_test_split(X,Y,test_size=0.3,random_state=2)
        from sklearn import linear_model
        regLasso = linear_model.LinearRegression()
        regLasso.fit(X_train,y_train)
        y_pred = regLasso.predict(X_test)
        print((y_pred - y_test).abs().mean())