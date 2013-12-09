for w in wlist:
    for a in alist:
        test3.adjustSUEweight(w)
        nb = MultinomialNB(alpha =a)
        nb.fit(test3.trainFeature,test3.trainLabel)
        result = nb.score(test3.devFeature,test3.devLabel)
        with open('nb3.summary','a') as f:
            f.write(str(w) + ',' + str(a) + ',' + str(result) + '\n')
        test3.adjustSUEweight(1/w)
