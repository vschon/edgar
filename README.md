Instructions
-----------

###1.3rd party Dependency
numpy
pandas
sklearn

###2.Modules
There are 4 modules in this project: form10DB, DataGenerator, forecaster, and utils

2.1 form10DB
form10DB is used to download and manage the 10 forms. It includes functions to
download 10 forms for all symbols from 1993 - present. Form10Manager class is the 
workhorse to download data and populate them into a structured database.

2.2 DataGenerator
DataGenerator module contains functions to generate structured dataset.

2.3 forecaster
forecaster module contains classes and functions to train classifiers and make
predictions.

2.4 utils
utils contain usefule helper functions to support other modules.

###3.How to use
In this demo project, the database and dataset have already been generated. So
there is no need to use the module form10DB and DataGenerator. These two modulles
are only needed when you want to repopulat the edgar database on your own machine.

The only module used in the demo project is forecaster. For demo, please change
the working directory to edgar directory:

>>>import forecaster
learner = forecaster.Forecaster()
#####read in training data for learner
learner.readTrainFile('Dictionary.3label.Industrials','3label.Industrials.train.key')
#####read in development data
learner.readDevFile('3label.Industrials.dev.key')
#####read in testing data
learner.readTestFile('3label.Industrials.test.key')

#####Training classifier
learner.classifier.fit(learner.trainFeature,learner.trainLabel)

#####Testing on development set
learner.classifier.score(learner.devFeature,learner.devLabel)

#####Get the accuracy on testing set
learner.classifier.score(learner.testFeature,learner.testLabel)




