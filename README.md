# Lifetime-Value

Lifetime customer value value code. This is written in scala and uses MLLIB and meant to run on Spark.

It is attached without a data source because this is prototype to actual client work that later went into production

This project was a collaboration between me and Raduu Stojan.



There are 2 files:

1. the model pipeline - trains the model and save it to an S3 apth


2. The scoring pipeline - scores the dataset using the model generated in file 1



** quick note: the model pipeline is built out to output 1 model where user "days since app install" is a feature. But, the scoring pipeline is built out for a slightly different variation of model pipeline, where user "days since app install" is not a feature, but we rather build a model per each value of "days since app install." 
