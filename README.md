# Spark Streaming Mllib/Stream Example

Purpose of this project to do the entire pipeline of loading/modeling data and applying it to streaming data and storing it back into a database system.

### Main Entry point is the Jupyter Notebook -> spark-nytimes-mllib-stream.ipynb

## Notes:
* Spark 2.0 allows pyspark to save mllib Pipelines, you can currently do this with Spark 1.6 with scala.
* spark-cassandra-connecter is not avaliable yet for Spark 2.0
* To shutdown the StreamingContext you need to either send a shutdown hook signal via SIGTERM or you can easily do by just restarting your notebook.
* Purpose of creating features.py is so that if you were to try and create a production process you can just import this file in your model building script or your spark streaming script and apply the transformations.
