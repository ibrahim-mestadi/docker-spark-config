# TP1 - ML Spark
# Exercice 1: Decision Tree Classifier

from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml import Pipeline


# Create a Spark session
spark = SparkSession.builder.appName("DecisionTreeExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load the dataset
dataset_path = "/opt/spark/data/sample_libsvm_data.txt"
data = spark.read.format("libsvm").load(dataset_path)

# Split the dataset into training and test sets
(trainingData, testData) = data.randomSplit([0.7, 0.3])
print('trainingData', trainingData)
print('testData', testData)


# Create the Decision Tree model with given parameters
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", impurity="gini", maxDepth=5, maxBins=32)

# Train the model
model = dt.fit(trainingData)

# Make predictions
predictions = model.transform(testData)

# Show the predictions
predictions.select("prediction", "label", "features").show()

# Stop the Spark session
spark.stop()