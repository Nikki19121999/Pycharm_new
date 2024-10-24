from pyspark import SparkContext

def square(x):
    return x ** 2

sc = SparkContext("local", "RDD Example")


numbers = [1, 2, 3, 4, 5]
rdd = sc.parallelize(numbers)

squares = rdd.map(square)

print("Squares:", squares.collect())

sc.stop()
