from pyspark import SparkContext

# Creating SparkContext
sc = SparkContext("local", "pipe_transformation")

# Creating RDD
rdd = sc.parallelize(range(10))

# Define a simple shell script
script = """
#!/bin/bash
while read LINE; do
    echo "Processing: $LINE"
done
"""

# Save the script to a file
with open("script.sh", "w") as f:
    f.write(script)

# Make the script executable (using octal representation for file permissions)
import os
os.chmod("script.sh", 0o777)  # Ensure script is executable

# Specify the full path to the script file
script_path = os.path.abspath("script.sh")

# Applying pipe transformation to run the script on each partition of RDD
result_rdd = rdd.pipe(script_path)

# Collecting results to driver
result = result_rdd.collect()

# Print the result
print(result)
