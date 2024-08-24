# Building the pipeline

Build with Ant using the target 'jars'. Jar file will be placed in folder 'jars'.

# Executing the pipeline

java -jar anonymize-commute-health-v{version}.jar -i {input.csv} -o {output.csv}

# Example

java -jar jars/anonymize-commute-health-v0.1.jar -i data/CommData.csv -o data/output.csv