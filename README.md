***WORK IN PROGRESS***

# Awesome Machine Learning Pipeline
End to end machine learning pipeline running on Google Cloud Platform

## Data Processor
the folder named data_processing, contains a Cloud Dataflow enabled data pre processing pipeline.

### Setup storage bucket:
Create a root storage bucket in your account and then add following folders to it:

Assuming you create a bucket called aweml, it should look like:

1. gs://aweml/metadata/stage1_labels.txt
2. gs://aweml/scans/
3. gs://aweml/output
4. gs://aweml/stage
5. gs://aweml/tmp

The scans folder should contain dicom scan folders

### Local Setup
You need 
1. Python 2.7
2. pip
3. virtualenv

Assuming you have required GCP python dependencies, all other require dependencies can be intalled from the requirements file.
run pip install requirements.txt

### What is in the setup.py and deps folder ?
When you run your workload on cloud data flow, it is spinning up hundreds of environments on your behalf, it need to know what kind environments are those and what dependencies to download.
