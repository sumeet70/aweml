#WORK IN PROGRESS

# Awesome Machine Learning Pipeline
End to end machine learning pipeline running on Google Cloud Platform

##Data Processor
Contains a Cloud Dataflow enabled data pre processing pipeline.

###Setup storage bucket:
Create a root storage bucket in your account and then add following folders to it:

Assuming you create a bucket called aweml, it should look like:

gs://aweml/metadata/stage1_labels.txt
gs://aweml/scans/
gs://aweml/output
gs://aweml/stage
gs://aweml/tmp

The scans folder should contain dicom scan folders

###Local Setup
You need 
Python 2.7
pip
virtualenv

Assuming you have required GCP python dependencies, all other require dependencies can be intalled from the requirements file.
run pip install requirements.txt

###What is in the setup.py and deps folder ?
When you run your workload on cloud data flow, it is spinning up hundreds of environments on your behalf, it need to know what kind environments are those and what dependencies to download.