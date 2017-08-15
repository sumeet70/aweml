from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from pre_processors import tasks
from pre_processors import segmentation
from pre_processors import ncz

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', dest='input',
                      default='gs://aweml/metadata/stage1_labels.txt',
                      help='Input file to process.')
  parser.add_argument('--output', dest='output',
                      default='gs://aweml/output',
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
	  '--runner=DirectRunner',
      #'--runner=DataflowRunner',
      '--project=mw-ml-comp-sumeet-singh',
      '--staging_location=gs://aweml/stage/',
      '--temp_location=gs://aweml/tmp/',
      '--job_name=lc-preprocess-job',
      '--setup_file=./setup.py',
      '--extra_package=./deps/opencv_python-3.2.0.8-cp27-cp27m-manylinux1_x86_64.whl'
  ])

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  logging.info(pipeline_args);  
    
  with beam.Pipeline(options=pipeline_options) as p:

    lines_from_input_file = p | ReadFromText(known_args.input)
    data = (lines_from_input_file 
        | 'Extract labels' >> (beam.ParDo(tasks.extract_labels()))
        | 'Load Scans' >> (beam.ParDo(tasks.load_scan()))
        | 'Convert to Hounsefield Units' >> (beam.ParDo(tasks.get_pixels_hu()))
        | 'Isotropic Resampling' >> (beam.ParDo(tasks.resample()))
        | 'Segment Lung Tissue' >> (beam.ParDo(segmentation.segment_lung_tissue()))
        | 'Norm Conform Zero Center' >> (beam.ParDo(ncz.normalize_conform_zero_center()))
        | 'Save Data' >> (beam.ParDo(tasks.save_data()))
    )
    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()