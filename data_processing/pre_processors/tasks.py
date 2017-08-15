import apache_beam as beam
import os
import dicom
import numpy as np 
import scipy.ndimage
import logging
from google.cloud import storage
import ct_scan as ct

#INPUT_FOLDER = 'data/stage1_sample/'
INPUT_FOLDER = 'gs://aweml/scan/stage-a'
       
class extract_labels(beam.DoFn):

    def process(self,line_from_input_file):
        patient_id, has_cancer = line_from_input_file.split(',',1)
        scan = ct.CtScan(patient_id, has_cancer)
        yield scan


class load_scan(beam.DoFn):
    def process(self, ctscan):  

        patient_images_folder = INPUT_FOLDER + ctscan.patient_id
        
        if os.path.isdir(patient_images_folder) :
            slices = [dicom.read_file(patient_images_folder + '/' + s) for s in os.listdir(patient_images_folder)]
            slices.sort(key = lambda x: float(x.ImagePositionPatient[2]))
            try:
                slice_thickness = np.abs(slices[0].ImagePositionPatient[2] - slices[1].ImagePositionPatient[2])
            except:
                slice_thickness = np.abs(slices[0].SliceLocation - slices[1].SliceLocation)
            
            for s in slices:
                s.SliceThickness = slice_thickness

            ctscan.slices = slices;
            yield ctscan
            
class get_pixels_hu(beam.DoFn):    
    def process(self,ctscan):
        # Take each slice and stack into a np array
        image = np.stack([s.pixel_array for s in ctscan.slices])
    
        # Can fit everything in < 32K so make it np.int16  
        image = image.astype(np.int16)
     
        # Set outside-of-scan pixels to 0
        # The intercept is usually -1024, so air is approximately 0
        outside_image = image.min()
        image[image == outside_image] = 0
    
        # Convert to Hounsfield units (HU)
        for slice_number in range(len(ctscan.slices)):
        
            # the Dicom record gives you the slope and intercept to convert to HU
            intercept = ctscan.slices[slice_number].RescaleIntercept
            slope = ctscan.slices[slice_number].RescaleSlope
    
            # if the slope is something other than one, then multiply the image data
            # by the slope and add the intercept (convert to float for multiplication)
            if slope != 1:
                image[slice_number] = slope * image[slice_number].astype(np.float64)
                image[slice_number] = image[slice_number].astype(np.int16)
    
            # convert the image data back to int16
            image[slice_number] += np.int16(intercept)
        
        ctscan.slices_as_hu = np.array(image, dtype=np.int16)
        yield ctscan
        
        
class resample(beam.DoFn):
    def process(self, ctscan):
        
        new_spacing = [1,1,1]
        
        # Determine current pixel spacing.  Since all the slices are from  the same
        # scan we assume they all have the same thickness and pixel spacing
        spacing = np.array([ctscan.slices[0].SliceThickness] + ctscan.slices[0].PixelSpacing, dtype=np.float32)
    
        # calculate the resizing factors to transform to an isotropic resolution of [1,1,1]
        resize_factor = spacing / new_spacing
        new_real_shape = ctscan.slices_as_hu.shape * resize_factor
        new_shape = np.round(new_real_shape)
        real_resize_factor = new_shape / ctscan.slices_as_hu.shape
        new_spacing = spacing / real_resize_factor
        
        #debug only -----------------------------------------
        #print ('Spacing            ->', spacing)
        #print ('New Spacing        ->', new_spacing)
        #print ('Resize Factor      ->', resize_factor)
        #print ('Image Shape        ->', image.shape)
        #print ('New Real Shape     ->', new_real_shape)
        #print ('New Shape          ->', new_shape)
        #print ('Real Resize Factor ->', real_resize_factor)
        #print ('New Spacing        ->', new_spacing)
        #debug only -----------------------------------------
    
        # recast the image
        ctscan.slices_as_hu = scipy.ndimage.interpolation.zoom(ctscan.slices_as_hu, real_resize_factor, mode='nearest')
        ctscan.slices = None
        
        yield ctscan
    
    

class save_data(beam.DoFn):
    def process(self, ctscan):  
               
        client = storage.Client()
        bucket = client.get_bucket('aweml')
        blob = bucket.blob('output/' + ctscan.patient_id)
        
        if ctscan.has_cancer == "1": one_hot=np.array([0,1])
        elif ctscan.has_cancer == "0": one_hot=np.array([1,0])

        one_hot_encoded = np.append(ctscan.ncz_data, one_hot)        
        blob.upload_from_string(one_hot_encoded.tobytes());
        logging.info("Processed output saved for scan id: " + ctscan.patient_id)
        