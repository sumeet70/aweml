import apache_beam as beam
import math
import cv2
import numpy as np 

IMAGE_PX_SIZE = 64
HM_SLICES = 64

class normalize_conform_zero_center(beam.DoFn):
    def process(self, ctscan):
        image = self.normalize(ctscan.segmented_lungs_core)
        image = self.conform_img(image, img_px_size=IMAGE_PX_SIZE, hm_slices=HM_SLICES)
        image = self.zero_center(image)
        
        ctscan.segmented_lungs_core = None
        ctscan.ncz_data = image
        yield ctscan
    
    #-----------------------------------------------------------------------------------------------------------
    # normalize(image): Take segmented lung image and normalize pixel values to be between 0 and 1
    #
    # parameters:      image - a segmented lung image like that returned from segment_lung_mask(...)
    #
    # returns:         a normalized image
    #
    #-----------------------------------------------------------------------------------------------------------
    def normalize(self,image):
        
        MIN_BOUND = -1000.0
        MAX_BOUND = 400.0
    
        image = (image - MIN_BOUND) / (MAX_BOUND - MIN_BOUND)
        image[image>1] = 1.
        image[image<0] = 0.
        return image

    #-----------------------------------------------------------------------------------------------------------
    # chunks( l, n, hm_slices ): Yields a group of slices as a chunk to be collapsed.
    #
    # parameters:      l - img array
    #                  n - number of slices to be averaged into  hm_slices
    #                  hm_slices - number of slices to reduce to
    #
    # returns:         Yields a group of slices as a chunk to be collapsed.
    #
    #-----------------------------------------------------------------------------------------------------------
    def chunks(self, l, n ):
        count=0
        for i in range(0, len(l), n):
            if(count < HM_SLICES):
                yield l[i:i + n]
                count += 1
    
    #----------------------------------------
    # mean(l): Gives the mean of a list
    #----------------------------------------
    def mean(self,l):
        return sum(l) / len(l)
    
    #-----------------------------------------------------------------------------------------------------------
    # conform_img(img, img_px_size, hm_slices): Conforms an isotropic hu based image to a standard
    #                                                         size irrespective of number of slices.
    #
    # parameters:      img - A dicom CT scan stacked, converted to hu, normalized
    #                  img_px_size - the x and y size of an individual slice
    #                  hm_slices - number of slices to reduce to
    #
    # returns:         an array with the conformed image
    #
    #-----------------------------------------------------------------------------------------------------------
    def conform_img(self, img, img_px_size, hm_slices):
        
        resized_CTs = []
        new_slices = []
        
        for slice in img:
            one_slice_resized = cv2.resize(slice, (img_px_size, img_px_size))
            resized_CTs.append(one_slice_resized)
    
        chunk_sizes = int(math.floor(len(resized_CTs) / HM_SLICES))
        
        for slice_chunk in self.chunks(resized_CTs, chunk_sizes):
            slice_chunk = list(map(self.mean, zip(*slice_chunk)))
            new_slices.append(slice_chunk)
    
        conformed_img = np.array(new_slices)
    
        return conformed_img
    
    #-----------------------------------------------------------------------------------------------------------
    # zero_center(image): Shift normalized image data and move the range so it is 0 centered at the PIXEL_MEAN
    #
    # parameters:      image - a segmented lung image like that returned from segment_lung_mask(...)
    #
    # returns:         a zero centered image
    #
    #-----------------------------------------------------------------------------------------------------------
    def zero_center(self, image):
      
        PIXEL_MEAN = 0.25
        
        image = image - PIXEL_MEAN
        return image
        