import apache_beam as beam
from skimage import measure
import numpy as np 

class segment_lung_tissue(beam.DoFn):
    
    #-----------------------------------------------------------------------------------------------------------
    # largest_label_volume(im, bg=-1): Takes an image in the form returned by measure.label() and returns the 
    #                                  the label with the largest volume in this slice.  
    #
    # parameters:      im - pixel list after labeling by measure.label()
    #                  bg - the pixel value for the background to be ignored in the counts
    #
    # returns:         the value of the label with the largest volume in the slice
    #
    #-----------------------------------------------------------------------------------------------------------
    def largest_label_volume(self, im, bg=-1):
        
        vals, counts = np.unique(im, return_counts=True)
    
        counts = counts[vals != bg]
        vals = vals[vals != bg]
    
        return vals[np.argmax(counts)]
    
    def segment_lung_mask(self, image, fill_lung_structures=True):
        # not actually binary, but 1 and 2. 
        # 0 is treated as background, which we do not want
        # if the pixel in hu is greater than -320 its true so 1
        # if not its false so 0 then add one so [-1024, -320, 1000]
        # comes bac as [1,1,2]
        binary_image = np.array(image > -320, dtype=np.int8)+1
      
        # assigns connected regions the same value so inside the lungs
        # should all have the same value
        labels = measure.label(binary_image)
    
        # Pick the pixel in the very corner to determine which label is air.
        #   Improvement: Pick multiple background labels from around the patient
        #   More resistant to "trays" on which the patient lays cutting the air 
        #   around the person in half
        background_label = labels[0,0,0]
    
        #Fill the air around the person
        binary_image[background_label == labels] = 2
    
    
        # Method of filling the lung structures 
        if fill_lung_structures:
        # For every slice we determine the largest solid structure
          for i, axial_slice in enumerate(binary_image):
            axial_slice = axial_slice - 1
            labeling = measure.label(axial_slice)
            l_max = self.largest_label_volume(labeling, bg=0)
            
            if l_max is not None: #This slice contains some lung
                binary_image[i][labeling != l_max] = 1
    
        binary_image -= 1 #Make the image actual binary
        binary_image = 1 - binary_image # Invert it, lungs are now 1
    
        # Remove other air pockets insided body
        labels = measure.label(binary_image, background=0)
        l_max = self.largest_label_volume(labels, bg=0)
        if l_max is not None: # There are air pockets
            binary_image[labels != l_max] = 0
    
        return binary_image    
    
    def process(self, ctscan):
        ct_scan_segmented_lungs_filled = self.segment_lung_mask(ctscan.slices_as_hu, True)
        ct_scan_segmented_lungs = self.segment_lung_mask(ctscan.slices_as_hu, False)
        ct_scan_segmented_lungs_core = ct_scan_segmented_lungs_filled - ct_scan_segmented_lungs
        ctscan.slices_as_hu = None
        ctscan.segmented_lungs_core = ct_scan_segmented_lungs_core
        yield ctscan
        