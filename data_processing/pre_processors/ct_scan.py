class CtScan:

    def __init__(self,patient_id = None, has_cancer = None):
        
        self.patient_id = patient_id
        self.has_cancer = has_cancer
        self.slices = None
        self.clices_as_hu = None
        self.segmented_lungs_core = None
        self.ncz_data = None
        self.input_folder = None