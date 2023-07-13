import argparse
import os
import shutil
import subprocess
from article_dataset_builder.S3 import S3
import json
import time
from article_dataset_builder.harvest import generateStoragePath

class Nlm2tei(object):
    """
    Convert existing NLM/JATS files (PMC) in a data repository into TEI XML format similar as Grobid output.
    This is using Pub2TEI (https://github.com/kermitt2/Pub2TEI) and it is done in batch to have good runtime. 

    Note: this requires a JRE 8 or more
    """
    def __init__(self, config_path='./config.json'):
        self.config = None   
        self._load_config(config_path)

        self.s3 = None
        if self.config["bucket_name"] is not None and len(self.config["bucket_name"]) is not 0:
            self.s3 = S3.S3(self.config)

    def _load_config(self, path='./config.json'):
        """
        Load the json configuration 
        """
        config_json = open(path).read()
        self.config = json.loads(config_json)

        # check Pub2TEI directory indicated on the config file
        if not os.path.isdir(self.config["pub2tei_path"]):
            print("Error: path to Pub2TEI is not valid, please clone https://github.com/kermitt2/Pub2TEI", 
                  "and indicate the path to the cloned directory in the config file)")

    def _create_batch_input(self):
        """
        Walk through the data directory, grab all the .nxml files and put them in a single temporary working directory
        """
        temp_dir = os.path.join(self.config["data_path"], "pub2tei_tmp")
        # remove tmp dir if already exists
        if os.path.isdir(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))

        # create the tmp dir
        try:  
            os.makedirs(temp_dir)
        except OSError:  
            print ("Creation of the directory %s failed" % temp_dir)
        else:  
            print ("Successfully created the directory %s" % temp_dir)

        # walk through the data directory, copy .nxml files to the temp directory
        for root, dirs, files in os.walk(self.config["data_path"]):
            for the_file in files:
                # normally all NLM/JATS files are stored with extension .nxml, but for safety we also cover .nlm extension
                if the_file.endswith(".nxml") or the_file.endswith(".nlm"):
                    #print(root, the_file)
                    if not os.path.isfile(os.path.join(temp_dir,the_file)):
                        shutil.copy(os.path.join(root,the_file), temp_dir)

        # add dummy DTD files for JATS to avoid errors and crazy online DTD download
        open(os.path.join(temp_dir,"JATS-archivearticle1.dtd"), 'a').close()
        open(os.path.join(temp_dir,"JATS-archivearticle1-mathml3.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle1-mathml3.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle1.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle3.dtd"), 'a').close()
        open(os.path.join(temp_dir,"journalpublishing.dtd"), 'a').close()
        open(os.path.join(temp_dir,"archivearticle.dtd"), 'a').close()
        return temp_dir

    def process_batch(self, dir_path):
        """
        Apply Pub2TEI to all the files of indicated directory
        """
        temp_dir_out = os.path.join(dir_path, "out")
        try:  
            os.makedirs(temp_dir_out)
        except OSError:  
            print ("Creation of the directory %s failed" % temp_dir_out)

        cmd = "java -jar " + os.path.join(self.config["pub2tei_path"],"Samples","saxon9he.jar") + " -s:" + dir_path + \
            " -xsl:" + os.path.join(self.config["pub2tei_path"],"Stylesheets","Publishers.xsl") + \
            " -o:" + temp_dir_out + " -dtd:off -a:off -expand:off " + \
            " --parserFeature?uri=http%3A//apache.org/xml/features/nonvalidating/load-external-dtd:false -t" 
        #print(cmd)
        try:
            result = subprocess.check_call(cmd, shell=True)
        except subprocess.CalledProcessError as e:   
            print("e.returncode", e.returncode)
            print("e.output", e.output)
            #if e.output is not None and e.output.startswith('error: {'):
            if  e.output is not None:
                error = json.loads(e.output[7:]) # Skip "error: "
                print("error code:", error['code'])
                print("error message:", error['message'])
                result = error['message']
            else:
                result = e.returncode
        return str(result)

    def _manage_batch_results(self, temp_dir):
        """
        Copy results from the temporary working directory to the data directory, clean temp stuff
        """
        if not os.path.isdir(temp_dir):
            print("provided directory is not valid:", temp_dir)
            return

        temp_dir_out = os.path.join(temp_dir, "out")
        if not os.path.isdir(temp_dir_out):
            print("result temp dir is not valid:", temp_dir_out)
            return

        for f in os.listdir(temp_dir_out):
            if f.endswith(".nxml.xml") or f.endswith(".nxml") or f.endswith(".nlm"):
                # move the file back to its storage location (which can be S3)
                identifier = f.split(".")[0]
                if self.s3 is not None:
                    # upload results on S3 bucket
                    self.s3.upload_file_to_s3(identifier+".pub2tei.tei.xml", generateStoragePath(identifier), storage_class='ONEZONE_IA')
                else:   
                    dest_path = os.path.join(self.config["data_path"], generateStoragePath(identifier), identifier+".pub2tei.tei.xml")
                    shutil.copyfile(os.path.join(temp_dir_out,f), dest_path)
        
        # clean temp dir
        try:
            shutil.rmtree(temp_dir)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
        

    def process(self):
        """
        Launch the conversion process
        """
        start_time = time.time()
        temp_dir = self._create_batch_input()    
        self.process_batch(temp_dir)
        self._manage_batch_results(temp_dir)  
        # TBD: consolidate raw reference string present in the converted TEI
        runtime = round(time.time() - start_time, 3)
        print("\nruntime: %s seconds " % (runtime))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "COVIDataset harvester")
    parser.add_argument("--config", default="./config.json", help="path to the config file, default is ./config.json") 
    args = parser.parse_args()
    config_path = args.config
    
    nlm2tei = Nlm2tei(config_path=config_path)
    nlm2tei.process()
