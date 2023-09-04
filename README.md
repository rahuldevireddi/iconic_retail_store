Recommended way to run this via Python venv or Conda venv
If you don't mind installing the packages, then you can directly execute 'run.sh' script
Also we can run the individual modules
For example, first run bronze/bronze_module.py followed by silver/silver_module.py followed by gold/gold_module.py
On Airflow we can generate DAG of jobs and with dependency on each other in the above order

step 1)
  Extract the solution zip file into some path
  
    unzip iconic_retail_store.zip
    cd ~/iconic_coding_challenge
    
  Create python virtual environment
  
    python3 -m venv iconic


step 2)
  Activate venv
    -- On Linux/Mac
    
    source iconic/bin/activate


step 3)
  Make sure you have pip3 installed in your system and run the necessary packages
    
    pip3 install .   (This will install all the necessary packages)
    
    OR
    
    python3 setup.py sdist bdist_wheel

    python3 install dist/iconicretail-1.0.tar.gz


step 4)
  Execute it via spark-submit command, adjust the input_path, output_path accordingly
  In the production environment the input is either loaded from HDFS or S3 or Azure Data Lake/Blob storage so as output location
  Also on the cluster, we need to pass master node details and tune driver memory, executor memory and set number of cores ... etc

  ```bash
  spark-submit --py-files dist/iconicretail-1.0.tar.gz main.py --input_path "iconicretail/bronze/data/raw" --output_path --output_path "iconicretail/gold/"
  ```
