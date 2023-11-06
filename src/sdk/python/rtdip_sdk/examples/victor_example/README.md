# Spark Single Node Notebook AWS

This article provides a guide on how to create a conda based self-contained environment to run LFEnergy RTDIP that integrates the following components:
* Java JDK and Apache Spark (Single node configuration). Currently, v3.4.1 Spark (PySpark) has been configured and tested.
* AWS Libraries (e.g for accessing files in AWS S3 if required).
* Jupyter Notebook server.

The components of this environment are all pinned to a specific source distribution of RTDIP and have been tested in x86 Windows (using gitbash) and Linux environments.

## Prerequisites
* Docker desktop or another local Docker environment (e.g. Ubuntu Docker).
* gitbash environment for Windows environments.

When the installation completes, a Jupyter notebook will be running locally on port 8080. 
Please check that this port is available or change the configuration in the installer if required.

# Deploy
Run *run_in_docker.sh*. After the installer completes:
* At http://localhost:8080/ a jupyter notebook server will be running. Notebooks can be created to run for example new RTDIP pipelines.
* To test the environment, create a new notebook and copy the contents of MISO_pipeline_sample.py and run the notebook. This pipeline queries MISO (Midcontinent Independent System Operator) and saves the results of the query locally under a newly created directory called spark-warehouse.
* For debugging purposes and running from inside the container other RTDIP pipeplines, a new file *conda_environment_lfenergy.sh* is created. Please use this file  to activate the  conda environment (e.g. *source ./conda_environment_lfenergy.sh*) within the container.

