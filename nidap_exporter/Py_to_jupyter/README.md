### Instructions

(NOTE: I have had trouble with git on the vs code terminal on HPC on demand. I have done all of my git operations in a regular terminal ssh session)

1. ssh to biowulf.nih.gov

2. Clone this repo and check out the python_support branch

3. navigate to the python workbook on Foundry and click the settings cog -> Export git repository
    3.a on biowulf, navigate to the desired destination and paste the command given in this step into your terminal

4.  navigate to nidap-export/nidap_exporter/Py_to_jupyter and run the command `python3 transform.py {FULL PATH TO DOWNLOADED REPOSITORY}`
    4.a NOTE that you may need to install graphlib, the rest of the code is with vanilla python.
    4.b NOTE/TODO currently, we look at the workbook and manually identify what nodes are the root nodes. We can automatically extract  manually inputted data (such as `get_manual_phenotypes`),but for dataset entry points (such as `mcmicro_output_annotation`) we need to specify the name and rid

5. go to https://hpcondemand.nih.gov/ and start a vscode session. NOTE the default resources are not adequate , I select 4 cores and 16 GB memory. So far we are only using CPU nodes. 
    5.a. NOTE: you will need to install the jupter extension if this is your first time running this.

6. In Vscode on HPC On Demand, navigate to the downloaded repository and open `pipeline.ipynb` (created in step 4).
    6.a. for mcmicro_output_annotation, replace `/path/to/your/data` with the path to your downloaded dataset. 
    6.b. if the subsequent functions include file names(such as `get_user_specification`) you must replace them with the complete path to that resource

7. in the notebook, execute the cells IN ORDER. it is imparative that you run the first cell with the `sys.path.insert` command, this will ensure you get the latest SPAC libraries. 
    7.a these cells will produce many output files, necessary for the pipeline's execution, in the data subdirectory of the repo directory. 
