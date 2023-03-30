1. Export the API KEY
```
export LANCIUM_API_KEY=<YOUR SECRET ACCOUNT KEY>
```

2. Add an image from docker hub 
```
lcli image add --name "centos7-singularity" --description "ADC Centos 7 image + singularity configuration" --type docker_image --url docker://fbarreir/adc-centos7-singularity:latest test/centos7-singularity
```

2. Upload the `pilots_starter.py` (distributed with this package [here](https://github.com/HSF/harvester/blob/lancium/pandaharvester/harvestercloud/pilots_starter.py)) and the `voms proxy`. The local paths are set in `contants.py`.
```
python file_contants.py
```

3. Submit a job. The job will mount the files from step 2 into '/jobDir'   
```
python submit.py
```
This will return the job id.

3. Get the status of the job. You can either list all jobs   
```
python monitor.py
```
or for a particular job
```
python monitor.py <job ID>
```