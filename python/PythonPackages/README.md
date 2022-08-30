# Packaging Instructrions for PyFlink on Kinesis Data Analytics

## Pip-installed dependencies

First, let's look at how to include pip-installed dependencies in your application package. The following "diagram" illustrates what your file structure needs to look like: 

```
PythonPackages
│   README.md
│   python-packages.py    
│
└───boto3
│   │   session.py
│   │   utils.py
│   │   ...
│   
└───botocore
│   │   args.py
│   │   auth.py
│       ...
└───lib
│    │ flink-sql-connector-kinesis_2.11-1.13.2.jar 
│    │ ...
...

```

At a high level, you have to copy the files and folders that are located within your python environments `site-packages` folder as shown in the depiction above. Btw, we recommend [miniconda](https://docs.conda.io/en/latest/miniconda.html) for environment management.

Now, you may be wondering how to ensure that you don't include unnecessary packages while also ensuring that you don't leave out any transitive dependencies. For instance, if you take a dependency on boto3, it in turn references botocore; in other words, botocore is a transitive dependency of boto3. To take this into account, we recommend the following approach:

1. Create a standalone Python environment (conda or similar) on your local machine.
2. Take note of the initial list of packages in that environment's site_packages. You'll see packages like `wheel`, which you don't want to include.
3. Now pip-install all dependencies that your app needs. In our case, it's just `boto3`.
4. Note the packages that were *added* to the site_packages folder after step 3 above. These are the folders you need to include in your package, organized as shown above. You're essentially capturing a "diff" of the packages between steps 2 and 3 above to identify the right package dependencies for your application.

NOTE: The location of `site_packages` for a conda env is `miniforge/envs/<your_env>/lib/python3.8/site_packages`. Replace `<your_env>` with the name of your conda env.

## Ensure your used connector is included

Copy the version of the connector you are using (e.g. `flink-sql-connector-kinesis_2.11-1.13.2.jar`) in the lib directory. 
Make sure the connector version corresponds to the used Apache Flink version in Kinesis Data Analytics.

## Package the application

For packaging your application **DON'T** use the compress tool in Finder or Windows Explorer. 
This will create an invalid code package for KDA. On Linux / macOS you can use the following command:
```shell
zip kdaApp.zip * lib/*
```

## Set application properties in KDA

Add the application properties to the KDA application as defined in `application_properties.json`. 
There is no need to define a `pyFiles` property,
as KDA will unzip the application code and the additional packages will be available in the base path.