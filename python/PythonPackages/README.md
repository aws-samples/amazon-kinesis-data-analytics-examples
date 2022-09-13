# Packaging Instructions for PyFlink on Kinesis Data Analytics

## Including Python dependencies

Let's look at how to include dependencies in your application package. The following "diagram" illustrates what your file structure should look like: 

```
PythonPackages
│   README.md
│   python-packages.py    
│
└───my_deps
       └───boto3
       │   │   session.py
       │   │   utils.py
       │   │   ...
       │   
       └───botocore
       │   │   args.py
       │   │   auth.py
       │    ...
       └───mynonpypimodule
       │   │   mymodulefile1.py
       │   │   mymodulefile2.py
            ...
└───lib
│    │ flink-sql-connector-kinesis-1.15.2.jar 
│    │ ...
...

```

At a high level, *for PyPi dependencies*, you have to copy the files and folders that are located within your python environments `site-packages` folder as shown in the depiction above. Btw, we recommend [miniconda](https://docs.conda.io/en/latest/miniconda.html) for environment management.

Now, you may be wondering how to ensure that you don't include unnecessary packages while also ensuring that you don't leave out any transitive dependencies. For instance, if you take a dependency on boto3, it in turn references botocore; in other words, botocore is a transitive dependency of boto3. To take this into account, we recommend the following approach:

1. Create a standalone Python environment (conda or similar) on your local machine.
2. Take note of the initial list of packages in that environment's site_packages. You'll see packages like `wheel`, which you don't want to include.
3. Now pip-install all dependencies that your app needs.
4. Note the packages that were *added* to the site_packages folder after step 3 above. These are the folders you need to include in your package (under the `my_deps` folder), organized as shown above. You're essentially capturing a "diff" of the packages between steps 2 and 3 above to identify the right package dependencies for your application.
5. Supply `my_deps/` as an argument for the `pyFiles` property in the `kinesis.analytics.flink.run.options` property group as described below for the `jarfiles` property. Flink also allows you to specify python dependencies using the [`add_python_file` function](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/dependency_management/#python-dependencies), but it's important to keep in mind that you only need to specify one or the other - not both.


The location of `site_packages` for a *conda env* is `miniforge/envs/<your_env>/lib/python3.8/site_packages`. Replace `<your_env>` with the name of your conda env. And lastly, you can also include your own dependencies in the same way - by putting the corresponding Python files in `my_deps`. Note that you don't have to name the folder `my_deps`; the important part is "registering" the dependencies using either `pyFiles` or `add_python_file`.


## JAR dependencies

If your application depends on a connector, be sure to include the connector jar (e.g. `flink-sql-connector-kinesis-1.15.2.jar`) in your package; under the `lib` folder in the tree structure shown above. Note that you don't have to name the folder `lib`, you just have to include it somewhere in your package and also ensure that you specify the jar dependency using the `jarfile` property as described below. Make sure the connector version corresponds to the appropriate Apache Flink version in your Kinesis Data Analytics application.

If you have multiple dependencies, you have to create a fat jar and then include it using the `jarfile` property as described below. This is a Flink requirement as described [here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/dependency_management/#jar-dependencies).

*Important*: In addition to including the jar dependency in your package, you have to specify dependencies using the `jarfile` property in the `kinesis.analytics.flink.run.options` property group when you create your application. Please see the "Configure the Application" section [here](https://docs.aws.amazon.com/kinesisanalytics/latest/java/gs-python-createapp.html).

## Package the application

For packaging your application **DON'T** use the compress tool in Finder or Windows Explorer. 
This will create an invalid code package for KDA. On Linux / macOS you can use the following command:
```shell
zip kdaApp.zip * lib/*
```

## Set application properties in KDA

Add the application properties to the KDA application as defined in `application_properties.json`.