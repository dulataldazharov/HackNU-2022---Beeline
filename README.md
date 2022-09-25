# Beeline task for NU hackathon 

This project contains the code and data for NU hackathon prepared by the Beeline BigData team.


## Acknowledgements:
- Running project locally with docker
- Problem statement


## Running docker container locally:
In order to run jupyter notebooks with pre-installed pyspark locally with docker container do the next steps:

1. Build docker image

    ```
    docker build . -t nu_hackathon
    ```
2. Run container 
    ```
    docker run -it -p 8888:8888 nu_hackathon

    ```
3. To run python script, first install dependencies:
    pip install --upgrade pip ipython ipykernel
    ipython kernel install --name "python3" --user

4. To run the script:
    python3 main.py {path_to_file} --mode={explain | script}

## Problem statement

Based on the scripts recreate dependencies for each final processed column which we get from running src folder's scripts in such way that for each column we can get it's dependencies in this form:

```
{col1: {data_sources: ["data_source1.csv", "data_source2.csv", ...],
        cols_dependencies: [data_source1.col5, data_source2.col4, ...]}
 col2: {data_sources: ["data_source4.csv", "data_source2.csv", ...],
        col_dependencies: [data_source4.col5, data_source2.col1]},
 ...
}
```




