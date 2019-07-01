waze-tools
==============================

Common python tools to deal with waze data

Start Coding
------------

There is a ready workspace at a server with all libraries needed to work with geodata in python. To connect just

`go to 200.20.164.155:8080 in you browser`

the password has to be asked to @joaocarabetta.

Otherwise, you can run it **locally** with docker with two steps:

``` docker
sudo docker build -t wazetools .
sudo docker run -d     
            -p 8888:8080     
            -v $projdir:/home/jovyan/work     
            --user root     
            -e GRANT_SUDO=yes     
            wazetools:latest
```

and connect to it at

`localhost:8080`


Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a 
    │                         tag describing its use, accepted tags: [dev], [example], [analysis];
    │                         number (for ordering) if needed;
    │                         the creator's name, not needed for examples;  
    │                         short `-` delimited description.
    │                         `[analysis] c1.0-joaoc-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    ├── tox.ini            <- tox file with settings for running tox; see tox.testrun.org
    |
    └── Dockerfile         <- Builds geoprocessing enviorinment


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>


Acknowledgements
------------

Repo based on previou work of @JoaoCarabetta