# README
## Overview
Data is processed, transformed, and loaded into the Neo4j graph database.
Using the cleaned and modelled data, authors are disambiguated, recommendations in terms of
what authors could review the incoming publications are made, and the most influential authors are identified.

The sample data files in CSV format:
* publications.csv,
* authors.csv,
* topics.csv,
* publications_incoming.csv.

## Objectives
1. Draft the initial data model (nodes, relationship and labels) and ETL strategy in order
to load into Neo4j graph data is relevant included in publications, authors and topics csv files.
2. Clean the datasets up, considering , for example, possible duplicated authors.
4. Recommend a group of people to review the incoming publications.
5. Depict the more influential authors.

## Folders and file structure
* _Assignment for Knowledge Graph Engineer.pdf_ file depicting the assessment.
* _assignment\_slideck.pdf_ file is the slide deck depicting the solution process.
* _graphDB\_model.svg_ depicts the graph data model.
* _data/_ folder. Contains the data CSV files.
* _notebooks/_ folder. Contains the Jupyter notebooks:
    * to perform initial data exploration (_exploration.ipynb_),
    * to run the graph data science algorithms (_analysis.ipynb_)
* _src/_ folder. Contains the Python script to load the data into the graph db (_etl_pandas.py_).
 
