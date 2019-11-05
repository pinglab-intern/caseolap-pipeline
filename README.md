# CaseOLAP_Pipeline
Implementation of the caseOLAP pipeline for mapping relationships between oxidative stress and drugs.

## File overview
- __neo4j_functions/__: Folder containing functions/classes related to use of neo4j
- __lib/__: Folder containing static files like drug lists and oxidative stress markers
- __Explore_Drug_chemical_OxStress.ipynb__: Python notebook to look at reactome pathways related to a curated list of drugs and chemicals related to oxidative stress. Incorporates caseOLAP scores.
- __Create_Drug_Cooccurance_Graph.ipynb__: Creates a neo4J graph relating drug/chemical/pubmid occurances
- __Export_Subgraph.ipynb__: Queries Reactome for relations within 2 edges of drugs in a curated list. Saves a graphml format file.
- __Import_Subgraph.ipynb__: Imports the graphml fromat file in to a new neo4j graph database

