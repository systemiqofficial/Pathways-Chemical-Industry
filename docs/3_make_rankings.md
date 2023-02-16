# Ranking

## Overview

In this step of the model, all manufacturing technologies for the modelled chemicals is ranked per year across the 10 regions.
The model creates for each of the possible plant outcomes (i.e., decommission, retrofit or new build) an individual ranking table.
Depending on the simulated pathways, the ranking function chooses LCOX, CO2 emissions abated or LCOX per CO2 emissions abated as its decision variable. 
In addition, the different pathways determine which technologies are given priority. For all pathways other than BAU,
"initial" technologies are not considered and "transition" technologies are placed at the end of the ranking.

|Pathway|Ranking Variable|Initial Technologies|Transition Technologies|Description|
|---|---|---|---|---|
|BAU|LCOX|Yes|First|Business-as-usual scenario where lowest cost of all technologies drives decisions|
|Most Economic|LCOX|No|Last|Most economic pathway to decarbonisation where lowest cost of end-state + transition technologies drive decisions|
|Fastest Abatement|Emissions (scope 1+2) Abated|No|Last|Fastest pathway to decarbonisation where greatest abatement potential of end-state + transition technologies compared to conventional technologies drive decisions|
|No fossil|Emissions (scope 1+2+3) Abated|No|Last|Like fastest abatement, but we take scope 3 emissions into account. We don't allow new (abated or unabated) fossil to be built after a (configurable) year|
|No fossil strict|Emissions (scope 1+2+3) Abated|No|Last|Like no fossil, but we don't allow any fossil to be present at the end of the model run|

A full description of the ranking logic per pathway is in the [rank_technologies](https://github.com/systemiqofficial/chemicals-decarbonization/blob/main/flow/rank/rank_technologies.py) file

## Key Functions

The following two functions `rank_tech` and `rank_per_year` create the ranking.

### Rank Technologies

In the `rank_tech` function:
1. Technologies are selected that correspond to the chemical and to the rank table to be created (e.g., decommission ranking)
2. Respective emissions are merged to the origin and destination technologies, and the emission delta (emissions abated) is calculated
3. CAPEX, LCOX and TCO are merged to the corresponding source and target technology pathway
4. Rank variables, such as LCOX per CO2 emissions, are calculated
5. `rank_per_year` function creates the actual ranking per year and chemical
6. `rank_available_tech` function sort the actual ranking based on technology type and ranking variable


### Yearly Rank Function 

In the `rank_per_year` function:
1. Filter decommission technologies from new build ranking and retrofit
2. Disallow biomass relevant technologies if it was set in the configuration 
3. Rank technologies the same that are close to each other based on the rank variable

Next: [`Run pathway simulation`](https://github.com/systemiqofficial/chemicals-decarbonization/blob/main/docs/4_run_pathway_simulation.md)