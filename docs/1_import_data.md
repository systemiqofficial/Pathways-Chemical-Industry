# Import data
The source of truth for all data is the `Master Template`, an Excel file that contains all the data needed to run the model. Excel was chosen because it allows for easy handling of the data. In the future, the aim is to use a proper database, which makes it easier to validate data. The challenge then is to make it accessible for everyone. 

## Master template contents
The Master Template contains the following data:

|Tab|Description|Identifier columns|
|---|---|---|
|Emissions|Emissions factors of resources used in processes|Name, Scope, Region|
|Prices and Availability|Prices and availabilities of resources used in processes|Category, Name, Region|
|Tech Desc and Dates - All|Description of process technologies and date range of availability to be chosen by the model|Chemical, Technology|
|Tech Origin-Destination - All|Retrofit / new build options for all chemicals and technologies|Chemical, Origin, Destination|
|Multi chemicals ratios|The amounts produced of each chemical in multi chemical processes|Technology, Region|
|Decommission|Decommission rates over time for different technologies|Technology|
|Values - CHEMICAL|Data specific to a chemical, such as current-day production, plant size (see next subsection) |Process, Sub-process, Name, Region|
|Emissions share|What share of carbon should be assigned to emissions of that chemical (the rest is integrated in the chemical)|Chemical, Process, Name|

### Chemical specific data
The `Values - chemical` tabs contain chemical and plant specific data, in different categories. 
All data is presented as a time-series from 2020-2080. Although the model only simulates pathways from year 2020-2050, data is also required for years 2050-2080 in order to calculate TCO for plants built up until 2050 and will live for 30 years after.
- Demand data: what is the expected demand for this chemical
- Emissions data: the CCS capture rate of a technology, if applicable
- Energy: energy inputs of a technology (normalized per ton chemical)
- Raw material: raw material inputs of a technology (normalized per ton chemical)
- Process economics: data such as CAPEX (both as a new build and as a retrofit from a base conventional plant), O&M, capacity factor, plant lifetime 
- Production: current day production of a technology

### Other columns
- Python import: import this to Python. `FALSE` in this column means the data is used as an intermediate calculation in Excel, and is not relevant for Python.  
- Unit: Unit of this value, e.g. Mton/annum
- Years: value in this year
- Region: `World` if values are not differentiated per region; `[Region name]` if values are differentiated.
- Sensitivity/Pathway: the column has different values for certain pathways or sensitivities

## Importing the data
The base class for importing data is the `BaseImporter`

The data is imported using subclasses, described below. All this data ends up in CSVs in the `data/intermediate` directory, and all is in long format, where years are pivoted from columns to rows.

Generic data is imported by the `GenericDataImporter` 

Chemical specific data is imported by the `ChemicalDataImporter`

Next: [`Calculate variables`](https://github.com/systemiqofficial/chemicals-decarbonization/blob/main/docs/2_calculate_variables.md)