# Simulating Pathways

## Overview
This module is the machine room of the model and is described by the main function `optimize_pathway`. At first, we 
initialize the pathway, `DecarbonizationPathway`. This means, that based on the chosen chemicals by the user, not only the plants for the start year 
are initialized, but also relevant data (i.e., demand, ranking, availability, emissions, costs, plant specification, 
technologies/business cases) for the simulation are imported. In the second step, the model decides yearly based on 
the demand and the availability of resources, if plants should be decommissioned or built. In addition, for non-BAU 
pathways, plants are retrofitted per year based on by a pre-defined share of the current plant capacity. At the end, 
plant composition in each year are stored and are exported in a csv.-file.


## Key Functions

### Multi-chemical technologies
Some technologies produce more than one chemical (e.g. steam crackers). For all chemicals, is it first checked if is produced as a by-product of another chemical. If they are, that volume of the chemical should be taken into account and included in the stack already as non-negotiable.

### Decommission
In the `decommission` function, for the respective chemical and year:
1. The current plant stack is returned, and the production capacity of the current plant stack is determined. Using the current demand of the chemical, the model calculates the capacity surplus.
2. For some chemicals, such as propylene and ethylene, technologies like MTO produce propylene as a byproduct while 
   primarily producing ethylene. These technologies should then only be decommissioned for one of the two chemicals. Which chemical should take precedence for this - the primary chemical - for each of these multi-product processes is defined. 
   In that case, the model will filter those technologies and will not consider them anymore for the decommissioning.
3. While there is a positive capacity surplus, plants will be decommissioned to follow the demand trajectory.
4. Utilizing `select_plant_to_decommission` function, plants for decommissioning are selected based on the 
   decommissioning ranking and the technologies that are available for the current year.
5. The decommissioned plants are removed from the next year's plant stack.   

#### Force decommission
To simulate the phase-out of fossil technologies, they are phased out (force decommissioned) from 2035 onwards, with an increasing speed. The rates for this are a user defined input, which is imported from the Excel sheet in the beginning of the program.    

### Retrofit
In the `retrofit` function, for the respective chemical and year:
1. The current plant stack is returned, and the production capacity of the current plant stack is determined. Utilizing the current capacity of the plants and the configurable user variable (share of the capacity for yearly retrofit), the model calculates the capacity for yearly retrofit.
2. For some chemicals, such as propylene and ethylene, technologies, like MTO, generate propylene as a byproduct while primarily producing ethylene. These technologies should then only be retrofitted for one of the two chemicals. Which chemical should take precedence for this - the primary chemical - for each of these multi-product processes is defined. In that case, the model will filter those technologies and will not consider them anymore for retrofitting.
3. Technologies that would exceed the material constraints are removed and will not be considered for retrofitting.
4. Technologies that would violate the maximum ramp up rate of novel technologies are removed and will not be considered 
   for retrofitting.
5. Utilizing `select_best_transition` function, plants for retrofitting are selected based on the 
   retrofitting ranking and the technologies that are available for the current year.
6. The retrofitted plants are updated in the next year's plant stack, by removing the old technology and adding the new 
   technology into the next year's plant stack.   

#### Availability retrofit
Sometimes, technologies can overshoot input availabilities (for exaple, due to declining availability of black Methanol, or filling up of CCS capacity). When this happens, the model will retrofit any violating plants away, until the availability is no longer exceeded.  

### Build New
In the `build_new` function, for the respective chemical and year:
1. The current plant stack is returned, and the production capacity of the current plant stack is determined. Using
   the current demand of the chemical, the model calculates the capacity gap.
2. Technologies that would exceed the material constraints are removed and will not be considered for new builds.
3. Technologies that would violate the maximum ramp up rate of novel technologies are removed and will not be considered 
   for new builds.
4. Utilizing `select_best_transition` function, plants for building new plants are selected based on the 
   new build ranking and the technologies that are available for the current year.
5. The retrofitted plants are updated in the next year's plant stack, by removing the old technology and adding the new 
   technology into the next year's plant stack.   

### Tech Ramp Rate
To avoid technologies from ramping up too quickly, a tech ramp rate is used. This is defined in `config.py` as `MAX_TECH_RAMP_RATE`, and indicates the maximum year-on-year growth per technology. To allow new technology to kick off, we also have a `MAX_PLANTS_RAMP_UP` that are allowed to be built every year, before the rate is enforced.

### Material and CCS Constraints
Before we build a new plant or execute a retrofit, we check if material and CCS constraints are not violated. These are imported at the beginning of the program, and vary over time. 

Next: [`Calculate outputs`](https://github.com/systemiqofficial/chemicals-decarbonization/blob/main/docs/5_calculate_outputs.md)