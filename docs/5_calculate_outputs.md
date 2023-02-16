# Calculate outputs
After a model run, the resulting technology distribution is saved in `outputs/final/<MODEL_SCOPE>/<PATHWAY>/<CHEMICAL>`. In this phase of the program, we load these technology distributions, and use them to calculate the following variables:
- Weighted average CAPEX and levelized cost across regions
- The contribution to the levelized cost of different cost components (See the `calculate_variables` section for the cost components)
- Consumption of inputs, e.g. hydrogen, electricity, biomass
- Annual and cumulative emissions, CAPEX, CO2 captured
- Levelized cost contribution, broken down per region and technology

Next: [`Export outputs`](https://github.com/systemiqofficial/chemicals-decarbonization/blob/main/docs/6_export_outputs.md)