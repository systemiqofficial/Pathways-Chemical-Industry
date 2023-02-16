# Calculate variables
In this step, the imported input data is used to calculate variables that are used in the model. This is a three-step process:
1. Calculate emissions
2. Calculate cost (TCO and levelized cost of chemical)
3. Yearly consumption of inputs
 
## Emissions calculation
Emissions are calculated according to the flowchart below, for each chemical, technology, year and region. They are saved in `outputs/intermediate/emissions.csv`. 
```mermaid
flowchart LR
    subgraph generic_inputs[Generic inputs]
        emission_factors[Emission factors]
    end

    subgraph tech_inputs[Technology inputs]
        energy[Energy and raw material usage]
        ccs[CCS rate]
    end

    subgraph calc[Emissions calculation]
        emissions[Emissions scope 1/2/3]:::calc
        emissions_total[Emissions total]:::calc
        captured_carbon[Captured carbon]:::calc
        emission_factors & energy-->emissions
        ccs & emissions-->captured_carbon
        emissions & captured_carbon-->emissions_total
    end
    classDef calc fill:#f96, stroke:#f96;
```

If a retrofit has several sub-modules that are changed, the costs for each of those changes are added up to get to the total retrofit cost. 

## Cost calculation
Costs are calculated according to the flowchart below, for each chemical, technology, year and region. They are saved in `outputs/intermediate/cost.csv`. 

First, variable OPEX is calculated (costs are discounted using a discounting value configurable in `config.py`):

```mermaid
flowchart LR
    emissions_total[Total emissions]:::calc
    captured_carbon[Captured carbon]:::calc
    emissions_total & prices --> cost_carbon
    captured_carbon & prices --> cost_ccs

    subgraph generic_inputs[Generic inputs]
        prices["Prices (energy, raw material, CCS)"]
    end

    subgraph tech_inputs[Technology inputs]
        energy[Energy and raw material usage]
        capex_new["CAPEX new build"]
        o_m[O&M]
    end

    subgraph calc[OPEX calculation]
        cost_inputs[Cost of inputs]:::calc
        cost_carbon[Cost of carbon]:::calc
        cost_ccs[Cost of CCS]:::calc
        opex[OPEX]:::calc

        energy & prices --> cost_inputs
        cost_inputs & cost_carbon & cost_ccs & o_m --> opex
    end

    classDef calc fill:#f96, stroke:#f96;
```

This and the CAPEX is used to calculate TCO and levelized cost:

```mermaid
flowchart LR
    var_opex[Variable OPEX]:::calc
    
    subgraph tech_inputs[Technology inputs]
        capex_new["CAPEX new build"]
    end

    subgraph calc_tco[TCO Calculation]
        npv(NPV calculation)
        tco[Discounted TCO]:::calc
        lcox[Levelized cost of chemical]:::calc
        var_opex & capex_new --> npv
        npv --> tco
        tco --> lcox        

    end

    classDef calc fill:#f96, stroke:#f96;
```

## Yearly consumption calculation
As a last step, yearly consumption of all inputs is calculated by multiplying the inputs per ton of chemical by the total yearly volume produced of that chemical. 

Next: [`Make rankings`](https://github.com/systemiqofficial/chemicals-decarbonization/blob/main/docs/3_make_rankings.md)