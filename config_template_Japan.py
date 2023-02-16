# Log level: set to debug to get more detailed logging
# LOG_LEVEL = "DEBUG"
LOG_LEVEL = "INFO"

# Run the different pathways in parallel
RUN_PARALLEL = False

# MODEL_SCOPE = "World"
MODEL_SCOPE = "Japan"

# Start and end year for the pathway optimizer
START_YEAR = 2020
END_YEAR = 2050

# Chemicals to run the model for
CHEMICALS = [
    "Ammonia",
    "Urea",
    "Ammonium Nitrate",
    "Ethylene",
    "Propylene",
    "Butadiene",
    "Benzene",
    "Toluene",
    "Xylene",
    "Methanol",
]

# Which part of the model to run
# If running different pathways but with the same data, only the first 2 steps need to be run once at beginning
run_config = {
    "IMPORT_DATA",
    "CALCULATE_VARIABLES",
    "MAKE_RANKINGS",
    "OPTIMIZE_PATHWAY",
    "CALCULATE_OUTPUTS",
    "EXPORT_OUTPUTS",
    "PLOT_AVAILABILITIES",
    "MERGE_OUTPUTS",
}

# Which pathway(s) to take; this impacts how technologies are ranked (see ranking config below):
# most economic, fast abatement, no fossil, no fossil strict, business as usual
PATHWAYS = [
    "me",
    # "fa",
    # "nf",
    # "nfs",
    # "bau"
]

# Sensitivities: low fossil prices, constrained CCS, BAU demand, low demand
SENSITIVITIES = [
    "def",
    # "bdem",
    # "ldem",
    # "lfos",
    # "ccs"
]

# For the no fossil scenario: from this year, no more fossil is allowed to be built new
NO_FOSSIL_FROM_YEAR = 2030

# Should there be a carbon price in the model?
CARBON_PRICE = False

# Share of plants that we can maximally retrofit per year
RETROFIT_CAP = 0.05

# Allow initial tech until year X
INITIAL_TECH_ALLOWED_UNTIL_YEAR = 2025

# Only do second retrofits from this year
SECOND_RETROFIT_EARLIEST_YEAR = 2040

# Minimum age to decommission a plant
MINIMUM_AGE_DECOMMISSION = 20

# Maximum technology ramp up rate (e.g. 1.2 means this year's number of plants of tech A can only be 1.2x last year's)
MAX_TECH_RAMP_RATE = 1.3

# Maximum % share of total wedge in a region
REGIONAL_CAP = 0.3

# Rate to use for discounting cash flows
DISCOUNT_RATE = 0.09

# Lifetime to use for TCO calculation
ECONOMIC_LIFETIME_YEARS = 25

# Number of bins to use for joining similar ranks together. More bins = more precision
NUMBER_OF_BINS_RANKING = 300

# Sensitivity analyses
# Set to 1 to have the normal case, or for example:
# 1.2 for 20% higher price
# 0.8 for 20% lower price
CARBON_PRICE_ADJUSTMENT = 1
POWER_PRICE_ADJUSTMENT = 1
CCS_PRICE_ADJUSTMENT = 1

# Override plant parameters
PLANT_SPEC_OVERRIDE = {"assumed_plant_capacity": 100}

# Age (young/old) based optimization
# For these chemicals, plant age matters to prevent young plants being shut/retrofitted too early/at all
AGE_DEPENDENCY = ["Ethylene", "Propylene", "Butadiene", "Benzene", "Toluene", "Xylene"]


# Maximum plants that are allowed to violate the tech ramp rate
# Needed because during early days of a tech penetrating the wedge,
# it won't be able to build any integer number of plants without violating the tech ramp up rate
MAX_PLANTS_RAMP_UP = 4*(3000/PLANT_SPEC_OVERRIDE["assumed_plant_capacity"])

# Input chemicals for methanol demand
# These chemicals have production routes that use methanol as raw material and therefore affect overall methanol demand
METHANOL_DEPENDENCY = ["Ethylene", "Propylene", "Benzene", "Toluene", "Xylene"]

# Carry over methanol emissions to MTO tech every year when re-ranking
CARRYOVER_METHANOL_EMISSIONS = False

# Multiply Methanol demand with this to simulate growth from previous year
# This is required only to act as a ceiling of availability, and does not mean that all the methanol must be consumed
METHANOL_AVAILABILITY_FACTOR = 1.5

# Methanol green (clean) and black (dirty) technologies
# This helps classify which technologies produce methanol classed as black and which produce methanol classed as green
# This feeds into the volume availabilities of 'Methanol - Black' and 'Methanol - Green'
METHANOL_SUPPLY_TECH = {
    "Methanol - Green": [
        "Green H2 + DAC + MeOH synthesis",
        "Green H2 + point source CO2 + MeOH synthesis",
        "Coal gasification + green H2 + MeOH synthesis",
        "Coal gasification + CCS + MeOH synthesis",
        "Natural gas e-SMR + MeOH synthesis",
        "Natural gas + SMR + CCS + MeOH synthesis",
        "Natural gas GHR + ATR + MeOH synthesis",
        "Biomass Gasification + MeOH synthesis",
        "Biomass Gasification + CCS + MeOH synthesis",
        "Biomass Gasification + green H2 + MeOH synthesis",
        "MSW Gasification + CCS + MeOH synthesis",
        "MSW Gasification + green H2 + MeOH synthesis",
        "Plastics Gasification + CCS + MeOH synthesis",
        "Plastics Gasification + green H2 + MeOH synthesis",
    ],
    "Methanol - Black": [
        "Coal gasification + MeOH synthesis",
        "Natural gas SMR + MeOH synthesis",
        "MSW Gasification + MeOH synthesis",
        "Plastics Gasification + MeOH synthesis",
    ],
}
# Technologies that have methanol as a raw material feedstock
METHANOL_DEMAND_TECH = [
    "MTO - Black",
    "MTO - Green",
    "MTP - Black",
    "MTP - Green",
    "MTA - Black",
    "MTA - Green",
    "MTA - Black + Toluene Disproportionation",
    "MTA - Green + Toluene Disproportionation",
]

METHANOL_TYPES = [
    "Methanol - Black",
    "Methanol - Green",
]

