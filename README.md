# Chemicals decarbonization model
This repository contains the chemicals decarbonization model, as created by SYSTEMIQ/University of Cambridge/University of Tokyo funded by the Mitsibishu Chemicals Corporation (MCC).
This README describes how to set up and use the model. A more detailed description about how the model works can be found in `docs/`.

## Windows

### Installing Python

The simplest way is to install the necessary version of Python directly:
- [Python 3.9](https://www.python.org/downloads/release/python-39/)
- When installing, select the option to "add to PATH"
- If installing Python this way, you can use any terminal to run the commands below (e.g. Powershell, or git-bash).

Alternatively, if you are already using Conda, you can use it to get the correct version of Python.
- If using Conda, all the commands below should be in the "miniconda prompt" or "anaconda prompt"
- Navigate to `Pathways-Chemical-Industry` folder
- Create a Conda environment with the correct version of Python:
  ```shell
  conda create -n Pathways-Chemical-Industry python=3.9
  ```
  Don't "activate" the environment now.

Either way, we then need to [install virtualenv](https://uoa-eresearch.github.io/eresearch-cookbook/recipe/2014/11/26/python-virtual-env/), which manages all the dependencies of the code to get the correct versions installed. The simplest way is to run `pip install virtualenv`.
- Navigate to `Pathways-Chemical-Industry` folder
- Then run`virtualenv Pathways-Chemical-Industry`

## Setting up and running the model


### If using `Command Prompt`

1. Open a terminal / git-bash
2. Navigate to `Pathways-Chemical-Industry` folder and activate the environment
If using Conda
`conda activate Pathways-Chemical-Industry`
If using 
`virtualenv Pathways-Chemical-Industry`
3. Install project requirements using `pip install -r requirements.txt`
4. Create a `data/` directory, and add the input data sheet (`Master Template - python copy.xslx`) into it.
5. Make a copy of the `config_template.py` file and rename it to `config.py`. Make changes only to `config.py`.
6. Run the model using `python -m main`
7. Outputs are added in the `output/` directory

### If using `PyCharm`
1. Navigate to `chemicals-decarbonization` folder and activate the environment
2. Install project requirements using `pip install -r requirements.txt`
3. Create a `data/` directory, and add the input data sheet (`Master Template - python copy.xslx`) into it.
4. Make a copy of the `config_template.py` file and rename it to `config.py`. Make changes only to `config.py`.
5. Run the model using `python -m main`
6. Outputs are added in the `output/` directory


### Running the model on codespaces
Github offers a way to run software on their infrastructure: [Codespaces](https://github.com/features/codespaces). Setting up a codespace can be done directly from the repository, and can be accessed through the browser (or VSCode). The benefit of this is that you can use a bigger machine than you own (more cores / memory), to reduce workload on your own hardware. Be careful, this is not free!

The setup on codespaces is exactly the same as on your local machine.

## Configuration options
Now, you can run the model with different configurations, by changing these values in `config.py`:
- `RUN_PARALLEL` runs the model on multiple cores at the same time, speeding up computation by the number of cores you have 
- `MODEL_SCOPE` allows running the model worldwide, or only for Japan
- `CHEMICALS` defines the chemicals to run the model for
- `run_config` allows running parts of the model individually
- `PATHWAYS` define the pathways that you run the model for, and `SENSITIVITIES` the sensitivities. It will run all combinations; if you choose 2 pathways and 2 sensitivities, this results in 4 model runs. 

There are more configuration options, a complete explanation is in `config.py`.

## Contacts
Technical questions: [shajeeshan.lingeswaran@systemiq.earth](shajeeshan.lingeswaran@systemiq.earth) 

Subject matter questions: [jane.leung@systemiq.earth](jane.leung@systemiq.earth), [andreas.wagner@systemiq.earth](andreas.wagner@systemiq.earth), [fm392@cam.ac.uk](fm392@cam.ac.uk) [daisuke.kanazawa@ifi.u-tokyo.ac.jp](daisuke.kanazawa@ifi.u-tokyo.ac.jp)

