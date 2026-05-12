# Football_Betting_Analysis

## A brief description of what this repository is about

The project is focused on evaluating **how efficient football markets are**, both in terms of betting odds and player valuation, and whether a data-driven approach can reveal their weaknesses. \
The main goal is to be build a **statistical model** which predicts possible **match outcomes** using real performance data and then directly compare its predictions with **bookmaker odds**. \
This way, the project tests whether betting markets are truly based on objective information or whether there are measurable **inconsistencies** that can be identified through detailed and comprehensive **analysis**.

The analyses made in this project will try to reject this essential statement:
> The **betting markets** are higly efficient, meaning that bookmaker odds accurately reflect true match outcome probabilities given all available information.**

---

## Preregistration

This project follows a **preregistered research design** to ensure transparency, reproducibility, and methodological rigor.

The preregistration consists of:

* The core research questions and hypotheses (market efficiency, performance modeling, and prediction accuracy)
* The full data collection plan and preprocessing strategy
* The definition of key variables and engineered features (e.g., expected goals, team strength metrics)
* The modeling framework (statistical and machine learning approaches)
* The evaluation criteria used to assess predictive performance and market calibration

The study was preregistered **prior to conducting any formal analysis**, establishing a clear separation between hypothesis formulation and empirical results.

### Official OSF Registration

The authoritative, timestamped preregistration, along with any updates or amendments, is available on this link: \
----> [Football_Betting_Analysis](https://doi.org/10.17605/OSF.IO/7UYWV)

### Deviations

The planned creation of a statistical model was not achieved.

## Data sources

The following data sources were used in the development of this project:

* **Understat (expected goals)**
* **Football-data.co.uk (odds & results)**
* **Elo Ratings (elo type ratings)**

---

## Project Workflow

The project is made consistantly and structured into the following stages:

1. **Data cleaning part 1**
   * Explore and understanding the raw datasets
   * Cleaning the datasets and most of their features

2. **Data Cleaning Part 2**
   * Merging the datasets
   * Feature engeenering (xG-based strength,rolling forms,shots aggregations etc.)
   * Creating the final dataset

3. **Exploratory Data Analysis (EDA)**
   * Analyze distributions, trends, and relationships

## Installation and Project Setup

This project is designed to be fully reproducible and installable as a professional Python package.
The entire workflow — from data collection to analysis and modeling — can be reproduced using the provided commands and project structure.

### 1.Clone the Repository

Clone the project locally and move into the project directory.

git clone <https://github.com/kvenov/Football_Betting_Analysis.git>

### 2.Create a Virtual Environment

Create an isolated Python environment for the project.

Linux / macOS \
`python -m venv .venv`
`source .venv/bin/activate`

Windows \

`python -m venv .venv`
`.venv\Scripts\activate`

Using a virtual environment guarantees:

### 3.Install the Project

Install the project in editable mode.

`pip install -e .`

This command:

installs all required dependencies from pyproject.toml \
makes the project importable everywhere \
enables package-style imports \
allows notebooks and scripts to share the same modules cleanly

After installation, imports such as: \
`from football_betting_analysis.config.settings import LEAGUE`

### 4.Register Jupyter Kernel (Optional)

**For the Jupyter notebooks:** \
`python -m ipykernel install --user --name football-env`

This creates a dedicated kernel for the project environment.

### 5.Install Additional Packages

All dependencies must be managed through: **pyproject.toml**

Whenever a new package is added to the project: \
Add it to the dependencies section \
Re-run: `pip install -e .`

### 6.Loading Datasets \

**Understat/Football-Data.co.uk:** \
These two datasets are fetched and saved into folders using this command: `python -m football_betting_analysis.data.load_datasets`

**Elo Ratings** \
The elo ratings dataset should be downloaded from [Kaggle](https://www.kaggle.com/datasets/adamgbor/club-football-match-data-2000-2025) and stored into the `data/raw/elo_ratings` folder

## After installing the required packages and all of the datasets are loaded, everything will be ready to work
