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

* **Understat (expected goals)** - <https://collinb9.github.io/understatAPI/understatapi.api.html>
* **Football-data.co.uk (odds & results)** - <https://www.football-data.co.uk/spainm.php>
* **Elo Ratings (elo type ratings)** - <https://www.kaggle.com/datasets/adamgbor/club-football-match-data-2000-2025>
* **Transfermarkt data** - <https://www.kaggle.com/datasets/davidcariboo/player-scores>
* **Soccer data (from soccerdata api)** - <https://soccerdata.readthedocs.io/en/latest/>

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

4. **Machine Learning Workflow**
This project follows a modular, reproducible and experiment-driven Machine Learning workflow designed according to modern MLOps best practices. Every experiment is fully configurable, reproducible and tracked through **DVC**, **MLflow**, **Optuna**, and **Scikit-Learn Pipelines**.

## Overall Workflow

```text
                     RAW DATA
                         │
                         ▼
          Google Cloud Storage (Raw Data)
                         │
                         ▼
              Data Processing Notebooks
                         │
                         ▼
          Final Processed Dataset (Parquet)
                         │
                         ▼
        Dataset Version Creation Script
 (feature removal, experiment-specific dataset)
                         │
                         ▼
               Track Dataset with DVC
                         │
                         ▼
                Push Dataset to Cloud
                         │
                         ▼
        =====================================
                 MACHINE LEARNING
        =====================================
                         │
                         ▼
              Experiment Configuration
                         │
                         ▼
                  train.py Entry Point
                         │
                         ▼
                 Configuration Loader
                         │
                         ▼
                  DataLoader (DVC API)
                         │
                         ▼
          Pull Correct Dataset Version
                         │
                         ▼
            Sequential Train/Test Split
                         │
                         ▼
                PipelineBuilder
                         │
         ┌───────────────┼────────────────┐
         ▼               ▼                ▼
 Preprocessing    Feature Selection   Dimensionality Reduction
         │               │                │
         └───────────────┴────────────────┘
                         │
                         ▼
                   ML Model Builder
                         │
                         ▼
                  Training Pipeline
                         │
                         ▼
              Hyperparameter Tuning
                  (Optional Optuna)
                         │
                         ▼
                Final Model Training
                         │
                         ▼
                    Evaluation
                         │
                         ├──────── Metrics
                         ├──────── Confusion Matrix
                         ├──────── ROC Curves
                         ├──────── Precision-Recall
                         ├──────── Classification Report
                         ├──────── Prediction CSV
                         └──────── Probability Analysis
                         │
                         ▼
                 Model Serialization
                     (Joblib)
                         │
                         ▼
                 MLflow Experiment
         (configs, metrics, plots, model)
```

---

## Workflow Steps

## 1. Data Versioning

The project starts from the final processed dataset.

For each experiment a dedicated dataset version is created by:

* removing unnecessary features
* removing leakage features
* selecting only required columns
* fixing metadata if necessary

Each dataset version is tracked using **DVC**.

The experiment configuration stores the exact dataset version to be used.

---

## 2. Configuration Driven Experiments

Every experiment is completely defined through YAML configuration files.

The experiment configuration specifies:

* dataset version
* preprocessing
* feature selection
* dimensionality reduction
* machine learning model
* evaluation settings
* serialization
* logging
* MLflow
* Optuna tuning

No Python code needs to be modified to create a new experiment.

---

## 3. Data Loading

The `DataLoader` loads the exact dataset version directly from the DVC remote storage.

The experiment only specifies:

* dataset path
* DVC revision (Git tag/commit)

This guarantees perfect reproducibility.

---

## 4. Sequential Data Split

Football matches are chronological observations.

Therefore, the dataset is split sequentially without shuffling.

```text
     Past Matches ---------------------- Future Matches
     Training                            Testing
```

This prevents information leakage.

---

## 5. Automatic Pipeline Construction

The `PipelineBuilder` creates a Scikit-Learn pipeline entirely from the configuration.

Pipeline structure:

```text
     Preprocessing
          │
          ▼
     Feature Selection
          │
          ▼
     Dimensionality Reduction
          │
          ▼
     Machine Learning Model
```

Each stage is optional and controlled through the experiment configuration.

---

## 6. Preprocessing

Preprocessing is fully configuration-driven.

Different feature groups can use different preprocessing pipelines.

Example:

```text
     Match Statistics
     └── StandardScaler

     Odds
     └── log1p
     └── StandardScaler

     Rolling Statistics
     └── MinMaxScaler

     Teams
     └── Target Encoding

     Formations
     └── OneHot Encoding
```

Each feature group can have:

* imputation
* transformations
* scaling
* encoding

independently configured.

---

## 7. Feature Selection

Optional feature selection techniques include:

* Variance Threshold
* SelectKBest
* Mutual Information
* ANOVA F-Test
* Recursive Feature Elimination (RFE)
* Model-based Selection

Configured entirely through YAML.

---

## 8. Dimensionality Reduction

Optional dimensionality reduction methods include:

* PCA
* Kernel PCA
* Truncated SVD
* Fast ICA

Each experiment can choose a different technique.

---

## 9. Model Training

Models are created through the `ModelFactory`.

Supported models include:

* Logistic Regression
* Random Forest
* XGBoost
* AdaBoost
* Support Vector Machine
* Voting Classifier
* Stacking Classifier

Each model has its own configuration file.

---

## 10. Hyperparameter Optimization

Optuna can be enabled per experiment.

Workflow:

```text
     Pipeline
          │
          ▼
     Optuna Study
          │
          ▼
     Trial Parameters
          │
          ▼
     Pipeline Training
          │
          ▼
     Evaluation Metric
          │
          ▼
     Best Parameters
```

> The final model is retrained using the best hyperparameters.The optuna functionality is not fully implemented yet, so it should not be used in any experiment until is fully implemented and confiured!

---

## 11. Evaluation

Every trained model is automatically evaluated.

Available metrics include:

* Accuracy
* Balanced Accuracy
* Precision
* Recall
* Macro F1
* Weighted F1

Available visualizations:

* Confusion Matrix
* Normalized Confusion Matrix
* ROC Curves (OvR)
* Precision–Recall Curves
* Probability Distribution
* Classification Report

All evaluation artifacts are saved locally and logged to MLflow.

---

## 12. Model Serialization

The trained pipeline is serialized using Joblib.

```text
     models/logistic_baseline.joblib
```

The saved pipeline contains:

* preprocessing
* feature selection
* dimensionality reduction
* trained model

allowing direct inference without rebuilding the pipeline.

---

## 13. Experiment Tracking (MLflow)

Each experiment automatically logs:

### Parameters

* dataset version
* preprocessing configuration
* feature selection
* dimensionality reduction
* model parameters
* Optuna parameters

### Metrics

* Accuracy
* Precision
* Recall
* F1
* Balanced Accuracy

### Artifacts

* configuration files
* evaluation plots
* confusion matrices
* ROC curves
* prediction CSV
* serialized model

Every experiment is completely reproducible.

---

## 14. Prediction

Predictions reuse the serialized pipeline.

```text
   Load Model
         │
         ▼
   Load New Matches
         │
         ▼
   Automatic Preprocessing
         │
         ▼
   Prediction
         │
         ▼
   Prediction Probabilities
         │
         ▼
   predictions.csv
```

No manual preprocessing is required during inference.

---

## Project Architecture

```text
     Dataset Version (DVC)
               │
               ▼
          DataLoader
               │
               ▼
     Sequential Split
               │
               ▼
     PipelineBuilder
               │
               ├──────── Preprocessing
               ├──────── Feature Selection
               ├──────── Dimensionality Reduction
               └──────── Model
                         │
                         ▼
               Training Pipeline
                         │
                         ▼
               Optional Optuna
                         │
                         ▼
                    Final Training
                         │
                         ▼
                    Evaluation
                         │
               ┌──────────┴──────────┐
               ▼                     ▼
          Model (.joblib)       MLflow Logging
```

---

## Reproducibility

Every experiment is reproducible because it stores:

* exact dataset version (DVC)
* preprocessing configuration
* feature selection configuration
* dimensionality reduction configuration
* model configuration
* Optuna configuration
* evaluation configuration
* serialized pipeline
* MLflow experiment logs

Re-running the same experiment with the same configuration produces identical results.

---

## Installation and Project Setup

This project is designed to be fully reproducible and installable as a professional Python package.
The entire workflow — from data collection to analysis and modeling — can be reproduced using the provided commands and project structure.

### 1.Clone the Repository

Clone the project locally and move into the project directory.

* git clone <https://github.com/kvenov/Football_Betting_Analysis.git>

### 2.Create a Virtual Environment

Create an isolated Python environment for the project.

**Linux / macOS** \
`python -m venv .venv`
`source .venv/bin/activate`

**Windows** \
`python -m venv .venv`
`.venv\Scripts\activate`

### 3.Install the Project

Install the project in editable mode.

`pip install -e .`

This command will:

* installs all required dependencies from `pyproject.toml`
* makes the project importable everywhere
* enables package-style imports
* allows notebooks and scripts to share the same modules cleanly

After installation, imports such as: \
  `from football_betting_analysis.config.settings import LEAGUE` \
can be made from everywhere in the project

### 4.Register Jupyter Kernel (Optional)

**For the Jupyter notebooks:** \
`python -m ipykernel install --user --name football-env`

This creates a dedicated kernel for the project environment.

### 5.Install Additional Packages

All dependencies must be managed through: **pyproject.toml**

Whenever a new package is added to the project: \
Add it to the dependencies section and re-run: `pip install -e .`

### 6.Loading Datasets \

All of the datasets are fetched from **google cloude**.The google cloude server is made public so there aren't any restictions on that.

All of the data is loaded and saved into folders using this command: `python -m football_betting_analysis.data.load_datasets`

## After installing the required packages and all of the datasets are loaded, everything will be ready to work