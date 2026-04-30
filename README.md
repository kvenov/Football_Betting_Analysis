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

### Notes on Deviations

Any deviations from the original preregistration, as model adjustments, additional features, or methodological refinements, will be **explicitly documented within this repository** to maintain full transparency.

---

## Objectives and project development

Although the project main obejective is to build a **reliable and as realistic as possible statistical predictive model**, the project follows a predefined process of analysing different hypotheses and developing different important aspects, in order for the predictive model to be made sufficienty professional!

So this is the exact steps on which the project will be developed and on which the analyses will be made:

* **Defining true team performance**:
  * First, before comparing anything to betting markets and creating any models, the project defines what “true performance” actually means, which includes important aspects such as: expected goals, teams "actual" strength, attacking vs defending decomposion, Context-adjusted performance etc.

* **Building a predictive models for match outcomes**:
  * Once performance is defined, the next step is to formalize prediction with a data-driven **time-dependent** predictive model based on team strength and performance metrics which produces reliable probabilities for match outcomes.

  * As statistical models and techniques are considered: **Poisson Regresion, Rating or Elo ratings systems(Which are a way of ranking a football team based on *recent* performance), Dixon-Coles adjustment and time-dependent methods such as **Markov Chain(Monte Carlo)** and Time-Dependent Poisson Regression**

* **Comparing the model probabilities with bookmaker odds**:
  * As the predictive model is already created, it is compared with the bookmakers odds and estimated for accuracy and efficiency across its results!

* **Identifying influential metrics in betting markets**:
  * As a final step of the process, the projects tries to find differences between model probabilities and betting odds, which could be explained by measurable performance factors observed during matches.

  * The main idea here is to test what drives the changes of the bettings odds durring matches, and which factors mostly influence these changes.This knowledge is used to increase the efficiency of the predictive model by configuring some priorities at specific metrics.

Having said that I can state that:

### The project follows a structured, consistent and professional process by first, models football outcomes by constructing a latent team-strength representation from performance statistics, then using that strength to build a probabilistic predictive model, benchmarking the model against betting market efficiency, and analyzing systematic deviations between model and market

---

## Data sources

The following data sources were used in the development of this project:

* **StatsBomb (event-level data)**
* **Understat (expected goals)**
* **Football-data.co.uk (odds & results)**
* **Transfermarkt (player data)**
* **Elo Ratings (elo type ratings)**

---

## Project Workflow

The project is made consistantly and structured into the following stages:

1. **Data Understanding**
   * Explore raw datasets (StatsBomb, Understat, Football-Data etc)

2. **Data Cleaning & Preparation**
   * Handle missing values, standardize formats

3. **Exploratory Data Analysis (EDA)**
   * Analyze distributions, trends, and relationships

4. **Feature Engineering**
   * Construct performance metrics (xG-based strength, form, etc.)

5. **Modeling**
   * Poisson models, Elo ratings, ML models

6. **Evaluation**
   * Compare model probabilities vs bookmaker odds

7. **Analysis of Market Efficiency**
   * Identify systematic deviations and inefficiencies
