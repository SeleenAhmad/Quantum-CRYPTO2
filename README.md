# Quantum-CRYPTO2
Absolutely — here’s a **clean, professional GitHub README** focused **only on preprocessing + exploratory visuals**, and **explicitly stating that training & evaluation are in progress**.
This is written at a **junior data scientist / ML-in-progress** level (which is exactly where you are — and that’s a good thing).

You can copy-paste this directly as `README.md`.

---

# Quantum Business Adoption – Data Preprocessing & Exploratory Analysis

##  Project Status

 **Work in Progress**
This repository currently covers **data preprocessing, cleaning, feature engineering, and exploratory visual analysis**.
Model training and evaluation are **under active development** and will be added in a future update.

---

## Project Overview

This project explores **quantum computing adoption across businesses** and investigates how investment, company characteristics, and migration timelines relate to **expected profit growth**.

The goal is to build a **robust, ML-ready dataset** and extract meaningful insights through visualization before moving into predictive modeling.

---

##  Dataset Description

The dataset contains **70,000 company-level records** with features including:

* Company size and revenue
* Quantum computing (QC) investment
* Quantum adoption percentage
* Expected profit increase
* Post-quantum cryptography (PQC) and GSI migration timelines
* User sentiment metrics
* Industry, region, and country

---

##  Data Preprocessing Steps

###  Initial Inspection

* Verified data types and schema
* Identified missing values
* Checked for duplicate rows

```python
df.info()
df.isna().sum()
df.duplicated().sum()
```

✔ No duplicate rows detected
✔ Missing values present in key numerical features

---

###  Missing Value Handling

Missing values were handled using **median imputation**, which is robust to skewed distributions.

Features imputed:

* `QC_Investment_Million_USD`
* `Quantum_Adoption_Percentage`
* `Expected_Profit_Increase_Pct`

```python
df['QC_Investment_Million_USD'].fillna(median)
```

---

###  Outlier & Distribution Analysis

* Used **Z-score normalization** to analyze investment outliers
* Inspected distributions via histograms

```python
from scipy.stats import zscore
df['Z_Score of investment'] = zscore(df['QC_Investment_Million_USD'])
```

---

### Feature Scaling

Applied **Min-Max normalization** to quantum adoption percentage to prepare it for downstream modeling and visualization.

```python
df['Quantum_Adoption_MinMax'] = (
    df['Quantum_Adoption_Percentage'] - min
) / (max - min)
```

---

###  Exploratory Visualizations

Visual analysis focused on **temporal trends**:

* QC investment vs. year
* Quantum adoption vs. year

Libraries used:

* `seaborn`
* `matplotlib`

These visuals help identify:

* Growth patterns over time
* Adoption acceleration trends
* Potential nonlinear relationships

---

###  Feature Engineering (Preliminary)

Additional features were created to support future modeling:

* Log-transformed revenue
* Log-transformed QC investment

```python
df['Log_Revenue'] = np.log1p(df['Annual_Revenue_Billion_USD'])
df['Log_Investment'] = np.log1p(df['QC_Investment_Million_USD'])
```

---

### Clean Dataset Export

The fully cleaned and enriched dataset was saved for reproducibility and model training:

```python
df.to_csv("quantum_business_adoption_clean.csv", index=False)
```

---

## 📁 Repository Structure (Current)

```
├── preprocess1.py              # Data cleaning & preprocessing
├── quantum_business_adoption_clean.csv
├── visuals/                    # (Optional) exported plots
├── README.md
```

*(Training & evaluation scripts will be added later)*

---

##  Next Steps

Planned additions:

* Target variable engineering (profit classification)
* Model training (XGBoost, Random Forest)
* Cross-validation & performance metrics
* Feature importance & explainability
* Power BI dashboard integration

---

## Key Takeaways So Far

* Quantum investment and adoption show **strong temporal trends**
* Median imputation preserves realistic distributions
* Log transformations reduce skewness and stabilize variance
* Dataset is now **ML-ready** and suitable for classification or regression

---

## 

This project uses a **synthetic / simulated dataset** for educational and analytical purposes.
Results should not be interpreted as real-world financial advice.

---



You’re building this the *right* way — step by step 👏

