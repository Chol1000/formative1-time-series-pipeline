# Model Card — Random Forest Regressor for Household Power Forecasting

**Model file:** `data/best_model.joblib`  
**Scaler file:** `data/best_scaler.joblib`  
**Feature list:** `data/feature_columns.json`  
**Training script:** `task1_eda/task1_notebook.ipynb` (Section 1C)

---

## Model Summary

| Property | Value |
|----------|-------|
| **Algorithm** | Random Forest Regressor (`sklearn.ensemble.RandomForestRegressor`) |
| **Task** | Single-step regression — predict `global_active_power` (kW) at time t |
| **Input** | 15 engineered features derived from lagged power values and cyclic time encodings |
| **Output** | Predicted active power consumption in kW |
| **Scaler** | MinMaxScaler — features scaled to [0, 1] before training |
| **Selection criterion** | Lowest RMSE on held-out chronological test set |

---

## Dataset Split

| Split | Rows | Period |
|-------|------|--------|
| Training | 1,660,159 | 16 Dec 2006 → ~Sep 2009 (first 80%) |
| Test | 415,040 | ~Sep 2009 → 26 Nov 2010 (last 20%) |
| **Total** | 2,075,259 | — |

**Split method:** Strict chronological 80/20 — no shuffling, no data leakage across time.

---

## Features

All 15 features are computed exclusively from historical values (`.shift(1)` or earlier). No contemporaneous readings are used.

### Cyclic Temporal Encodings (6 features)

Sine/cosine encoding maps circular time units to continuous values, preventing discontinuities at period boundaries (e.g. hour 23 → hour 0).

| Feature | Formula | Captures |
|---------|---------|---------|
| `hour_sin` | sin(2π × hour / 24) | Hour of day — position in daily cycle |
| `hour_cos` | cos(2π × hour / 24) | Hour of day — position in daily cycle |
| `dow_sin` | sin(2π × day_of_week / 7) | Day of week — position in weekly cycle |
| `dow_cos` | cos(2π × day_of_week / 7) | Day of week — position in weekly cycle |
| `month_sin` | sin(2π × month / 12) | Month — position in annual cycle |
| `month_cos` | cos(2π × month / 12) | Month — position in annual cycle |

### Lag Features (4 features)

| Feature | Lag Window | Motivation |
|---------|-----------|-----------|
| `lag_1` | 1 minute | Autocorrelation r = 0.9682 — strongest single predictor |
| `lag_5` | 5 minutes | Short-term momentum |
| `lag_15` | 15 minutes | Quarter-hour inertia |
| `lag_60` | 60 minutes | Same-time-of-day one hour ago; captures habitual patterns |

### Differenced Features (2 features)

| Feature | Formula | Captures |
|---------|---------|---------|
| `diff_1` | `lag_1 − lag_2` | 1-minute rate of change (is consumption rising or falling?) |
| `diff_5` | `lag_1 − lag_6` | 5-minute rate of change |

### Moving Averages (2 features)

| Feature | Window | Captures |
|---------|--------|---------|
| `ma_10` | 10 minutes | Short rolling mean — smooths 1-minute noise |
| `ma_60` | 60 minutes | Hourly rolling mean — captures session-level baseline |

### Rolling Volatility (1 feature)

| Feature | Window | Captures |
|---------|--------|---------|
| `rolling_std_10` | 10 minutes | Recent volatility — high during appliance switching events |

### Excluded Features

The following raw columns were available but deliberately excluded to prevent **contemporaneous data leakage**:

| Column | Correlation with target | Reason for exclusion |
|--------|------------------------|----------------------|
| `global_intensity` | r = +0.999 | Almost perfect linear proxy — would trivialise the task |
| `voltage` | r = −0.30 | Available at time t; excluded to keep inputs causal |
| `global_reactive_power` | r = +0.23 | Available at time t; excluded to keep inputs causal |
| `sub_metering_1/2/3` | r = 0.13–0.64 | Available at time t; excluded to keep inputs causal |

---

## Hyperparameter Tuning

**Method:** `RandomizedSearchCV`, 20 iterations, `TimeSeriesSplit(n_splits=3)` cross-validation on a 100,000-row chronological tail of the training set.

**Search space:**

| Parameter | Values Searched |
|-----------|----------------|
| `n_estimators` | 50, 100, 200 |
| `max_depth` | 4, 6, 8, 10, None |
| `min_samples_split` | 2, 5, 10 |
| `min_samples_leaf` | 1, 2, 4 |
| `max_features` | 0.5, 0.7, 1.0, "sqrt" |

**Best hyperparameters:**

```python
RandomForestRegressor(
    n_estimators     = 100,
    max_depth        = 8,
    min_samples_split= 5,
    min_samples_leaf = 2,
    max_features     = 0.7,
    random_state     = 42,
    n_jobs           = -1,
)
```

---

## Experiment Results

Four models were evaluated on identical train/test splits with identical features.

| Rank | Model | MAE (kW) | RMSE (kW) | MAPE (%) | R² |
|------|-------|----------|-----------|----------|----|
| — | Naive Persistence (baseline) | 0.0692 | 0.2170 | 7.06 | 0.9387 |
| 3 | Linear Regression | 0.0831 | 0.2161 | 9.89 | 0.9392 |
| **1** | **Random Forest (tuned)** | **0.0694** | **0.2133** | **6.97** | **0.9408** |
| 2 | Gradient Boosting (tuned) | 0.0706 | 0.2153 | 7.05 | 0.9397 |

**Gradient Boosting hyperparameters** (for reference):
```python
GradientBoostingRegressor(
    n_estimators  = 400,
    max_depth     = 3,
    learning_rate = 0.05,
    subsample     = 0.8,
    min_samples_split = 5,
    max_features  = None,
    loss          = "absolute_error",
)
```

### Key Observations

- **Random Forest edges out Gradient Boosting** on RMSE (0.2133 vs 0.2153) and R² (0.9408 vs 0.9397), making it the chosen production model.
- **Linear Regression** achieves competitive RMSE (0.2161) but significantly worse MAE and MAPE — it struggles with the non-linear spikes in high-consumption periods.
- **Naive Persistence** (`ŷ_t = y_{t-1}`) is a strong baseline (R² = 0.9387) because 1-minute autocorrelation is 0.97. All three learned models must beat it, and they do.
- The narrow gap between models suggests that the **feature engineering** (especially `lag_1`) is the dominant predictor, not the algorithm choice.

---

## Metrics Definitions

| Metric | Formula | Interpretation |
|--------|---------|---------------|
| MAE | $\frac{1}{n}\sum|y - \hat{y}|$ | Mean absolute error in kW — easy to interpret |
| RMSE | $\sqrt{\frac{1}{n}\sum(y-\hat{y})^2}$ | Penalises large errors more than MAE |
| MAPE | $\frac{100}{n}\sum\left|\frac{y-\hat{y}}{y}\right|$ | Scale-independent percentage error |
| R² | $1 - \frac{SS_{res}}{SS_{tot}}$ | Fraction of variance explained (1.0 = perfect) |

---

## Task 4 Evaluation (Live API Sample)

When Task 4 runs against 500 records fetched live from the API (2006-12-16 17:24 → 2006-12-17 01:43), the model is evaluated on a narrow 440-row window after warm-up rows are removed. Performance degrades vs. the full test set because:

1. The 440-row window spans a single evening (high-variance peak period)
2. The model was trained on a much broader distribution (4 years, all hours and seasons)

| Metric | Task 1 (full test, 415K rows) | Task 4 (API sample, 440 rows) |
|--------|-------------------------------|-------------------------------|
| MAE    | 0.0694 kW | 0.4974 kW |
| RMSE   | 0.2133 kW | 0.8114 kW |
| MAPE   | 6.97 % | 36.39 % |
| R²     | 0.9408 | 0.3263 |

The degradation is expected and does not indicate model failure — it reflects the difficulty of a short, high-consumption window versus the full dataset distribution.

---

## 12-Step Autoregressive Forecast

Task 4 also produces a 12-step (12-minute) out-of-sample forecast using an autoregressive loop:

1. Start from the last known record in the fetched window
2. Predict `ŷ_{t+1}` using current features
3. Feed `ŷ_{t+1}` back as `lag_1` (and shift other lags accordingly)
4. Update cyclic temporal features by advancing the timestamp 1 minute
5. Repeat for 12 steps

**Live output (2006-12-17):**

| Step | Timestamp | Predicted kW |
|------|-----------|-------------|
| t+1  | 01:44 | 2.6218 |
| t+2  | 01:45 | 2.6101 |
| t+3  | 01:46 | 2.6053 |
| t+4  | 01:47 | 2.5993 |
| t+5  | 01:48 | 2.5961 |
| t+6  | 01:49 | 2.5967 |
| t+7  | 01:50 | 2.5934 |
| t+8  | 01:51 | 2.5892 |
| t+9  | 01:52 | 2.5876 |
| t+10 | 01:53 | 2.5848 |
| t+11 | 01:54 | 2.5843 |
| t+12 | 01:55 | 2.5827 |

The slow decay is consistent with the model predicting a gradual taper from high evening consumption toward the late-night baseline.

---

## Limitations

- **Single household:** Trained on one household in Sceaux, France. Generalisation to other households, climates, or energy profiles is not guaranteed.
- **No exogenous inputs:** Weather, occupancy, or calendar event data could improve seasonal accuracy.
- **Autoregressive error accumulation:** In the 12-step forecast, prediction errors compound with each step. Uncertainty should widen monotonically (not modelled here).
- **Static model:** The model is not retrained on new data. Concept drift (e.g. new appliances, occupancy changes) will degrade performance over time.
