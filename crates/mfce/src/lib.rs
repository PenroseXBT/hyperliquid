#![forbid(unsafe_code)]

//! Isolated bounded MFCE quantile models, lifecycle, and delayed observations.
//!
//! Native LightGBM handles deliberately never leave this crate. Training returns
//! bounded model strings so callers can run it on a blocking worker and move only
//! Rust-owned data back to the engine runtime. This crate has no execution adapter.

pub mod delayed;
pub mod lifecycle;

use lightgbm3::{Booster, Dataset};
use serde_json::{json, Value};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Version of the fixed MFCE1 model recipe and feature ABI enforced here.
pub const ENGINE_VERSION: &str = "MFCE1-LIGHTGBM-4.6.0-QPAIR-V2";
/// Maximum number of condition features accepted by a model.
pub const MAX_FEATURE_COUNT: usize = 96;
/// Minimum pooled rows required before native training is allowed.
pub const MIN_TRAINING_ROWS: usize = 32;
/// Maximum retained rows accepted by one training call.
pub const MAX_TRAINING_ROWS: usize = 8_192;
/// Maximum rows scored by one prediction call.
pub const MAX_PREDICTION_ROWS: usize = 8_192;
/// Per-quantile serialized model limit. Two such strings are the full model pair.
pub const MAX_MODEL_STRING_BYTES: usize = 2 * 1024 * 1024;

const NUM_ITERATIONS: u32 = 96;
const NUM_LEAVES: u32 = 15;
const MAX_DEPTH: u32 = 4;
const MIN_DATA_IN_LEAF: u32 = 8;
const MODEL_SEED: u32 = 1_729;
const PREDICTION_PARAMETERS: &str = "num_threads=1";

/// One of the two conditional executable after-cost RemainingEdge quantiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantile {
    Q10,
    Q50,
}

impl Quantile {
    fn alpha(self) -> f64 {
        match self {
            Self::Q10 => 0.10,
            Self::Q50 => 0.50,
        }
    }
}

impl Display for Quantile {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Q10 => formatter.write_str("q10"),
            Self::Q50 => formatter.write_str("q50"),
        }
    }
}

/// Validated, bounded serialized q10/q50 models.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuantileModelStrings {
    feature_count: usize,
    q10: String,
    q50: String,
}

impl QuantileModelStrings {
    /// Construct a model pair from durable strings, validating cheap bounds before
    /// any native parser is invoked.
    pub fn new(
        feature_count: usize,
        q10: impl Into<String>,
        q50: impl Into<String>,
    ) -> Result<Self, MfceError> {
        validate_feature_count(feature_count)?;
        let q10 = q10.into();
        let q50 = q50.into();
        validate_model_string(Quantile::Q10, &q10)?;
        validate_model_string(Quantile::Q50, &q50)?;
        Ok(Self {
            feature_count,
            q10,
            q50,
        })
    }

    pub fn feature_count(&self) -> usize {
        self.feature_count
    }

    pub fn q10(&self) -> &str {
        &self.q10
    }

    pub fn q50(&self) -> &str {
        &self.q50
    }

    pub fn total_bytes(&self) -> usize {
        self.q10.len() + self.q50.len()
    }

    pub fn into_parts(self) -> (usize, String, String) {
        (self.feature_count, self.q10, self.q50)
    }
}

/// One row of conditional executable after-cost RemainingEdge predictions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuantilePrediction {
    pub q10: f64,
    pub q50: f64,
}

/// A loaded pair of native boosters. It is intentionally thread-confined by the
/// underlying LightGBM handle types.
pub struct QuantileModelPair {
    feature_count: usize,
    q10: Booster,
    q50: Booster,
}

impl fmt::Debug for QuantileModelPair {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuantileModelPair")
            .field("feature_count", &self.feature_count)
            .field("q10_iterations", &self.q10.num_iterations())
            .field("q50_iterations", &self.q50.num_iterations())
            .finish_non_exhaustive()
    }
}

impl QuantileModelPair {
    /// Parse validated model strings and verify their embedded feature ABI.
    pub fn from_model_strings(models: &QuantileModelStrings) -> Result<Self, MfceError> {
        validate_feature_count(models.feature_count)?;
        validate_model_string(Quantile::Q10, &models.q10)?;
        validate_model_string(Quantile::Q50, &models.q50)?;

        let q10 = load_booster(Quantile::Q10, &models.q10)?;
        validate_loaded_booster(Quantile::Q10, &q10, models.feature_count)?;
        let q50 = load_booster(Quantile::Q50, &models.q50)?;
        validate_loaded_booster(Quantile::Q50, &q50, models.feature_count)?;

        Ok(Self {
            feature_count: models.feature_count,
            q10,
            q50,
        })
    }

    pub fn feature_count(&self) -> usize {
        self.feature_count
    }

    /// Serialize both incumbent boosters back into bounded durable strings.
    pub fn model_strings(&self) -> Result<QuantileModelStrings, MfceError> {
        let q10 = save_booster(Quantile::Q10, &self.q10)?;
        let q50 = save_booster(Quantile::Q50, &self.q50)?;
        QuantileModelStrings::new(self.feature_count, q10, q50)
    }

    /// Score one feature row.
    pub fn predict_one(&self, features: &[f64]) -> Result<QuantilePrediction, MfceError> {
        if features.len() != self.feature_count {
            return Err(MfceError::PredictionShapeMismatch {
                value_count: features.len(),
                feature_count: self.feature_count,
            });
        }
        let mut predictions = self.predict(features)?;
        predictions
            .pop()
            .ok_or(MfceError::UnexpectedPredictionCount {
                quantile: Quantile::Q10,
                expected: 1,
                actual: 0,
            })
    }

    /// Score a bounded row-major feature matrix.
    pub fn predict(&self, features: &[f64]) -> Result<Vec<QuantilePrediction>, MfceError> {
        let rows = validate_prediction_matrix(features, self.feature_count)?;
        let feature_count = self.feature_count as i32;
        let q10 = self
            .q10
            .predict_with_params(features, feature_count, true, PREDICTION_PARAMETERS)
            .map_err(|error| native_error("predict q10", error))?;
        let q50 = self
            .q50
            .predict_with_params(features, feature_count, true, PREDICTION_PARAMETERS)
            .map_err(|error| native_error("predict q50", error))?;

        validate_predictions(Quantile::Q10, rows, &q10)?;
        validate_predictions(Quantile::Q50, rows, &q50)?;
        Ok(q10
            .into_iter()
            .zip(q50)
            .map(|(q10, q50)| QuantilePrediction { q10, q50 })
            .collect())
    }
}

/// Train the fixed deterministic q10/q50 LightGBM recipe and return only bounded
/// model strings. `features` is a row-major matrix and weights, when supplied,
/// must be finite and strictly positive.
pub fn train_quantile_pair(
    features: &[f64],
    remaining_net_edges: &[f64],
    weights: Option<&[f64]>,
    feature_count: usize,
) -> Result<QuantileModelStrings, MfceError> {
    let rows = validate_training_data(features, remaining_net_edges, weights, feature_count)?;
    let labels = to_f32_labels(remaining_net_edges)?;
    let weights = weights.map(to_f32_weights).transpose()?;

    debug_assert_eq!(rows, labels.len());
    let q10 = train_booster(
        Quantile::Q10,
        features,
        &labels,
        weights.as_deref(),
        feature_count,
    )?;
    let q10 = save_booster(Quantile::Q10, &q10)?;
    let q50 = train_booster(
        Quantile::Q50,
        features,
        &labels,
        weights.as_deref(),
        feature_count,
    )?;
    let q50 = save_booster(Quantile::Q50, &q50)?;

    QuantileModelStrings::new(feature_count, q10, q50)
}

fn train_booster(
    quantile: Quantile,
    features: &[f64],
    labels: &[f32],
    weights: Option<&[f32]>,
    feature_count: usize,
) -> Result<Booster, MfceError> {
    let mut dataset = Dataset::from_slice(features, labels, feature_count as i32, true)
        .map_err(|error| native_error("create training dataset", error))?;
    if let Some(weights) = weights {
        dataset
            .set_weights(weights)
            .map_err(|error| native_error("set training weights", error))?;
    }
    Booster::train(dataset, &fixed_parameters(quantile))
        .map_err(|error| native_error("train quantile booster", error))
}

fn fixed_parameters(quantile: Quantile) -> Value {
    json!({
        "alpha": quantile.alpha(),
        "bagging_fraction": 1.0,
        "bagging_freq": 0,
        "bagging_seed": MODEL_SEED,
        "boosting": "gbdt",
        "deterministic": true,
        "extra_seed": MODEL_SEED,
        "feature_fraction": 1.0,
        "feature_fraction_seed": MODEL_SEED,
        "force_col_wise": true,
        "lambda_l1": 0.0,
        "lambda_l2": 1.0,
        "learning_rate": 0.05,
        "max_depth": MAX_DEPTH,
        "metric": "quantile",
        "min_data_in_leaf": MIN_DATA_IN_LEAF,
        "min_gain_to_split": 0.0,
        "num_iterations": NUM_ITERATIONS,
        "num_leaves": NUM_LEAVES,
        "num_threads": 1,
        "objective": "quantile",
        "seed": MODEL_SEED,
        "verbosity": -1
    })
}

fn load_booster(quantile: Quantile, model: &str) -> Result<Booster, MfceError> {
    Booster::from_string(model)
        .map_err(|error| native_error_for_quantile("load model string", quantile, error))
}

fn save_booster(quantile: Quantile, booster: &Booster) -> Result<String, MfceError> {
    let model = booster
        .save_string()
        .map_err(|error| native_error_for_quantile("save model string", quantile, error))?;
    validate_model_string(quantile, &model)?;
    Ok(model)
}

fn validate_loaded_booster(
    quantile: Quantile,
    booster: &Booster,
    expected_feature_count: usize,
) -> Result<(), MfceError> {
    let reported = booster.num_features();
    if reported <= 0 {
        return Err(MfceError::InvalidModelFeatureCount { quantile, reported });
    }
    let actual = reported as usize;
    if actual != expected_feature_count {
        return Err(MfceError::ModelFeatureMismatch {
            quantile,
            expected: expected_feature_count,
            actual,
        });
    }
    if booster.num_iterations() <= 0 {
        return Err(MfceError::ModelHasNoIterations { quantile });
    }
    Ok(())
}

fn validate_feature_count(feature_count: usize) -> Result<(), MfceError> {
    if !(1..=MAX_FEATURE_COUNT).contains(&feature_count) {
        return Err(MfceError::InvalidFeatureCount {
            actual: feature_count,
            maximum: MAX_FEATURE_COUNT,
        });
    }
    Ok(())
}

fn validate_training_data(
    features: &[f64],
    labels: &[f64],
    weights: Option<&[f64]>,
    feature_count: usize,
) -> Result<usize, MfceError> {
    validate_feature_count(feature_count)?;
    if features.len() % feature_count != 0 {
        return Err(MfceError::TrainingShapeMismatch {
            value_count: features.len(),
            feature_count,
        });
    }
    let rows = features.len() / feature_count;
    if !(MIN_TRAINING_ROWS..=MAX_TRAINING_ROWS).contains(&rows) {
        return Err(MfceError::InvalidTrainingRowCount {
            actual: rows,
            minimum: MIN_TRAINING_ROWS,
            maximum: MAX_TRAINING_ROWS,
        });
    }
    if labels.len() != rows {
        return Err(MfceError::LabelCountMismatch {
            expected: rows,
            actual: labels.len(),
        });
    }
    if let Some(weights) = weights {
        if weights.len() != rows {
            return Err(MfceError::WeightCountMismatch {
                expected: rows,
                actual: weights.len(),
            });
        }
    }
    if let Some(index) = features.iter().position(|value| !value.is_finite()) {
        return Err(MfceError::NonFiniteFeature { index });
    }
    if let Some(index) = labels.iter().position(|value| !value.is_finite()) {
        return Err(MfceError::NonFiniteLabel { index });
    }
    if let Some(weights) = weights {
        if let Some(index) = weights.iter().position(|value| !value.is_finite()) {
            return Err(MfceError::NonFiniteWeight { index });
        }
        if let Some(index) = weights.iter().position(|value| *value <= 0.0) {
            return Err(MfceError::NonPositiveWeight { index });
        }
    }
    Ok(rows)
}

fn validate_prediction_matrix(features: &[f64], feature_count: usize) -> Result<usize, MfceError> {
    validate_feature_count(feature_count)?;
    if features.len() % feature_count != 0 {
        return Err(MfceError::PredictionShapeMismatch {
            value_count: features.len(),
            feature_count,
        });
    }
    let rows = features.len() / feature_count;
    if !(1..=MAX_PREDICTION_ROWS).contains(&rows) {
        return Err(MfceError::InvalidPredictionRowCount {
            actual: rows,
            maximum: MAX_PREDICTION_ROWS,
        });
    }
    if let Some(index) = features.iter().position(|value| !value.is_finite()) {
        return Err(MfceError::NonFiniteFeature { index });
    }
    Ok(rows)
}

fn validate_model_string(quantile: Quantile, model: &str) -> Result<(), MfceError> {
    if model.is_empty() {
        return Err(MfceError::EmptyModelString { quantile });
    }
    if model.len() > MAX_MODEL_STRING_BYTES {
        return Err(MfceError::ModelStringTooLarge {
            quantile,
            actual: model.len(),
            maximum: MAX_MODEL_STRING_BYTES,
        });
    }
    if model.as_bytes().contains(&0) {
        return Err(MfceError::ModelStringContainsNul { quantile });
    }
    Ok(())
}

fn to_f32_labels(labels: &[f64]) -> Result<Vec<f32>, MfceError> {
    labels
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let converted = *value as f32;
            if converted.is_finite() {
                Ok(converted)
            } else {
                Err(MfceError::LabelOutOfF32Range { index })
            }
        })
        .collect()
}

fn to_f32_weights(weights: &[f64]) -> Result<Vec<f32>, MfceError> {
    weights
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let converted = *value as f32;
            if converted.is_finite() && converted > 0.0 {
                Ok(converted)
            } else {
                Err(MfceError::WeightOutOfF32Range { index })
            }
        })
        .collect()
}

fn validate_predictions(
    quantile: Quantile,
    expected: usize,
    predictions: &[f64],
) -> Result<(), MfceError> {
    if predictions.len() != expected {
        return Err(MfceError::UnexpectedPredictionCount {
            quantile,
            expected,
            actual: predictions.len(),
        });
    }
    if let Some(index) = predictions.iter().position(|value| !value.is_finite()) {
        return Err(MfceError::NonFinitePrediction { quantile, index });
    }
    Ok(())
}

fn native_error(operation: &'static str, error: lightgbm3::Error) -> MfceError {
    MfceError::LightGbm {
        operation,
        message: error.to_string(),
    }
}

fn native_error_for_quantile(
    operation: &'static str,
    quantile: Quantile,
    error: lightgbm3::Error,
) -> MfceError {
    MfceError::LightGbm {
        operation,
        message: format!("{quantile}: {error}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MfceError {
    InvalidFeatureCount {
        actual: usize,
        maximum: usize,
    },
    InvalidTrainingRowCount {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    InvalidPredictionRowCount {
        actual: usize,
        maximum: usize,
    },
    TrainingShapeMismatch {
        value_count: usize,
        feature_count: usize,
    },
    PredictionShapeMismatch {
        value_count: usize,
        feature_count: usize,
    },
    LabelCountMismatch {
        expected: usize,
        actual: usize,
    },
    WeightCountMismatch {
        expected: usize,
        actual: usize,
    },
    NonFiniteFeature {
        index: usize,
    },
    NonFiniteLabel {
        index: usize,
    },
    NonFiniteWeight {
        index: usize,
    },
    NonPositiveWeight {
        index: usize,
    },
    LabelOutOfF32Range {
        index: usize,
    },
    WeightOutOfF32Range {
        index: usize,
    },
    EmptyModelString {
        quantile: Quantile,
    },
    ModelStringTooLarge {
        quantile: Quantile,
        actual: usize,
        maximum: usize,
    },
    ModelStringContainsNul {
        quantile: Quantile,
    },
    InvalidModelFeatureCount {
        quantile: Quantile,
        reported: i32,
    },
    ModelFeatureMismatch {
        quantile: Quantile,
        expected: usize,
        actual: usize,
    },
    ModelHasNoIterations {
        quantile: Quantile,
    },
    UnexpectedPredictionCount {
        quantile: Quantile,
        expected: usize,
        actual: usize,
    },
    NonFinitePrediction {
        quantile: Quantile,
        index: usize,
    },
    LightGbm {
        operation: &'static str,
        message: String,
    },
}

impl Display for MfceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFeatureCount { actual, maximum } => write!(
                formatter,
                "feature count must be between 1 and {maximum}, got {actual}"
            ),
            Self::InvalidTrainingRowCount {
                actual,
                minimum,
                maximum,
            } => write!(
                formatter,
                "training rows must be between {minimum} and {maximum}, got {actual}"
            ),
            Self::InvalidPredictionRowCount { actual, maximum } => write!(
                formatter,
                "prediction rows must be between 1 and {maximum}, got {actual}"
            ),
            Self::TrainingShapeMismatch {
                value_count,
                feature_count,
            } => write!(
                formatter,
                "training matrix has {value_count} values, not a multiple of {feature_count} features"
            ),
            Self::PredictionShapeMismatch {
                value_count,
                feature_count,
            } => write!(
                formatter,
                "prediction matrix has {value_count} values, not a multiple of {feature_count} features"
            ),
            Self::LabelCountMismatch { expected, actual } => {
                write!(formatter, "expected {expected} labels, got {actual}")
            }
            Self::WeightCountMismatch { expected, actual } => {
                write!(formatter, "expected {expected} weights, got {actual}")
            }
            Self::NonFiniteFeature { index } => {
                write!(formatter, "feature at flat index {index} is not finite")
            }
            Self::NonFiniteLabel { index } => {
                write!(formatter, "label at index {index} is not finite")
            }
            Self::NonFiniteWeight { index } => {
                write!(formatter, "weight at index {index} is not finite")
            }
            Self::NonPositiveWeight { index } => {
                write!(formatter, "weight at index {index} is not positive")
            }
            Self::LabelOutOfF32Range { index } => {
                write!(formatter, "label at index {index} is outside finite f32 range")
            }
            Self::WeightOutOfF32Range { index } => {
                write!(formatter, "weight at index {index} is outside positive finite f32 range")
            }
            Self::EmptyModelString { quantile } => {
                write!(formatter, "{quantile} model string is empty")
            }
            Self::ModelStringTooLarge {
                quantile,
                actual,
                maximum,
            } => write!(
                formatter,
                "{quantile} model string is {actual} bytes; maximum is {maximum}"
            ),
            Self::ModelStringContainsNul { quantile } => {
                write!(formatter, "{quantile} model string contains an interior NUL")
            }
            Self::InvalidModelFeatureCount { quantile, reported } => write!(
                formatter,
                "{quantile} model reports invalid feature count {reported}"
            ),
            Self::ModelFeatureMismatch {
                quantile,
                expected,
                actual,
            } => write!(
                formatter,
                "{quantile} model has {actual} features; expected {expected}"
            ),
            Self::ModelHasNoIterations { quantile } => {
                write!(formatter, "{quantile} model has no boosting iterations")
            }
            Self::UnexpectedPredictionCount {
                quantile,
                expected,
                actual,
            } => write!(
                formatter,
                "{quantile} returned {actual} predictions; expected {expected}"
            ),
            Self::NonFinitePrediction { quantile, index } => {
                write!(formatter, "{quantile} prediction at index {index} is not finite")
            }
            Self::LightGbm { operation, message } => {
                write!(formatter, "LightGBM {operation} failed: {message}")
            }
        }
    }
}

impl Error for MfceError {}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_FEATURE_COUNT: usize = 5;

    fn synthetic_training_data(rows: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        let mut features = Vec::with_capacity(rows * TEST_FEATURE_COUNT);
        let mut labels = Vec::with_capacity(rows);
        let mut weights = Vec::with_capacity(rows);
        for row in 0..rows {
            let conviction = row as f64 / (rows - 1) as f64;
            let volatility = 0.15 + ((row * 17) % 31) as f64 / 100.0;
            let liquidity = 0.25 + ((row * 11) % 37) as f64 / 50.0;
            let transition = (row % 4) as f64;
            let technical_context = ((row * 7) % 23) as f64 / 11.0 - 1.0;
            features.extend_from_slice(&[
                conviction,
                volatility,
                liquidity,
                transition,
                technical_context,
            ]);
            let deterministic_residual = ((row * 37) % 19) as f64 / 10_000.0 - 0.0009;
            labels.push(
                -0.012 + 0.028 * conviction - 0.006 * volatility + 0.003 * liquidity
                    - 0.001 * transition
                    + 0.002 * technical_context
                    + deterministic_residual,
            );
            weights.push(0.5 + (row % 7) as f64 / 5.0);
        }
        (features, labels, weights)
    }

    #[test]
    fn rejects_invalid_training_inputs_before_native_calls() {
        let (mut features, labels, weights) = synthetic_training_data(MIN_TRAINING_ROWS);
        assert!(matches!(
            train_quantile_pair(
                &features[..features.len() - 1],
                &labels,
                None,
                TEST_FEATURE_COUNT
            ),
            Err(MfceError::TrainingShapeMismatch { .. })
        ));
        assert!(matches!(
            train_quantile_pair(
                &features,
                &labels[..labels.len() - 1],
                None,
                TEST_FEATURE_COUNT
            ),
            Err(MfceError::LabelCountMismatch { .. })
        ));
        assert!(matches!(
            train_quantile_pair(
                &features,
                &labels,
                Some(&weights[..weights.len() - 1]),
                TEST_FEATURE_COUNT
            ),
            Err(MfceError::WeightCountMismatch { .. })
        ));
        features[3] = f64::NAN;
        assert!(matches!(
            train_quantile_pair(&features, &labels, None, TEST_FEATURE_COUNT),
            Err(MfceError::NonFiniteFeature { index: 3 })
        ));
    }

    #[test]
    fn model_string_constructor_enforces_bounds_and_nul_rejection() {
        assert!(matches!(
            QuantileModelStrings::new(TEST_FEATURE_COUNT, "", "valid"),
            Err(MfceError::EmptyModelString {
                quantile: Quantile::Q10
            })
        ));
        assert!(matches!(
            QuantileModelStrings::new(TEST_FEATURE_COUNT, "bad\0model", "valid"),
            Err(MfceError::ModelStringContainsNul {
                quantile: Quantile::Q10
            })
        ));
        let oversized = "x".repeat(MAX_MODEL_STRING_BYTES + 1);
        assert!(matches!(
            QuantileModelStrings::new(TEST_FEATURE_COUNT, "valid", oversized),
            Err(MfceError::ModelStringTooLarge {
                quantile: Quantile::Q50,
                ..
            })
        ));
    }

    #[test]
    fn trains_deterministically_and_round_trips_both_quantiles() {
        let (features, labels, weights) = synthetic_training_data(192);
        let first =
            train_quantile_pair(&features, &labels, Some(&weights), TEST_FEATURE_COUNT).unwrap();
        let second =
            train_quantile_pair(&features, &labels, Some(&weights), TEST_FEATURE_COUNT).unwrap();
        assert_eq!(first, second);
        assert_ne!(first.q10(), first.q50());
        assert!(first.q10().len() <= MAX_MODEL_STRING_BYTES);
        assert!(first.q50().len() <= MAX_MODEL_STRING_BYTES);

        let pair = QuantileModelPair::from_model_strings(&first).unwrap();
        let prediction_rows = [11_usize, 93, 171]
            .into_iter()
            .flat_map(|row| {
                let start = row * TEST_FEATURE_COUNT;
                features[start..start + TEST_FEATURE_COUNT].iter().copied()
            })
            .collect::<Vec<_>>();
        let predictions = pair.predict(&prediction_rows).unwrap();
        assert_eq!(predictions.len(), 3);
        assert!(predictions
            .iter()
            .all(|prediction| prediction.q10.is_finite() && prediction.q50.is_finite()));
        assert!(predictions
            .iter()
            .any(|prediction| prediction.q10 < prediction.q50));

        let persisted_again = pair.model_strings().unwrap();
        let reloaded = QuantileModelPair::from_model_strings(&persisted_again).unwrap();
        assert_eq!(predictions, reloaded.predict(&prediction_rows).unwrap());
        assert_eq!(
            predictions[0],
            reloaded
                .predict_one(&prediction_rows[..TEST_FEATURE_COUNT])
                .unwrap()
        );

        let wrong_feature_abi = QuantileModelStrings::new(
            TEST_FEATURE_COUNT + 1,
            first.q10().to_owned(),
            first.q50().to_owned(),
        )
        .unwrap();
        assert!(matches!(
            QuantileModelPair::from_model_strings(&wrong_feature_abi),
            Err(MfceError::ModelFeatureMismatch {
                quantile: Quantile::Q10,
                expected,
                actual
            }) if expected == TEST_FEATURE_COUNT + 1 && actual == TEST_FEATURE_COUNT
        ));

        let mut non_finite = prediction_rows[..TEST_FEATURE_COUNT].to_vec();
        non_finite[2] = f64::INFINITY;
        assert!(matches!(
            pair.predict_one(&non_finite),
            Err(MfceError::NonFiniteFeature { index: 2 })
        ));
    }
}
