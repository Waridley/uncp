//! Similarity providers stub

pub trait SimilarityProvider {
	fn similarity_type(&self) -> &'static str {
		"stub"
	}
}
