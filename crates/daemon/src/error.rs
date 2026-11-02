use thiserror::Error;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to initialize logging: {0}")]
    Tracing(BoxError),

    #[error(transparent)]
    Core(#[from] shade_core::Error),
}
