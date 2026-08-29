use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnboardingJob {
    pub job_id: String,
    pub tenant_id: String,
    pub account_id: String,
    pub attempt: u8,
    pub failure: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDisposition {
    Retry,
    DeadLetter { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobError {
    InvalidAttempt(u8),
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAttempt(value) => write!(f, "attempt must be at least 1, got {value}"),
        }
    }
}

impl std::error::Error for JobError {}

pub fn classify(job: &OnboardingJob, max_attempts: u8) -> Result<JobDisposition, JobError> {
    if job.attempt == 0 {
        return Err(JobError::InvalidAttempt(job.attempt));
    }
    if job.attempt >= max_attempts {
        Ok(JobDisposition::DeadLetter {
            reason: format!(
                "onboarding exhausted {max_attempts} attempts: {}",
                job.failure
            ),
        })
    } else {
        Ok(JobDisposition::Retry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_tenant_onboarding_moves_to_dead_letter() {
        let job = OnboardingJob {
            job_id: "onboard-1042".into(),
            tenant_id: "tenant-acme".into(),
            account_id: "acct-7".into(),
            attempt: 3,
            failure: "account provisioning rejected".into(),
        };

        assert_eq!(
            classify(&job, 3).unwrap(),
            JobDisposition::DeadLetter {
                reason: "onboarding exhausted 3 attempts: account provisioning rejected".into()
            }
        );
    }
}
