use saas_dead_letter_operator::infrai_queue::{json_escape, InfraiError, QueueClient};
use saas_dead_letter_operator::onboarding_job::{
    classify, JobDisposition, JobError, OnboardingJob,
};
use std::env;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

#[derive(Debug)]
enum CliError {
    Usage,
    Job(JobError),
    Infrai(InfraiError),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(f, "usage: dlqctl fail <job-id> <tenant-id> <account-id> <attempt> <failure> | dlqctl inspect"),
            Self::Job(e) => e.fmt(f),
            Self::Infrai(e) => e.fmt(f),
        }
    }
}
impl From<JobError> for CliError {
    fn from(value: JobError) -> Self {
        Self::Job(value)
    }
}
impl From<InfraiError> for CliError {
    fn from(value: InfraiError) -> Self {
        Self::Infrai(value)
    }
}

fn main() {
    if let Err(error) = block_on(run()) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), CliError> {
    let args: Vec<String> = env::args().skip(1).collect();
    let client = QueueClient::from_env()?;
    match args.first().map(String::as_str) {
        Some("fail") if args.len() == 6 => {
            let job = OnboardingJob {
                job_id: args[1].clone(),
                tenant_id: args[2].clone(),
                account_id: args[3].clone(),
                attempt: args[4].parse().map_err(|_| CliError::Usage)?,
                failure: args[5].clone(),
            };
            match classify(&job, 3)? {
                JobDisposition::Retry => {
                    println!("retry job={} attempt={}", job.job_id, job.attempt)
                }
                JobDisposition::DeadLetter { reason } => {
                    let payload = format!(
                        "{{\"kind\":\"tenant_onboarding_failed\",\"job_id\":\"{}\",\"tenant_id\":\"{}\",\"account_id\":\"{}\",\"attempt\":{},\"reason\":\"{}\"}}",
                        json_escape(&job.job_id), json_escape(&job.tenant_id), json_escape(&job.account_id), job.attempt, json_escape(&reason));
                    client
                        .publish_dead_letter(
                            &payload,
                            &format!("dlq:{}:{}", job.job_id, job.attempt),
                        )
                        .await?;
                    println!(
                        "dead-lettered job={} tenant={} account={}",
                        job.job_id, job.tenant_id, job.account_id
                    );
                }
            }
        }
        Some("inspect") if args.len() == 1 => {
            for message in client.consume_dead_letters(10, 60).await? {
                println!(
                    "message={} payload={}",
                    message.message_id, message.payload_json
                );
                client
                    .acknowledge(
                        &message.message_id,
                        &format!("admin-ack:{}", message.message_id),
                    )
                    .await?;
                println!("acknowledged message={}", message.message_id);
            }
        }
        _ => return Err(CliError::Usage),
    }
    Ok(())
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => std::thread::park(),
        }
    }
}
