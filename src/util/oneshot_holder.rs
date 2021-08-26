use std::collections::vec_deque::Drain;
use std::collections::VecDeque;
// use std::error::Error;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Receiver;

// type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct OneShotHolder<E> {
    size: usize,
    inflight: VecDeque<tokio::sync::oneshot::Receiver<Result<(), E>>>,
}

impl<E> OneShotHolder<E> {
    pub fn new(size: usize) -> OneShotHolder<E> {
        OneShotHolder {
            size,
            inflight: VecDeque::with_capacity(size),
        }
    }

    pub async fn add(
        &mut self,
        item: tokio::sync::oneshot::Receiver<Result<(), E>>,
    ) -> Result<Result<(), E>, RecvError> {
        let t;
        if self.inflight.len() >= self.size {
            let fut = self.inflight.pop_front().unwrap();
            t = fut.await;
        } else {
            t = Ok(Ok(()));
        }
        self.inflight.push_back(item);
        t
    }

    pub fn drain(&mut self) -> Drain<'_, Receiver<Result<(), E>>> {
        self.inflight.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn one_result() {
        let mut holder = OneShotHolder::new(2);

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), Error>>();
        let t = holder.add(rx).await;
    }
}
