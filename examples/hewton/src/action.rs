//! `Action<I, Message>` — a leaf `update` return that bundles a
//! `Task<Message>` with an optional `Instruction` the parent interprets.
//! Useful when a leaf needs a structured escape hatch for effects only
//! the parent can perform (e.g. against parent-owned resources).

#![allow(dead_code)]

use iced::Task;
use std::fmt;

#[must_use = "`Action.task` must be returned to the runtime to take effect; normally in your `update` or `new` functions."]
pub struct Action<I, Message> {
    pub instruction: Option<I>,
    pub task: Task<Message>,
}

impl<I, Message> Action<I, Message> {
    #[allow(dead_code)]
    pub fn none() -> Self {
        Self {
            instruction: None,
            task: Task::none(),
        }
    }

    #[allow(dead_code)]
    pub fn instruction(instruction: impl Into<I>) -> Self {
        Self {
            instruction: Some(instruction.into()),
            task: Task::none(),
        }
    }

    #[allow(dead_code)]
    pub fn task(task: Task<Message>) -> Self {
        Self {
            instruction: None,
            task,
        }
    }

    #[allow(dead_code)]
    pub fn map<N>(self, f: impl Fn(Message) -> N + Send + 'static) -> Action<I, N>
    where
        Message: Send + 'static,
        N: Send + 'static,
    {
        Action {
            instruction: self.instruction,
            task: self.task.map(f),
        }
    }

    #[allow(dead_code)]
    pub fn map_instruction<N>(self, f: impl Fn(I) -> N + Send + 'static) -> Action<N, Message>
    where
        I: Send + 'static,
        N: Send + 'static,
    {
        Action {
            instruction: self.instruction.map(f),
            task: self.task,
        }
    }

    #[allow(dead_code)]
    pub fn with_instruction(mut self, instruction: I) -> Self {
        self.instruction = Some(instruction);
        self
    }
}

impl<I: fmt::Debug, Message> fmt::Debug for Action<I, Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Action")
            .field("instruction", &self.instruction)
            .finish()
    }
}

impl<I, Message> From<Task<Message>> for Action<I, Message> {
    fn from(task: Task<Message>) -> Self {
        Self::task(task)
    }
}

impl<T, Message> Action<T, Message> {
    #[allow(dead_code)]
    pub fn into<I>(self) -> Action<I, Message>
    where
        T: Into<I>,
    {
        Action {
            instruction: self.instruction.map(Into::into),
            task: self.task,
        }
    }
}
