import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import random

class DQN(nn.Module):
    def __init__(self, state_dim, action_dim):
        super(DQN, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, action_dim)
        )

    def forward(self, x):
        return self.net(x)

class DQNLearner:
    def __init__(self, n_states=2, n_actions=5, lr=1e-3, gamma=0.9, epsilon=0.1):
        self.model = DQN(n_states, n_actions)
        self.optimizer = optim.Adam(self.model.parameters(), lr=lr)
        self.gamma = gamma
        self.epsilon = epsilon
        self.memory = []
        self.batch_size = 64

    def select_action(self, state):
        if random.random() < self.epsilon:
            return random.randint(0, 2)
        state = torch.FloatTensor(state).unsqueeze(0)
        q_values = self.model(state)
        return torch.argmax(q_values).item()

    def store(self, transition):
        self.memory.append(transition)
        if len(self.memory) > 5000:
            self.memory.pop(0)

    def train_step(self):
        if len(self.memory) < self.batch_size:
            return
        batch = random.sample(self.memory, self.batch_size)
        states, actions, rewards, next_states = zip(*batch)
        states = torch.FloatTensor(states)
        actions = torch.LongTensor(actions).unsqueeze(1)
        rewards = torch.FloatTensor(rewards).unsqueeze(1)
        next_states = torch.FloatTensor(next_states)

        q_vals = self.model(states).gather(1, actions)
        next_q_vals = self.model(next_states).max(1)[0].unsqueeze(1)
        targets = rewards + self.gamma * next_q_vals

        loss = nn.MSELoss()(q_vals, targets)
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()
