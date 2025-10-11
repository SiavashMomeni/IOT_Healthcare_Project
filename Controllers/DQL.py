# Controllers/DQL.py
import random
import collections
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim

# --------- hyperparams (قابل تنظیم) ----------
STATE_DIM = 6
ACTION_DIM = 2
LR = 1e-3
GAMMA = 0.99
EPS_START = 1.0
EPS_END = 0.05
EPS_DECAY = 0.995
BATCH_SIZE = 64
REPLAY_CAP = 20000
TARGET_UPDATE = 200     # steps to sync target net
MIN_REPLAY_FOR_TRAIN = 200
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# ----------------------------------------------

class DQN(nn.Module):
    def __init__(self, state_dim=STATE_DIM, action_dim=ACTION_DIM):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 128),
            nn.ReLU(),
            nn.Linear(128, action_dim)
        )

    def forward(self, x):
        return self.net(x)

class ReplayBuffer:
    def __init__(self, capacity=REPLAY_CAP):
        self.buf = collections.deque(maxlen=capacity)

    def add(self, s, a, r, s2, done):
        self.buf.append((s,a,r,s2,done))

    def sample(self, batch_size=BATCH_SIZE):
        batch = random.sample(self.buf, batch_size)
        s,a,r,s2,d = zip(*batch)
        return np.vstack(s), np.array(a), np.array(r, dtype=np.float32), np.vstack(s2), np.array(d, dtype=np.float32)

    def __len__(self):
        return len(self.buf)

class DeepQLearner:
    def __init__(self, state_dim=STATE_DIM, action_dim=ACTION_DIM, lr=LR, gamma=GAMMA):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.model = DQN(state_dim, action_dim).to(DEVICE)
        self.target = DQN(state_dim, action_dim).to(DEVICE)
        self.target.load_state_dict(self.model.state_dict())
        self.optim = optim.Adam(self.model.parameters(), lr=lr)
        self.gamma = gamma

        self.replay = ReplayBuffer()
        self.epsilon = EPS_START
        self.step = 0

    def select_action(self, state, eval_mode=False):
        # state: numpy array (state_dim,)
        if (not eval_mode) and (random.random() < self.epsilon):
            return random.randint(0, self.action_dim-1)
        state_t = torch.FloatTensor(state).unsqueeze(0).to(DEVICE)
        with torch.no_grad():
            q = self.model(state_t)
        return int(torch.argmax(q, dim=1).item())

    def store_transition(self, s,a,r,s2,done):
        self.replay.add(s,a,r,s2,done)
        self.step += 1
        # decay epsilon
        if self.epsilon > EPS_END:
            self.epsilon *= EPS_DECAY

    def train_step(self):
        if len(self.replay) < max(MIN_REPLAY_FOR_TRAIN, BATCH_SIZE):
            return None
        s,a,r,s2,done = self.replay.sample(BATCH_SIZE)
        s = torch.FloatTensor(s).to(DEVICE)
        a = torch.LongTensor(a).to(DEVICE)
        r = torch.FloatTensor(r).to(DEVICE)
        s2 = torch.FloatTensor(s2).to(DEVICE)
        done = torch.FloatTensor(done).to(DEVICE)

        q_pred = self.model(s).gather(1, a.unsqueeze(1)).squeeze(1)
        with torch.no_grad():
            q_next = self.target(s2).max(1)[0]
        q_target = r + self.gamma * q_next * (1.0 - done)
        loss = nn.functional.mse_loss(q_pred, q_target)

        self.optim.zero_grad()
        loss.backward()
        self.optim.step()

        # target sync
        if self.step % TARGET_UPDATE == 0:
            self.target.load_state_dict(self.model.state_dict())
        return loss.item()
