# Volume Booster Bot

A high-performance, adaptive Solana volume boosting bot designed to generate organic-looking trading activity on various DEXs.

## Features

- **Multi-DEX Support**: Compatible with Raydium (AMM, CLMM, CPMM), PumpFun, Meteora, Orca, and more.
- **Adaptive Trading Modes**:
  - `adaptive`: Adjusts buy/sell ratio based on market sentiment and volatility.
  - `buy_first` / `sell_first`: Structured sequences.
  - `random`: Unpredictable patterns for stealth.
- **Advanced Stealth**:
  - Jittered amounts and delays.
  - Wallet rotation with dynamic cooldowns.
  - Randomized "personalities" (Flipper, Hodler, Momentum) for each wallet.
- **MEV Protection**: Integrated Jito support to prevent sandwich attacks.
- **Risk Management**:
  - Circuit breakers for consecutive failures or high failure rates.
  - Emergency stop-loss based on master wallet balance.
  - Pre-trade checks for liquidity and price impact via Birdeye API.
- **Wallet Management**:
  - Auto-generates and funds thousands of wallets.
  - Smart rebalancing to top up low balances and consolidate dust.
  - Parallel withdrawal system to drain all funds back to a sink wallet.

## Prerequisites

- Node.js v16+
- A Solana RPC URL (Helius, QuickNode, or Alchemy recommended for mainnet).
- A funded master wallet (private key).

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd volume-booster-bot
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Configure the bot:**
   - Copy the example configuration:
     ```bash
     cp .env.example .env
     ```
   - Edit `.env` and fill in your details:
     - `RPC_URL`: Your Solana RPC endpoint.
     - `MASTER_PRIVATE_KEY`: The private key of the wallet that will fund the trading wallets.
     - `MEME_COIN_MINT`: The mint address of the token you want to trade.
     - `MARKET`: The DEX to trade on (e.g., `RAYDIUM_AMM`).
     - `ENABLE_JITO`: Set to `true` for MEV protection.

## Usage

### Start the Bot
```bash
npm start
```

### Keyboard Triggers
While the bot is running, you can use the following keys in the terminal:
- **`w`**: Trigger an immediate withdrawal of all funds from trading wallets to the sink wallet and stop the bot.

## Configuration Options

See `.env.example` for a full list of options. Key parameters include:

- **`TRADE_MODE`**: Strategy to use (`adaptive`, `random`, etc.).
- **`CONCURRENCY`**: Number of parallel wallets to process.
- **`SWAP_AMOUNT`**: Base size of each trade in SOL.
- **`PARTIAL_SELL_ENABLED`**: If true, sells random % of tokens instead of 100%.

## Disclaimer

This software is for educational purposes only. Use at your own risk. The authors are not responsible for any financial losses incurred while using this bot. Always test on Devnet first.
