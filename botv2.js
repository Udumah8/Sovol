import { config } from 'dotenv';
config();
import axios from 'axios';
import {
  Connection,
  Keypair,
  PublicKey,

  LAMPORTS_PER_SOL,
  Transaction,
  SystemProgram,
} from '@solana/web3.js';
import { SolanaTrade } from 'solana-trade';
import {
  getAssociatedTokenAddressSync,
  getAccount,

  createCloseAccountInstruction,
} from '@solana/spl-token';
import fs from 'fs';
import jsonfile from 'jsonfile';
import pLimit from 'p-limit';
import { setTimeout as delay } from 'timers/promises';

// --- STEALTH ENHANCEMENT: Wallet Seasoning ---
const USDC_MINT = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
const BURN_ADDRESS = new PublicKey('1ncinerator11111111111111111111111111111111');
// --- END STEALTH ENHANCEMENT ---

class VolumeBoosterBot {
  constructor() {
    this.connection = new Connection(process.env.RPC_URL, 'confirmed');
    this.trader = new SolanaTrade(process.env.RPC_URL);
    this.memeMint = new PublicKey(process.env.MEME_COIN_MINT);
    this.solMint = new PublicKey('So11111111111111111111111111111111111111112');
    this.memeSymbol = process.env.MEME_COIN_SYMBOL || 'solana'; // Fallback to SOL for API
    this.market = process.env.MARKET;
    const supportedMarkets = [
      'PUMP_FUN',
      'PUMP_SWAP',
      'RAYDIUM_AMM',
      'RAYDIUM_CLMM',
      'RAYDIUM_CPMM',
      'RAYDIUM_LAUNCHPAD',
      'ORCA_WHIRLPOOL',
      'METEORA_DLMM',
      'METEORA_DAMM_V1',
      'METEORA_DAMM_V2',
      'METEORA_DBC',
      'MOONIT',
      'HEAVEN',
      'SUGAR',
      'BOOP_FUN'
    ];
    if (!this.market || !supportedMarkets.includes(this.market)) {
      throw new Error(`Invalid MARKET in .env. Must be one of: ${supportedMarkets.join(', ')}`);
    }
    this.tradeMode = process.env.TRADE_MODE || 'adaptive'; // Enhanced: adaptive, buy_first, sell_first, buy_only, sell_only, random
    const supportedModes = ['adaptive', 'buy_first', 'sell_first', 'buy_only', 'sell_only', 'random'];
    if (!supportedModes.includes(this.tradeMode)) {
      throw new Error(`Invalid TRADE_MODE in .env. Must be one of: ${supportedModes.join(', ')}`);
    }
    this.buyProb = parseFloat(process.env.BUY_PROB) || 0.5; // New: Base buy probability (0.0 to 1.0) for flexible biasing in adaptive/random modes
    if (this.buyProb < 0 || this.buyProb > 1) {
      throw new Error('BUY_PROB must be between 0.0 and 1.0');
    }
    this.numActionsPerCycle = parseInt(process.env.NUM_ACTIONS_PER_CYCLE) || 2; // New: Number of actions per wallet cycle (for random mode flexibility)
    this.baseSwapAmount = parseFloat(process.env.SWAP_AMOUNT) * LAMPORTS_PER_SOL;
    this.baseDelayMs = parseInt(process.env.DELAY_MS);
    this.jitterPct = parseInt(process.env.JITTER_PCT) / 100;
    this.sessionPauseMin = parseInt(process.env.SESSION_PAUSE_MIN);
    this.behaviorProfile = process.env.BEHAVIOR_PROFILE || 'retail';
    this.maxWallets = process.env.MAX_WALLETS === 'all' ? Infinity : parseInt(process.env.MAX_WALLETS);
    this.numWalletsToGenerate = parseInt(process.env.NUM_WALLETS_TO_GENERATE) || 10;
    this.fundAmount = parseFloat(process.env.FUND_AMOUNT) * LAMPORTS_PER_SOL;
    // --- STEALTH ENHANCEMENT: Master Wallet Decoupling ---
    this.sinkPrivateKey = process.env.SINK_PRIVATE_KEY ? JSON.parse(process.env.SINK_PRIVATE_KEY) : null;
    this.sinkKeypair = this.sinkPrivateKey ? Keypair.fromSecretKey(new Uint8Array(this.sinkPrivateKey)) : null;
    const relayerPrivateKeys = process.env.RELAYER_PRIVATE_KEYS ? JSON.parse(process.env.RELAYER_PRIVATE_KEYS) : [];
    if (relayerPrivateKeys.length === 0 && process.env.ENABLE_REBALANCING !== 'false') {
      // We don't throw an error, to allow running without rebalancing.
      console.warn('⚠️  Stealth Warning: RELAYER_PRIVATE_KEYS is not set in .env. P2P rebalancing will be disabled.');
    }
    this.relayerKeypairs = relayerPrivateKeys.map(pk => Keypair.fromSecretKey(new Uint8Array(pk)));
    // --- END STEALTH ENHANCEMENT ---
    this.minWithdrawSol = parseFloat(process.env.MIN_WITHDRAW_SOL || '0.001') * LAMPORTS_PER_SOL;
    this.concurrency = parseInt(process.env.CONCURRENCY) || 50;
    this.batchSize = parseInt(process.env.BATCH_SIZE) || 100;
    this.retryAttempts = parseInt(process.env.RETRY_ATTEMPTS) || 3;
    this.twapParts = parseInt(process.env.TWAP_PARTS) || 5;
    this.twapMaxDelay = parseInt(process.env.TWAP_MAX_DELAY) || 10000;
    this.volThreshold = parseFloat(process.env.VOL_THRESHOLD) || 0.05;
    this.rampCycles = parseInt(process.env.RAMP_CYCLES) || 30;
    this.isDevnet = process.env.RPC_URL.includes('devnet');
    this.wallets = [];
    this.currentWalletIndex = 0;
    this.cycleCount = 0;
    this.totalVolume = 0;
    this.isRunning = false;
    this.limiter = pLimit(this.concurrency);
    this.sentimentScore = 50; // Default neutral
    this.lastSentimentFetch = 0;
    this.recentPrices = []; // For vol calc
    this.walletPersonalities = {}; // Per-wallet behavior

    // Wallet Rotation Strategy
    this.walletCooldowns = new Map(); // Track last trade time per wallet
    this.walletTradeCount = new Map(); // Track total trades per wallet
    this.minCooldownMs = parseInt(process.env.MIN_WALLET_COOLDOWN_MS) || 300000; // 5 min default
    this.maxCooldownMs = parseInt(process.env.MAX_WALLET_COOLDOWN_MS) || 1800000; // 30 min default
    this.shuffleWallets = process.env.SHUFFLE_WALLETS !== 'false'; // Default true
    this.rotationPool = []; // Available wallets for next batch

    // Birdeye API Integration
    this.birdeyeApiKey = process.env.BIRDEYE_API_KEY || '';
    this.birdeyeBaseUrl = 'https://public-api.birdeye.so';
    this.useBirdeye = !!this.birdeyeApiKey;
    this.marketData = {
      price: 0,
      priceChange24h: 0,
      volume24h: 0,
      liquidity: 0,
      priceImpact: 0,
      lastUpdate: 0,
    };
    this.minLiquidity = parseFloat(process.env.MIN_LIQUIDITY_USD) || 5000; // $5k min
    this.maxPriceImpact = parseFloat(process.env.MAX_PRICE_IMPACT_PCT) || 5; // 5% max
    this.marketDataCacheDuration = 60000; // 1 min cache

    // Smart Rebalancing
    this.enableRebalancing = process.env.ENABLE_REBALANCING !== 'false'; // Default true
    this.minWalletBalance = parseFloat(process.env.MIN_WALLET_BALANCE_SOL || '0.005') * LAMPORTS_PER_SOL;
    this.targetWalletBalance = parseFloat(process.env.TARGET_WALLET_BALANCE_SOL || '0.05') * LAMPORTS_PER_SOL;
    this.rebalanceInterval = parseInt(process.env.REBALANCE_INTERVAL_CYCLES) || 50; // Every 50 cycles
    this.dustThreshold = parseFloat(process.env.DUST_THRESHOLD_SOL || '0.001') * LAMPORTS_PER_SOL;

    // Circuit Breakers
    this.enableCircuitBreaker = process.env.ENABLE_CIRCUIT_BREAKER !== 'false'; // Default true
    this.maxConsecutiveFailures = parseInt(process.env.MAX_CONSECUTIVE_FAILURES) || 10;
    this.maxFailureRate = parseFloat(process.env.MAX_FAILURE_RATE_PCT) || 50; // 50%
    this.failureRateWindow = parseInt(process.env.FAILURE_RATE_WINDOW) || 20; // Last 20 trades
    this.emergencyStopLoss = parseFloat(process.env.EMERGENCY_STOP_LOSS_PCT) || 30; // 30% loss
    this.consecutiveFailures = 0;
    this.recentTrades = []; // Track recent trade results
    this.initialSinkBalance = 0;

    if (this.sinkKeypair) console.log(`Sink Wallet: ${this.sinkKeypair.publicKey.toBase58()}`);
    if (this.relayerKeypairs.length > 0) console.log(`Relayer Wallets: ${this.relayerKeypairs.length} loaded`);
    if (this.useBirdeye) console.log(`Birdeye API: Enabled`);
    this.walletFile = process.env.WALLET_FILE || 'wallets.json';

    // --- STEALTH ENHANCEMENT: Wallet Seasoning ---
    this.enableSeasoning = process.env.ENABLE_SEASONING === 'true'; // Default false
    this.seasoningMinTxs = parseInt(process.env.SEASONING_MIN_TXS) || 3;
    this.seasoningMaxTxs = parseInt(process.env.SEASONING_MAX_TXS) || 10;
    this.seasoningDelayMs = parseInt(process.env.SEASONING_DELAY_MS) || 5000;
    // --- END STEALTH ENHANCEMENT ---

    // --- MERGED FEATURES (FINAL2025) ---
    // Jito MEV Protection
    this.enableJito = process.env.ENABLE_JITO !== 'false'; // Default true
    this.jitoFee = parseFloat(process.env.JITO_PRIORITY_FEE_SOL || '0.002');
    this.jitoTipBuy = parseFloat(process.env.JITO_TIP_SOL_BUY || '0.0012');
    this.jitoTipSell = parseFloat(process.env.JITO_TIP_SOL_SELL || '0.0018');

    // Auto-Scaling
    this.autoScale = process.env.AUTO_SCALE_CONCURRENCY !== 'false'; // Default true
    if (this.autoScale) {
      // Will be recalculated in ensureWallets based on actual count
      this.concurrency = 10;
      this.batchSize = 10;
    }

    // Partial Sell
    this.partialSellEnabled = process.env.PARTIAL_SELL_ENABLED !== 'false'; // Default true
    this.partialSellMin = parseFloat(process.env.PARTIAL_SELL_MIN_PCT || '22');
    this.partialSellMax = parseFloat(process.env.PARTIAL_SELL_MAX_PCT || '68');

    // Memory Efficient Tracking
    this.allPubkeys = new Set();
    this.funded = new Set();
    this.activeWallets = []; // Replaces this.wallets for current batch
    this.wallets = []; // Deprecated, kept empty or minimal

    // Keyboard Triggers
    this.enableKeyboard = process.env.ENABLE_KEYBOARD_TRIGGERS !== 'false';
  }

  async withRetry(fn, walletName = '') {
    let lastError;
    for (let i = 0; i < this.retryAttempts; i++) {
      try {
        return await fn();
      } catch (error) {
        lastError = error;
        const waitMs = Math.pow(2, i) * 1000; // Exponential backoff
        console.warn(`${walletName} Retry ${i + 1}/${this.retryAttempts} after ${waitMs}ms: ${error.message}`);
        await delay(waitMs);
      }
    }
    throw lastError;
  }

  async loadOrGenerateWallets() {
    let data = [];
    if (fs.existsSync(this.walletFile)) {
      data = jsonfile.readFileSync(this.walletFile);
      // Normalize data
      data = data.map(w => ({
        pubkey: w.pubkey || w.publicKey || Keypair.fromSecretKey(new Uint8Array(w.privateKey)).publicKey.toBase58(),
        privateKey: w.privateKey,
        name: w.name || `Wallet`,
        isSeasoned: w.isSeasoned || false // --- STEALTH ENHANCEMENT ---
      }));
      this.allPubkeys = new Set(data.map(w => w.pubkey));
      console.log(`Found ${data.length.toLocaleString()} existing wallets`);
    }

    if (data.length < this.numWalletsToGenerate) {
      console.log(`Generating ${this.numWalletsToGenerate.toLocaleString()} wallets...`);
      const generationBatch = 1000;
      while (data.length < this.numWalletsToGenerate) {
        const batch = [];
        const remaining = this.numWalletsToGenerate - data.length;
        const size = Math.min(generationBatch, remaining);
        for (let i = 0; i < size; i++) {
          const kp = Keypair.generate();
          const w = {
            pubkey: kp.publicKey.toBase58(),
            privateKey: Array.from(kp.secretKey),
            name: `Wallet${data.length + i + 1}`,
            isSeasoned: false // --- STEALTH ENHANCEMENT ---
          };
          batch.push(w);
          this.allPubkeys.add(w.pubkey);
        }
        data.push(...batch);
        jsonfile.writeFileSync(this.walletFile, data, { spaces: 2 });
        console.log(`${data.length.toLocaleString()}/${this.numWalletsToGenerate.toLocaleString()}`);
        await delay(10);
      }
    }

    // Auto-Scale
    if (this.autoScale) {
      this.concurrency = Math.min(50, Math.max(3, Math.floor(data.length / 200) + 3));
      this.batchSize = Math.min(20, Math.max(2, Math.floor(data.length / 300) + 2));
      this.limiter = pLimit(this.concurrency);
      console.log(`Auto-Scaled: Concurrency=${this.concurrency}, BatchSize=${this.batchSize}`);
    }

    // Auto Fund
    await this.fundWalletsInParallel(data);
  }

  async fundWalletsInParallel(walletsData) {
    if (this.relayerKeypairs.length === 0) {
      console.log('No relayer wallets configured, skipping initial funding check.');
      return;
    }

    console.log(`Checking funding for ${walletsData.length} wallets...`);
    const toCheck = walletsData.filter(w => !this.funded.has(w.pubkey));

    const fundBatchSize = 50;
    for (let i = 0; i < toCheck.length; i += fundBatchSize) {
      const batch = toCheck.slice(i, i + fundBatchSize);
      await Promise.all(batch.map(async (w) => {
        if (this.funded.has(w.pubkey)) return;
        const kp = Keypair.fromSecretKey(new Uint8Array(w.privateKey));

        try {
          let bal = await this.connection.getBalance(kp.publicKey);
          if (bal >= this.fundAmount * 0.8) {
            this.funded.add(w.pubkey);
            return;
          }

          let remaining = this.fundAmount - bal;
          const parts = Math.floor(Math.random() * 4) + 1;

          for (let p = 0; p < parts && remaining > 0.005 * LAMPORTS_PER_SOL; p++) {
            const part = Math.floor(remaining * (0.6 + Math.random() * 0.8) / (parts - p));
            try {
              // --- STEALTH ENHANCEMENT: Use random relayer for funding ---
              const relayer = this.relayerKeypairs[Math.floor(Math.random() * this.relayerKeypairs.length)];
              const tx = new Transaction().add(SystemProgram.transfer({
                fromPubkey: relayer.publicKey,
                toPubkey: kp.publicKey,
                lamports: part,
              }));
              const sig = await this.connection.sendTransaction(tx, [relayer], { skipPreflight: true });
              // --- END STEALTH ENHANCEMENT ---
              await this.connection.confirmTransaction(sig, 'confirmed');
              remaining -= part;
              console.log(`Funded ${w.name} part ${p + 1}/${parts}: ${(part / LAMPORTS_PER_SOL).toFixed(4)} SOL`);
              await delay(1000 + Math.random() * 2000);
            } catch (e) {
              await delay(2000);
            }
          }
          this.funded.add(w.pubkey);
        } catch (e) { }
      }));
      console.log(`Funding progress: ${Math.min(i + fundBatchSize, toCheck.length)}/${toCheck.length}`);
    }
  }

  async fundSingleWallet(wallet) {
    // Kept for compatibility if called individually, but fundWalletsInParallel is preferred
    return this.withRetry(async () => {
      const pubkey = wallet.keypair.publicKey;
      const currentBalance = await this.connection.getBalance(pubkey);
      if (currentBalance >= this.fundAmount) return;

      if (this.relayerKeypairs.length === 0) throw new Error('No relayer wallets for funding');
      // --- STEALTH ENHANCEMENT: Use random relayer for funding ---
      const relayer = this.relayerKeypairs[Math.floor(Math.random() * this.relayerKeypairs.length)];
      const tx = new Transaction().add(SystemProgram.transfer({
        fromPubkey: relayer.publicKey,
        toPubkey: pubkey,
        lamports: this.fundAmount,
      }));
      const sig = await this.connection.sendTransaction(tx, [relayer], { skipPreflight: true });
      // --- END STEALTH ENHANCEMENT ---
      await this.connection.confirmTransaction(sig, 'confirmed');
    }, wallet.name);
  }

  async loadActiveBatch() {
    const data = jsonfile.readFileSync(this.walletFile);
    const now = Date.now();

    // Filter by cooldowns
    const ready = data.filter(w => {
      const key = w.pubkey || w.publicKey;
      const lastTrade = this.walletCooldowns.get(key) || 0;
      const cooldown = this.getWalletCooldown(key);
      return now - lastTrade >= cooldown;
    });

    if (ready.length === 0) return [];

    // Shuffle
    const shuffled = this.shuffleArray(ready);

    // Select batch
    const selected = shuffled.slice(0, this.batchSize);

    // Hydrate keypairs only for active batch
    this.activeWallets = selected.map(w => ({
      keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
      name: w.name || (w.pubkey ? w.pubkey.slice(0, 6) : 'Wallet'),
      pubkey: w.pubkey || w.publicKey
    }));

    // Compatibility alias
    // this.wallets = this.activeWallets; // Deprecated alias

    console.log(`Loaded batch: ${this.activeWallets.length} wallets (Pool: ${ready.length}/${data.length})`);
    return this.activeWallets;
  }

  shuffleArray(array) {
    const shuffled = [...array];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
  }

  getNextWalletBatch() {
    return this.activeWallets; // Already loaded by runLoop calling loadActiveBatch
  }

  getWalletCooldown(walletKey) {
    // Dynamic cooldown based on trade frequency
    const tradeCount = this.walletTradeCount.get(walletKey) || 0;

    // More trades = longer cooldown (anti-pattern detection)
    const cooldownMultiplier = 1 + (tradeCount % 5) * 0.2; // Increases every 5 trades
    const baseCooldown = this.minCooldownMs + Math.random() * (this.maxCooldownMs - this.minCooldownMs);

    return Math.floor(baseCooldown * cooldownMultiplier);
  }

  markWalletUsed(wallet) {
    const walletKey = wallet.keypair.publicKey.toBase58();
    this.walletCooldowns.set(walletKey, Date.now());
    this.walletTradeCount.set(walletKey, (this.walletTradeCount.get(walletKey) || 0) + 1);
  }

  getJitteredValue(base, isAmount = false) {
    const variance = (Math.random() - 0.5) * 2 * this.jitterPct;
    let value = base * (1 + variance);
    if (isAmount) value = Math.max(value, LAMPORTS_PER_SOL * 0.0001);
    return Math.floor(value);
  }

  getRandomDelay() {
    return this.getJitteredValue(this.baseDelayMs, false);
  }

  async getProfileAdjustedAmount(wallet, isBuy) {
    let amount = this.getJitteredValue(this.baseSwapAmount, true);
    if (this.behaviorProfile === 'whale') amount *= 5;
    else if (this.behaviorProfile === 'mixed') amount *= (Math.random() < 0.3 ? 3 : 1);
    if (!isBuy) amount = await this.getCurrentMemeBalanceInLamports(wallet);
    return Math.min(amount, isBuy ? this.baseSwapAmount * 2 : amount);
  }

  async getCurrentMemeBalanceInLamports(wallet) {
    const tokenAccount = getAssociatedTokenAddressSync(this.memeMint, wallet.keypair.publicKey);
    try {
      const accountInfo = await getAccount(this.connection, tokenAccount);
      return Number(accountInfo.amount);
    } catch {
      return 0;
    }
  }

  async checkBalances(wallet) {
    const solBalance = await this.connection.getBalance(wallet.keypair.publicKey);
    if (solBalance < 0.01 * LAMPORTS_PER_SOL) {
      console.warn(`${wallet.name} low balance! Skipping.`);
      return false;
    }
    return true;
  }

  async fetchSentiment() {
    if (Date.now() - this.lastSentimentFetch < 3600000) return this.sentimentScore; // Hourly
    try {
      const res = await axios.get('https://api.alternative.me/fng/?limit=1'); // CMC Fear & Greed
      this.sentimentScore = parseInt(res.data.data[0].value);
      this.lastSentimentFetch = Date.now();
      console.log(`Sentiment updated: ${this.sentimentScore} (Fear & Greed Index)`);
    } catch (e) {
      console.error('Sentiment fetch failed:', e.message);
    }
    return this.sentimentScore;
  }

  async fetchVolatility() {
    // Use Birdeye if available, fallback to CoinGecko
    if (this.useBirdeye) {
      return this.calculateVolatilityFromBirdeye();
    }

    try {
      const res = await axios.get(`https://api.coingecko.com/api/v3/simple/price?ids=${this.memeSymbol}&vs_currencies=usd&include_24hr_change=false&include_24hr_vol=false&include_last_updated_at=true`);
      const price = res.data[this.memeSymbol].usd;
      this.recentPrices.push(price);
      if (this.recentPrices.length > 60) this.recentPrices.shift(); // 1h rolling
      if (this.recentPrices.length < 2) return 0;
      const returns = this.recentPrices.slice(1).map((p, i) => (p - this.recentPrices[i]) / this.recentPrices[i]);
      const vol = returns.reduce((a, b) => a + b * b, 0) / returns.length;
      return Math.sqrt(vol);
    } catch (e) {
      console.error('Vol fetch failed:', e.message);
      return 0;
    }
  }

  calculateVolatilityFromBirdeye() {
    // Calculate volatility from price change
    const priceChange = Math.abs(this.marketData.priceChange24h);
    return priceChange / 100; // Convert percentage to decimal
  }

  async fetchBirdeyeMarketData() {
    if (!this.useBirdeye) return false;

    const now = Date.now();
    if (now - this.marketData.lastUpdate < this.marketDataCacheDuration) {
      return true; // Use cached data
    }

    try {
      const headers = { 'X-API-KEY': this.birdeyeApiKey };
      const tokenAddress = this.memeMint.toBase58();

      // Fetch token overview (price, volume, liquidity)
      const overviewRes = await axios.get(
        `${this.birdeyeBaseUrl}/defi/token_overview?address=${tokenAddress}`,
        { headers }
      );

      if (overviewRes.data.success && overviewRes.data.data) {
        const data = overviewRes.data.data;
        this.marketData.price = data.price || 0;
        this.marketData.priceChange24h = data.priceChange24h || 0;
        this.marketData.volume24h = data.volume24h || 0;
        this.marketData.liquidity = data.liquidity || 0;
        this.marketData.lastUpdate = now;

        console.log(`📊 Birdeye: Price=$${this.marketData.price.toFixed(8)} | 24h: ${this.marketData.priceChange24h.toFixed(2)}% | Vol: $${(this.marketData.volume24h / 1000).toFixed(1)}k | Liq: $${(this.marketData.liquidity / 1000).toFixed(1)}k`);
        return true;
      }
    } catch (e) {
      console.error('Birdeye fetch failed:', e.message);
    }
    return false;
  }

  async estimatePriceImpact(amountInSol, isBuy) {
    if (!this.useBirdeye) return 0;

    try {
      const headers = { 'X-API-KEY': this.birdeyeApiKey };
      const tokenAddress = this.memeMint.toBase58();
      const amountInUsd = amountInSol * await this.getSolPriceUsd();

      // Use Birdeye's price check endpoint
      const res = await axios.get(
        `${this.birdeyeBaseUrl}/defi/price?address=${tokenAddress}&check_liquidity=true`,
        { headers }
      );

      if (res.data.success && res.data.data) {
        const liquidity = res.data.data.liquidity || this.marketData.liquidity;
        if (liquidity === 0) return 100; // No liquidity = 100% impact

        // Estimate impact: (trade size / liquidity) * 100
        const impact = (amountInUsd / liquidity) * 100;
        return Math.min(impact, 100); // Cap at 100%
      }
    } catch (e) {
      console.error('Price impact estimation failed:', e.message);
    }
    return 0;
  }

  async getSolPriceUsd() {
    try {
      const headers = this.birdeyeApiKey ? { 'X-API-KEY': this.birdeyeApiKey } : {};
      const res = await axios.get(
        `${this.birdeyeBaseUrl}/defi/price?address=So11111111111111111111111111111111111111112`,
        { headers }
      );
      if (res.data.success && res.data.data) {
        return res.data.data.value || 150; // Fallback to ~$150
      }
    } catch (e) {
      console.error('SOL price fetch failed:', e.message);
    }
    return 150; // Default fallback
  }

  async checkMarketConditions(amountInSol, isBuy) {
    if (!this.useBirdeye) return { safe: true, reason: '' };

    await this.fetchBirdeyeMarketData();

    // Check liquidity
    if (this.marketData.liquidity < this.minLiquidity) {
      return {
        safe: false,
        reason: `Low liquidity: $${this.marketData.liquidity.toFixed(0)} < $${this.minLiquidity}`,
      };
    }

    // Check price impact
    const priceImpact = await this.estimatePriceImpact(amountInSol, isBuy);
    if (priceImpact > this.maxPriceImpact) {
      return {
        safe: false,
        reason: `High price impact: ${priceImpact.toFixed(2)}% > ${this.maxPriceImpact}%`,
      };
    }

    return { safe: true, reason: '', priceImpact };
  }

  assignPersonalities() {
    const personalities = ['flipper', 'hodler', 'momentum'];
    this.allPubkeys.forEach(pubkey => {
      this.walletPersonalities[pubkey] = personalities[Math.floor(Math.random() * personalities.length)];
    });
  }

  getPersonality(pubkey) {
    const key = typeof pubkey === 'string' ? pubkey : pubkey.toBase58();
    return this.walletPersonalities[key] || 'flipper';
  }

  getSentimentBias() {
    const score = this.sentimentScore;
    if (score > 50) return 0.8; // High buy bias
    if (score < 30) return 0.3; // High sell bias
    return 0.5; // Neutral
  }

  async getAdaptiveAmount(wallet, isBuy, baseAmount) {
    // Fetch market data if using Birdeye
    if (this.useBirdeye) {
      await this.fetchBirdeyeMarketData();
    }

    const vol = await this.fetchVolatility();
    let amount = baseAmount;

    // Volatility adjustment
    if (vol > this.volThreshold) amount *= 0.5; // Scale down in vol
    else if (vol < 0.01) amount *= 1.5; // Scale up in calm

    // Liquidity-based adjustment (Birdeye)
    if (this.useBirdeye && this.marketData.liquidity > 0) {
      const liquidityRatio = this.marketData.liquidity / 10000; // Normalize to $10k
      if (liquidityRatio < 1) amount *= 0.5; // Reduce in low liquidity
      else if (liquidityRatio > 10) amount *= 1.2; // Increase in high liquidity
    }

    // Personality adjustment
    const personality = this.getPersonality(wallet.keypair.publicKey);
    if (personality === 'hodler' && isBuy) amount *= 2; // Bigger buys
    else if (personality === 'momentum') amount *= (await this.fetchSentiment()) / 50; // Scale with sentiment

    return Math.floor(Math.max(amount, LAMPORTS_PER_SOL * 0.0001));
  }

  getTradeActions(wallet) {
    if (this.tradeMode === 'buy_only') {
      return Array(this.numActionsPerCycle).fill(true); // Only buy, repeated
    } else if (this.tradeMode === 'sell_only') {
      return Array(this.numActionsPerCycle).fill(false); // Only sell, repeated
    } else if (this.tradeMode === 'buy_first') {
      return [true, ...Array(this.numActionsPerCycle - 1).fill(false)]; // Buy first, then sells
    } else if (this.tradeMode === 'sell_first') {
      return [false, ...Array(this.numActionsPerCycle - 1).fill(true)]; // Sell first, then buys
    } else if (this.tradeMode === 'random') {
      // Independent random buys/sells based on buyProb
      return Array.from({ length: this.numActionsPerCycle }, () => Math.random() < this.buyProb);
    } else { // adaptive
      // Determine overall direction, then sequence based on it
      const buyFirst = this.isBuyFirst(wallet);
      const baseActions = buyFirst ? [true, false] : [false, true];
      // Repeat or pad to numActionsPerCycle for flexibility
      const actions = [];
      for (let i = 0; i < this.numActionsPerCycle; i++) {
        actions.push(baseActions[i % baseActions.length]);
      }
      return actions;
    }
  }

  isBuyFirst(wallet) {
    // Enhanced with buyProb for more flexibility
    const sentimentBias = this.getSentimentBias();
    const personality = this.getPersonality(wallet.keypair.publicKey);
    let prob = this.buyProb * sentimentBias;
    if (personality === 'hodler') prob = Math.min(0.9, prob * 1.5); // Boost buy for hodlers
    else if (personality === 'flipper') prob = 0.5; // Ignore bias
    else if (personality === 'momentum') prob = Math.min(1.0, prob * 1.2); // Amplify
    return Math.random() < prob;
  }

  async twapSwap(isBuy, wallet, amount) {
    const partAmount = Math.floor(amount / this.twapParts);
    const remainder = amount % partAmount;
    const parts = Array(this.twapParts).fill(partAmount);
    parts[0] += remainder; // Distribute

    let anySuccess = false;
    for (let i = 0; i < parts.length; i++) {
      if (!this.isRunning) break;
      const part = await this.getAdaptiveAmount(wallet, isBuy, parts[i]); // Re-adjust per part
      if (part <= 0) continue;
      console.log(`${wallet.name} TWAP part ${i + 1}/${parts.length}: ${isBuy ? 'Buy' : 'Sell'} ~${(part / LAMPORTS_PER_SOL).toFixed(6)}`);
      const success = await this.performSingleSwap(isBuy, wallet, part);
      if (success) anySuccess = true;
      if (!success) break;
      if (i < parts.length - 1) {
        const partDelay = Math.random() * this.twapMaxDelay;
        await delay(partDelay);
      }
    }
    return anySuccess;
  }

  async performSingleSwap(isBuy, wallet, amount) { // Non-TWAP single
    try {
      const slippage = this.behaviorProfile === 'retail' ? 1 : 0.3; // %
      let sig;

      // Jito Config
      const jitoConfig = this.enableJito ? {
        sender: 'JITO',
        antimev: true,
        priorityFeeSol: this.jitoFee,
        tipAmountSol: isBuy ? this.jitoTipBuy : this.jitoTipSell
      } : {};

      if (isBuy) {
        const solAmount = amount / LAMPORTS_PER_SOL;
        sig = await this.trader.buy({
          market: this.market,
          wallet: wallet.keypair,
          mint: this.memeMint,
          amount: solAmount,
          slippage,
          ...jitoConfig
        });
      } else {
        sig = await this.trader.sell({
          market: this.market,
          wallet: wallet.keypair,
          mint: this.memeMint,
          amount, // raw tokens
          slippage,
          ...jitoConfig
        });
      }
      console.log(`${wallet.name} Swap TX: https://solscan.io/tx/${sig}`);

      // Track success for circuit breaker
      this.recordTradeResult(true);

      return !!sig;
    } catch (error) {
      console.error(`${wallet.name} Single swap failed: ${error.message}`);

      // Track failure for circuit breaker
      this.recordTradeResult(false);

      return false;
    }
  }

  // Circuit Breaker Methods
  recordTradeResult(success) {
    if (!this.enableCircuitBreaker) return;

    this.recentTrades.push(success);
    if (this.recentTrades.length > this.failureRateWindow * 2) {
      this.recentTrades = this.recentTrades.slice(-this.failureRateWindow);
    }

    if (success) {
      this.consecutiveFailures = 0;
    } else {
      this.consecutiveFailures++;
    }
  }

  async checkCircuitBreakers() {
    if (!this.enableCircuitBreaker) return { tripped: false, reason: '' };

    // Check consecutive failures
    if (this.consecutiveFailures >= this.maxConsecutiveFailures) {
      return {
        tripped: true,
        reason: `🔴 CIRCUIT BREAKER: ${this.consecutiveFailures} consecutive failures`,
      };
    }

    // Check failure rate
    if (this.recentTrades.length >= this.failureRateWindow) {
      const failures = this.recentTrades.filter(r => !r).length;
      const failureRate = (failures / this.recentTrades.length) * 100;

      if (failureRate > this.maxFailureRate) {
        return {
          tripped: true,
          reason: `🔴 CIRCUIT BREAKER: ${failureRate.toFixed(1)}% failure rate (last ${this.failureRateWindow} trades)`,
        };
      }
    }

    // Check emergency stop loss (if master wallet exists)
    if (this.sinkKeypair && this.initialSinkBalance > 0) {
      const currentBalance = await this.connection.getBalance(this.sinkKeypair.publicKey);
      const loss = ((this.initialSinkBalance - currentBalance) / this.initialSinkBalance) * 100;

      if (loss > this.emergencyStopLoss) {
        return {
          tripped: true,
          reason: `🔴 CIRCUIT BREAKER: ${loss.toFixed(1)}% loss from initial balance`,
        };
      }
    }

    return { tripped: false, reason: '' };
  }

  // --- STEALTH ENHANCEMENT: P2P Rebalancing Methods ---
  async rebalanceWallets() {
    if (!this.enableRebalancing || this.relayerKeypairs.length === 0) {
      console.log('⚖️  Rebalancing skipped (disabled or no relayer wallets for top-up)');
      return;
    }

    console.log('\n⚖️  === P2P Stealth Rebalancing Started ===');

    const walletsToCheck = this.activeWallets;
    if (walletsToCheck.length < 2) {
      console.log('Not enough active wallets to perform P2P rebalancing.');
      return;
    }
    const walletBalances = [];
    let totalWalletBalance = 0;

    // 1. Collect balances
    for (const wallet of walletsToCheck) {
      const balance = await this.connection.getBalance(wallet.keypair.publicKey);
      walletBalances.push({ wallet, balance });
      totalWalletBalance += balance;
    }

    const avgBalance = totalWalletBalance / walletsToCheck.length;
    console.log(`Average active wallet balance: ${(avgBalance / LAMPORTS_PER_SOL).toFixed(4)} SOL`);

    // 2. Identify wallets needing funds and those with surplus
    let needsFunding = walletBalances.filter(wb => wb.balance < this.minWalletBalance).sort((a, b) => a.balance - b.balance);
    let hasSurplus = walletBalances.filter(wb => wb.balance > this.targetWalletBalance * 1.5).sort((a, b) => b.balance - a.balance);

    console.log(`Wallets needing funds: ${needsFunding.length}`);
    console.log(`Wallets with surplus: ${hasSurplus.length}`);

    // 3. Perform P2P Internal Funding
    if (needsFunding.length > 0 && hasSurplus.length > 0) {
      console.log('Attempting P2P internal funding...');
      let surplusIndex = 0;

      for (const needy of needsFunding) {
        if (surplusIndex >= hasSurplus.length) break;

        const neededAmount = this.targetWalletBalance - needy.balance;
        const surplusProvider = hasSurplus[surplusIndex];

        if (surplusProvider.wallet.pubkey === needy.wallet.pubkey) continue;

        const availableSurplus = surplusProvider.balance - this.targetWalletBalance;

        if (availableSurplus > neededAmount) {
          try {
            const tx = new Transaction().add(SystemProgram.transfer({
              fromPubkey: surplusProvider.wallet.keypair.publicKey,
              toPubkey: needy.wallet.keypair.publicKey,
              lamports: neededAmount,
            }));
            const sig = await this.connection.sendTransaction(tx, [surplusProvider.wallet.keypair], { skipPreflight: true });
            await this.connection.confirmTransaction(sig, 'confirmed');

            console.log(`  ✓ P2P: ${surplusProvider.wallet.name} -> ${needy.wallet.name} (${(neededAmount / LAMPORTS_PER_SOL).toFixed(4)} SOL)`);

            surplusProvider.balance -= neededAmount;
            needy.balance += neededAmount;
            await delay(1000 + Math.random() * 2000); // Small delay between P2P txs

          } catch (e) {
            console.error(`  ✗ P2P transfer failed: ${e.message}`);
          }
        }

        if (surplusProvider.balance - this.targetWalletBalance < this.dustThreshold) {
          surplusIndex++;
        }
      }
    }

    // 4. Consolidate remaining surplus to Sink Wallet
    const remainingSurplus = hasSurplus.filter(wb => wb.balance > this.targetWalletBalance * 1.1);
    if (remainingSurplus.length > 0 && this.sinkKeypair) {
      await this.consolidateSurplusToSink(remainingSurplus);
    }

    console.log('⚖️  === Rebalancing Complete ===\n');
  }

  async consolidateSurplusToSink(surplusWallets) {
    console.log('Consolidating remaining surplus to sink wallet...');

    for (const { wallet, balance } of surplusWallets) {
      const surplus = balance - this.targetWalletBalance;

      if (surplus > this.dustThreshold) {
        try {
          const randomPct = 0.90 + Math.random() * 0.08; // 90-98%
          const transferAmount = Math.floor(surplus * randomPct);

          if (transferAmount < this.dustThreshold) continue;

          const tx = new Transaction().add(SystemProgram.transfer({
            fromPubkey: wallet.keypair.publicKey,
            toPubkey: this.sinkKeypair.publicKey,
            lamports: transferAmount,
          }));

          const sig = await this.connection.sendTransaction(tx, [wallet.keypair], {
            skipPreflight: true,
            maxRetries: 2
          });
          await this.connection.confirmTransaction(sig, 'confirmed');

          console.log(`  ✓ ${wallet.name}: Consolidated ${(transferAmount / LAMPORTS_PER_SOL).toFixed(4)} SOL to sink`);

          await delay(500 + Math.random() * 1500); // Random delay

        } catch (e) {
          console.error(`  ✗ ${wallet.name}: Consolidation to sink failed - ${e.message}`);
        }
      }
    }
  }
  // --- END STEALTH ENHANCEMENT ---

  async performSwap(isBuy, wallet) {
    await this.fetchSentiment(); // Update before trade

    // Partial Sell Logic
    if (!isBuy && this.partialSellEnabled) {
      const currentTokens = await this.getCurrentMemeBalanceInLamports(wallet);
      if (currentTokens < 1000) return false; // Dust check

      const pct = this.partialSellMin + Math.random() * (this.partialSellMax - this.partialSellMin);
      const sellAmount = Math.floor(currentTokens * (pct / 100));

      // Override amount for sell
      return await this.performSingleSwap(false, wallet, sellAmount);
    }

    const baseAmount = await this.getProfileAdjustedAmount(wallet, isBuy);
    const adjustedAmount = await this.getAdaptiveAmount(wallet, isBuy, baseAmount);
    const rampFactor = Math.min(1, (this.cycleCount + 1) / this.rampCycles);
    const finalAmount = adjustedAmount * rampFactor;

    if (finalAmount <= 0) {
      console.log(`${wallet.name} Skipping: No balance.`);
      return false;
    }

    // Check market conditions with Birdeye
    const amountInSol = isBuy ? finalAmount / LAMPORTS_PER_SOL : 0.01; // Estimate for sells
    const marketCheck = await this.checkMarketConditions(amountInSol, isBuy);

    if (!marketCheck.safe) {
      console.log(`${wallet.name} ⚠️  Trade skipped: ${marketCheck.reason}`);
      return false;
    }

    const unit = isBuy ? 'SOL' : 'tokens';
    const displayAmount = isBuy ? (finalAmount / LAMPORTS_PER_SOL).toFixed(4) : finalAmount.toLocaleString();
    const impactStr = marketCheck.priceImpact ? ` | Impact: ${marketCheck.priceImpact.toFixed(2)}%` : '';
    console.log(`${wallet.name} ${isBuy ? 'Buying' : 'Selling'} ~${displayAmount} ${unit} (vol: ${(await this.fetchVolatility()).toFixed(4)}, sentiment: ${this.sentimentScore}${impactStr})`);

    const personality = this.getPersonality(wallet.keypair.publicKey);
    if (personality === 'hodler' && isBuy && Math.random() < 0.3) {
      console.log(`${wallet.name} Hodling: Skipping sell cycle.`);
      return true; // Simulate hold
    }

    const useTwap = finalAmount > 0.005 * LAMPORTS_PER_SOL;
    if (useTwap) {
      return await this.twapSwap(isBuy, wallet, finalAmount);
    } else {
      return await this.performSingleSwap(isBuy, wallet, finalAmount);
    }
  }

  async processWalletCycle(wallet) {
    return this.withRetry(async () => {
      if (!(await this.checkBalances(wallet))) return;

      const walletKey = wallet.keypair.publicKey.toBase58();
      const tradeCount = this.walletTradeCount.get(walletKey) || 0;

      let cycleVolume = 0;
      const actions = this.getTradeActions(wallet);
      console.log(`${wallet.name} Cycle actions: ${actions.map(a => a ? 'Buy' : 'Sell').join(', ')} [Trades: ${tradeCount}]`);

      for (const isBuy of actions) {
        if (!isBuy) {
          const tokens = await this.getCurrentMemeBalanceInLamports(wallet);
          if (tokens < 1000) { // dust
            console.log(`${wallet.name} No tokens to sell, skipping`);
            continue;
          }
        }

        const solBeforeTrade = await this.connection.getBalance(wallet.keypair.publicKey);
        const success = await this.performSwap(isBuy, wallet);
        if (success) {
          const solAfterTrade = await this.connection.getBalance(wallet.keypair.publicKey);
          const solDelta = isBuy ? (solBeforeTrade - solAfterTrade) : (solAfterTrade - solBeforeTrade);
          cycleVolume += solDelta / LAMPORTS_PER_SOL;
        }
        await delay(this.getRandomDelay());
      }

      // Mark wallet as used after successful cycle
      this.markWalletUsed(wallet);

      this.totalVolume += cycleVolume;
      console.log(`${wallet.name} completed. Total volume: ${(this.totalVolume).toFixed(2)} SOL`);
    }, wallet.name);
  }

  async runLoop() {
    let batchNum = 0;
    while (this.isRunning) {
      // Check circuit breakers before each batch
      const circuitCheck = await this.checkCircuitBreakers();
      if (circuitCheck.tripped) {
        console.error(`\n${circuitCheck.reason}`);
        console.error('Bot stopped for safety. Review logs and restart manually.\n');
        await this.stop();
        process.exit(1);
      }

      // Load active batch (replaces getNextWalletBatch)
      await this.loadActiveBatch();
      const walletBatch = this.activeWallets;

      if (walletBatch.length === 0) {
        console.log('No wallets available, waiting for cooldowns...');
        await delay(30000); // Wait 30s before checking again
        continue;
      }

      batchNum++;
      this.cycleCount++; // Global cycle
      console.log(`\n--- Batch ${batchNum} (Cycle ${this.cycleCount}, Ramp: ${Math.min(1, (this.cycleCount / this.rampCycles) * 100).toFixed(0)}%) ---`);

      const swapPromises = walletBatch.map(wallet => this.limiter(() => this.processWalletCycle(wallet)));
      const results = await Promise.allSettled(swapPromises);
      const successes = results.filter(r => r.status === 'fulfilled').length;
      console.log(`Batch ${batchNum} complete: ${successes}/${walletBatch.length} successful.`);

      // Periodic wallet rotation stats
      if (batchNum % 10 === 0) {
        this.printRotationStats();
      }

      // Periodic rebalancing
      if (this.enableRebalancing && this.cycleCount % this.rebalanceInterval === 0) {
        await this.rebalanceWallets();
      }

      if (batchNum % this.sessionPauseMin === 0) {
        const pauseMs = this.getJitteredValue(60000 * (1 + Math.random() * 4), false);
        console.log(`Session pause: ${pauseMs / 1000}s...`);
        await delay(pauseMs);
      }

      // Random inter-batch delay for stealth
      const interBatchDelay = Math.random() * 10000 + 5000; // 5-15s
      await delay(interBatchDelay);
    }
  }

  printRotationStats() {
    console.log('\n=== Wallet Rotation Stats ===');
    const now = Date.now();
    let onCooldown = 0;
    let available = 0;

    // We can't iterate all wallets easily if we only load active batch.
    // But we can check the file or just report on active batch.
    // For now, let's report on active batch + cooldown map size

    console.log(`Active Batch: ${this.activeWallets.length}`);
    console.log(`Wallets on Cooldown (tracked): ${this.walletCooldowns.size}`);

    // Show top 5 most active wallets
    const sortedWallets = Array.from(this.walletTradeCount.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);

    if (sortedWallets.length > 0) {
      console.log('Top Active Wallets (by pubkey):');
      sortedWallets.forEach(([key, count], i) => {
        console.log(`  ${i + 1}. ${key.slice(0, 8)}...: ${count} trades`);
      });
    }
    console.log('=============================\n');
  }

  async withdrawAllFunds() {
    if (!this.sinkKeypair) {
      console.log('No sink wallet configured. Skipping withdrawal.');
      return;
    }

    console.log('Starting parallel withdrawal of ALL wallets...');
    // Read from file to ensure we get everyone
    let allWalletsData = [];
    if (fs.existsSync(this.walletFile)) {
      allWalletsData = jsonfile.readFileSync(this.walletFile);
    }

    // Process in chunks to avoid memory explosion if 100k
    const chunkSize = 50;
    let successCount = 0;

    for (let i = 0; i < allWalletsData.length; i += chunkSize) {
      const chunk = allWalletsData.slice(i, i + chunkSize).map(w => ({
        keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
        name: w.name || (w.pubkey ? w.pubkey.slice(0, 6) : 'Wallet')
      }));

      const promises = chunk.map(wallet => this.limiter(() => this.withdrawSingleWallet(wallet)));
      const results = await Promise.allSettled(promises);
      successCount += results.filter(r => r.status === 'fulfilled').length;
      console.log(`Withdrawal progress: ${Math.min(i + chunkSize, allWalletsData.length)}/${allWalletsData.length}`);
    }

    console.log(`Withdrawal complete: ${successCount}/${allWalletsData.length} successful.`);
    const sinkBalance = await this.connection.getBalance(this.sinkKeypair.publicKey);
    console.log(`Final sink SOL: ${(sinkBalance / LAMPORTS_PER_SOL).toFixed(4)}`);

    // Sink Token Cleanup
    if (this.sinkKeypair) {
      const sinkTokenAcc = getAssociatedTokenAddressSync(this.memeMint, this.sinkKeypair.publicKey);
      try {
        const info = await this.connection.getAccountInfo(sinkTokenAcc);
        if (info && parseInt(info.data.slice(64, 72), 'hex') === 0) { // balance == 0
          const closeTx = new Transaction().add(
            createCloseAccountInstruction(sinkTokenAcc, this.sinkKeypair.publicKey, this.sinkKeypair.publicKey)
          );
          await this.connection.sendTransaction(closeTx, [this.sinkKeypair]);
          console.log("Sink token account closed & rent reclaimed");
        }
      } catch (e) { }
    }
  }

  async withdrawSingleWallet(wallet) {
    return this.withRetry(async () => {
      console.log(`\n--- Withdrawing ${wallet.name} ---`);

      // Sell meme
      const memeBalance = await this.getCurrentMemeBalanceInLamports(wallet);
      if (memeBalance > 5000) {
        try {
          const jitoConfig = this.enableJito ? {
            sender: 'JITO',
            antimev: true,
            tipAmountSol: this.jitoTipSell
          } : {};

          await this.trader.sell({
            market: this.market,
            wallet: wallet.keypair,
            mint: this.memeMint,
            amount: memeBalance, // raw tokens
            slippage: 20,
            ...jitoConfig
          });
          console.log(`${wallet.name} Sold remaining tokens`);
          await delay(2000);
        } catch (e) { console.warn(`Sell failed: ${e.message}`); }
      }

      // Close ATA
      const tokenAccount = getAssociatedTokenAddressSync(this.memeMint, wallet.keypair.publicKey);
      try {
        const info = await this.connection.getAccountInfo(tokenAccount);
        if (info) {
          const closeTx = new Transaction().add(createCloseAccountInstruction(
            tokenAccount,
            wallet.keypair.publicKey,
            wallet.keypair.publicKey
          ));
          const closeSig = await this.connection.sendTransaction(closeTx, [wallet.keypair], { skipPreflight: true });
          await this.connection.confirmTransaction(closeSig, 'confirmed');
          console.log(`${wallet.name} ATA closed`);
        }
      } catch (ataError) { }

      // Transfer SOL
      const solBalance = await this.connection.getBalance(wallet.keypair.publicKey);
      if (solBalance > this.minWithdrawSol) {
        const transferAmount = solBalance - (0.0001 * LAMPORTS_PER_SOL);
        const tx = new Transaction().add(SystemProgram.transfer({
          fromPubkey: wallet.keypair.publicKey,
          toPubkey: this.sinkKeypair.publicKey,
          lamports: transferAmount,
        }));
        const sig = await this.connection.sendTransaction(tx, [wallet.keypair], { skipPreflight: true, maxRetries: 3 });
        await this.connection.confirmTransaction(sig, 'confirmed');
        console.log(`${wallet.name} Drained ${(transferAmount / LAMPORTS_PER_SOL).toFixed(6)} SOL`);
      }
    }, wallet.name);
  }

  // --- STEALTH ENHANCEMENT: Wallet Seasoning Method ---
  async seasonWallets() {
    if (!this.enableSeasoning) {
      console.log('Wallet seasoning is disabled.');
      return;
    }

    console.log('\n🌿 === Wallet Seasoning Started ===');
    const walletsData = jsonfile.readFileSync(this.walletFile);
    const unseasonedWallets = walletsData.filter(w => !w.isSeasoned);

    if (unseasonedWallets.length === 0) {
      console.log('All wallets are already seasoned.');
      console.log('🌿 === Seasoning Complete ===\n');
      return;
    }

    console.log(`Found ${unseasonedWallets.length} unseasoned wallets to process.`);

    for (let i = 0; i < unseasonedWallets.length; i++) {
      const walletData = unseasonedWallets[i];
      const wallet = {
        keypair: Keypair.fromSecretKey(new Uint8Array(walletData.privateKey)),
        name: walletData.name,
        pubkey: walletData.pubkey
      };

      try {
        const balance = await this.connection.getBalance(wallet.keypair.publicKey);
        if (balance < 0.005 * LAMPORTS_PER_SOL) {
          console.log(`  - Skipping ${wallet.name} (insufficient SOL for seasoning)`);
          continue;
        }

        const numTxs = this.seasoningMinTxs + Math.floor(Math.random() * (this.seasoningMaxTxs - this.seasoningMinTxs + 1));
        console.log(`  - Seasoning ${wallet.name} with ${numTxs} transactions...`);

        for (let j = 0; j < numTxs; j++) {
          const actionType = Math.random() < 0.7 ? 'swap' : 'burn'; // 70% swaps, 30% burns

          if (actionType === 'swap') {
            const isSolToUsdc = Math.random() < 0.5;
            const amount = (0.0001 + Math.random() * 0.0004) * LAMPORTS_PER_SOL; // Tiny amount
            try {
              if (isSolToUsdc) {
                await this.trader.buy({ market: 'RAYDIUM_AMM', wallet: wallet.keypair, mint: USDC_MINT, amount: amount / LAMPORTS_PER_SOL, slippage: 5 });
                console.log(`    ${j + 1}/${numTxs}: Swapped ~${(amount / LAMPORTS_PER_SOL).toFixed(5)} SOL for USDC`);
              } else {
                const usdcBalance = await this.getTokenBalance(wallet.keypair.publicKey, USDC_MINT);
                if (usdcBalance > 0) {
                  await this.trader.sell({ market: 'RAYDIUM_AMM', wallet: wallet.keypair, mint: USDC_MINT, amount: usdcBalance, slippage: 5 });
                  console.log(`    ${j + 1}/${numTxs}: Swapped USDC back to SOL`);
                } else {
                  console.log(`    ${j + 1}/${numTxs}: Skipped USDC->SOL swap (no USDC balance)`);
                }
              }
            } catch (e) {
              console.error(`    ! Swap failed for ${wallet.name}: ${e.message.slice(0, 50)}...`);
            }
          } else { // burn
            try {
              const burnAmount = Math.floor((0.00001 + Math.random() * 0.00004) * LAMPORTS_PER_SOL);
              const tx = new Transaction().add(SystemProgram.transfer({
                fromPubkey: wallet.keypair.publicKey,
                toPubkey: BURN_ADDRESS,
                lamports: burnAmount,
              }));
              await this.connection.sendTransaction(tx, [wallet.keypair], { skipPreflight: true });
              console.log(`    ${j + 1}/${numTxs}: Burned ${(burnAmount / LAMPORTS_PER_SOL).toFixed(6)} SOL`);
            } catch (e) {
              console.error(`    ! Burn failed for ${wallet.name}: ${e.message.slice(0, 50)}...`);
            }
          }
          const delayMs = this.seasoningDelayMs + (Math.random() - 0.5) * (this.seasoningDelayMs * 0.5);
          await delay(delayMs);
        }

        // Mark as seasoned and save
        const originalIndex = walletsData.findIndex(w => w.pubkey === wallet.pubkey);
        if (originalIndex !== -1) {
          walletsData[originalIndex].isSeasoned = true;
          jsonfile.writeFileSync(this.walletFile, walletsData, { spaces: 2 });
        }

      } catch (error) {
        console.error(`  ! Error seasoning ${wallet.name}: ${error.message}`);
      }
    }

    console.log('🌿 === Seasoning Complete ===\n');
  }

  async getTokenBalance(owner, mint) {
    try {
      const tokenAccount = getAssociatedTokenAddressSync(mint, owner);
      const accountInfo = await getAccount(this.connection, tokenAccount);
      return Number(accountInfo.amount);
    } catch {
      return 0;
    }
  }
  // --- END STEALTH ENHANCEMENT ---

  async init() {
    await this.loadOrGenerateWallets();
    this.assignPersonalities();

    // --- STEALTH ENHANCEMENT: Lifecycle Integration ---
    await this.seasonWallets();
    // --- END STEALTH ENHANCEMENT ---

    // Initial market data fetch
    if (this.useBirdeye) {
      console.log('Fetching initial market data from Birdeye...');
      await this.fetchBirdeyeMarketData();
    }

    this.isRunning = true;
    console.log(`\n🤖 Scaled bot starting with Advanced Features`);
    console.log(`Wallets: ${this.allPubkeys.size} (Active Batch: ${this.activeWallets.length}) | Concurrency: ${this.concurrency} | Batch: ${this.batchSize}`);
    console.log(`Network: ${this.isDevnet ? 'Devnet' : 'Mainnet'} | Market: ${this.market}`);
    console.log(`Trade Mode: ${this.tradeMode} | Buy Prob: ${this.buyProb} | Actions/Cycle: ${this.numActionsPerCycle}`);
    console.log(`Wallet Cooldown: ${(this.minCooldownMs / 60000).toFixed(1)}-${(this.maxCooldownMs / 60000).toFixed(1)} min`);
    console.log(`Shuffle: ${this.shuffleWallets ? 'Enabled' : 'Disabled'}`);
    console.log(`Birdeye API: ${this.useBirdeye ? 'Enabled ✓' : 'Disabled'}`);
    if (this.useBirdeye) {
      console.log(`Min Liquidity: $${this.minLiquidity.toLocaleString()} | Max Price Impact: ${this.maxPriceImpact}%`);
    }
    console.log(`Smart Rebalancing: ${this.enableRebalancing ? 'Enabled ✓' : 'Disabled'}`);
    if (this.enableRebalancing) {
      console.log(`  Target Balance: ${(this.targetWalletBalance / LAMPORTS_PER_SOL).toFixed(3)} SOL | Interval: Every ${this.rebalanceInterval} cycles`);
    }
    console.log(`Circuit Breaker: ${this.enableCircuitBreaker ? 'Enabled ✓' : 'Disabled'}`);
    if (this.enableCircuitBreaker) {
      console.log(`  Max Failures: ${this.maxConsecutiveFailures} | Max Failure Rate: ${this.maxFailureRate}% | Stop Loss: ${this.emergencyStopLoss}%`);
    }

    // Always record initial master balance
    if (this.sinkKeypair) {
      this.initialSinkBalance = await this.connection.getBalance(this.sinkKeypair.publicKey);
      console.log(`Initial sink balance locked: ${(this.initialSinkBalance / LAMPORTS_PER_SOL).toFixed(4)} SOL`);
    }

    console.log('');
    await this.runLoop();
  }

  async stop() {
    this.isRunning = false;
    console.log('Bot stopping...');
    if (this.sinkKeypair) await this.withdrawAllFunds();
    console.log(`Final total volume: ${(this.totalVolume).toFixed(2)} SOL. Bot stopped.`);
  }
}

// Run
const bot = new VolumeBoosterBot();

// Keyboard Triggers
if (bot.enableKeyboard) {
  process.stdin.setRawMode(true);
  process.stdin.resume();
  process.stdin.on('data', key => {
    if (key.toString().toLowerCase().includes('w')) {
      console.log('\nMANUAL TRIGGER: Withdrawing all funds...');
      bot.stop();
    }
  });
}

process.on('SIGINT', () => bot.stop());
bot.init().catch(console.error);
