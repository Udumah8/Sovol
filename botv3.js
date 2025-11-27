
/**
 * Volume Booster Bot - Production-Grade Solana Trading Bot
 *
 * This bot implements advanced volume boosting strategies with stealth features,
 * circuit breakers, and multi-tier market data integration.
 *
 * @author Kilo Code
 * @version 2.0.0
 * @license MIT
 */

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
  ComputeBudgetProgram,
} from '@solana/web3.js';
import { SolanaTrade } from 'solana-trade';
import {
  TOKEN_PROGRAM_ID,
  getAssociatedTokenAddressSync,
  getAccount,
  createCloseAccountInstruction,
} from '@solana/spl-token';
import fs from 'fs/promises';
import jsonfile from 'jsonfile';
import pLimit from 'p-limit';
import { setTimeout as delay } from 'timers/promises';
import winston from 'winston'; // Assuming winston is installed for structured logging

// Constants
const USDC_MINT = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
const BURN_ADDRESS = new PublicKey('1ncinerator11111111111111111111111111111111');

/**
 * Configuration Validator and Manager
 * Ensures all environment variables are validated and sanitized
 */
class ConfigManager {
  constructor() {
    this.validateAndLoadConfig();
  }

  /**
   * Validates and loads all configuration from environment variables
   * @throws {Error} If any required config is invalid
   */
  validateAndLoadConfig() {
    // Required configs
    this.rpcUrl = this.validateString('RPC_URL', process.env.RPC_URL);
    this.memeCoinMint = this.validatePublicKey('MEME_COIN_MINT', process.env.MEME_COIN_MINT);
    this.memeCoinSymbol = this.validateString('MEME_COIN_SYMBOL', process.env.MEME_COIN_SYMBOL || 'solana');
    this.market = this.validateMarket('MARKET', process.env.MARKET);

    // Wallet configs
    this.maxWallets = this.validateNumber('MAX_WALLETS', process.env.MAX_WALLETS, 100, 100000, Infinity);
    this.numWalletsToGenerate = this.validateNumber('NUM_WALLETS_TO_GENERATE', process.env.NUM_WALLETS_TO_GENERATE, 1, 10000, 10);
    this.fundAmount = this.validateSolAmount('FUND_AMOUNT', process.env.FUND_AMOUNT, 0.001, 10, 0.05);

    // Trading configs
    this.tradeMode = this.validateTradeMode('TRADE_MODE', process.env.TRADE_MODE || 'adaptive');
    this.buyProb = this.validateProbability('BUY_PROB', process.env.BUY_PROB, 0.5);
    this.numActionsPerCycle = this.validateNumber('NUM_ACTIONS_PER_CYCLE', process.env.NUM_ACTIONS_PER_CYCLE, 1, 10, 2);
    this.swapAmount = this.validateSolAmount('SWAP_AMOUNT', process.env.SWAP_AMOUNT, 0.0001, 1, 0.01);

    // Timing configs
    this.baseDelayMs = this.validateNumber('DELAY_MS', process.env.DELAY_MS, 100, 60000, 5000);
    this.jitterPct = this.validatePercentage('JITTER_PCT', process.env.JITTER_PCT, 10);

    // Stealth configs
    this.sinkPrivateKey = this.validateOptionalJsonArray('SINK_PRIVATE_KEY', process.env.SINK_PRIVATE_KEY);
    this.relayerPrivateKeys = this.validateOptionalJsonArray('RELAYER_PRIVATE_KEYS', process.env.RELAYER_PRIVATE_KEYS);
    this.enableRebalancing = this.validateBoolean('ENABLE_REBALANCING', process.env.ENABLE_REBALANCING, true);

    // Market data configs
    this.birdeyeApiKey = this.validateOptionalString('BIRDEYE_API_KEY', process.env.BIRDEYE_API_KEY);
    this.memeCoinPairAddress = this.validateOptionalString('MEME_COIN_PAIR_ADDRESS', process.env.MEME_COIN_PAIR_ADDRESS);
    this.minLiquidity = this.validateNumber('MIN_LIQUIDITY_USD', process.env.MIN_LIQUIDITY_USD, 1000, 10000000, 5000);
    this.maxPriceImpact = this.validatePercentage('MAX_PRICE_IMPACT_PCT', process.env.MAX_PRICE_IMPACT_PCT, 5);

    // Circuit breaker configs
    this.enableCircuitBreaker = this.validateBoolean('ENABLE_CIRCUIT_BREAKER', process.env.ENABLE_CIRCUIT_BREAKER, true);
    this.maxConsecutiveFailures = this.validateNumber('MAX_CONSECUTIVE_FAILURES', process.env.MAX_CONSECUTIVE_FAILURES, 1, 100, 10);
    this.maxFailureRate = this.validatePercentage('MAX_FAILURE_RATE_PCT', process.env.MAX_FAILURE_RATE_PCT, 50);
    this.emergencyStopLoss = this.validatePercentage('EMERGENCY_STOP_LOSS_PCT', process.env.EMERGENCY_STOP_LOSS_PCT, 30);

    // Other configs
    this.concurrency = this.validateNumber('CONCURRENCY', process.env.CONCURRENCY, 1, 1000, 50);
    this.batchSize = this.validateNumber('BATCH_SIZE', process.env.BATCH_SIZE, 1, 1000, 100);
    this.retryAttempts = this.validateNumber('RETRY_ATTEMPTS', process.env.RETRY_ATTEMPTS, 1, 10, 3);

    // Wallet seasoning
    this.enableSeasoning = this.validateBoolean('ENABLE_SEASONING', process.env.ENABLE_SEASONING, false);
    this.seasoningMinTxs = this.validateNumber('SEASONING_MIN_TXS', process.env.SEASONING_MIN_TXS, 1, 20, 3);
    this.seasoningMaxTxs = this.validateNumber('SEASONING_MAX_TXS', process.env.SEASONING_MAX_TXS, 1, 50, 10);
    this.seasoningDelayMs = this.validateNumber('SEASONING_DELAY_MS', process.env.SEASONING_DELAY_MS, 1000, 30000, 5000);

    // Jito MEV
    this.enableJito = this.validateBoolean('ENABLE_JITO', process.env.ENABLE_JITO, true);
    this.jitoFee = this.validateSolAmount('JITO_PRIORITY_FEE_SOL', process.env.JITO_PRIORITY_FEE_SOL, 0.0001, 0.01, 0.002);
    this.jitoTipBuy = this.validateSolAmount('JITO_TIP_SOL_BUY', process.env.JITO_TIP_SOL_BUY, 0.0001, 0.01, 0.0012);
    this.jitoTipSell = this.validateSolAmount('JITO_TIP_SOL_SELL', process.env.JITO_TIP_SOL_SELL, 0.0001, 0.01, 0.0018);

    // Auto-scaling
    this.autoScale = this.validateBoolean('AUTO_SCALE_CONCURRENCY', process.env.AUTO_SCALE_CONCURRENCY, true);

    // Partial sell
    this.partialSellEnabled = this.validateBoolean('PARTIAL_SELL_ENABLED', process.env.PARTIAL_SELL_ENABLED, true);
    this.partialSellMin = this.validatePercentage('PARTIAL_SELL_MIN_PCT', process.env.PARTIAL_SELL_MIN_PCT, 22);
    this.partialSellMax = this.validatePercentage('PARTIAL_SELL_MAX_PCT', process.env.PARTIAL_SELL_MAX_PCT, 68);

    // Behavior
    this.behaviorProfile = this.validateBehaviorProfile('BEHAVIOR_PROFILE', process.env.BEHAVIOR_PROFILE || 'retail');

    // Wallet rotation
    this.minWalletCooldownMs = this.validateNumber('MIN_WALLET_COOLDOWN_MS', process.env.MIN_WALLET_COOLDOWN_MS, 60000, 3600000, 300000);
    this.maxWalletCooldownMs = this.validateNumber('MAX_WALLET_COOLDOWN_MS', process.env.MAX_WALLET_COOLDOWN_MS, 60000, 3600000, 1800000);
    this.shuffleWallets = this.validateBoolean('SHUFFLE_WALLETS', process.env.SHUFFLE_WALLETS, true);

    // Rebalancing
    this.minWalletBalance = this.validateSolAmount('MIN_WALLET_BALANCE_SOL', process.env.MIN_WALLET_BALANCE_SOL, 0.001, 1, 0.005);
    this.targetWalletBalance = this.validateSolAmount('TARGET_WALLET_BALANCE_SOL', process.env.TARGET_WALLET_BALANCE_SOL, 0.001, 1, 0.05);
    this.rebalanceInterval = this.validateNumber('REBALANCE_INTERVAL_CYCLES', process.env.REBALANCE_INTERVAL_CYCLES, 1, 1000, 50);
    this.dustThreshold = this.validateSolAmount('DUST_THRESHOLD_SOL', process.env.DUST_THRESHOLD_SOL, 0.0001, 0.01, 0.001);

    // Session
    this.sessionPauseMin = this.validateNumber('SESSION_PAUSE_MIN', process.env.SESSION_PAUSE_MIN, 1, 100, 10);

    // TWAP
    this.twapParts = this.validateNumber('TWAP_PARTS', process.env.TWAP_PARTS, 1, 20, 5);
    this.twapMaxDelay = this.validateNumber('TWAP_MAX_DELAY', process.env.TWAP_MAX_DELAY, 1000, 60000, 10000);

    // Vol threshold
    this.volThreshold = this.validatePercentage('VOL_THRESHOLD', process.env.VOL_THRESHOLD, 0.05);

    // Ramp cycles
    this.rampCycles = this.validateNumber('RAMP_CYCLES', process.env.RAMP_CYCLES, 1, 1000, 30);

    // Keyboard triggers
    this.enableKeyboard = this.validateBoolean('ENABLE_KEYBOARD_TRIGGERS', process.env.ENABLE_KEYBOARD_TRIGGERS, false);

    // Wallet file
    this.walletFile = this.validateString('WALLET_FILE', process.env.WALLET_FILE || 'wallets.json');

    // Derived configs
    this.isDevnet = this.rpcUrl.includes('devnet');
    this.useBirdeye = !!this.birdeyeApiKey;
  }

  validateString(key, value) {
    if (!value || typeof value !== 'string' || value.trim() === '') {
      throw new Error(`Invalid ${key}: must be a non-empty string`);
    }
    return value.trim();
  }

  validateOptionalString(key, value) {
    return value && typeof value === 'string' ? value.trim() : null;
  }

  validateNumber(key, value, min, max, defaultValue) {
    const num = value ? parseFloat(value) : defaultValue;
    if (isNaN(num) || num < min || num > max) {
      throw new Error(`Invalid ${key}: must be a number between ${min} and ${max}`);
    }
    return num;
  }

  validateSolAmount(key, value, min, max, defaultValue) {
    const amount = this.validateNumber(key, value, min, max, defaultValue);
    return amount * LAMPORTS_PER_SOL;
  }

  validatePercentage(key, value, defaultValue) {
    const pct = this.validateNumber(key, value, 0, 100, defaultValue);
    return pct / 100;
  }

  validateProbability(key, value, defaultValue) {
    return this.validateNumber(key, value, 0, 1, defaultValue);
  }

  validateBoolean(key, value, defaultValue) {
    if (value === undefined || value === null) return defaultValue;
    const str = value.toString().toLowerCase();
    if (str === 'true' || str === '1' || str === 'yes') return true;
    if (str === 'false' || str === '0' || str === 'no') return false;
    throw new Error(`Invalid ${key}: must be a boolean (true/false)`);
  }

  validatePublicKey(key, value) {
    try {
      return new PublicKey(this.validateString(key, value));
    } catch (error) {
      throw new Error(`Invalid ${key}: must be a valid Solana public key`);
    }
  }

  validateOptionalJsonArray(key, value) {
    if (!value) return null;
    try {
      const parsed = JSON.parse(value);
      if (!Array.isArray(parsed)) {
        throw new Error('Must be an array');
      }
      return parsed;
    } catch (error) {
      throw new Error(`Invalid ${key}: must be a valid JSON array`);
    }
  }

  validateMarket(key, value) {
    const supportedMarkets = [
      'PUMP_FUN', 'PUMP_SWAP', 'RAYDIUM_AMM', 'RAYDIUM_CLMM', 'RAYDIUM_CPMM',
      'RAYDIUM_LAUNCHPAD', 'ORCA_WHIRLPOOL', 'METEORA_DLMM', 'METEORA_DAMM_V1',
      'METEORA_DAMM_V2', 'METEORA_DBC', 'MOONIT', 'HEAVEN', 'SUGAR', 'BOOP_FUN'
    ];
    const market = this.validateString(key, value);
    if (!supportedMarkets.includes(market)) {
      throw new Error(`Invalid ${key}: must be one of ${supportedMarkets.join(', ')}`);
    }
    return market;
  }

  validateTradeMode(key, value) {
    const supportedModes = ['adaptive', 'buy_first', 'sell_first', 'buy_only', 'sell_only', 'random'];
    const mode = this.validateString(key, value);
    if (!supportedModes.includes(mode)) {
      throw new Error(`Invalid ${key}: must be one of ${supportedModes.join(', ')}`);
    }
    return mode;
  }

  validateBehaviorProfile(key, value) {
    const supportedProfiles = ['retail', 'whale', 'mixed'];
    const profile = this.validateString(key, value);
    if (!supportedProfiles.includes(profile)) {
      throw new Error(`Invalid ${key}: must be one of ${supportedProfiles.join(', ')}`);
    }
    return profile;
  }
}

/**
 * Structured Logger with Winston
 */
class Logger {
  constructor() {
    this.logger = winston.createLogger({
      level: 'info',
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
      ),
      defaultMeta: { service: 'volume-booster-bot' },
      transports: [
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.colorize(),
            winston.format.simple()
          )
        }),
        new winston.transports.File({ filename: 'bot.log' })
      ]
    });
  }

  info(message, meta = {}) {
    this.logger.info(message, meta);
  }

  warn(message, meta = {}) {
    this.logger.warn(message, meta);
  }

  error(message, meta = {}) {
    this.logger.error(message, meta);
  }

  debug(message, meta = {}) {
    this.logger.debug(message, meta);
  }
}

/**
 * Wallet Management System
 * Handles wallet generation, loading, funding, and rotation
 */
class WalletManager {
  /**
   * @param {ConfigManager} config
   * @param {Connection} connection
   * @param {Logger} logger
   */
  constructor(config, connection, logger) {
    this.config = config;
    this.connection = connection;
    this.logger = logger;
    this.walletData = [];
    this.allPubkeys = new Set();
    this.funded = new Set();
    this.activeWallets = [];
    this.walletCooldowns = new Map();
    this.walletTradeCount = new Map();
    this.walletPersonalities = new Map();
    this.sinkKeypair = config.sinkPrivateKey ? Keypair.fromSecretKey(new Uint8Array(config.sinkPrivateKey)) : null;
    this.relayerKeypairs = config.relayerPrivateKeys ? config.relayerPrivateKeys.map(pk => Keypair.fromSecretKey(new Uint8Array(pk))) : [];
  }

  /**
   * Loads or generates wallets based on configuration
   */
  async loadOrGenerateWallets() {
    try {
      const exists = await this.fileExists(this.config.walletFile);
      if (exists) {
        this.walletData = await jsonfile.readFile(this.config.walletFile);
        this.normalizeWalletData();
        this.allPubkeys = new Set(this.walletData.map(w => w.pubkey));
        this.logger.info(`Loaded ${this.walletData.length.toLocaleString()} existing wallets`);
      }

      if (this.walletData.length < this.config.numWalletsToGenerate) {
        await this.generateWallets();
      }

      if (this.config.autoScale) {
        this.config.concurrency = Math.min(50, Math.max(3, Math.floor(this.walletData.length / 200) + 3));
        this.config.batchSize = Math.min(20, Math.max(2, Math.floor(this.walletData.length / 300) + 2));
      }

      await this.fundWalletsInParallel();
    } catch (error) {
      this.logger.error('Failed to load or generate wallets', { error: error.message });
      throw error;
    }
  }

  /**
   * Normalizes wallet data structure
   */
  normalizeWalletData() {
    this.walletData = this.walletData.map(w => ({
      pubkey: w.pubkey || w.publicKey || Keypair.fromSecretKey(new Uint8Array(w.privateKey)).publicKey.toBase58(),
      privateKey: w.privateKey,
      name: w.name || `Wallet`,
      isSeasoned: w.isSeasoned || false
    }));
  }

  /**
   * Generates additional wallets to meet the required count
   */
  async generateWallets() {
    const remaining = this.config.numWalletsToGenerate - this.walletData.length;
    this.logger.info(`Generating ${remaining.toLocaleString()} wallets...`);

    const batchSize = 1000;
    for (let i = 0; i < remaining; i += batchSize) {
      const size = Math.min(batchSize, remaining - i);
      const batch = [];
      for (let j = 0; j < size; j++) {
        const kp = Keypair.generate();
        const wallet = {
          pubkey: kp.publicKey.toBase58(),
          privateKey: Array.from(kp.secretKey),
          name: `Wallet${this.walletData.length + i + j + 1}`,
          isSeasoned: false
        };
        batch.push(wallet);
        this.allPubkeys.add(wallet.pubkey);
      }
      this.walletData.push(...batch);
      this.logger.info(`${this.walletData.length.toLocaleString()}/${this.config.numWalletsToGenerate.toLocaleString()}`);
      await delay(10);
    }

    await jsonfile.writeFile(this.config.walletFile, this.walletData, { spaces: 2 });
  }

  /**
   * Funds wallets in parallel using relayer wallets
   */
  async fundWalletsInParallel() {
    if (this.relayerKeypairs.length === 0) {
      this.logger.info('No relayer wallets configured, skipping funding');
      return;
    }

    this.logger.info(`Checking funding for ${this.walletData.length} wallets...`);
    const toCheck = this.walletData.filter(w => !this.funded.has(w.pubkey));

    const fundBatchSize = 50;
    for (let i = 0; i < toCheck.length; i += fundBatchSize) {
      const batch = toCheck.slice(i, i + fundBatchSize);
      const promises = batch.map(wallet => this.fundSingleWallet(wallet));
      await Promise.allSettled(promises);
      this.logger.info(`Funding progress: ${Math.min(i + fundBatchSize, toCheck.length)}/${toCheck.length}`);
    }
  }

  /**
   * Funds a single wallet
   * @param {Object} wallet
   */
  async fundSingleWallet(wallet) {
    if (this.funded.has(wallet.pubkey)) return;

    const kp = Keypair.fromSecretKey(new Uint8Array(wallet.privateKey));
    try {
      const balance = await this.connection.getBalance(kp.publicKey);
      if (balance >= this.config.fundAmount * 0.8) {
        this.funded.add(wallet.pubkey);
        return;
      }

      const remaining = this.config.fundAmount - balance;
      const parts = Math.floor(Math.random() * 4) + 1;

      for (let p = 0; p < parts && remaining > 0.005 * LAMPORTS_PER_SOL; p++) {
        const remainingParts = parts - p;
        const part = remainingParts > 0 ? Math.floor(Math.max(remaining * (0.6 + Math.random() * 0.8) / remainingParts, 0.005 * LAMPORTS_PER_SOL)) : remaining;

        const relayer = this.relayerKeypairs[Math.floor(Math.random() * this.relayerKeypairs.length)];
        const tx = new Transaction().add(SystemProgram.transfer({
          fromPubkey: relayer.publicKey,
          toPubkey: kp.publicKey,
          lamports: part,
        }));

        const sig = await this.connection.sendTransaction(tx, [relayer], { skipPreflight: true });
        await this.connection.confirmTransaction(sig, 'confirmed');
        this.logger.info(`Funded ${wallet.name}: ${(part / LAMPORTS_PER_SOL).toFixed(4)} SOL`);
        await delay(1000 + Math.random() * 2000);
      }
      this.funded.add(wallet.pubkey);
    } catch (error) {
      this.logger.warn(`Failed to fund ${wallet.name}`, { error: error.message });
    }
  }

  /**
   * Loads the next batch of active wallets for trading
   */
  async loadActiveBatch() {
    const now = Date.now();
    const ready = this.walletData.filter(w => {
      const lastTrade = this.walletCooldowns.get(w.pubkey) || 0;
      const cooldown = this.getWalletCooldown(w.pubkey);
      return now - lastTrade >= cooldown;
    });

    if (ready.length === 0) return [];

    const shuffled = this.config.shuffleWallets ? this.shuffleArray(ready) : ready;
    const selected = shuffled.slice(0, this.config.batchSize);

    this.activeWallets = selected.map(w => ({
      keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
      name: w.name || w.pubkey.slice(0, 6),
      pubkey: w.pubkey
    }));

    this.logger.info(`Loaded batch: ${this.activeWallets.length} wallets (Pool: ${ready.length}/${this.walletData.length})`);
    return this.activeWallets;
  }

  /**
   * Shuffles an array using Fisher-Yates algorithm
   * @param {Array} array
   * @returns {Array}
   */
  shuffleArray(array) {
    const shuffled = [...array];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
  }

  /**
   * Gets the cooldown period for a wallet
   * @param {string} walletKey
   * @returns {number}
   */
  getWalletCooldown(walletKey) {
    const tradeCount = this.walletTradeCount.get(walletKey) || 0;
    const cooldownMultiplier = 1 + (tradeCount % 5) * 0.2;
    const baseCooldown = this.config.minWalletCooldownMs + Math.random() * (this.config.maxWalletCooldownMs - this.config.minWalletCooldownMs);
    return Math.floor(baseCooldown * cooldownMultiplier);
  }

  /**
   * Marks a wallet as used after trading
   * @param {Object} wallet
   */
  markWalletUsed(wallet) {
    const walletKey = wallet.keypair.publicKey.toBase58();
    this.walletCooldowns.set(walletKey, Date.now());
    this.walletTradeCount.set(walletKey, (this.walletTradeCount.get(walletKey) || 0) + 1);
  }

  /**
   * Assigns personalities to wallets for varied behavior
   */
  assignPersonalities() {
    const personalities = ['flipper', 'hodler', 'momentum'];
    this.allPubkeys.forEach(pubkey => {
      this.walletPersonalities.set(pubkey, personalities[Math.floor(Math.random() * personalities.length)]);
    });
  }

  /**
   * Gets the personality of a wallet
   * @param {string|PublicKey} pubkey
   * @returns {string}
   */
  getPersonality(pubkey) {
    const key = typeof pubkey === 'string' ? pubkey : pubkey.toBase58();
    return this.walletPersonalities.get(key) || 'flipper';
  }

  /**
   * Checks if a file exists
   * @param {string} path
   * @returns {Promise<boolean>}
   */
  async fileExists(path) {
    try {
      await fs.access(path);
      return true;
    } catch {
      return false;
    }
  }
}

/**
 * Market Data Provider with Multi-Tier Fallback
 */
class MarketDataProvider {
  /**
   * @param {ConfigManager} config
   * @param {Connection} connection
   * @param {Logger} logger
   */
  constructor(config, connection, logger) {
    this.config = config;
    this.connection = connection;
    this.logger = logger;
    this.cache = {
      price: 0,
      priceChange24h: 0,
      volume24h: 0,
      liquidity: 0,
      lastUpdate: 0,
      source: 'none',
    };
    this.cacheDuration = 60000; // 1 minute
    this.axiosConfig = {
      timeout: 8000,
      headers: this.config.useBirdeye ? { 'X-API-KEY': this.config.birdeyeApiKey } : {}
    };
  }

  /**
   * Gets market data using tiered fallback system
   * @returns {Promise<Object>}
   */
  async getMarketData() {
    const now = Date.now();
    if (now - this.cache.lastUpdate < this.cacheDuration) {
      return this.cache;
    }

    // Tier 1: Birdeye
    if (this.config.useBirdeye) {
      try {
        const response = await axios.get(
          `https://public-api.birdeye.so/defi/price?address=${this.config.memeCoinMint.toBase58()}&include_liquidity=true&include_volume=true`,
          this.axiosConfig
        );

        if (response.data.success && response.data.data) {
          const data = response.data.data;
          this.cache = {
            price: data.value,
            priceChange24h: data.priceChange24h,
            volume24h: data.volume24h,
            liquidity: data.liquidity,
            lastUpdate: now,
            source: 'Birdeye',
          };
          this.logger.info('Market data fetched from Birdeye', {
            price: this.cache.price,
            liquidity: this.cache.liquidity
          });
          return this.cache;
        }
      } catch (error) {
        this.logger.warn('Birdeye fetch failed, falling back', { error: error.message });
      }
    }

    // Tier 2: DexScreener
    if (this.config.memeCoinPairAddress) {
      try {
        const response = await axios.get(
          `https://api.dexscreener.com/latest/dex/pairs/solana/${this.config.memeCoinPairAddress}`,
          { timeout: this.axiosConfig.timeout }
        );

        if (response.data && response.data.pair) {
          const pair = response.data.pair;
          this.cache = {
            price: parseFloat(pair.priceUsd),
            priceChange24h: parseFloat(pair.priceChange.h24),
            volume24h: parseFloat(pair.volume.h24),
            liquidity: parseFloat(pair.liquidity.usd),
            lastUpdate: now,
            source: 'DexScreener',
          };
          this.logger.info('Market data fetched from DexScreener', {
            price: this.cache.price,
            liquidity: this.cache.liquidity
          });
          return this.cache;
        }
      } catch (error) {
        this.logger.warn('DexScreener fetch failed, falling back', { error: error.message });
      }
    }

    // Tier 3: CoinGecko
    try {
      const response = await axios.get(
        `https://api.coingecko.com/api/v3/simple/price?ids=${this.config.memeCoinSymbol}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true`,
        { timeout: this.axiosConfig.timeout }
      );

      if (response.data && response.data[this.config.memeCoinSymbol]) {
        const data = response.data[this.config.memeCoinSymbol];
        const lastLiquidity = this.cache.liquidity || 0;

        this.cache = {
          price: data.usd,
          priceChange24h: data.usd_24h_change,
          volume24h: data.usd_24h_vol,
          liquidity: lastLiquidity,
          lastUpdate: now,
          source: 'CoinGecko',
        };
        this.logger.info('Market data fetched from CoinGecko (stale liquidity)', {
          price: this.cache.price,
          liquidity: this.cache.liquidity
        });
        return this.cache;
      }
    } catch (error) {
      this.logger.error('All market data sources failed', { error: error.message });
    }

    return this.cache; // Return stale data
  }

  /**
   * Gets SOL price in USD
   * @returns {Promise<number>}
   */
  async getSolPriceUsd() {
    // Tier 1: DexScreener
    try {
      const response = await axios.get(
        'https://api.dexscreener.com/latest/dex/pairs/solana/So11111111111111111111111111111111111111112_EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        { timeout: this.axiosConfig.timeout }
      );
      if (response.data && response.data.pair) {
        return parseFloat(response.data.pair.priceUsd);
      }
    } catch (error) {
      this.logger.warn('SOL price DexScreener failed, falling back', { error: error.message });
    }

    // Tier 2: CoinGecko
    try {
      const response = await axios.get(
        'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
        { timeout: this.axiosConfig.timeout }
      );
      if (response.data && response.data.solana) {
        return response.data.solana.usd;
      }
    } catch (error) {
      this.logger.error('SOL price CoinGecko failed', { error: error.message });
    }

    return 150; // Fallback
  }

  /**
   * Fetches volatility data
   * @returns {Promise<number>}
   */
  async fetchVolatility() {
    try {
      const response = await axios.get(
        `https://api.coingecko.com/api/v3/coins/${this.config.memeCoinSymbol}/market_chart?vs_currency=usd&days=7`,
        { timeout: this.axiosConfig.timeout }
      );

      if (response.data && response.data.prices && response.data.prices.length > 1) {
        const prices = response.data.prices.map(p => p[1]);
        const returns = prices.slice(1).map((p, i) => (p - prices[i]) / prices[i]);
        if (returns.length < 2) return 0;
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (returns.length - 1);
        return Math.sqrt(variance);
      }
    } catch (error) {
      this.logger.error('Volatility fetch failed', { error: error.message });
    }
    return 0;
  }

  /**
   * Estimates price impact
   * @param {number} amountInSol
   * @returns {Promise<number>}
   */
  async estimatePriceImpact(amountInSol) {
    await this.getMarketData();
    const solPriceUsd = await this.getSolPriceUsd();
    const amountInUsd = amountInSol * solPriceUsd;
    const liquidity = this.cache.liquidity;

    if (liquidity === 0) return 100;
    const impact = (amountInUsd / liquidity) * 100;
    return Math.min(impact, 100);
  }

  /**
   * Checks if market conditions are safe for trading
   * @param {number} amountInSol
   * @returns {Promise<Object>}
   */
  async checkMarketConditions(amountInSol) {
    await this.getMarketData();

    if (this.cache.liquidity < this.config.minLiquidity) {
      return {
        safe: false,
        reason: `Low liquidity: $${this.cache.liquidity.toFixed(0)} < $${this.config.minLiquidity} (Source: ${this.cache.source})`,
      };
    }

    const priceImpact = await this.estimatePriceImpact(amountInSol);
    if (priceImpact > this.config.maxPriceImpact) {
      return {
        safe: false,
        reason: `High price impact: ${priceImpact.toFixed(2)}% > ${this.config.maxPriceImpact}%`,
      };
    }

    return { safe: true, reason: '', priceImpact };
  }
}

/**
 * Trading Engine with Advanced Strategies
 */
class TradingEngine {
  /**
   * @param {ConfigManager} config
   * @param {Connection} connection
   * @param {MarketDataProvider} marketData
   * @param {Logger} logger
   */
  constructor(config, connection, marketData, logger) {
    this.config = config;
    this.connection = connection;
    this.marketData = marketData;
    this.logger = logger;
    this.trader = new SolanaTrade(this.config.rpcUrl);
    this.sentimentScore = 50;
    this.lastSentimentFetch = 0;
    this.recentPrices = [];
    this.limiter = pLimit(this.config.concurrency);
  }

  /**
   * Gets jittered value for randomization
   * @param {number} base
   * @param {boolean} isAmount
   * @returns {number}
   */
  getJitteredValue(base, isAmount = false) {
    const variance = (Math.random() - 0.5) * 2 * this.config.jitterPct;
    let value = base * (1 + variance);
    if (isAmount) value = Math.max(value, LAMPORTS_PER_SOL * 0.0001);
    return Math.floor(value);
  }

  /**
   * Gets random delay
   * @returns {number}
   */
  getRandomDelay() {
    return this.getJitteredValue(this.config.baseDelayMs, false);
  }

  /**
   * Fetches market sentiment
   * @returns {Promise<number>}
   */
  async fetchSentiment() {
    if (Date.now() - this.lastSentimentFetch < 3600000) return this.sentimentScore;
    try {
      const response = await axios.get('https://api.alternative.me/fng/?limit=1', { timeout: 5000 });
      this.sentimentScore = parseInt(response.data.data[0].value);
      this.lastSentimentFetch = Date.now();
      this.logger.info('Sentiment updated', { score: this.sentimentScore });
    } catch (error) {
      this.logger.error('Sentiment fetch failed', { error: error.message });
    }
    return this.sentimentScore;
  }

  /**
   * Gets sentiment-based bias
   * @returns {number}
   */
  getSentimentBias() {
    const score = this.sentimentScore;
    if (score > 50) return 0.8;
    if (score < 30) return 0.3;
    return 0.5;
  }

  /**
   * Gets profile-adjusted amount
   * @param {Object} wallet
   * @param {boolean} isBuy
   * @returns {Promise<number>}
   */
  async getProfileAdjustedAmount(wallet, isBuy) {
    let amount = this.getJitteredValue(this.config.swapAmount, true);
    let maxCapMultiplier = 2;

    // Profile scaling
    if (this.config.behaviorProfile === 'whale') {
      amount *= 5;
      maxCapMultiplier = 5;
    } else if (this.config.behaviorProfile === 'mixed') {
      amount *= (Math.random() < 0.3 ? 3 : 1);
      maxCapMultiplier = 3;
    }

    // Personality scaling
    const personality = this.marketData.getPersonality(wallet.keypair.publicKey);
    if (personality === 'hodler' && isBuy) {
      amount *= 2;
      maxCapMultiplier = Math.max(maxCapMultiplier, 4);
    } else if (personality === 'momentum') {
      amount *= (await this.fetchSentiment()) / 50;
      maxCapMultiplier = Math.max(maxCapMultiplier, 4);
    }

    if (!isBuy) {
      amount = await this.getCurrentMemeBalance(wallet);
    }

    return Math.floor(Math.min(amount, this.config.swapAmount * maxCapMultiplier));
  }

  /**
   * Gets current meme token balance
   * @param {Object} wallet
   * @returns {Promise<bigint>}
   */
  async getCurrentMemeBalance(wallet) {
    const tokenAccount = getAssociatedTokenAddressSync(this.config.memeCoinMint, wallet.keypair.publicKey);
    try {
      const accountInfo = await getAccount(this.connection, tokenAccount);
      return accountInfo.amount;
    } catch {
      return 0n;
    }
  }

  /**
   * Gets adaptive amount with market adjustments
   * @param {Object} wallet
   * @param {boolean} isBuy
   * @param {number} baseAmount
   * @returns {Promise<number>}
   */
  async getAdaptiveAmount(wallet, isBuy, baseAmount) {
    let amount = baseAmount;
    const marketData = await this.marketData.getMarketData();
    const vol = await this.marketData.fetchVolatility();

    if (vol > this.config.volThreshold) amount *= 0.5;
    else if (vol < 0.01) amount *= 1.5;

    if (this.config.useBirdeye && marketData.liquidity > 0) {
      const liquidityRatio = marketData.liquidity / 10000;
      if (liquidityRatio < 1) amount *= 0.5;
      else if (liquidityRatio > 10) amount *= 1.2;
    }

    return Math.floor(Math.max(amount, LAMPORTS_PER_SOL * 0.0001));
  }

  /**
   * Gets trade actions for a wallet cycle
   * @param {Object} wallet
   * @returns {Array<boolean>}
   */
  getTradeActions(wallet) {
    if (this.config.tradeMode === 'buy_only') {
      return Array(this.config.numActionsPerCycle).fill(true);
    } else if (this.config.tradeMode === 'sell_only') {
      return Array(this.config.numActionsPerCycle).fill(false);
    } else if (this.config.tradeMode === 'buy_first') {
      return [true, ...Array(this.config.numActionsPerCycle - 1).fill(false)];
    } else if (this.config.tradeMode === 'sell_first') {
      return [false, ...Array(this.config.numActionsPerCycle - 1).fill(true)];
    } else if (this.config.tradeMode === 'random') {
      return Array.from({ length: this.config.numActionsPerCycle }, () => Math.random() < this.config.buyProb);
    } else { // adaptive
      const buyFirst = this.isBuyFirst(wallet);
      const baseActions = buyFirst ? [true, false] : [false, true];
      const actions = [];
      for (let i = 0; i < this.config.numActionsPerCycle; i++) {
        actions.push(baseActions[i % baseActions.length]);
      }
      return actions;
    }
  }

  /**
   * Determines if wallet should buy first
   * @param {Object} wallet
   * @returns {boolean}
   */
  isBuyFirst(wallet) {
    const sentimentBias = this.getSentimentBias();
    const personality = this.marketData.getPersonality(wallet.keypair.publicKey);
    let prob = this.config.buyProb * sentimentBias;
    if (personality === 'hodler') prob = Math.min(0.9, prob * 1.5);
    else if (personality === 'flipper') prob = 0.5;
    else if (personality === 'momentum') prob = Math.min(1.0, prob * 1.2);
    return Math.random() < prob;
  }

  /**
   * Performs TWAP (Time-Weighted Average Price) swap
   * @param {boolean} isBuy
   * @param {Object} wallet
   * @param {number} amount
   * @returns {Promise<boolean>}
   */
  async twapSwap(isBuy, wallet, amount) {
    const partAmount = Math.floor(amount / this.config.twapParts);
    const remainder = amount % this.config.twapParts;
    const parts = Array(this.config.twapParts).fill(partAmount);
    parts[0] += remainder;

    let anySuccess = false;
    for (let i = 0; i < parts.length; i++) {
      if (!this.isRunning) break;
      const part = await this.getAdaptiveAmount(wallet, isBuy, parts[i]);
      if (part <= 0) continue;

      this.logger.info(`${wallet.name} TWAP part ${i + 1}/${parts.length}`, {
        action: isBuy ? 'Buy' : 'Sell',
        amount: (part / LAMPORTS_PER_SOL).toFixed(6)
      });

      const success = await this.performSingleSwap(isBuy, wallet, part);
      if (success) anySuccess = true;
      if (!success) break;

      if (i < parts.length - 1) {
        const partDelay = Math.random() * this.config.twapMaxDelay;
        await delay(partDelay);
      }
    }
    return anySuccess;
  }

  /**
   * Performs a single swap
   * @param {boolean} isBuy
   * @param {Object} wallet
   * @param {number} amount
   * @returns {Promise<boolean>}
   */
  async performSingleSwap(isBuy, wallet, amount) {
    try {
      const slippage = this.config.behaviorProfile === 'retail' ? 1 : 0.3;
      let sig;

      const jitoConfig = this.config.enableJito ? {
        sender: 'JITO',
        antimev: true,
        priorityFeeSol: this.config.jitoFee,
        tipAmountSol: isBuy ? this.config.jitoTipBuy : this.config.jitoTipSell
      } : {};

      if (isBuy) {
        const solAmount = amount / LAMPORTS_PER_SOL;
        sig = await this.trader.buy({
          market: this.config.market,
          wallet: wallet.keypair,
          mint: this.config.memeCoinMint,
          amount: solAmount,
          slippage,
          ...jitoConfig
        });
      } else {
        sig = await this.trader.sell({
          market: this.config.market,
          wallet: wallet.keypair,
          mint: this.config.memeCoinMint,
          amount,
          slippage,
          ...jitoConfig
        });
      }

      this.logger.info(`${wallet.name} Swap TX`, {
        tx: `https://solscan.io/tx/${sig}`,
        action: isBuy ? 'Buy' : 'Sell'
      });

      return !!sig;
    } catch (error) {
      this.logger.error(`${wallet.name} Single swap failed`, { error: error.message });
      return false;
    }
  }

  /**
   * Performs a swap with partial sell logic
   * @param {boolean} isBuy
   * @param {Object} wallet
   * @returns {Promise<boolean>}
   */
  async performSwap(isBuy, wallet) {
    await this.fetchSentiment();

    // Partial sell logic
    if (!isBuy && this.config.partialSellEnabled) {
      const currentTokens = await this.getCurrentMemeBalance(wallet);
      if (currentTokens < 1000n) return false;

      const pct = this.config.partialSellMin + Math.random() * (this.config.partialSellMax - this.config.partialSellMin);
      const pctBigInt = BigInt(Math.floor(pct));
      const sellAmountBigInt = (currentTokens * pctBigInt) / 100n;

      if (sellAmountBigInt > Number.MAX_SAFE_INTEGER) {
        this.logger.warn('Sell amount too large for safe conversion', { wallet: wallet.name });
      }

      const sellAmount = Number(sellAmountBigInt);
      return await this.performSingleSwap(false, wallet, sellAmount);
    }

    const baseAmount = await this.getProfileAdjustedAmount(wallet, isBuy);
    const adjustedAmount = await this.getAdaptiveAmount(wallet, isBuy, baseAmount);
    const rampFactor = Math.min(1, (this.cycleCount + 1) / this.config.rampCycles);
    const finalAmount = adjustedAmount * rampFactor;

    if (finalAmount <= 0) {
      this.logger.info(`${wallet.name} Skipping: No balance`);
      return false;
    }

    const amountInSol = isBuy ? finalAmount / LAMPORTS_PER_SOL : 0.01;
    const marketCheck = await this.marketData.checkMarketConditions(amountInSol);

    if (!marketCheck.safe) {
      this.logger.warn(`${wallet.name} Trade skipped`, { reason: marketCheck.reason });
      return false;
    }

    const unit = isBuy ? 'SOL' : 'tokens';
    const displayAmount = isBuy ? (finalAmount / LAMPORTS_PER_SOL).toFixed(4) : finalAmount.toLocaleString();
    const impactStr = marketCheck.priceImpact ? `Impact: ${marketCheck.priceImpact.toFixed(2)}%` : '';

    this.logger.info(`${wallet.name} ${isBuy ? 'Buying' : 'Selling'} ~${displayAmount} ${unit}`, {
      volatility: (await this.marketData.fetchVolatility()).toFixed(4),
      sentiment: this.sentimentScore,
      impact: impactStr
    });

    const personality = this.marketData.getPersonality(wallet.keypair.publicKey);
    if (personality === 'hodler' && isBuy && Math.random() < 0.3) {
      this.logger.info(`${wallet.name} Hodling: Skipping sell cycle`);
      return true;
    }

    const useTwap = finalAmount > 0.005 * LAMPORTS_PER_SOL;
    if (useTwap) {
      return await this.twapSwap(isBuy, wallet, finalAmount);
    } else {
      return await this.performSingleSwap(isBuy, wallet, finalAmount);
    }
  }

  /**
   * Processes a wallet cycle
   * @param {Object} wallet
   * @param {CircuitBreaker} circuitBreaker
   * @returns {Promise<Object>}
   */
  async processWalletCycle(wallet, circuitBreaker) {
    try {
      const balance = await this.connection.getBalance(wallet.keypair.publicKey);
      if (balance < 0.01 * LAMPORTS_PER_SOL) {
        this.logger.warn(`${wallet.name} low balance, skipping`);
        return { success: false, volume: 0 };
      }

      const walletKey = wallet.keypair.publicKey.toBase58();
      const tradeCount = this.marketData.walletTradeCount.get(walletKey) || 0;

      let cycleVolume = 0;
      const actions = this.getTradeActions(wallet);
      this.logger.info(`${wallet.name} Cycle actions`, {
        actions: actions.map(a => a ? 'Buy' : 'Sell').join(', '),
        trades: tradeCount
      });

      for (const isBuy of actions) {
        if (!isBuy) {
          const tokens = await this.getCurrentMemeBalance(wallet);
          if (Number(tokens) < 1000) {
            this.logger.info(`${wallet.name} No tokens to sell, skipping`);
            continue;
          }
        }

        const solBefore = await this.connection.getBalance(wallet.keypair.publicKey);
        const success = await this.performSwap(isBuy, wallet);
        if (success) {
          const solAfter = await this.connection.getBalance(wallet.keypair.publicKey);
          const solDelta = isBuy ? (solBefore - solAfter) : (solAfter - solBefore);
          cycleVolume += solDelta / LAMPORTS_PER_SOL;
          circuitBreaker.recordTradeResult(true);
        } else {
          circuitBreaker.recordTradeResult(false);
        }

        await delay(this.getRandomDelay());
      }

      this.marketData.markWalletUsed(wallet);
      this.totalVolume += cycleVolume;
      this.logger.info(`${wallet.name} completed`, { volume: cycleVolume.toFixed(2), totalVolume: this.totalVolume.toFixed(2) });

      return { success: true, volume: cycleVolume };
    } catch (error) {
      this.logger.error(`${wallet.name} Cycle failed`, { error: error.message });
      circuitBreaker.recordTradeResult(false);
      return { success: false, volume: 0 };
    }
  }
}

/**
 * Circuit Breaker System
 */
class CircuitBreaker {
  /**
   * @param {ConfigManager} config
   * @param {Logger} logger
   */
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.consecutiveFailures = 0;
    this.recentTrades = [];
    this.initialSinkBalance = 0;
    this.checkCounter = 0;
  }

  /**
   * Records a trade result
   * @param {boolean} success
   */
  recordTradeResult(success) {
    if (!this.config.enableCircuitBreaker) return;

    this.recentTrades.push(success);
    if (this.recentTrades.length > this.config.failureRateWindow) {
      this.recentTrades.shift();
    }

    if (success) {
      this.consecutiveFailures = 0;
    } else {
      this.consecutiveFailures++;
    }
  }

  /**
   * Checks if circuit breaker should trip
   * @param {Connection} connection
   * @param {Keypair|null} sinkKeypair
   * @returns {Promise<Object>}
   */
  async checkCircuitBreakers(connection, sinkKeypair) {
    if (!this.config.enableCircuitBreaker) return { tripped: false, reason: '' };

    // Check consecutive failures
    if (this.consecutiveFailures >= this.config.maxConsecutiveFailures) {
      return {
        tripped: true,
        reason: `Circuit Breaker: ${this.consecutiveFailures} consecutive failures`,
      };
    }

    // Check failure rate
    if (this.recentTrades.length >= this.config.failureRateWindow) {
      const failures = this.recentTrades.filter(r => !r).length;
      const failureRate = (failures / this.recentTrades.length) * 100;

      if (failureRate > this.config.maxFailureRate) {
        return {
          tripped: true,
          reason: `Circuit Breaker: ${failureRate.toFixed(1)}% failure rate (last ${this.config.failureRateWindow} trades)`,
        };
      }
    }

    // Check emergency stop loss
    this.checkCounter++;
    if (sinkKeypair && this.initialSinkBalance > 0 && this.checkCounter % 5 === 0) {
      const currentBalance = await connection.getBalance(sinkKeypair.publicKey);
      const loss = ((this.initialSinkBalance - currentBalance) / this.initialSinkBalance) * 100;

      if (loss > this.config.emergencyStopLoss) {
        return {
          tripped: true,
          reason: `Circuit Breaker: ${loss.toFixed(1)}% loss from initial balance`,
        };
      }
    }

    return { tripped: false, reason: '' };
  }
}

/**
 * Wallet Rebalancing System
 */
class WalletRebalancer {
  /**
   * @param {ConfigManager} config
   * @param {WalletManager} walletManager
   * @param {Connection} connection
   * @param {Logger} logger
   */
  constructor(config, walletManager, connection, logger) {
    this.config = config;
    this.walletManager = walletManager;
    this.connection = connection;
    this.logger = logger;
  }

  /**
   * Rebalances wallets for stealth
   * @returns {Promise<void>}
   */
  async rebalanceWallets() {
    if (!this.config.enableRebalancing || this.walletManager.relayerKeypairs.length === 0) {
      this.logger.info('Rebalancing skipped: disabled or no relayer wallets');
      return;
    }

    this.logger.info('Starting P2P Stealth Rebalancing');

    const walletsToCheck = this.walletManager.activeWallets;
    if (walletsToCheck.length < 2) {
      this.logger.info('Not enough active wallets for rebalancing');
      return;
    }

    const walletBalances = [];
    let totalBalance = 0;

    for (const wallet of walletsToCheck) {
      const balance = await this.connection.getBalance(wallet.keypair.publicKey);
      walletBalances.push({ wallet, balance });
      totalBalance += balance;
    }

    const avgBalance = totalBalance / walletsToCheck.length;
    this.logger.info('Average active wallet balance', { balance: (avgBalance / LAMPORTS_PER_SOL).toFixed(4) });

    let needsFunding = walletBalances.filter(wb => wb.balance < this.config.minWalletBalance).sort((a, b) => a.balance - b.balance);
    let hasSurplus = walletBalances.filter(wb => wb.balance > this.config.targetWalletBalance * 1.5).sort((a, b) => b.balance - a.balance);

    this.logger.info('Rebalancing stats', {
      needsFunding: needsFunding.length,
      hasSurplus: hasSurplus.length
    });

    // P2P Internal Funding
    if (needsFunding.length > 0 && hasSurplus.length > 0) {
      this.logger.info('Attempting P2P internal funding');
      let surplusIndex = 0;

      for (const needy of needsFunding) {
        if (surplusIndex >= hasSurplus.length) break;

        const neededAmount = this.config.targetWalletBalance - needy.balance;
        const surplusProvider = hasSurplus[surplusIndex];

        if (surplusProvider.wallet.pubkey === needy.wallet.pubkey) continue;

        const availableSurplus = surplusProvider.balance - this.config.targetWalletBalance;

        if (availableSurplus > neededAmount) {
          try {
            const tx = new Transaction().add(SystemProgram.transfer({
              fromPubkey: surplusProvider.wallet.keypair.publicKey,
              toPubkey: needy.wallet.keypair.publicKey,
              lamports: neededAmount,
            }));
            const sig = await this.connection.sendTransaction(tx, [surplusProvider.wallet.keypair], { skipPreflight: true });
            await this.connection.confirmTransaction(sig, 'confirmed');

            this.logger.info('P2P Transfer', {
              from: surplusProvider.wallet.name,
              to: needy.wallet.name,
              amount: (neededAmount / LAMPORTS_PER_SOL).toFixed(4)
            });

            surplusProvider.balance -= neededAmount;
            needy.balance += neededAmount;
            await delay(1000 + Math.random() * 2000);

          } catch (error) {
            this.logger.error('P2P transfer failed', { error: error.message });
          }
        }

        if (surplusProvider.balance - this.config.targetWalletBalance < this.config.dustThreshold) {
          surplusIndex++;
        }
      }
    }

    // Consolidate surplus to sink
    const remainingSurplus = hasSurplus.filter(wb => wb.balance > this.config.targetWalletBalance * 1.1);
    if (remainingSurplus.length > 0 && this.walletManager.sinkKeypair) {
      await this.consolidateSurplusToSink(remainingSurplus);
    }

    this.logger.info('Rebalancing complete');
  }

  /**
   * Consolidates surplus to sink wallet
   * @param {Array} surplusWallets
   */
  async consolidateSurplusToSink(surplusWallets) {
    this.logger.info('Consolidating surplus to sink wallet');

    for (const { wallet, balance } of surplusWallets) {
      const surplus = balance - this.config.targetWalletBalance;

      if (surplus > this.config.dustThreshold) {
        try {
          const randomPct = 0.90 + Math.random() * 0.08;
          const transferAmount = Math.floor(surplus * randomPct);

          if (transferAmount < this.config.dustThreshold) continue;

          const tx = new Transaction().add(SystemProgram.transfer({
            fromPubkey: wallet.keypair.publicKey,
            toPubkey: this.walletManager.sinkKeypair.publicKey,
            lamports: transferAmount,
          }));

          const sig = await this.connection.sendTransaction(tx, [wallet.keypair], {
            skipPreflight: true,
            maxRetries: 2
          });
          await this.connection.confirmTransaction(sig, 'confirmed');

          this.logger.info('Consolidated to sink', {
            wallet: wallet.name,
            amount: (transferAmount / LAMPORTS_PER_SOL).toFixed(4)
          });

          await delay(500 + Math.random() * 1500);

        } catch (error) {
          this.logger.error('Consolidation to sink failed', {
            wallet: wallet.name,
            error: error.message
          });
        }
      }
    }
  }
}

/**
 * Wallet Seasoning System
 */
class WalletSeasoner {
  /**
   * @param {ConfigManager} config
   * @param {WalletManager} walletManager
   * @param {Connection} connection
   * @param {Logger} logger
   */
  constructor(config, walletManager, connection, logger) {
    this.config = config;
    this.walletManager = walletManager;
    this.connection = connection;
    this.logger = logger;
  }

  /**
   * Seasons wallets for stealth
   * @returns {Promise<void>}
   */
  async seasonWallets() {
    if (!this.config.enableSeasoning) {
      this.logger.info('Wallet seasoning disabled');
      return;
    }

    this.logger.info('Starting Wallet Seasoning');

    const walletData = await jsonfile.readFile(this.config.walletFile);
    const unseasonedWallets = walletData.filter(w => !w.isSeasoned);

    if (unseasonedWallets.length === 0) {
      this.logger.info('All wallets already seasoned');
      this.logger.info('Seasoning complete');
      return;
    }

    this.logger.info(`Processing ${unseasonedWallets.length} unseasoned wallets`);

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
          this.logger.info(`Skipping ${wallet.name}: insufficient SOL`);
          continue;
        }

        const numTxs = this.config.seasoningMinTxs + Math.floor(Math.random() * (this.config.seasoningMaxTxs - this.config.seasoningMinTxs + 1));
        this.logger.info(`Seasoning ${wallet.name} with ${numTxs} transactions`);

        for (let j = 0; j < numTxs; j++) {
          const actionType = Math.random() < 0.7 ? 'swap' : 'burn';

          if (actionType === 'swap') {
            const isSolToUsdc = Math.random() < 0.5;
            const amount = (0.0001 + Math.random() * 0.0004) * LAMPORTS_PER_SOL;

            try {
              if (isSolToUsdc) {
                await this.trader.buy({
                  market: 'RAYDIUM_AMM',
                  wallet: wallet.keypair,
                  mint: USDC_MINT,
                  amount: amount / LAMPORTS_PER_SOL,
                  slippage: 5
                });
                this.logger.info(`Seasoning swap: SOL to USDC`, { amount: (amount / LAMPORTS_PER_SOL).toFixed(5) });
              } else {
                const usdcBalance = await this.getTokenBalance(wallet.keypair.publicKey, USDC_MINT);
                if (Number(usdcBalance) > 0) {
                  await this.trader.sell({
                    market: 'RAYDIUM_AMM',
                    wallet: wallet.keypair,
                    mint: USDC_MINT,
                    amount: Number(usdcBalance),
                    slippage: 5
                  });
                  this.logger.info(`Seasoning swap: USDC to SOL`);
                } else {
                  this.logger.info(`Skipping USDC->SOL: no balance`);
                }
              }
            } catch (error) {
              this.logger.warn(`Seasoning swap failed`, { wallet: wallet.name, error: error.message.slice(0, 50) });
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
              this.logger.info(`Seasoning burn`, { amount: (burnAmount / LAMPORTS_PER_SOL).toFixed(6) });
            } catch (error) {
              this.logger.warn(`Seasoning burn failed`, { wallet: wallet.name, error: error.message.slice(0, 50) });
            }
          }

          const delayMs = this.config.seasoningDelayMs + (Math.random() - 0.5) * (this.config.seasoningDelayMs * 0.5);
          await delay(delayMs);
        }

        // Mark as seasoned
        const originalIndex = walletData.findIndex(w => w.pubkey === wallet.pubkey);
        if (originalIndex !== -1) {
          walletData[originalIndex].isSeasoned = true;
          await jsonfile.writeFile(this.config.walletFile, walletData, { spaces: 2 });
          this.walletManager.walletData = walletData;
        }

      } catch (error) {
        this.logger.error(`Error seasoning ${wallet.name}`, { error: error.message });
      }
    }

    this.logger.info('Seasoning complete');
  }

  /**
   * Gets token balance
   * @param {PublicKey} owner
   * @param {PublicKey} mint
   * @returns {Promise<bigint>}
   */
  async getTokenBalance(owner, mint) {
    try {
      const tokenAccount = getAssociatedTokenAddressSync(mint, owner);
      const accountInfo = await getAccount(this.connection, tokenAccount);
      return accountInfo.amount;
    } catch {
      return 0n;
    }
  }
}

/**
 * Main Volume Booster Bot - Orchestrator
 */
class VolumeBoosterBot {
  /**
   * Constructor - Initializes all systems
   */
  constructor() {
    this.config = new ConfigManager();
    this.logger = new Logger();
    this.connection = new Connection(this.config.rpcUrl, 'confirmed');
    this.walletManager = new WalletManager(this.config, this.connection, this.logger);
    this.marketData = new MarketDataProvider(this.config, this.connection, this.logger);
    this.tradingEngine = new TradingEngine(this.config, this.connection, this.marketData, this.logger);
    this.circuitBreaker = new CircuitBreaker(this.config, this.logger);
    this.rebalancer = new WalletRebalancer(this.config, this.walletManager, this.connection, this.logger);
    this.seasoner = new WalletSeasoner(this.config, this.walletManager, this.connection, this.logger);

    this.isRunning = false;
    this.cycleCount = 0;
    this.totalVolume = 0;
  }

  /**
   * Initializes the bot
   */
  async init() {
    try {
      this.logger.info('Initializing Volume Booster Bot');

      await this.walletManager.loadOrGenerateWallets();
      this.walletManager.assignPersonalities();

      await this.seasoner.seasonWallets();

      this.logger.info('Fetching initial market data');
      await this.marketData.getMarketData();

      this.isRunning = true;
      this.logger.info('Bot starting', {
        wallets: this.walletManager.allPubkeys.size,
        activeBatch: this.walletManager.activeWallets.length,
        concurrency: this.config.concurrency,
        batchSize: this.config.batchSize,
        network: this.config.isDevnet ? 'Devnet' : 'Mainnet',
        market: this.config.market,
        tradeMode: this.config.tradeMode,
        buyProb: this.config.buyProb,
        actionsPerCycle: this.config.numActionsPerCycle,
        cooldown: `${(this.config.minWalletCooldownMs / 60000).toFixed(1)}-${(this.config.maxWalletCooldownMs / 60000).toFixed(1)} min`,
        shuffle: this.config.shuffleWallets,
        birdeye: this.config.useBirdeye,
        rebalancing: this.config.enableRebalancing,
        circuitBreaker: this.config.enableCircuitBreaker
      });

      if (this.config.useBirdeye) {
        this.logger.info('Market settings', {
          minLiquidity: this.config.minLiquidity.toLocaleString(),
          maxPriceImpact: this.config.maxPriceImpact
        });
      }

      if (this.config.enableRebalancing) {
        this.logger.info('Rebalancing settings', {
          targetBalance: (this.config.targetWalletBalance / LAMPORTS_PER_SOL).toFixed(3),
          interval: `Every ${this.config.rebalanceInterval} cycles`
        });
      }

      if (this.config.enableCircuitBreaker) {
        this.logger.info('Circuit breaker settings', {
          maxFailures: this.config.maxConsecutiveFailures,
          maxFailureRate: this.config.maxFailureRate,
          stopLoss: this.config.emergencyStopLoss
        });
      }

      if (this.walletManager.sinkKeypair) {
        this.circuitBreaker.initialSinkBalance = await this.connection.getBalance(this.walletManager.sinkKeypair.publicKey);
        this.logger.info('Initial sink balance locked', {
          balance: (this.circuitBreaker.initialSinkBalance / LAMPORTS_PER_SOL).toFixed(4)
        });
      }

      await this.runLoop();
    } catch (error) {
      this.logger.error('Bot initialization failed', { error: error.message });
      throw error;
    }
  }

  /**
   * Main trading loop
   */
  async runLoop() {
    let batchNum = 0;

    while (this.isRunning) {
      const circuitCheck = await this.circuitBreaker.checkCircuitBreakers(this.connection, this.walletManager.sinkKeypair);
      if (circuitCheck.tripped) {
        this.logger.error('Circuit breaker tripped', { reason: circuitCheck.reason });
        this.logger.error('Bot stopped for safety');
        await this.stop();
        process.exit(1);
      }

      await this.walletManager.loadActiveBatch();
      const walletBatch = this.walletManager.activeWallets;

      if (walletBatch.length === 0) {
        this.logger.info('No wallets available, waiting for cooldowns');
        await delay(30000);
        continue;
      }

      batchNum++;
      this.cycleCount++;
      this.logger.info(`Starting batch ${batchNum}`, {
        cycle: this.cycleCount,
        ramp: Math.min(1, (this.cycleCount / this.config.rampCycles) * 100).toFixed(0)
      });

      const promises = walletBatch.map(wallet =>
        this.tradingEngine.limiter(() => this.tradingEngine.processWalletCycle(wallet, this.circuitBreaker))
      );
      const results = await Promise.allSettled(promises);
      const successes = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
      this.logger.info(`Batch ${batchNum} complete`, {
        successful: successes,
        total: walletBatch.length
      });

      if (batchNum % 10 === 0) {
        this.printRotationStats();
      }

      if (this.config.enableRebalancing && this.cycleCount % this.config.rebalanceInterval === 0) {
        await this.rebalancer.rebalanceWallets();
      }

      if (batchNum % this.config.sessionPauseMin === 0) {
        const pauseMs = this.tradingEngine.getJitteredValue(60000 * (1 + Math.random() * 4), false);
        this.logger.info('Session pause', { duration: (pauseMs / 1000).toFixed(0) });
        await delay(pauseMs);
      }

      const interBatchDelay = Math.random() * 10000 + 5000;
      await delay(interBatchDelay);
    }
  }

  /**
   * Prints wallet rotation statistics
   */
  printRotationStats() {
    this.logger.info('Wallet Rotation Stats', {
      activeBatch: this.walletManager.activeWallets.length,
      onCooldown: this.walletManager.walletCooldowns.size
    });

    const sortedWallets = Array.from(this.walletManager.walletTradeCount.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);

    if (sortedWallets.length > 0) {
      this.logger.info('Top Active Wallets');
      sortedWallets.forEach(([key, count], i) => {
        this.logger.info(`  ${i + 1}. ${key.slice(0, 8)}...: ${count} trades`);
      });
    }
  }

  /**
   * Stops the bot and performs cleanup
   */
  async stop() {
    this.isRunning = false;
    this.logger.info('Bot stopping');

    await this.closeAllTokenAccounts();
    if (this.walletManager.sinkKeypair) {
      await this.withdrawAllFunds();
    }

    this.logger.info('Bot stopped', {
      totalVolume: this.totalVolume.toFixed(2)
    });
  }

  /**
   * Withdraws all funds to sink wallet
   */
  async withdrawAllFunds() {
    if (!this.walletManager.sinkKeypair) {
      this.logger.info('No sink wallet configured');
      return;
    }

    this.logger.info('Starting parallel withdrawal');

    const allWalletsData = await jsonfile.readFile(this.config.walletFile);
    const chunkSize = 50;
    let successCount = 0;

    for (let i = 0; i < allWalletsData.length; i += chunkSize) {
      const chunk = allWalletsData.slice(i, i + chunkSize).map(w => ({
        keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
        name: w.name || w.pubkey.slice(0, 6)
      }));

      const promises = chunk.map(wallet => this.withdrawSingleWallet(wallet));
      const results = await Promise.allSettled(promises);
      successCount += results.filter(r => r.status === 'fulfilled').length;
      this.logger.info('Withdrawal progress', {
        processed: Math.min(i + chunkSize, allWalletsData.length),
        total: allWalletsData.length
      });
    }

    const sinkBalance = await this.connection.getBalance(this.walletManager.sinkKeypair.publicKey);
    this.logger.info('Withdrawal complete', {
      successful: successCount,
      total: allWalletsData.length,
      finalSinkBalance: (sinkBalance / LAMPORTS_PER_SOL).toFixed(4)
    });

    // Sink token cleanup
    if (this.walletManager.sinkKeypair) {
      const tokensToCheck = [this.config.memeCoinMint, USDC_MINT];
      for (const mint of tokensToCheck) {
        const sinkTokenAcc = getAssociatedTokenAddressSync(mint, this.walletManager.sinkKeypair.publicKey);
        try {
          const accountInfo = await getAccount(this.connection, sinkTokenAcc);
          const balance = accountInfo.amount;
          if (balance === 0n) {
            const closeTx = new Transaction().add(createCloseAccountInstruction(
              sinkTokenAcc,
              this.walletManager.sinkKeypair.publicKey,
              this.walletManager.sinkKeypair.publicKey
            ));
            await this.connection.sendTransaction(closeTx, [this.walletManager.sinkKeypair]);
            this.logger.info('Sink token account closed', { mint: mint.toBase58().slice(0, 8) });
          }
        } catch (error) {
          // Ignore if account doesn't exist
        }
      }
    }
  }

  /**
   * Withdraws funds from a single wallet
   * @param {Object} wallet
   */
  async withdrawSingleWallet(wallet) {
    try {
      const balance = await this.connection.getBalance(wallet.keypair.publicKey);
      const safeBuffer = LAMPORTS_PER_SOL * 0.0005;
      const priorityFee = 10000;
      let transferAmount = balance - safeBuffer - priorityFee;

      transferAmount -= this.tradingEngine.getJitteredValue(safeBuffer / 10, true);

      if (transferAmount < LAMPORTS_PER_SOL * 0.0001) return false;

      const tx = new Transaction().add(
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFee }),
        SystemProgram.transfer({
          fromPubkey: wallet.keypair.publicKey,
          toPubkey: this.walletManager.sinkKeypair.publicKey,
          lamports: transferAmount,
        })
      );

      const signature = await this.connection.sendTransaction(tx, [wallet.keypair]);
      this.logger.info('Swept SOL from wallet', {
        wallet: wallet.keypair.publicKey.toBase58(),
        amount: (transferAmount / LAMPORTS_PER_SOL).toFixed(6),
        tx: signature
      });
      return true;
    } catch (error) {
      this.logger.error('Failed to sweep SOL', {
        wallet: wallet.keypair.publicKey.toBase58(),
        error: error.message
      });
      return false;
    }
  }

  /**
   * Closes all empty token accounts for rent recovery
   */
  async closeAllTokenAccounts() {
    this.logger.info('Closing empty token accounts');

    const allWalletsData = await jsonfile.readFile(this.config.walletFile);
    const chunkSize = 50;

    for (let i = 0; i < allWalletsData.length; i += chunkSize) {
      const chunk = allWalletsData.slice(i, i + chunkSize).map(w => ({
        keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
        name: w.name || w.pubkey.slice(0, 6)
      }));

      const promises = chunk.map(wallet => this.closeWalletTokenAccounts(wallet));
      await Promise.allSettled(promises);
      this.logger.info('Token account closure progress', {
        processed: Math.min(i + chunkSize, allWalletsData.length),
        total: allWalletsData.length
      });
    }
  }

  /**
   * Closes token accounts for a single wallet
   * @param {Object} wallet
   */
  async closeWalletTokenAccounts(wallet) {
    const mintsToClose = [this.config.memeCoinMint, USDC_MINT];

    for (const mint of mintsToClose) {
      try {
        const ataAddress = getAssociatedTokenAddressSync(mint, wallet.keypair.publicKey);
        const accountInfo = await getAccount(this.connection, ataAddress);

        if (accountInfo.amount !== 0n) {
          this.logger.warn('Skipping ATA close: non-zero balance', {
            wallet: wallet.name,
            mint: mint.toBase58().slice(0, 8),
            balance: accountInfo.amount.toString()
          });
          continue;
        }

        const closeIx = createCloseAccountInstruction(
          ataAddress,
          wallet.keypair.publicKey,
          wallet.keypair.publicKey,
          [],
          TOKEN_PROGRAM_ID
        );

        const tx = new Transaction().add(closeIx);
        const signature = await this.connection.sendTransaction(tx, [wallet.keypair]);

        this.logger.info('Closed ATA', {
          wallet: wallet.name,
          mint: mint.toBase58().slice(0, 8),
          tx: signature
        });
      } catch (error) {
        if (!error.message.includes('AccountNotFound')) {
          this.logger.error('Failed to close ATA', {
            wallet: wallet.name,
            mint: mint.toBase58().slice(0, 8),
            error: error.message
          });
        }
      }
    }
  }
}

// Run the bot
const bot = new VolumeBoosterBot();

// Keyboard triggers
if (bot.config.enableKeyboard) {
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
