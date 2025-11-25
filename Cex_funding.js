const fs = require('fs');
const jsonfile = require('jsonfile');

// CONFIGURATION
const WALLET_FILE = 'wallets.json'; // Your existing wallet file
const OUTPUT_FILE = 'binance_batch_withdraw.csv'; // Output file
const FUND_AMOUNT_SOL = 0.5; // Target amount per wallet
const JITTER_PCT = 0.05; // 5% Randomness (Anti-Detection)

// Main Logic
try {
    if (!fs.existsSync(WALLET_FILE)) {
        throw new Error(`${WALLET_FILE} not found. Generate wallets first.`);
    }

    const wallets = jsonfile.readFileSync(WALLET_FILE);
    
    // CSV Header (Check your exchange's specific requirement)
    // Common formats:
    // Binance: Address,Amount,Coin
    // Kraken: Address,Amount,Reference
    let csvContent = "Address,Amount,Coin\n"; 

    console.log(`Processing ${wallets.length} wallets...`);

    let totalSolNeeded = 0;

    wallets.forEach(wallet => {
        // 1. Get Public Key
        // Handle different formats from your previous code
        const address = wallet.pubkey || wallet.publicKey;

        // 2. Calculate Random Amount
        // Base +/- Jitter
        const jitter = (Math.random() - 0.5) * 2 * JITTER_PCT; // -0.05 to +0.05
        const amount = FUND_AMOUNT_SOL * (1 + jitter);
        
        // Round to 4 decimals (standard for CEX)
        const finalAmount = parseFloat(amount.toFixed(4));
        
        totalSolNeeded += finalAmount;

        // 3. Append to CSV
        // Format: Address, Amount, SOL
        csvContent += `${address},${finalAmount},SOL\n`;
    });

    fs.writeFileSync(OUTPUT_FILE, csvContent);

    console.log(`\n✅ Success! Generated ${OUTPUT_FILE}`);
    console.log(`----------------------------------------`);
    console.log(`Total Wallets: ${wallets.length}`);
    console.log(`Total SOL Required: ${totalSolNeeded.toFixed(4)} SOL`);
    console.log(`Average per wallet: ${(totalSolNeeded / wallets.length).toFixed(4)} SOL`);
    console.log(`\n👉 NEXT STEP: Upload this CSV to your Exchange's 'Batch Withdrawal' section.`);

} catch (error) {
    console.error("Error:", error.message);
}
