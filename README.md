# Cost_Effective_Travel ✈️💸

> **"Don't just travel cheap. Travel rich."**
>
> A CLI tool that calculates the **Real Purchasing Power Index (PPI)** of your budget to recommend the most financially efficient travel destinations.

---
input:python src/main.py --budget 1000000 --days 10 
-change the number

## 1. Goal & Vision
* **Goal:** Develop a data analysis tool that combines **Real-time Exchange Trends** (API) and **Local Survival Costs** (CSV) to identify destinations where your money has the highest value.
* **Vision:** To help travelers avoid financial pitfalls by revealing the true "Purchasing Power" of their budget, moving beyond simple exchange rate comparisons.

## 2. Key Features
* **🌍 Real-time Financial Analysis:** Fetches live exchange rates and compares them with **1-year moving averages** to identify undervalued currencies (Discounted Rates).
* **🍔 Survival Basket Logic:** Calculates the minimum daily living cost based on real-world metrics: **3 Big Macs + 2 Starbucks Lattes + Daily Rent**.
* **📊 Purchasing Power Index (PPI):** Generates a standardized score (Baseline: 100).
    * **Score > 100:** You are "Rich" (Budget > Survival Cost).
    * **Score < 100:** You are "Poor" (Budget < Survival Cost).
* **🎯 Curated Destinations:** Analyzes ~30 popular destinations preferred by travelers to ensure relevant recommendations.
* **⚡ CLI-First Design:** Lightweight and fast command-line interface with automated report generation (`result.txt`).

## 3. How It Works (The Logic)
Our algorithm doesn't just look for cheap prices. It looks for **Value**.

> **Final Score = ( Daily Budget / Adjusted Survival Cost ) * 100**

1.  **Daily Budget:** Your Total Budget / Travel Duration.
2.  **Local Survival Basket (LSB):** Minimum cost to survive a day in local currency.
3.  **Rate Index (R):** Checks if the currency is cheaper than usual (Discount Factor).
4.  **Result:** If the score is **150**, it means you have **1.5x** the money needed for a comfortable life.

*(For detailed formulas, please check our [Wiki - Algorithm Logic](https://github.com/YOUR-ID/Cost_Effective_Travel/wiki/Algorithm-Logic))*

## 4. Tech Stack
* **Language:** Python 3.10+
* **Core Libraries:**
    * `requests`: For fetching Exchange Rate API.
    * `pandas`: For processing Cost of Living CSV data.
    * `python-dotenv`: For secure API key management.
* **Architecture:** Modular Design (`api`, `data`, `logic`, `services`, `report`).
* **CI/CD:** GitHub Actions for automated testing.

## 5. Installation & Usage

### Prerequisites
* Python 3.10 or higher
* Korea Eximbank API Key

### Setup
```bash
# 1. Clone the repository
git clone https://github.com/YOUR-ID/Cost_Effective_Travel.git
cd Cost_Effective_Travel

# 2. Create Virtual Environment
python -m venv venv
# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate

# 3. Install Dependencies
pip install -r requirements.txt

# 4. Set up API Key
# Create a .env file in the root directory and add your key:
# EXIM_API_KEY=your_key_here
```

### Usage
Run the main script with your budget (KRW) and duration (Days).
```bash
python src/main.py --budget 2000000 --days 10
```

## 6. Governance
* **License:** MIT License
* **Code of Conduct:** We follow the [Contributor Covenant](CODE_OF_CONDUCT.md).
* **Contribution:** Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a Pull Request.
