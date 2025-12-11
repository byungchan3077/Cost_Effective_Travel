import argparse
import sys
from typing import List, Dict, Any
# Import the core service module
from services.travel_service import run_analysis_pipeline 

# --- [Output Helper Functions] ---

def display_error(message: str):
    """Prints a highlighted error message for process failure."""
    print("\n" + "=" * 60)
    print(f"!!! ANALYSIS FAILED !!!")
    print(f"Reason: {message}")
    print("=" * 60 + "\n")


def display_rankings(results: List[Dict[str, Any]], total_budget: float, days: int):
    """
    Safely prints the final ranking table for all analyzed countries.
    """
    if not results:
        print("\n[INFO] No valid analysis results to display.")
        return

    # Sort results by 'ppi_score' in descending order (High Score = Better)
    sorted_results = sorted(results, key=lambda x: x.get('ppi_score', 0), reverse=True)
    
    daily_budget = total_budget / days
    
    print("\n" + "═" * 70)
    print("      Purchasing Power Index (PPI) Travel Recommendation")
    print(f"      Reference Daily Budget: {daily_budget:,.0f} KRW/day")
    print("═" * 70)
    print(f"{'Rank':<4} {'Code':<10} | {'PPI Score':<10} | Status")
    print("-" * 70)
    
    # Iterate through ALL results
    for rank, item in enumerate(sorted_results, 1): 
        code = item.get('country_code', '---')
        score = item.get('ppi_score', 0)
        
        # Determine status based on PPI score (Threshold: 1.0)
        if score >= 1.5:
            status = "🤑 PLENTY (Very Affordable)"
        elif score >= 1.0:
            status = "✅ SAFE (Affordable)"
        elif score >= 0.8:
            status = "⚠️ TIGHT (Budget Tight)"
        else:
            status = "❌ SHORT (Budget Insufficient)"
        
        print(
            f"{rank: <4}. {code:<10} | {score: 7.2f}    | {status}"
        )
    
    print("-" * 70)
    print("NOTE: PPI > 1.0 means your budget covers the local survival costs.")


def main():
    # 1. Argument Parsing
    parser = argparse.ArgumentParser(description="Cost Effective Travel - PPI Calculator.")
    parser.add_argument("--budget", type=float, required=True, help="Total travel budget (e.g., 2000000)")
    parser.add_argument("--days", type=int, required=True, help="Travel duration (e.g., 10 days)")
    args = parser.parse_args()

    # 2. Input Validation
    if args.budget <= 0 or args.days <= 0:
        display_error("Budget and days must be positive values (> 0).")
        sys.exit(1)

    # 3. Execute Service and Receive Results
    # Service function returns a tuple: (results_list, status_message)
    results, status_message = run_analysis_pipeline(args.budget, args.days)

    # 4. Output Based on Status
    if status_message == "Success":
        display_rankings(results, args.budget, args.days)
    else:
        # Display error message returned by the Service layer
        display_error(status_message)

if __name__ == "__main__":
    main()