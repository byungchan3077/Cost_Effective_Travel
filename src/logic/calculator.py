def calculate_trend_factor(current_rate: float, ma_rate: float) -> float:
    """
    Calculate the exchange rate trend factor (deviation from moving average).

    Args:
        current_rate (float): Current exchange rate.
        ma_rate (float): Moving average exchange rate (e.g., 50-day MA).

    Returns:
        float: Trend factor 'r'.
               Negative value indicates the currency is undervalued (Good for travel).
               Positive value indicates it is overvalued (Bad for travel).
    """
    
    if ma_rate == 0:
        return 0.0
    return (current_rate - ma_rate) / ma_rate



def calculate_tei(
    budget: float,
    duration: int,
    local_daily_cost: float,
    current_rate: float,
    ma_rate: float
) -> dict:
    
    """
    Calculate Travel Efficiency Index (TEI) focused on economic purchasing power.

    Args:
        budget (float): Total travel budget in KRW.
        duration (int): Travel duration in days.
        local_daily_cost (float): Local daily survival cost in local currency.
        current_rate (float): Current exchange rate (KRW per unit).
        ma_rate (float): Moving average exchange rate.

    Returns:
        dict: Dictionary containing TEI score (Purchasing Power) and metrics.
    """
    
    if duration <= 0 or local_daily_cost <= 0:
        return {"tei_score": 0.0, "error": "Invalid duration or cost"}

    # 1. Calculate daily budget in KRW
    my_daily_budget_krw = budget / duration

    # 2. Calculate exchange rate trend factor (r)
    trend_r = calculate_trend_factor(current_rate, ma_rate)

    # 3. Calculate adjusted exchange rate (effective rate)
    # If r < 0 (undervalued), effective rate decreases -> purchasing power increases
    adjusted_rate = current_rate * (1 + trend_r)

    # 4. Convert local cost to KRW using adjusted rate
    local_cost_krw = local_daily_cost * adjusted_rate

    # 5. Calculate Purchasing Power Index (PPI)
    if local_cost_krw <= 0:
        return {"tei_score": 0.0, "error": "Invalid calculated cost"}

    purchasing_power = my_daily_budget_krw / local_cost_krw

    # 6. Final Score is purely based on Purchasing Power
    return {
        "tei_score": round(purchasing_power, 2), 
        "trend_impact": round(trend_r * 100, 2),  
        "is_undervalued": trend_r < 0,
        "adjusted_rate": round(adjusted_rate, 2)
    }
