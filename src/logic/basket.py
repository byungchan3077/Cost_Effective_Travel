def calculate_lsb(
    meal_cost: float,
    drink_cost: float,
    accommodation_cost: float
) -> float:
    """
    Calculate Local Survival Budget (LSB) based on the standard formula.

    Formula: (3 * Meal) + (2 * Drink) + (1 * Accommodation)

    Args:
        meal_cost (float): Cost of a single meal.
        drink_cost (float): Cost of a single drink.
        accommodation_cost (float): Cost of one night's accommodation.

    Returns:
        float: Total daily survival cost. Returns 0.0 for negative inputs.
    """
    if meal_cost < 0 or drink_cost < 0 or accommodation_cost < 0:
        return 0.0

    total_cost = (3 * meal_cost) + (2 * drink_cost) + (1 * accommodation_cost)
    return float(total_cost)
